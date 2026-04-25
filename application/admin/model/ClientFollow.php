<?php

namespace app\admin\model;

use app\admin\service\ClientFollowTodoPermissionService;
use think\Db;
use think\Model;

/**
 * 待办跟进列表（数据源 crm_leads，基于 next_up_time 筛选视图）。
 */
class ClientFollow extends Model
{
    protected $table = 'crm_leads';

    /**
     * 待办跟进分页列表。
     *
     * @param int $page
     * @param int $limit
     * @param array $keyword 可选：kh_name、phone、pr_user、next_up_start、next_up_end、inquiry_id、port_id、only_overdue
     * @param array $currentAdmin 预留（与 Session 扩展一致）；当前可见范围仍以登录用户为准，由 ClientFollowTodoPermissionService 处理
     * @param string $scene todo_manage|team_todo
     * @return array|null 与 getClientSearchListAll 一致，无数据时返回 null
     */
    public function getTodoFollowList($page, $limit, $keyword = [], $currentAdmin = [], $scene = 'todo_manage')
    {
        $query = $this->buildTodoFollowBaseQuery((array)$keyword, $currentAdmin, $scene);

        $result = $query
            ->leftJoin('crm_contacts c', "c.leads_id = l.id AND c.is_delete = 0 AND c.contact_type IN (1,3)")
            ->field([
                'l.id',
                'l.kh_name',
                'l.product_name',
                'l.last_up_records',
                'l.last_up_time',
                'l.next_up_time',
                'l.pr_user',
                'l.inquiry_id',
                'l.port_id',
                'l.joint_person',
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 1, c.contact_value, NULL) ORDER BY c.id SEPARATOR ',') AS main_phone",
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 3, c.contact_value, NULL) ORDER BY c.id SEPARATOR '<br>') AS aux_phone",
            ])
            ->group('l.id')
            ->order('l.next_up_time asc, l.id asc')
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();

        return ($result['total'] == 0) ? null : $result;
    }

    /**
     * 待办跟进基础查询（不含分页、不含 contacts 聚合）。
     *
     * @param array $keyword kh_name、phone、pr_user、next_up_start、next_up_end、inquiry_id、port_id、only_overdue
     * @param array $currentAdmin
     * @param string $scene todo_manage|team_todo
     * @return \think\db\Query
     */
    public function buildTodoFollowBaseQuery(array $keyword = [], $currentAdmin = [], $scene = 'todo_manage')
    {
        $now = date('Y-m-d H:i:s');

        $query = Db::table('crm_leads')->alias('l')
            ->where('l.status', 1)
            ->where('l.issuccess', -1)
            ->where(function ($q) {
                $q->whereNotNull('l.next_up_time')->where('l.next_up_time', '<>', '');
            });

        $keyword = array_merge([
            'kh_name' => '',
            'phone' => '',
            'pr_user' => '',
            'next_up_start' => '',
            'next_up_end' => '',
            'inquiry_id' => '',
            'port_id' => '',
            'only_overdue' => '',
        ], $keyword);

        $startRaw = $keyword['next_up_start'];
        $endRaw = $keyword['next_up_end'];
        $hasCustomRange = (($startRaw !== '' && $startRaw !== null) || ($endRaw !== '' && $endRaw !== null));

        // 默认待办口径：不传时间范围时仅看「已到期」(<= 当前时刻)。
        // 传了时间范围时，按用户输入区间过滤（用于“今日待跟进”等快捷筛选）。
        if (!$hasCustomRange) {
            $query->where('l.next_up_time', '<=', $now);
        }

        if ($startRaw !== '' && $startRaw !== null) {
            $parsedStart = $this->parseNextUpTimeBound($startRaw, false);
            if ($parsedStart !== null) {
                $query->where('l.next_up_time', '>=', $parsedStart);
            }
        }

        if ($endRaw !== '' && $endRaw !== null) {
            $parsedEnd = $this->parseNextUpTimeBound($endRaw, true);
            if ($parsedEnd !== null) {
                $query->where('l.next_up_time', '<=', $parsedEnd);
            }
        }

        // 仅看逾期：下次跟进时间早于「今天 0 点」（仍满足待办：<= 当前时间）
        if ($this->isTruthyFlag($keyword['only_overdue'])) {
            $query->where('l.next_up_time', '<', date('Y-m-d 00:00:00'));
        }

        if ($keyword['kh_name'] !== '' && $keyword['kh_name'] !== null) {
            $kw = trim((string)$keyword['kh_name']);
            if ($kw !== '') {
                $query->where('l.kh_name', 'like', '%' . $kw . '%');
            }
        }

        // 主电话、辅助电话（contact_type 1 / 3）
        if ($keyword['phone'] !== '' && $keyword['phone'] !== null) {
            $phoneKw = trim((string)$keyword['phone']);
            if ($phoneKw !== '') {
                $this->applyTodoFollowPhoneFilter($query, $phoneKw, 'l');
            }
        }

        if ($keyword['pr_user'] !== '' && $keyword['pr_user'] !== null) {
            $pu = trim((string)$keyword['pr_user']);
            if ($pu !== '') {
                $query->where('l.pr_user', 'like', '%' . $pu . '%');
            }
        }

        if ($keyword['inquiry_id'] !== '' && $keyword['inquiry_id'] !== null) {
            $inquiryId = (int)$keyword['inquiry_id'];
            if ($inquiryId > 0) {
                $query->where('l.inquiry_id', '=', $inquiryId);
            }
        }

        if ($keyword['port_id'] !== '' && $keyword['port_id'] !== null) {
            $portId = (int)$keyword['port_id'];
            if ($portId > 0) {
                $query->whereRaw('(l.port_id = ' . $portId . ' OR FIND_IN_SET(' . $portId . ', l.port_id) > 0)');
            }
        }

        (new ClientFollowTodoPermissionService())->applyLeadsVisibilityScope($query, 'l', $scene);

        return $query;
    }

    /**
     * 解析下次跟进时间筛选项（支持 Y-m-d 或完整日期时间）。
     *
     * @param bool $isEndBound true 表示「结束」：纯日期按当天 23:59:59
     * @return string|null Y-m-d H:i:s
     */
    private function parseNextUpTimeBound($value, $isEndBound)
    {
        $v = trim((string)$value);
        if ($v === '') {
            return null;
        }
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $v)) {
            $ts = strtotime($v);
            if ($ts === false) {
                return null;
            }
            return $isEndBound ? date('Y-m-d 23:59:59', $ts) : date('Y-m-d 00:00:00', $ts);
        }
        $ts = strtotime($v);
        if ($ts === false) {
            return null;
        }
        return date('Y-m-d H:i:s', $ts);
    }

    /**
     * 待办跟进页手机号筛选（主号/辅号）：以 EXISTS 子查询命中 contacts，避免受外层聚合字段影响。
     *
     * @param \think\db\Query $query
     * @param string $phoneKw
     * @param string $leadAlias
     * @return void
     */
    private function applyTodoFollowPhoneFilter($query, $phoneKw, $leadAlias = 'l')
    {
        $rawKeyword = trim((string)$phoneKw);
        $digitKeyword = preg_replace('/\D+/', '', $rawKeyword);

        // 关键词无效时按无结果处理，避免误返回全量。
        if ($rawKeyword === '' && $digitKeyword === '') {
            $query->where($leadAlias . '.id', '=', -1);
            return;
        }

        // 方案A：先筛 contacts，再反查 leads_id，规避 TP5.1 子查询复杂绑定导致的 2031。
        $contactsQuery = Db::table('crm_contacts')
            ->alias('cfp')
            ->where('cfp.is_delete', 0)
            ->whereIn('cfp.contact_type', [1, 3]);

        $contactsQuery->where(function ($w) use ($rawKeyword, $digitKeyword) {
            if ($rawKeyword !== '') {
                $w->where('cfp.contact_value', 'like', '%' . $this->escapeLike($rawKeyword) . '%');
            }
            if ($digitKeyword !== '' && $digitKeyword !== $rawKeyword) {
                $w->whereOr('cfp.contact_value', 'like', '%' . $this->escapeLike($digitKeyword) . '%');
            }
        });

        $leadIds = $contactsQuery->column('cfp.leads_id');
        if (empty($leadIds)) {
            $query->where($leadAlias . '.id', '=', -1);
            return;
        }

        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds))));
        if (empty($leadIds)) {
            $query->where($leadAlias . '.id', '=', -1);
            return;
        }

        $query->whereIn($leadAlias . '.id', $leadIds);
    }

    /**
     * LIKE 关键字转义，避免 %/_ 作为通配符误扩大匹配范围。
     *
     * @param string $value
     * @return string
     */
    private function escapeLike($value)
    {
        return strtr((string)$value, [
            '\\' => '\\\\',
            '%' => '\%',
            '_' => '\_',
        ]);
    }

    /**
     * @param mixed $v
     */
    private function isTruthyFlag($v)
    {
        if ($v === true || $v === 1) {
            return true;
        }
        if (is_string($v)) {
            $s = strtolower(trim($v));
            return $s === '1' || $s === 'true' || $s === 'yes' || $s === 'on';
        }
        return false;
    }
}
