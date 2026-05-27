<?php

namespace app\admin\service;

use think\Db;
use think\facade\Session;
use think\facade\Log;
use app\admin\service\LiberumConfigService;

class LiberumFirstTimeoutService
{
    /**
     * @var LiberumConfigService
     */
    protected $liberumConfigService;

    /**
     * 字段存在性缓存
     *
     * @var array
     */
    protected static $columnExistsCache = [];

    protected function getLiberumConfigService()
    {
        if (!$this->liberumConfigService) {
            $this->liberumConfigService = new LiberumConfigService();
        }
        return $this->liberumConfigService;
    }

    /**
     * 首次超时分配列表
     *
     * @param array $params
     * @return array{count:int,data:array}
     */
    public function getList($params = [])
    {
        $params = is_array($params) ? $params : [];
        try {

            $page = max(1, (int)($params['page'] ?? 1));
            $limit = (int)($params['limit'] ?? 0);
            if ($limit <= 0) {
                $limit = (int)config('pageSize');
            }
            if ($limit <= 0) {
                $limit = 20;
            }
            if ($limit > 200) {
                $limit = 200;
            }

            $firstFollowDays = $this->getLiberumConfigService()->getIntValue('first_follow_days', 7, 1, 3650);
            $ruleEffectiveDate = $this->getLiberumConfigService()->getDateValue('rule_effective_date', '2026-06-01');
            $enableOperatorPool = $this->getLiberumConfigService()->getIntValue('enable_operator_pool', 1, 0, 1);
            $firstTimeoutOnlyOnce = $this->getLiberumConfigService()->getIntValue('first_timeout_only_once', 1, 0, 1);
            $operatorReleaseDays = $this->getLiberumConfigService()->getIntValue('operator_release_days', 90, 1, 3650);

            // 关闭运营池时直接返回空列表（不报错）
            if ((int)$enableOperatorPool !== 1) {
                return ['count' => 0, 'data' => []];
            }

        $query = Db::table('crm_leads')->alias('l')
            ->where('l.status', 1)
            ->where('l.issuccess', -1);
        $adminInfo = $this->getCurrentAdminInfo();
        $this->applyOperatorScope($query, $adminInfo);
        if ((int)$firstTimeoutOnlyOnce === 1 && $this->tableHasColumn('crm_leads', 'first_timeout_handled')) {
            $query->whereRaw('(l.first_timeout_handled IS NULL OR l.first_timeout_handled <> 1)');
        }

        $this->applyFirstTimeoutRuleFilters($query, $firstFollowDays, $ruleEffectiveDate);

        // 没有有效跟进记录（crm_comment）
        $query->whereRaw($this->buildNoFollowExistsSql('l.id'));

        $this->applySearchFilters($query, $params);

        $countQuery = clone $query;
        $count = (int)$countQuery->count();
        if ($count <= 0) {
            return ['count' => 0, 'data' => []];
        }

        $fields = ['l.id', 'l.kh_name', 'l.pr_user', 'l.inquiry_id', 'l.port_id', 'l.at_time', 'l.ut_time', 'l.status', 'l.issuccess'];
        if ($this->tableHasColumn('crm_leads', 'to_kh_time')) {
            $fields[] = 'l.to_kh_time';
        }
        if ($this->tableHasColumn('crm_leads', 'first_timeout_handled')) {
            $fields[] = 'l.first_timeout_handled';
        }

        $rows = $query
            ->field(implode(',', $fields))
            ->orderRaw($this->buildAtTimeOrderSql('l.at_time') . ', l.id asc')
            ->page($page, $limit)
            ->select();

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        if (empty($rows)) {
            return ['count' => $count, 'data' => []];
        }

        $leadIds = [];
        $inquiryIds = [];
        $portIdValues = [];
        foreach ($rows as $row) {
            $leadId = (int)($row['id'] ?? 0);
            if ($leadId > 0) {
                $leadIds[] = $leadId;
            }

            $inquiryId = (int)($row['inquiry_id'] ?? 0);
            if ($inquiryId > 0) {
                $inquiryIds[] = $inquiryId;
            }

            $portRaw = trim((string)($row['port_id'] ?? ''));
            if ($portRaw !== '') {
                foreach (explode(',', $portRaw) as $item) {
                    $pid = (int)trim($item);
                    if ($pid > 0) {
                        $portIdValues[] = $pid;
                    }
                }
            }
        }

        $leadIds = array_values(array_unique($leadIds));
        $inquiryIds = array_values(array_unique($inquiryIds));
        $portIdValues = array_values(array_unique($portIdValues));

        $mainPhoneMap = $this->buildMainPhoneMap($leadIds);
        $inquiryMap = $this->buildInquiryMap($inquiryIds);
        $portMap = $this->buildPortMap($portIdValues);

        $data = [];
        foreach ($rows as $row) {
            $leadId = (int)($row['id'] ?? 0);
            $atTimeRaw = $row['at_time'] ?? '';
            $atTimestamp = $this->normalizeTimeToTimestamp($atTimeRaw);
            $atTimeDisplay = $atTimestamp > 0 ? date('Y-m-d H:i:s', $atTimestamp) : (string)$atTimeRaw;

            $timeoutDays = 0;
            if ($atTimestamp > 0) {
                $diff = time() - $atTimestamp;
                if ($diff > 0) {
                    $timeoutDays = (int)floor($diff / 86400);
                }
            }

            $portName = '';
            $portRaw = trim((string)($row['port_id'] ?? ''));
            if ($portRaw !== '') {
                $parts = explode(',', $portRaw);
                $names = [];
                foreach ($parts as $item) {
                    $pid = (int)trim($item);
                    if ($pid > 0 && isset($portMap[$pid])) {
                        $names[] = (string)$portMap[$pid];
                    }
                }
                if (!empty($names)) {
                    $portName = implode(',', array_values(array_unique($names)));
                }
            }

            $data[] = [
                'id' => $leadId,
                'kh_name' => (string)($row['kh_name'] ?? ''),
                'main_phone' => (string)($mainPhoneMap[$leadId] ?? ''),
                'inquiry_name' => (string)($inquiryMap[(int)($row['inquiry_id'] ?? 0)] ?? ''),
                'port_name' => $portName,
                'pr_user' => (string)($row['pr_user'] ?? ''),
                'at_time' => $atTimeDisplay,
                'timeout_days' => $timeoutDays,
                'last_follow_time' => '',
                'status_text' => $this->buildStatusText($row, $operatorReleaseDays),
                'recent_operate_time' => $this->buildRecentOperateTime($row),
                'operation' => '',
            ];
        }

            return [
                'count' => $count,
                'data' => $data,
            ];
        } catch (\Throwable $e) {
            Log::error([
                'first_timeout_sql_error' => $e->getMessage(),
                'params' => $params
            ]);
            return ['count' => 0, 'data' => []];
        }
    }

    /**
     * 重新分配可选业务员列表
     *
     * @return array<int,array{name:string,value:string}>
     */
    public function getAssignUserOptions()
    {
        if (!$this->tableHasColumn('admin', 'username')) {
            return [];
        }

        $fields = ['username'];
        if ($this->tableHasColumn('admin', 'admin_id')) {
            $fields[] = 'admin_id';
        }
        if ($this->tableHasColumn('admin', 'group_id')) {
            $fields[] = 'group_id';
        }
        if ($this->tableHasColumn('admin', 'inquiry_id')) {
            $fields[] = 'inquiry_id';
        }

        $rows = Db::table('admin')
            ->where('username', '<>', '')
            ->field(implode(',', array_values(array_unique($fields))))
            ->order('username asc')
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        $currentAdmin = $this->getCurrentAdminInfo();
        $currentInquiryId = (int)($currentAdmin['inquiry_id'] ?? 0);

        $seen = [];
        $list = [];
        foreach ($rows as $row) {
            $username = trim((string)($row['username'] ?? ''));
            if ($username === '' || isset($seen[$username])) {
                continue;
            }

            $candidate = [
                'admin_id' => (int)($row['admin_id'] ?? 0),
                'username' => $username,
                'group_id' => (int)($row['group_id'] ?? 0),
                'inquiry_id' => trim((string)($row['inquiry_id'] ?? '')),
            ];
            if ($this->isSuperAdmin($candidate)) {
                continue;
            }

            $seen[$username] = true;
            $candidateInquiryId = (int)$candidate['inquiry_id'];
            $list[] = [
                'name' => $username,
                'value' => $username,
                '_priority' => ($currentInquiryId > 0 && $candidateInquiryId === $currentInquiryId) ? 0 : 1,
            ];
        }

        if (empty($list)) {
            return [];
        }

        usort($list, function ($a, $b) {
            $pa = (int)($a['_priority'] ?? 1);
            $pb = (int)($b['_priority'] ?? 1);
            if ($pa !== $pb) {
                return $pa <=> $pb;
            }
            return strcmp((string)($a['name'] ?? ''), (string)($b['name'] ?? ''));
        });

        foreach ($list as &$item) {
            unset($item['_priority']);
        }
        unset($item);

        return array_values($list);
    }

    /**
     * 首次超时客户重新分配
     *
     * @param int $id
     * @param string $assignUser
     * @param array $operatorInfo
     * @return array
     */
    public function assign($id, $assignUser, $operatorInfo = [])
    {
        $id = (int)$id;
        $assignUser = trim((string)$assignUser);
        $operatorInfo = is_array($operatorInfo) ? $operatorInfo : [];
        $operatorName = trim((string)($operatorInfo['username'] ?? Session::get('username')));

        if ($id <= 0) {
            return ['code' => -200, 'msg' => '参数错误：客户ID无效', 'data' => []];
        }
        if ($assignUser === '') {
            return ['code' => -200, 'msg' => '请选择分配业务员', 'data' => []];
        }

        $assignAdminInfo = $this->findAdminByUsername($assignUser);
        if (empty($assignAdminInfo) || $this->isSuperAdmin($assignAdminInfo)) {
            return ['code' => -200, 'msg' => '请选择有效业务员', 'data' => []];
        }

        $firstFollowDays = $this->getLiberumConfigService()->getIntValue('first_follow_days', 7, 1, 3650);
        $ruleEffectiveDate = $this->getLiberumConfigService()->getDateValue('rule_effective_date', '2026-06-01');
        $enableOperatorPool = $this->getLiberumConfigService()->getIntValue('enable_operator_pool', 1, 0, 1);
        $firstTimeoutOnlyOnce = $this->getLiberumConfigService()->getIntValue('first_timeout_only_once', 1, 0, 1);

        if ((int)$enableOperatorPool !== 1) {
            return ['code' => -200, 'msg' => '首次超时运营池未启用，无法操作', 'data' => []];
        }

        Db::startTrans();
        try {
            $adminInfo = $this->getCurrentAdminInfo();
            $query = Db::table('crm_leads')->alias('l')
                ->where('l.id', $id)
                ->where('l.status', 1)
                ->where('l.issuccess', -1);
            $this->applyOperatorScope($query, $adminInfo);
            if ((int)$firstTimeoutOnlyOnce === 1 && $this->tableHasColumn('crm_leads', 'first_timeout_handled')) {
                $query->whereRaw('(l.first_timeout_handled IS NULL OR l.first_timeout_handled <> 1)');
            }
            $this->applyFirstTimeoutRuleFilters($query, $firstFollowDays, $ruleEffectiveDate);
            $query->whereRaw($this->buildNoFollowExistsSql('l.id'));

            $lead = $query
                ->field('l.id,l.pr_user,l.status,l.issuccess')
                ->lock(true)
                ->find();
            if (!is_array($lead) || empty($lead)) {
                Db::rollback();
                return ['code' => -200, 'msg' => '客户已被处理或无权限操作', 'data' => []];
            }

            $now = date('Y-m-d H:i:s');
            $updateData = [
                'pr_user' => $assignUser,
                'to_kh_time' => $now,
                'ut_time' => $now,
                'status' => 1,
            ];
            if ($this->tableHasColumn('crm_leads', 'joint_person')) {
                $updateData['joint_person'] = '';
            }
            if ((int)$firstTimeoutOnlyOnce === 1 && $this->tableHasColumn('crm_leads', 'first_timeout_handled')) {
                $updateData['first_timeout_handled'] = 1;
            }

            $affected = Db::table('crm_leads')
                ->where('id', $id)
                ->where('status', 1)
                ->where('issuccess', -1)
                ->update($updateData);
            if ((int)$affected !== 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '客户已被处理或无权限操作', 'data' => []];
            }

            try {
                Log::record(
                    '首次超时重新分配：客户ID=' . $id
                    . '，原负责人=' . (string)($lead['pr_user'] ?? '')
                    . '，新负责人=' . $assignUser
                    . '，操作人=' . $operatorName,
                    'info'
                );
            } catch (\Throwable $e) {
                // 写日志失败不影响主流程
            }

            Db::commit();
            return ['code' => 0, 'msg' => '重新分配成功', 'data' => []];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '重新分配失败：' . $e->getMessage(), 'data' => []];
        }
    }

    /**
     * 批量重新分配
     *
     * @param array $ids
     * @param string $assignUser
     * @param array $operatorInfo
     * @return array
     */
    public function batchAssign($ids = [], $assignUser = '', $operatorInfo = [])
    {
        $ids = is_array($ids) ? $ids : [];
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));
        $assignUser = trim((string)$assignUser);
        $operatorInfo = is_array($operatorInfo) ? $operatorInfo : [];

        if (empty($ids)) {
            return [
                'code' => -200,
                'msg' => '请选择需要重新分配的客户',
                'data' => ['success_count' => 0, 'fail_count' => 0, 'fail_list' => []],
            ];
        }
        if ($assignUser === '') {
            return [
                'code' => -200,
                'msg' => '请选择分配业务员',
                'data' => ['success_count' => 0, 'fail_count' => 0, 'fail_list' => []],
            ];
        }

        $successCount = 0;
        $failCount = 0;
        $failList = [];
        foreach ($ids as $id) {
            $result = $this->assign($id, $assignUser, $operatorInfo);
            if ((int)($result['code'] ?? -200) === 0) {
                $successCount++;
            } else {
                $failCount++;
                $failList[] = [
                    'id' => $id,
                    'msg' => (string)($result['msg'] ?? '处理失败'),
                ];
            }
        }

        $msg = '批量处理完成：成功' . $successCount . '条，失败' . $failCount . '条';
        return [
            'code' => $successCount > 0 ? 0 : -200,
            'msg' => $msg,
            'data' => [
                'success_count' => $successCount,
                'fail_count' => $failCount,
                'fail_list' => $failList,
            ],
        ];
    }

    /**
     * 获取首次超时页面配置状态
     *
     * @return array
     */
    public function getPageConfigStatus()
    {
        $configMap = $this->getLiberumConfigService()->getConfigMap();
        return [
            'first_follow_days' => (int)($configMap['first_follow_days'] ?? 7),
            'enable_operator_pool' => (int)($configMap['enable_operator_pool'] ?? 1),
            'rule_effective_date' => (string)($configMap['rule_effective_date'] ?? '2026-06-01'),
            'operator_release_days' => (int)($configMap['operator_release_days'] ?? 90),
            'first_timeout_only_once' => (int)($configMap['first_timeout_only_once'] ?? 1),
        ];
    }

    /**
     * 获取当前登录管理员信息（以 admin 表为准）
     *
     * @return array
     */
    protected function getCurrentAdminInfo()
    {
        $adminId = (int)Session::get('aid');
        if ($adminId <= 0) {
            return [];
        }

        if (!$this->tableHasColumn('admin', 'admin_id')) {
            return [];
        }

        $fields = ['admin_id'];
        if ($this->tableHasColumn('admin', 'username')) {
            $fields[] = 'username';
        }
        if ($this->tableHasColumn('admin', 'group_id')) {
            $fields[] = 'group_id';
        }
        if ($this->tableHasColumn('admin', 'inquiry_id')) {
            $fields[] = 'inquiry_id';
        }
        if ($this->tableHasColumn('admin', 'port_id')) {
            $fields[] = 'port_id';
        }

        $row = Db::table('admin')
            ->where('admin_id', $adminId)
            ->field(implode(',', $fields))
            ->find();
        if (!is_array($row) || empty($row)) {
            return [];
        }

        return [
            'admin_id' => (int)($row['admin_id'] ?? 0),
            'username' => trim((string)($row['username'] ?? '')),
            'group_id' => (int)($row['group_id'] ?? 0),
            'inquiry_id' => trim((string)($row['inquiry_id'] ?? '')),
            'port_id' => trim((string)($row['port_id'] ?? '')),
        ];
    }

    /**
     * 是否超级管理员
     *
     * @param array $adminInfo
     * @return bool
     */
    protected function isSuperAdmin(array $adminInfo)
    {
        $adminId = (int)($adminInfo['admin_id'] ?? 0);
        $groupId = (int)($adminInfo['group_id'] ?? 0);
        $username = strtolower(trim((string)($adminInfo['username'] ?? '')));

        if ($adminId === 1) {
            return true;
        }
        if ($groupId === 1) {
            return true;
        }
        if ($username === 'admin') {
            return true;
        }
        if (in_array($adminId, [395, 350, 375, 387], true)) {
            return true;
        }

        return false;
    }

    /**
     * 应用运营人员可见范围
     *
     * @param \think\db\Query $query
     * @param array $adminInfo
     * @return void
     */
    protected function applyOperatorScope($query, array $adminInfo)
    {
        if ($this->isSuperAdmin($adminInfo)) {
            return;
        }

        // 历史库字段缺失时：非超级管理员不可见
        if (
            !$this->tableHasColumn('admin', 'inquiry_id')
            || !$this->tableHasColumn('admin', 'port_id')
            || !$this->tableHasColumn('crm_leads', 'inquiry_id')
            || !$this->tableHasColumn('crm_leads', 'port_id')
        ) {
            $query->whereRaw('1=0');
            return;
        }

        $inquiryId = (int)trim((string)($adminInfo['inquiry_id'] ?? ''));
        $portIds = $this->parseIdList($adminInfo['port_id'] ?? '');

        if ($inquiryId <= 0 || empty($portIds)) {
            $query->whereRaw('1=0');
            return;
        }

        $query->where('l.inquiry_id', $inquiryId);

        $orParts = [];
        foreach ($portIds as $portId) {
            $pid = (int)$portId;
            if ($pid <= 0) {
                continue;
            }
            $orParts[] = '(l.port_id = "' . $pid . '" OR FIND_IN_SET(' . $pid . ', l.port_id))';
        }

        if (empty($orParts)) {
            $query->whereRaw('1=0');
            return;
        }

        $query->whereRaw('(' . implode(' OR ', $orParts) . ')');
    }

    /**
     * 查询过滤
     *
     * @param \think\db\Query $query
     * @param array $params
     * @return void
     */
    protected function applySearchFilters($query, array $params)
    {
        $khName = trim((string)($params['kh_name'] ?? ''));
        if ($khName !== '') {
            $query->whereLike('l.kh_name', '%' . $khName . '%');
        }

        $mainPhone = trim((string)($params['main_phone'] ?? ''));
        if ($mainPhone !== '') {
            $contactQuery = Db::table('crm_contacts')
                ->whereIn('contact_type', [1, 3])
                ->whereLike('contact_value', '%' . $mainPhone . '%');
            if ($this->tableHasColumn('crm_contacts', 'is_delete')) {
                $contactQuery->where('is_delete', 0);
            }
            $matchedLeadIds = $contactQuery->column('DISTINCT leads_id');
            $matchedLeadIds = is_array($matchedLeadIds) ? array_values(array_unique(array_map('intval', $matchedLeadIds))) : [];
            $matchedLeadIds = array_values(array_filter($matchedLeadIds, function ($id) {
                return $id > 0;
            }));
            if (empty($matchedLeadIds)) {
                $query->whereRaw('1=0');
            } else {
                $query->whereIn('l.id', $matchedLeadIds);
            }
        }

        $inquiryName = trim((string)($params['inquiry_name'] ?? ''));
        if ($inquiryName !== '') {
            $inquiryIds = Db::table('crm_inquiry')
                ->whereLike('inquiry_name', '%' . $inquiryName . '%')
                ->column('id');
            $inquiryIds = is_array($inquiryIds) ? array_values(array_unique(array_map('intval', $inquiryIds))) : [];
            $inquiryIds = array_values(array_filter($inquiryIds, function ($id) {
                return $id > 0;
            }));
            if (empty($inquiryIds)) {
                $query->whereRaw('1=0');
            } else {
                $query->whereIn('l.inquiry_id', $inquiryIds);
            }
        }

        $portName = trim((string)($params['port_name'] ?? ''));
        if ($portName !== '') {
            $portIds = Db::table('crm_inquiry_port')
                ->whereLike('port_name', '%' . $portName . '%')
                ->column('id');
            $portIds = is_array($portIds) ? array_values(array_unique(array_map('intval', $portIds))) : [];
            $portIds = array_values(array_filter($portIds, function ($id) {
                return $id > 0;
            }));

            if (empty($portIds)) {
                $query->whereRaw('1=0');
            } else {
                $orParts = [];
                foreach ($portIds as $portId) {
                    $pid = (int)$portId;
                    if ($pid <= 0) {
                        continue;
                    }
                    $orParts[] = '(l.port_id = "' . $pid . '" OR FIND_IN_SET(' . $pid . ', l.port_id))';
                }
                if (empty($orParts)) {
                    $query->whereRaw('1=0');
                } else {
                    $query->whereRaw('(' . implode(' OR ', $orParts) . ')');
                }
            }
        }

        $atTimeRange = trim((string)($params['at_time_range'] ?? ''));
        if ($atTimeRange !== '') {
            $range = explode(' - ', $atTimeRange, 2);
            if (is_array($range) && count($range) === 2) {
                $startText = trim((string)($range[0] ?? ''));
                $endText = trim((string)($range[1] ?? ''));
                $startTs = strtotime($startText);
                $endTs = strtotime($endText);
                if ($startTs !== false && $endTs !== false && $startTs <= $endTs) {
                    $startTs = (int)$startTs;
                    $endTs = (int)$endTs;
                    $startDt = addslashes(date('Y-m-d H:i:s', $startTs));
                    $endDt = addslashes(date('Y-m-d H:i:s', $endTs));
                    $query->whereRaw(
                        '(('
                        . 'l.at_time REGEXP "^[0-9]+$" AND CAST(l.at_time AS UNSIGNED) BETWEEN ' . $startTs . ' AND ' . $endTs
                        . ') OR ('
                        . '(l.at_time REGEXP "^[0-9]+$") = 0 AND l.at_time BETWEEN "' . $startDt . '" AND "' . $endDt . '"'
                        . '))'
                    );
                }
            }
        }
    }

    /**
     * 应用首次超时基础规则（创建时间生效 + 超时）
     *
     * @param \think\db\Query $query
     * @param int $firstFollowDays
     * @param string $ruleEffectiveDate
     * @return void
     */
    protected function applyFirstTimeoutRuleFilters($query, $firstFollowDays, $ruleEffectiveDate)
    {
        $firstFollowDays = max(1, (int)$firstFollowDays);
        $ruleEffectiveDate = trim((string)$ruleEffectiveDate);
        if ($ruleEffectiveDate === '') {
            $ruleEffectiveDate = '2026-06-01';
        }

        $effectiveStart = $ruleEffectiveDate . ' 00:00:00';
        $effectiveTs = strtotime($effectiveStart);
        if ($effectiveTs === false) {
            $effectiveStart = '2026-06-01 00:00:00';
            $effectiveTs = strtotime($effectiveStart);
        }
        $timeoutBefore = date('Y-m-d H:i:s', time() - $firstFollowDays * 86400);
        $timeoutTs = strtotime($timeoutBefore);
        if ($timeoutTs === false) {
            $timeoutTs = time();
        }
        $effectiveTs = (int)$effectiveTs;
        $timeoutTs = (int)$timeoutTs;
        $effectiveStart = addslashes($effectiveStart);
        $timeoutBefore = addslashes($timeoutBefore);

        // 规则1：客户创建时间 >= 生效日期（兼容 at_time 为时间戳或字符串）
        $query->whereRaw(
            '(('
            . '(l.at_time REGEXP "^[0-9]+$" AND CAST(l.at_time AS UNSIGNED) >= ' . $effectiveTs . ')'
            . ') OR ('
            . '(l.at_time REGEXP "^[0-9]+$") = 0 AND l.at_time >= "' . $effectiveStart . '"'
            . '))'
        );

        // 规则2：首次超时（优先按 to_kh_time 计时；无 to_kh_time 时按 at_time）
        $query->whereRaw(
            '('
            . '(l.to_kh_time REGEXP "^[0-9]+$" AND CAST(l.to_kh_time AS UNSIGNED) > 0 AND CAST(l.to_kh_time AS UNSIGNED) <= ' . $timeoutTs . ')'
            . ' OR '
            . '((l.to_kh_time REGEXP "^[0-9]+$") = 0 AND l.to_kh_time IS NOT NULL AND l.to_kh_time <> "" AND l.to_kh_time <> "0000-00-00 00:00:00" AND l.to_kh_time <= "' . $timeoutBefore . '")'
            . ' OR '
            . '('
            . '(l.to_kh_time IS NULL OR l.to_kh_time = "" OR l.to_kh_time = "0000-00-00 00:00:00" OR (l.to_kh_time REGEXP "^[0-9]+$" AND CAST(l.to_kh_time AS UNSIGNED) = 0))'
            . ' AND '
            . '(('
            . '(l.at_time REGEXP "^[0-9]+$" AND CAST(l.at_time AS UNSIGNED) <= ' . $timeoutTs . ')'
            . ') OR ('
            . '(l.at_time REGEXP "^[0-9]+$") = 0 AND l.at_time <= "' . $timeoutBefore . '"'
            . '))'
            . ')'
            . ')'
        );
    }

    /**
     * 查询“无跟进” SQL
     *
     * @param string $leadIdField
     * @return string
     */
    protected function buildNoFollowExistsSql($leadIdField)
    {
        $leadIdField = trim((string)$leadIdField);
        if ($leadIdField === '') {
            $leadIdField = 'l.id';
        }

        $where = 'c.leads_id = ' . $leadIdField;
        if ($this->tableHasColumn('crm_comment', 'is_delete')) {
            $where .= ' AND c.is_delete = 0';
        }

        return 'NOT EXISTS (SELECT 1 FROM crm_comment c WHERE ' . $where . ')';
    }

    /**
     * 构建创建时间排序表达式（兼容字符串和时间戳）
     *
     * @param string $field
     * @return string
     */
    protected function buildAtTimeOrderSql($field)
    {
        $field = trim((string)$field);
        if ($field === '') {
            $field = 'l.at_time';
        }

        return 'CASE WHEN ' . $field . ' REGEXP "^[0-9]+$" THEN CAST(' . $field . ' AS UNSIGNED) ELSE UNIX_TIMESTAMP(' . $field . ') END asc';
    }

    /**
     * 主电话映射：contact_type=1 且 is_delete=0 的第一个号码
     *
     * @param array $leadIds
     * @return array<int,string>
     */
    protected function buildMainPhoneMap(array $leadIds)
    {
        $result = [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return $result;
        }

        $query = Db::table('crm_contacts')
            ->field('leads_id,contact_value')
            ->whereIn('leads_id', $leadIds)
            ->where('contact_type', 1)
            ->order('id asc');
        if ($this->tableHasColumn('crm_contacts', 'is_delete')) {
            $query->where('is_delete', 0);
        }

        $rows = $query->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        foreach ($rows as $row) {
            $leadId = (int)($row['leads_id'] ?? 0);
            if ($leadId <= 0 || isset($result[$leadId])) {
                continue;
            }
            $result[$leadId] = trim((string)($row['contact_value'] ?? ''));
        }

        return $result;
    }

    /**
     * 渠道映射
     *
     * @param array $inquiryIds
     * @return array<int,string>
     */
    protected function buildInquiryMap(array $inquiryIds)
    {
        $result = [];
        $inquiryIds = array_values(array_unique(array_filter(array_map('intval', $inquiryIds), function ($id) {
            return $id > 0;
        })));
        if (empty($inquiryIds)) {
            return $result;
        }

        $rows = Db::table('crm_inquiry')
            ->whereIn('id', $inquiryIds)
            ->field('id,inquiry_name')
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id > 0) {
                $result[$id] = (string)($row['inquiry_name'] ?? '');
            }
        }

        return $result;
    }

    /**
     * 端口映射
     *
     * @param array $portIds
     * @return array<int,string>
     */
    protected function buildPortMap(array $portIds)
    {
        $result = [];
        $portIds = array_values(array_unique(array_filter(array_map('intval', $portIds), function ($id) {
            return $id > 0;
        })));
        if (empty($portIds)) {
            return $result;
        }

        $rows = Db::table('crm_inquiry_port')
            ->whereIn('id', $portIds)
            ->field('id,port_name')
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id > 0) {
                $result[$id] = (string)($row['port_name'] ?? '');
            }
        }

        return $result;
    }

    /**
     * 统一时间转时间戳（兼容 int/string）
     *
     * @param mixed $time
     * @return int
     */
    protected function normalizeTimeToTimestamp($time)
    {
        if (is_int($time)) {
            return $time > 0 ? $time : 0;
        }
        if (is_numeric($time) && (string)(int)$time === trim((string)$time)) {
            $ts = (int)$time;
            return $ts > 0 ? $ts : 0;
        }

        $str = trim((string)$time);
        if ($str === '' || strtolower($str) === 'null' || strtolower($str) === 'undefined') {
            return 0;
        }
        if ($str === '0000-00-00 00:00:00') {
            return 0;
        }

        $ts = strtotime($str);
        return $ts === false ? 0 : (int)$ts;
    }

    /**
     * 将单值/逗号分隔ID字符串转为整型数组
     *
     * @param mixed $value
     * @return array<int>
     */
    protected function parseIdList($value)
    {
        $value = trim((string)$value);
        if ($value === '') {
            return [];
        }

        $parts = explode(',', $value);
        $ids = [];
        foreach ($parts as $item) {
            $id = (int)trim((string)$item);
            if ($id > 0) {
                $ids[] = $id;
            }
        }

        return array_values(array_unique($ids));
    }

    /**
     * 计算状态说明
     *
     * @param array $row
     * @param int $operatorReleaseDays
     * @return string
     */
    protected function buildStatusText(array $row, $operatorReleaseDays)
    {
        $operatorReleaseDays = max(1, (int)$operatorReleaseDays);
        $isHandled = (int)($row['first_timeout_handled'] ?? 0) === 1;
        if ($isHandled) {
            return '已重新分配';
        }

        $atTs = $this->normalizeTimeToTimestamp($row['at_time'] ?? '');
        if ($atTs > 0) {
            $diffDays = (int)floor(max(0, time() - $atTs) / 86400);
            if ($diffDays >= $operatorReleaseDays) {
                return '待转普通公海';
            }
        }

        return '待运营分配';
    }

    /**
     * 计算最近操作时间
     *
     * @param array $row
     * @return string
     */
    protected function buildRecentOperateTime(array $row)
    {
        $isHandled = (int)($row['first_timeout_handled'] ?? 0) === 1;
        if (!$isHandled) {
            return '';
        }

        $ts = $this->normalizeTimeToTimestamp($row['to_kh_time'] ?? '');
        if ($ts <= 0) {
            return '';
        }
        return date('Y-m-d H:i:s', $ts);
    }

    /**
     * 通过用户名查询管理员信息
     *
     * @param string $username
     * @return array
     */
    protected function findAdminByUsername($username)
    {
        $username = trim((string)$username);
        if ($username === '' || !$this->tableHasColumn('admin', 'username')) {
            return [];
        }

        $fields = ['username'];
        if ($this->tableHasColumn('admin', 'admin_id')) {
            $fields[] = 'admin_id';
        }
        if ($this->tableHasColumn('admin', 'group_id')) {
            $fields[] = 'group_id';
        }
        if ($this->tableHasColumn('admin', 'inquiry_id')) {
            $fields[] = 'inquiry_id';
        }
        if ($this->tableHasColumn('admin', 'port_id')) {
            $fields[] = 'port_id';
        }

        $row = Db::table('admin')
            ->where('username', $username)
            ->field(implode(',', array_values(array_unique($fields))))
            ->find();
        if (!is_array($row) || empty($row)) {
            return [];
        }

        return [
            'admin_id' => (int)($row['admin_id'] ?? 0),
            'username' => trim((string)($row['username'] ?? '')),
            'group_id' => (int)($row['group_id'] ?? 0),
            'inquiry_id' => trim((string)($row['inquiry_id'] ?? '')),
            'port_id' => trim((string)($row['port_id'] ?? '')),
        ];
    }

    /**
     * 字段是否存在（兼容历史库）
     *
     * @param string $tableName
     * @param string $columnName
     * @return bool
     */
    protected function tableHasColumn($tableName, $columnName)
    {
        $tableName = strtolower(trim((string)$tableName));
        $columnName = strtolower(trim((string)$columnName));
        if ($tableName === '' || $columnName === '') {
            return false;
        }

        $cacheKey = $tableName . '.' . $columnName;
        if (array_key_exists($cacheKey, self::$columnExistsCache)) {
            return (bool)self::$columnExistsCache[$cacheKey];
        }

        $exists = $this->hasColumn($tableName, $columnName);
        self::$columnExistsCache[$cacheKey] = $exists ? 1 : 0;
        return $exists;
    }

    /**
     * 检测表字段是否存在（本地实现，避免依赖 BaseAdminService）
     *
     * @param string $table
     * @param string $column
     * @return bool
     */
    protected function hasColumn($table, $column)
    {
        $table = strtolower(trim((string)$table));
        $column = strtolower(trim((string)$column));
        if ($table === '' || $column === '') {
            return false;
        }

        try {
            $rows = Db::query("SHOW COLUMNS FROM `{$table}`");
            if (!is_array($rows) || empty($rows)) {
                return false;
            }

            foreach ($rows as $row) {
                $field = strtolower(trim((string)($row['Field'] ?? '')));
                if ($field !== '' && $field === $column) {
                    return true;
                }
            }
        } catch (\Throwable $e) {
            return false;
        }

        return false;
    }
}
