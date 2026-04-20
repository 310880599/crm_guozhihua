<?php

namespace app\admin\service;

use app\admin\model\ClientFollow;
use think\Db;
use Throwable;

class ClientFollowService
{
    /**
     * 保存客户跟进记录，并同步更新 crm_leads 跟进字段
     *
     * @param int|string $leadsId
     * @param string $content
     * @param string|null $nextUpTime
     * @param array $operatorInfo ['admin_id' => int, 'username' => string]
     * @return array
     */
    public function saveFollow($leadsId, $content, $nextUpTime, $operatorInfo = [])
    {
        $leadsId = (int)$leadsId;
        $content = trim((string)$content);
        $nextUpTime = trim((string)$nextUpTime);
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));

        if ($leadsId <= 0) {
            return $this->fail('缺少客户ID');
        }
        if ($operatorId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }
        if ($content === '') {
            return $this->fail('请输入跟进内容');
        }

        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$client) {
            return $this->fail('客户不存在');
        }

        if (!$this->canOperateClient($client, $operatorId, $operatorName)) {
            return $this->fail('您没有权限操作该客户');
        }

        list($ok, $nextUpValue, $nextUpErrMsg) = $this->normalizeNextUpTimeValue($nextUpTime);
        if (!$ok) {
            return $this->fail($nextUpErrMsg);
        }

        $nowTimestamp = time();
        $nowDateTime = date('Y-m-d H:i:s', $nowTimestamp);

        Db::startTrans();
        try {
            Db::table('crm_comment')->insert([
                'leads_id' => $leadsId,
                'user_id' => $operatorId,
                'reply_msg' => $content,
                'create_date' => $nowTimestamp,
            ]);

            Db::table('crm_leads')->where('id', $leadsId)->update([
                'last_up_records' => $content,
                'last_up_time' => $nowDateTime,
                'next_up_time' => $nextUpValue,
                'ut_time' => $nowDateTime,
            ]);

            Db::commit();

            return [
                'code' => 0,
                'msg' => '保存成功',
                'data' => [
                    'leads_id' => $leadsId,
                    'reply_msg' => $content,
                    'create_date' => date('Y年m月d日 H:i', $nowTimestamp),
                    'next_up_time' => $this->formatNextUpTimeForDisplay($nextUpValue),
                ],
            ];
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail('保存跟进失败：' . $e->getMessage());
        }
    }

    /**
     * 当前用户是否有权限操作客户（admin_id=1 超管放行）
     */
    private function canOperateClient(array $client, $operatorId, $operatorName)
    {
        if ((int)$operatorId === 1) {
            return true;
        }

        $ownerName = trim((string)($client['pr_user'] ?? ''));
        $isOwner = ($ownerName !== '' && $ownerName === $operatorName);
        if ($isOwner) {
            return true;
        }

        return $this->isJointPerson($client, $operatorId);
    }

    /**
     * 判断是否协同人：兼容 JSON 数组和逗号分隔两种存储格式
     */
    private function isJointPerson(array $client, $operatorId)
    {
        $jointRaw = trim((string)($client['joint_person'] ?? ''));
        if ($jointRaw === '') {
            return false;
        }

        $jointIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jointRaw)) {
            $decoded = json_decode($jointRaw, true);
            if (is_array($decoded)) {
                $jointIds = $decoded;
            }
        } else {
            $jointIds = array_values(array_filter(explode(',', $jointRaw)));
        }

        return in_array((string)$operatorId, array_map('strval', $jointIds), true);
    }

    /**
     * 标准化 next_up_time：
     * - 有值：校验并格式化为 Y-m-d H:i:s
     * - 无值：按字段可空性写入 NULL 或空字符串
     */
    private function normalizeNextUpTimeValue($nextUpTime)
    {
        if ($nextUpTime !== '') {
            $timestamp = strtotime($nextUpTime);
            if ($timestamp === false) {
                return [false, null, '下次跟进时间格式不正确'];
            }
            return [true, date('Y-m-d H:i:s', $timestamp), ''];
        }

        return [true, $this->emptyNextUpTimeValue(), ''];
    }

    /**
     * next_up_time 置空值的兼容处理
     */
    private function emptyNextUpTimeValue()
    {
        try {
            $column = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'next_up_time'");
            if (!empty($column) && isset($column[0]['Null']) && strtoupper((string)$column[0]['Null']) === 'YES') {
                return null;
            }
        } catch (Throwable $e) {
            // 忽略字段探测异常，默认回退为空字符串
        }

        return '';
    }

    /**
     * next_up_time 展示值格式化（仅用于返回前端展示）。
     */
    public function formatNextUpTimeForDisplay($nextUpTime)
    {
        if ($nextUpTime === null) {
            return '';
        }

        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null' || strtolower($raw) === 'undefined') {
            return '';
        }

        $timestamp = strtotime($raw);
        if ($timestamp === false) {
            return $raw;
        }

        return date('Y-m-d H:i:s', $timestamp);
    }

    /**
     * 跟进弹窗客户信息组装（控制器调度，service 统一组装展示字段）。
     */
    public function buildFollowClientDetailData(array $client, $clientId)
    {
        if (!array_key_exists('next_up_time', $client)) {
            $client['next_up_time'] = '';
        }
        $client['next_up_time'] = $this->formatNextUpTimeForDisplay($client['next_up_time']);

        // 主/辅电话（1=主，3=辅）
        $mainPhone = '';
        $auxPhone = '';
        $contacts = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', (int)$clientId)
            ->whereIn('contact_type', [1, 3])
            ->order('id', 'asc')
            ->field('contact_type, contact_value')
            ->select();
        foreach ($contacts as $c) {
            if ($c['contact_type'] == 1 && $mainPhone === '') {
                $mainPhone = $c['contact_value'];
            } elseif ($c['contact_type'] == 3 && $auxPhone === '') {
                $auxPhone = $c['contact_value'];
            }
        }
        $client['main_phone'] = $mainPhone;
        $client['aux_phone'] = $auxPhone;

        // 客户状态展示名
        $khStatusValue = $client['kh_status'] ?? '';
        $statusName = $khStatusValue;
        if (is_numeric($khStatusValue)) {
            $statusInfo = Db::table('crm_client_status')->where('id', $khStatusValue)->find();
            if ($statusInfo) {
                $statusName = $statusInfo['status_name'];
            }
        }
        $client['kh_status_name'] = $statusName;

        // 客户级别展示名（兼容新老数据）
        $rankMap = Db::table('crm_client_rank')->column('rank_name', 'id');
        $rankNameMap = [];
        foreach ($rankMap as $rankId => $rankName) {
            $rankNameMap[(string)$rankId] = trim((string)$rankName);
        }
        $rawKhRank = trim((string)($client['kh_rank'] ?? ''));
        if ($rawKhRank === '') {
            $client['kh_rank_name'] = '';
        } elseif (preg_match('/^\d+$/', $rawKhRank)) {
            $client['kh_rank_name'] = isset($rankNameMap[$rawKhRank]) && trim((string)$rankNameMap[$rawKhRank]) !== ''
                ? trim((string)$rankNameMap[$rawKhRank])
                : $rawKhRank;
        } else {
            $client['kh_rank_name'] = $rawKhRank;
        }

        // 所属渠道名称
        $inquiryValue = $client['inquiry_id'] ?? '';
        $inquiryName = $inquiryValue;
        if (is_numeric($inquiryValue)) {
            $inquiryInfo = Db::table('crm_inquiry')->where('id', $inquiryValue)->find();
            if ($inquiryInfo) {
                $inquiryName = $inquiryInfo['inquiry_name'];
            }
        }
        $client['inquiry_name'] = $inquiryName;

        // 运营端口名称
        $portValue = $client['port_id'] ?? '';
        $portName = $portValue;
        if (is_numeric($portValue)) {
            $portInfo = Db::table('crm_inquiry_port')->where('id', $portValue)->find();
            if ($portInfo) {
                $portName = $portInfo['port_name'];
            }
        }
        $client['port_name'] = $portName;

        // 产品名称
        $productValue = $client['product_name'] ?? '';
        $productName = $productValue;
        if (is_numeric($productValue) && !empty($productValue)) {
            $productInfo = Db::table('crm_products')->where('id', $productValue)->find();
            if ($productInfo) {
                $productName = $productInfo['product_name'];
            }
        }
        $client['product_name'] = $productName;

        // 协同人
        $jointPersonIds = [];
        $jointPersonNames = [];
        if (!empty($client['joint_person'])) {
            $jp = $client['joint_person'];
            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                $tmp = json_decode($jp, true);
                if (is_array($tmp)) {
                    $jointPersonIds = $tmp;
                }
            } else {
                $jointPersonIds = array_values(array_filter(explode(',', $jp)));
            }

            if (!empty($jointPersonIds)) {
                $adminMap = Db::table('admin')
                    ->whereIn('admin_id', $jointPersonIds)
                    ->column('username', 'admin_id');
                foreach ($jointPersonIds as $uid) {
                    $jointPersonNames[] = $adminMap[$uid] ?? (string)$uid;
                }
            }
        }
        $client['joint_person_ids'] = $jointPersonIds;
        $client['joint_person_names'] = implode('、', $jointPersonNames);

        return $client;
    }

    /**
     * 获取并格式化跟进历史（统一时间格式）。
     */
    public function getFollowComments($clientId)
    {
        $comments = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where(['leads_id' => (int)$clientId])
            ->field('com.*, adm.username, adm.avatar')
            ->order('com.create_date desc')
            ->select();

        foreach ($comments as &$comment) {
            $comment['create_date'] = date('Y年m月d日 H:i', $comment['create_date']);
        }
        unset($comment);

        return $comments;
    }

    /**
     * 待办跟进管理表格数据（layui table）：Model 只负责分页查询，本方法负责全部展示层组装。
     *
     * @param int $page
     * @param int $limit
     * @param array $keyword 与 ClientFollow::buildTodoFollowBaseQuery 一致
     * @param array $currentAdmin 预留，透传 Model
     * @return array{code:int,msg:string,data:array,count:int,rel:int}
     */
    public function getTodoFollowTableData($page, $limit, $keyword = [], $currentAdmin = [])
    {
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);
        $keyword = $this->normalizeTodoSearchKeyword($keyword);

        $list = (new ClientFollow())->getTodoFollowList($page, $limit, $keyword, $currentAdmin);
        if ($list === null || empty($list['data'])) {
            return [
                'code' => 0,
                'msg' => '获取成功!',
                'data' => [],
                'count' => 0,
                'rel' => 1,
            ];
        }

        $rows = $list['data'];
        $total = (int)($list['total'] ?? 0);

        $inquiryIds = array_unique(array_filter(array_column($rows, 'inquiry_id')));
        $portIds = array_unique(array_filter(array_column($rows, 'port_id')));
        $productIds = [];
        foreach ($rows as $r) {
            $pv = $r['product_name'] ?? '';
            if ($pv !== '' && $pv !== null && is_numeric($pv)) {
                $productIds[] = (int)$pv;
            }
        }
        $productIds = array_unique(array_filter($productIds));

        $inquiryMap = [];
        if (!empty($inquiryIds)) {
            $inquiryMap = Db::table('crm_inquiry')->whereIn('id', $inquiryIds)->column('inquiry_name', 'id');
        }
        $portMap = [];
        if (!empty($portIds)) {
            $portMap = Db::table('crm_inquiry_port')->whereIn('id', $portIds)->column('port_name', 'id');
        }
        $productMap = [];
        if (!empty($productIds)) {
            $productMap = Db::table('crm_products')->whereIn('id', $productIds)->column('product_name', 'id');
        }

        $jointIdSet = [];
        foreach ($rows as $r) {
            foreach ($this->parseJointPersonIds($r['joint_person'] ?? '') as $jid) {
                if ($jid > 0) {
                    $jointIdSet[$jid] = true;
                }
            }
        }
        $jointAdminMap = [];
        if (!empty($jointIdSet)) {
            $jointAdminMap = Db::table('admin')
                ->whereIn('admin_id', array_keys($jointIdSet))
                ->column('username', 'admin_id');
        }

        $out = [];
        foreach ($rows as $row) {
            $out[] = $this->buildTodoFollowTableRow($row, $inquiryMap, $portMap, $productMap, $jointAdminMap);
        }

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $out,
            'count' => $total,
            'rel' => 1,
        ];
    }

    /**
     * 待办跟进搜索条件标准化（Service 统一入口，Controller 只负责接收与转发）。
     *
     * @param mixed $keyword
     * @return array
     */
    private function normalizeTodoSearchKeyword($keyword)
    {
        $keyword = is_array($keyword) ? $keyword : [];
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

        $keyword['kh_name'] = $this->normalizeSearchScalar($keyword['kh_name']);
        $keyword['phone'] = $this->normalizeSearchScalar($keyword['phone']);
        $keyword['pr_user'] = $this->normalizeSearchScalar($keyword['pr_user']);
        $keyword['next_up_start'] = $this->normalizeSearchScalar($keyword['next_up_start']);
        $keyword['next_up_end'] = $this->normalizeSearchScalar($keyword['next_up_end']);
        $keyword['inquiry_id'] = $this->normalizeSearchScalar($keyword['inquiry_id']);
        $keyword['port_id'] = $this->normalizeSearchScalar($keyword['port_id']);

        $onlyOverdueRaw = $keyword['only_overdue'];
        $keyword['only_overdue'] = $this->isTruthyFlag($onlyOverdueRaw) ? '1' : '';

        return $keyword;
    }

    /**
     * 搜索标量统一标准化：trim + 兜底 null/undefined。
     *
     * @param mixed $value
     * @return string
     */
    private function normalizeSearchScalar($value)
    {
        if ($value === null) {
            return '';
        }

        $s = trim((string)$value);
        $lower = strtolower($s);
        if ($lower === 'null' || $lower === 'undefined') {
            return '';
        }

        return $s;
    }

    /**
     * 布尔型筛选统一识别（兼容 1/true/on/yes）。
     *
     * @param mixed $v
     * @return bool
     */
    private function isTruthyFlag($v)
    {
        if ($v === true || $v === 1 || $v === '1') {
            return true;
        }
        if (is_string($v)) {
            $s = strtolower(trim($v));
            return $s === 'true' || $s === 'yes' || $s === 'on';
        }
        return false;
    }

    /**
     * 单行待办列表展示数据（时间、电话、渠道、逾期与前端标签均在 Service 内完成）。
     *
     * @param array $row Model 行
     * @param array $inquiryMap id => inquiry_name
     * @param array $portMap id => port_name
     * @param array $productMap id => product_name
     * @param array $jointAdminMap admin_id => username
     * @return array
     */
    private function buildTodoFollowTableRow(array $row, array $inquiryMap, array $portMap, array $productMap, array $jointAdminMap)
    {
        $nextRaw = array_key_exists('next_up_time', $row) ? $row['next_up_time'] : null;
        $lastRaw = array_key_exists('last_up_time', $row) ? $row['last_up_time'] : null;

        $row['next_up_time_display'] = $this->formatNextUpTimeForDisplay($nextRaw);
        $row['last_up_time_display'] = $this->formatLastUpTimeForDisplay($lastRaw);

        $phones = $this->buildTodoFollowPhoneDisplay(
            $row['main_phone'] ?? null,
            $row['aux_phone'] ?? null
        );
        $row['main_phone_display'] = $phones['main_phone_display'];
        $row['aux_phone_display'] = $phones['aux_phone_display'];
        $row['phone_summary'] = $phones['phone_summary'];

        $iid = $row['inquiry_id'] ?? null;
        $row['inquiry_name'] = $this->mapInquiryName($iid, $inquiryMap);

        $pid = $row['port_id'] ?? null;
        $row['port_name'] = $this->mapPortName($pid, $portMap);

        $prodVal = $row['product_name'] ?? null;
        $row['product_name_display'] = $this->mapProductNameDisplay($prodVal, $productMap);

        $row['kh_name_display'] = $this->scalarToDisplay($row['kh_name'] ?? null, '-');
        $row['pr_user_display'] = $this->scalarToDisplay($row['pr_user'] ?? null, '-');
        $row['last_up_records_display'] = $this->scalarToDisplay($row['last_up_records'] ?? null, '-');

        $row['joint_person_display'] = $this->formatJointPersonNamesForTable($row['joint_person'] ?? '', $jointAdminMap);

        $row['overdue_days'] = $this->calcOverdueDays($nextRaw);
        $od = (int)$row['overdue_days'];
        $row['overdue_days_display'] = $od > 0 ? ($od . '天') : '—';

        $row['is_todo_follow'] = $this->isTodoFollow($nextRaw);

        $tag = $this->buildTodoFollowTag($nextRaw, (int)$row['overdue_days']);
        $row['todo_tag'] = $tag['todo_tag'];
        $row['todo_status_text'] = $tag['todo_status_text'];

        return $row;
    }

    /**
     * 解析 joint_person 为 admin_id 列表（JSON 数组或逗号分隔）。
     *
     * @param mixed $jointRaw
     * @return int[]
     */
    private function parseJointPersonIds($jointRaw)
    {
        $jointRaw = trim((string)$jointRaw);
        if ($jointRaw === '') {
            return [];
        }

        $jointIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jointRaw)) {
            $decoded = json_decode($jointRaw, true);
            if (is_array($decoded)) {
                $jointIds = $decoded;
            }
        } else {
            $jointIds = array_values(array_filter(explode(',', $jointRaw)));
        }

        $out = [];
        foreach ($jointIds as $id) {
            $n = (int)$id;
            if ($n > 0) {
                $out[] = $n;
            }
        }
        return $out;
    }

    /**
     * 协同人展示名（与 buildFollowClientDetailData 口径一致）。
     *
     * @param array $adminIdToName admin_id => username
     */
    private function formatJointPersonNamesForTable($jointRaw, array $adminIdToName)
    {
        $ids = $this->parseJointPersonIds($jointRaw);
        if (empty($ids)) {
            return '-';
        }
        $names = [];
        foreach ($ids as $id) {
            $name = isset($adminIdToName[$id]) ? $adminIdToName[$id] : (isset($adminIdToName[(string)$id]) ? $adminIdToName[(string)$id] : '');
            $name = trim((string)$name);
            $names[] = $name !== '' ? $name : (string)$id;
        }
        return empty($names) ? '-' : implode('、', $names);
    }

    /**
     * 上次跟进时间展示（与下次跟进时间同一套日期时间规范化）。
     *
     * @param mixed $lastUpTime
     * @return string
     */
    public function formatLastUpTimeForDisplay($lastUpTime)
    {
        return $this->formatNextUpTimeForDisplay($lastUpTime);
    }

    /**
     * 主电话 / 辅助电话：Model 已聚合为 main_phone、aux_phone，此处统一去空、去标签、供表格展示。
     *
     * @return array{main_phone_display:string,aux_phone_display:string,phone_summary:string}
     */
    private function buildTodoFollowPhoneDisplay($mainRaw, $auxRaw)
    {
        $main = $this->normalizePhoneChunk($mainRaw);
        $aux = $this->normalizePhoneChunk($auxRaw);
        if ($aux !== '') {
            $aux = str_replace(['<br>', '<br/>', '<br />'], '、', $aux);
            $aux = trim(preg_replace('/\s+/u', ' ', strip_tags($aux)));
        }

        $mainDisp = $main === '' ? '-' : $main;
        $auxDisp = $aux === '' ? '-' : $aux;

        $summary = $mainDisp;
        if ($auxDisp !== '-') {
            $summary = ($mainDisp === '-') ? $auxDisp : ($mainDisp . ' / ' . $auxDisp);
        }
        if ($summary === '-') {
            $summary = '-';
        }

        return [
            'main_phone_display' => $mainDisp,
            'aux_phone_display' => $auxDisp,
            'phone_summary' => $summary,
        ];
    }

    /**
     * @param mixed $chunk
     * @return string
     */
    private function normalizePhoneChunk($chunk)
    {
        if ($chunk === null) {
            return '';
        }
        $s = trim((string)$chunk);
        if ($s === '' || strtolower($s) === 'null' || strtolower($s) === 'undefined') {
            return '';
        }
        return $s;
    }

    /**
     * @param mixed $inquiryId
     * @param array $inquiryMap
     * @return string
     */
    private function mapInquiryName($inquiryId, array $inquiryMap)
    {
        if ($inquiryId === null || $inquiryId === '') {
            return '-';
        }
        if (is_numeric($inquiryId)) {
            $id = (int)$inquiryId;
            if (isset($inquiryMap[$id]) && trim((string)$inquiryMap[$id]) !== '') {
                return trim((string)$inquiryMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$inquiryId);
        return $s === '' ? '-' : $s;
    }

    /**
     * @param mixed $portId
     * @param array $portMap
     * @return string
     */
    private function mapPortName($portId, array $portMap)
    {
        if ($portId === null || $portId === '') {
            return '-';
        }
        if (is_numeric($portId)) {
            $id = (int)$portId;
            if (isset($portMap[$id]) && trim((string)$portMap[$id]) !== '') {
                return trim((string)$portMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$portId);
        return $s === '' ? '-' : $s;
    }

    /**
     * @param mixed $productVal
     * @param array $productMap
     * @return string
     */
    private function mapProductNameDisplay($productVal, array $productMap)
    {
        if ($productVal === null || $productVal === '') {
            return '-';
        }
        if (is_numeric($productVal)) {
            $id = (int)$productVal;
            if (isset($productMap[$id]) && trim((string)$productMap[$id]) !== '') {
                return trim((string)$productMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$productVal);
        return $s === '' ? '-' : $s;
    }

    /**
     * 标量展示：空/null/伪空 统一为占位符。
     *
     * @param mixed $value
     * @param string $placeholder
     * @return string
     */
    private function scalarToDisplay($value, $placeholder = '-')
    {
        if ($value === null) {
            return $placeholder;
        }
        $s = trim((string)$value);
        if ($s === '' || strtolower($s) === 'null' || strtolower($s) === 'undefined') {
            return $placeholder;
        }
        return $s;
    }

    /**
     * 前端列表标签：逾期 N 天 / 今日待办 / 待跟进。
     *
     * @param mixed $nextUpTime
     * @param int $overdueDays
     * @return array{todo_tag:string,todo_status_text:string}
     */
    private function buildTodoFollowTag($nextUpTime, $overdueDays)
    {
        if ($overdueDays > 0) {
            return [
                'todo_tag' => 'overdue',
                'todo_status_text' => '逾期' . $overdueDays . '天',
            ];
        }
        $raw = $nextUpTime === null ? '' : trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return ['todo_tag' => 'unknown', 'todo_status_text' => '-'];
        }
        $ts = strtotime($raw);
        if ($ts !== false && date('Y-m-d', $ts) === date('Y-m-d')) {
            return [
                'todo_tag' => 'today',
                'todo_status_text' => '今日待办',
            ];
        }
        return [
            'todo_tag' => 'due',
            'todo_status_text' => '待跟进',
        ];
    }

    /**
     * 逾期天数：下次跟进日期的「日历日」早于「今天」时，返回相隔天数；否则 0。
     * 与「仅看逾期」口径一致：跨自然日才算逾期天数。
     *
     * @param mixed $nextUpTime
     * @return int
     */
    public function calcOverdueDays($nextUpTime)
    {
        if ($nextUpTime === null) {
            return 0;
        }
        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return 0;
        }
        $ts = strtotime($raw);
        if ($ts === false) {
            return 0;
        }
        $dueDay = date('Y-m-d', $ts);
        $today = date('Y-m-d');
        if ($dueDay >= $today) {
            return 0;
        }
        $dueStart = strtotime($dueDay . ' 00:00:00');
        $todayStart = strtotime($today . ' 00:00:00');
        return (int)(($todayStart - $dueStart) / 86400);
    }

    /**
     * 是否满足「待办时间」语义：有下次跟进时间且已到期（<= 当前时刻）。
     * 不校验 status / issuccess（由列表查询保证）。
     *
     * @param mixed $nextUpTime
     * @return bool
     */
    public function isTodoFollow($nextUpTime)
    {
        if ($nextUpTime === null) {
            return false;
        }
        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return false;
        }
        $ts = strtotime($raw);
        if ($ts === false) {
            return false;
        }
        return $ts <= time();
    }

    private function fail($msg)
    {
        return ['code' => 1, 'msg' => $msg, 'data' => []];
    }
}
