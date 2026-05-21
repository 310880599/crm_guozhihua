<?php

namespace app\admin\service;

use app\admin\behavior\ContactMap;
use think\Db;
use think\facade\Log;

/**
 * 自动公海候选服务
 *
 * 本服务仅负责：
 * 1. 候选客户筛选
 * 2. 候选规则计算
 * 3. 候选原因生成
 * 4. 最后跟进时间计算
 * 5. 超期天数计算
 *
 * 索引建议（仅注释，不执行SQL）：
 * - crm_leads: status, issuccess, at_time, pr_user（可按查询特点做联合索引）
 * - crm_comment: leads_id, create_date（建议联合索引 leads_id + create_date）
 * - crm_contacts: leads_id, contact_value
 */
class LiberumAutoCandidateService extends BaseAdminService
{
    /**
     * @var LiberumConfigService
     */
    protected $liberumConfigService;

    protected function getLiberumConfigService()
    {
        if (!$this->liberumConfigService) {
            $this->liberumConfigService = new LiberumConfigService();
        }
        return $this->liberumConfigService;
    }

    protected function getAutoLiberumDays()
    {
        return $this->getLiberumConfigService()->getIntValue('auto_liberum_days', 90, 1, 3650);
    }

    /**
     * 手机号缓存
     *
     * @var array<int,string>
     */
    protected static $phoneMapCache = [];

    /**
     * 手机号反查 leads_id 缓存
     *
     * @var array<string,array>
     */
    protected static $phoneSearchCache = [];

    /**
     * 当前批次客户缓存（key: lead_id）
     *
     * @var array<int,array>
     */
    protected $leadMap = [];

    /**
     * 当前批次跟进统计缓存（key: lead_id）
     *
     * @var array<int,array>
     */
    protected $followStatsMap = [];

    /**
     * 当前批次候选原因缓存（key: lead_id）
     *
     * @var array<int,array>
     */
    protected $candidateReasonMap = [];

    /**
     * 字段存在性缓存
     *
     * @var array
     */
    protected static $columnExistsCache = [];

    /**
     * 确认单个候选客户流入公海
     *
     * @param int $leadId
     * @param array $operatorInfo
     * @return array
     */
    public function confirmToLiberum($leadId, $operatorInfo = [], $manualGhTypeId = 0)
    {
        $leadId = (int)$leadId;
        $manualGhTypeId = (int)$manualGhTypeId;
        $operatorInfo = is_array($operatorInfo) ? $operatorInfo : [];
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));

        if ($leadId <= 0) {
            return ['code' => -200, 'msg' => '参数错误：客户ID无效', 'data' => []];
        }
        if ($manualGhTypeId <= 0) {
            return ['code' => -200, 'msg' => '请选择需要流入的公海类型', 'data' => []];
        }

        $ghTypeInfo = Db::table('crm_liberum_type')
            ->where('id', $manualGhTypeId)
            ->where('is_deleted', 0)
            ->field('id,type_name')
            ->find();
        if (empty($ghTypeInfo)) {
            return ['code' => -200, 'msg' => '请选择有效的公海类型', 'data' => []];
        }
        $ghTypeId = (int)($ghTypeInfo['id'] ?? 0);
        $ghTypeName = trim((string)($ghTypeInfo['type_name'] ?? ''));
        if ($ghTypeId <= 0) {
            return ['code' => -200, 'msg' => '请选择有效的公海类型', 'data' => []];
        }

        Db::startTrans();
        try {
            $lead = Db::table('crm_leads')
                ->where('id', $leadId)
                ->lock(true)
                ->find();
            if (empty($lead)) {
                Db::rollback();
                return ['code' => -200, 'msg' => '客户不存在', 'data' => []];
            }
            if (
                (!isset($lead['create_time']) || trim((string)$lead['create_time']) === '')
                && isset($lead['at_time'])
                && trim((string)$lead['at_time']) !== ''
            ) {
                $lead['create_time'] = $lead['at_time'];
            }

            if ((int)($lead['status'] ?? 0) !== 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '该客户当前不属于正常客户，无法流入公海', 'data' => []];
            }

            if ((int)($lead['issuccess'] ?? 0) === 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '成交客户不能流入公海', 'data' => []];
            }

            // 二次规则校验：必须使用后端实时规则，不能依赖前端结果。
            $this->leadMap = [$leadId => $lead];
            $this->followStatsMap = [];
            $this->candidateReasonMap = [];
            if (!$this->isCandidate($lead)) {
                Db::rollback();
                return ['code' => -200, 'msg' => '该客户当前已不符合自动流入公海规则', 'data' => []];
            }

            $now = date('Y-m-d H:i:s');
            $updateData = [
                'status' => 2,
            ];
            if ($this->hasColumnCached('crm_leads', 'to_gh_time')) {
                $updateData['to_gh_time'] = $now;
            }
            if ($this->hasColumnCached('crm_leads', 'pr_gh_type')) {
                $updateData['pr_gh_type'] = $ghTypeId;
            }
            if ($this->hasColumnCached('crm_leads', 'ut_time')) {
                $updateData['ut_time'] = $now;
            }

            $affected = Db::table('crm_leads')
                ->where('id', $leadId)
                ->where('status', 1)
                ->where('issuccess', '<>', 1)
                ->update($updateData);
            if ((int)$affected !== 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '客户状态已变化，请刷新后重试', 'data' => []];
            }

            $reasonInfo = $this->buildCandidateReason($leadId);
            $logReason = $this->buildAutoInReasonText((string)($reasonInfo['rule'] ?? ''));

            $insertLogData = $this->buildInLogInsertData($lead, $ghTypeId, $ghTypeName, $logReason, $now, $operatorId, $operatorName);
            if (empty($insertLogData)) {
                Db::rollback();
                return ['code' => -200, 'msg' => '流入日志字段不可用，无法写入记录', 'data' => []];
            }

            $insertRows = Db::table('crm_liberum_in_log')->insert($insertLogData);
            if ((int)$insertRows !== 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '写入流入记录失败', 'data' => []];
            }

            Db::commit();
            return [
                'code' => 0,
                'msg' => '确认流入公海成功',
                'data' => [
                    'lead_id' => $leadId,
                    'liberum_type' => $ghTypeId,
                ],
            ];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '确认流入公海失败：' . $e->getMessage(), 'data' => []];
        }
    }

    /**
     * 批量确认候选客户流入公海
     *
     * @param array $leadIds
     * @param array $operatorInfo
     * @return array
     */
    public function batchConfirmToLiberum($leadIds = [], $operatorInfo = [], $manualGhTypeId = 0)
    {
        $manualGhTypeId = (int)$manualGhTypeId;
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if ($manualGhTypeId <= 0) {
            return [
                'code' => -200,
                'msg' => '请选择需要流入的公海类型',
                'data' => [
                    'success_count' => 0,
                    'fail_count' => 0,
                    'fail_list' => [],
                ],
            ];
        }
        if (empty($leadIds)) {
            return [
                'code' => -200,
                'msg' => '请选择需要流入公海的客户',
                'data' => [
                    'success_count' => 0,
                    'fail_count' => 0,
                    'fail_list' => [],
                ],
            ];
        }

        $successCount = 0;
        $failCount = 0;
        $failList = [];

        foreach ($leadIds as $leadId) {
            $result = $this->confirmToLiberum($leadId, $operatorInfo, $manualGhTypeId);
            if ((int)($result['code'] ?? -200) === 0) {
                $successCount++;
                continue;
            }

            $failCount++;
            $failList[] = [
                'id' => $leadId,
                'msg' => (string)($result['msg'] ?? '处理失败'),
            ];
        }

        $msg = '批量处理完成：成功' . $successCount . '条，失败' . $failCount . '条';
        if ($successCount > 0) {
            return [
                'code' => 0,
                'msg' => $msg,
                'data' => [
                    'success_count' => $successCount,
                    'fail_count' => $failCount,
                    'fail_list' => $failList,
                ],
            ];
        }

        return [
            'code' => -200,
            'msg' => $msg,
            'data' => [
                'success_count' => $successCount,
                'fail_count' => $failCount,
                'fail_list' => $failList,
            ],
        ];
    }

    /**
     * 获取自动公海候选列表（分页 + 筛选）
     *
     * @param array $params 支持：page、limit、kh_name、phone、pr_user
     * @return array{count:int,data:array}
     */
    public function getCandidateList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = (int)($params['limit'] ?? 10);
        if ($limit <= 0) {
            $limit = 10;
        }

        // 每次查询先清空批次级缓存，避免脏数据串场。
        $this->leadMap = [];
        $this->followStatsMap = [];
        $this->candidateReasonMap = [];

        $totalStart = microtime(true);
        $batchSelectMs = 0;
        $scannedBatches = 0;
        $scannedLeadsCount = 0;
        $finalCandidateCount = 0;
        $dataCount = 0;
        $total = 0;
        $fastStop = false;
        try {
            $autoDays = (int)$this->getAutoLiberumDays();
            $deadline = date('Y-m-d H:i:s', time() - $autoDays * 86400);
            $khNameKeyword = trim((string)($params['kh_name'] ?? ($params['client_keyword'] ?? '')));
            $prUserKeyword = trim((string)($params['pr_user'] ?? ($params['pr_user_keyword'] ?? '')));
            $phoneKeyword = trim((string)($params['phone'] ?? ''));
            if ($prUserKeyword !== '' && ctype_digit($prUserKeyword)) {
                $adminName = Db::table('admin')
                    ->where('admin_id', (int)$prUserKeyword)
                    ->value('username');
                $adminName = trim((string)$adminName);
                if ($adminName !== '') {
                    $prUserKeyword = $adminName;
                }
            }
            $candidateRuleKeyword = trim((string)($params['candidate_rule'] ?? ''));
            $overdueDaysMin = isset($params['overdue_days']) && $params['overdue_days'] !== ''
                ? max(0, (int)$params['overdue_days'])
                : null;

            $baseQuery = Db::table('crm_leads')->alias('l')
                ->where('l.status', 1)
                ->where('l.issuccess', '<>', 1);

            if ($khNameKeyword !== '') {
                $baseQuery->whereLike('l.kh_name', '%' . $khNameKeyword . '%');
            }
            if ($prUserKeyword !== '') {
                $baseQuery->whereLike('l.pr_user', '%' . $prUserKeyword . '%');
            }
            $hasSearchCondition = ($khNameKeyword !== '' || $prUserKeyword !== '' || $phoneKeyword !== '');
            $batchSize = $hasSearchCondition ? 1000 : 2000;
            $needStart = ($page - 1) * $limit;
            $needEnd = $page * $limit;
            $candidateIndex = 0;
            $data = [];
            $lastId = 0;

            if ($phoneKeyword !== '') {
                $matchedLeadIds = $this->findLeadIdsByPhone($phoneKeyword);
                if (empty($matchedLeadIds)) {
                    $totalMs = (int)round((microtime(true) - $totalStart) * 1000);
                    Log::record(
                        '自动公海候选查询性能：page=' . $page
                        . ', limit=' . $limit
                        . ', batch_size=' . $batchSize
                        . ', scanned_batches=' . $scannedBatches
                        . ', scanned_leads_count=' . $scannedLeadsCount
                        . ', auto_liberum_days=' . $autoDays
                        . ', fast_stop=0'
                        . ', final_candidate_count=' . $finalCandidateCount
                        . ', data_count=' . $dataCount
                        . ', total_ms=' . $totalMs
                        . ', deadline=' . $deadline,
                        'info'
                    );
                    return ['count' => 0, 'data' => []];
                }
                $baseQuery->whereIn('l.id', $matchedLeadIds);
            }

            $validAtTimeSql = $this->buildValidDateTimeSql('l.at_time');
            $validToKhTimeSql = $this->buildValidDateTimeSql('l.to_kh_time');
            $invalidToKhTimeSql = $this->buildInvalidDateTimeSql('l.to_kh_time');
            $validLastUpTimeSql = $this->buildValidDateTimeSql('l.last_up_time');

            // SQL仅用于预筛选候选池；最终规则命中必须以 PHP evaluateCandidateRule()/isCandidate() 为准。
            $baseQuery->whereRaw(
                '('
                . '((' . $invalidToKhTimeSql . ') AND (' . $validAtTimeSql . ') AND l.at_time <= :deadline_at)'
                . ' OR '
                . '((' . $validToKhTimeSql . ') AND l.to_kh_time <= :deadline_to_kh)'
                . ' OR '
                . '((' . $validLastUpTimeSql . ') AND l.last_up_time <= :deadline_last_up)'
                . ')',
                [
                    'deadline_at' => $deadline,
                    'deadline_to_kh' => $deadline,
                    'deadline_last_up' => $deadline,
                ]
            );

            while (true) {
                $query = clone $baseQuery;
                if ($lastId > 0) {
                    $query->where('l.id', '<', $lastId);
                }

                $listStart = microtime(true);
                $leads = $query
                    ->field('l.id,l.kh_name,l.kh_contact,l.pr_user,l.at_time,l.to_kh_time,l.last_up_time,l.status,l.issuccess')
                    ->order('l.id', 'desc')
                    ->limit($batchSize)
                    ->select();
                $batchSelectMs += (int)round((microtime(true) - $listStart) * 1000);

                if (is_object($leads) && method_exists($leads, 'toArray')) {
                    $leads = $leads->toArray();
                } elseif (!is_array($leads)) {
                    $leads = [];
                }

                if (empty($leads)) {
                    break;
                }

                $scannedBatches++;
                $batchCount = count($leads);
                $scannedLeadsCount += $batchCount;
                $lastBatchLeadId = (int)($leads[$batchCount - 1]['id'] ?? 0);
                $lastId = $lastBatchLeadId > 0 ? $lastBatchLeadId : 0;

                $this->leadMap = [];
                $this->followStatsMap = [];
                $this->candidateReasonMap = [];

                $leadIds = [];
                foreach ($leads as $lead) {
                    $leadId = (int)($lead['id'] ?? 0);
                    if ($leadId <= 0) {
                        continue;
                    }
                    $leadIds[] = $leadId;
                    $this->leadMap[$leadId] = $lead;
                }
                if (empty($leadIds)) {
                    if ($lastId <= 0) {
                        break;
                    }
                    continue;
                }

                $this->followStatsMap = $this->buildFollowStatsMap($leadIds);

                foreach ($leadIds as $leadId) {
                    $lead = $this->leadMap[$leadId] ?? [];
                    $reasonInfo = $this->buildCandidateReason($leadId);
                    $candidateRule = (string)($reasonInfo['rule'] ?? '');
                    if ($candidateRule === '') {
                        continue;
                    }

                    $overdueDays = (int)($reasonInfo['overdue_days'] ?? 0);
                    if ($candidateRuleKeyword !== '' && !$this->containsText($candidateRule, $candidateRuleKeyword)) {
                        continue;
                    }
                    if ($overdueDaysMin !== null && $overdueDays < $overdueDaysMin) {
                        continue;
                    }

                    if ($candidateIndex >= $needStart && $candidateIndex < $needEnd && count($data) < $limit) {
                        $data[] = [
                            'id' => $leadId,
                            'kh_name' => (string)($lead['kh_name'] ?? ''),
                            'phone' => trim((string)($lead['kh_contact'] ?? '')),
                            'pr_user' => (string)($lead['pr_user'] ?? ''),
                            'create_time' => (string)($lead['at_time'] ?? ''),
                            'last_follow_time' => (string)$this->getLastFollowTime($leadId),
                            'overdue_days' => $overdueDays,
                            'candidate_rule' => $candidateRule,
                            'candidate_reason' => (string)($reasonInfo['reason'] ?? ''),
                            'status' => (int)($lead['status'] ?? 0),
                            'issuccess' => (int)($lead['issuccess'] ?? 0),
                        ];
                    }
                    $candidateIndex++;
                    if ($candidateIndex >= $needEnd && count($data) >= $limit) {
                        $fastStop = true;
                        break;
                    }
                }

                if ($fastStop) {
                    break;
                }
                if ($lastId <= 0) {
                    break;
                }
            }

            $finalCandidateCount = $candidateIndex;
            // 快速分页模式：提前停止时返回近似总数，保证前端可继续翻页。
            $total = $fastStop ? ($needEnd + 1) : $finalCandidateCount;

            if (!empty($data)) {
                $pageLeadIds = array_values(array_unique(array_map('intval', array_column($data, 'id'))));
                $phoneMap = $this->buildPhoneMap($pageLeadIds);
                foreach ($data as &$row) {
                    if (trim((string)($row['phone'] ?? '')) === '') {
                        $row['phone'] = (string)($phoneMap[(int)$row['id']] ?? '');
                    }
                }
                unset($row);
            }

            $dataCount = count($data);

            $totalMs = (int)round((microtime(true) - $totalStart) * 1000);
            Log::record(
                '自动公海候选查询性能：page=' . $page
                . ', limit=' . $limit
                . ', batch_size=' . $batchSize
                . ', scanned_batches=' . $scannedBatches
                . ', scanned_leads_count=' . $scannedLeadsCount
                . ', auto_liberum_days=' . $autoDays
                . ', batch_select_ms=' . $batchSelectMs
                . ', fast_stop=' . ($fastStop ? 1 : 0)
                . ', final_candidate_count=' . $finalCandidateCount
                . ', data_count=' . $dataCount
                . ', total_ms=' . $totalMs
                . ', deadline=' . $deadline,
                'info'
            );
            return [
                'count' => $total,
                'data' => $data,
            ];
        } catch (\Throwable $e) {
            Log::record('自动公海候选查询失败：' . $e->getMessage(), 'error');
            return ['count' => 0, 'data' => []];
        }
    }

    /**
     * 判断单个客户是否符合候选规则
     *
     * @param array $lead 客户数据（至少需要 id/create_time/status/issuccess）
     * @return bool
     */
    protected function isCandidate($lead)
    {
        $lead = is_array($lead) ? $lead : [];
        $leadId = (int)($lead['id'] ?? 0);
        if ($leadId <= 0) {
            return false;
        }

        // 规则5：成交客户永不进入候选。
        if ((int)($lead['issuccess'] ?? 0) === 1) {
            return false;
        }

        // 本阶段仅筛 status=1；这里再做一次防御性兜底。
        if ((int)($lead['status'] ?? 0) !== 1) {
            return false;
        }

        $ruleInfo = $this->evaluateCandidateRule($lead);
        return (string)($ruleInfo['rule'] ?? '') !== '';
    }

    /**
     * 生成候选原因
     *
     * @param int $leadId
     * @return array{rule:string,reason:string}
     */
    protected function buildCandidateReason($leadId)
    {
        $leadId = (int)$leadId;
        if ($leadId <= 0) {
            return ['rule' => '', 'reason' => ''];
        }
        if (isset($this->candidateReasonMap[$leadId])) {
            return $this->candidateReasonMap[$leadId];
        }

        $lead = $this->leadMap[$leadId] ?? Db::table('crm_leads')
            ->where('id', $leadId)
            ->field('id,at_time as create_time,at_time,to_kh_time,last_up_time,status,issuccess')
            ->find();
        if (!is_array($lead) || empty($lead)) {
            return ['rule' => '', 'reason' => ''];
        }
        if (
            (!isset($lead['create_time']) || trim((string)$lead['create_time']) === '')
            && isset($lead['at_time'])
            && trim((string)$lead['at_time']) !== ''
        ) {
            $lead['create_time'] = $lead['at_time'];
        }

        if ((int)($lead['issuccess'] ?? 0) === 1) {
            $this->candidateReasonMap[$leadId] = ['rule' => '', 'reason' => '成交客户不进入候选'];
            return $this->candidateReasonMap[$leadId];
        }

        $this->candidateReasonMap[$leadId] = $this->evaluateCandidateRule($lead);
        return $this->candidateReasonMap[$leadId];
    }

    /**
     * 获取最后跟进时间（Y-m-d H:i:s）
     *
     * @param int $leadId
     * @return string
     */
    protected function getLastFollowTime($leadId)
    {
        $leadId = (int)$leadId;
        if ($leadId <= 0) {
            return '';
        }

        if (isset($this->followStatsMap[$leadId])) {
            return (string)($this->followStatsMap[$leadId]['last_follow_time'] ?? '');
        }

        if (isset($this->leadMap[$leadId])) {
            $lead = is_array($this->leadMap[$leadId]) ? $this->leadMap[$leadId] : [];
            $startInfo = $this->getRuleStartPoint($lead);
            $startTs = (int)($startInfo['start_ts'] ?? 0);
            $lastUpTime = $lead['last_up_time'] ?? '';
            $lastUpTs = $this->isValidDateTime($lastUpTime) ? $this->normalizeTimeToTimestamp($lastUpTime) : 0;
            if ($lastUpTs > 0 && ($startTs <= 0 || $lastUpTs >= $startTs)) {
                return date('Y-m-d H:i:s', $lastUpTs);
            }
        }

        $lastTs = Db::table('crm_comment')
            ->where('leads_id', $leadId)
            ->max('create_date');
        $lastTs = $this->normalizeTimeToTimestamp($lastTs);
        return $lastTs > 0 ? date('Y-m-d H:i:s', $lastTs) : '';
    }

    /**
     * 计算超期天数
     *
     * @param mixed $time 时间字符串或时间戳
     * @return int
     */
    protected function getOverdueDays($time)
    {
        $ts = $this->normalizeTimeToTimestamp($time);
        if ($ts <= 0) {
            return 0;
        }

        $diff = time() - $ts;
        if ($diff <= 0) {
            return 0;
        }

        return (int)floor($diff / 86400);
    }

    /**
     * 批量构建跟进统计（单次批量查询 + PHP 线性计算）
     *
     * @param array $leadIds
     * @return array<int,array>
     */
    protected function buildFollowStatsMap($leadIds = [])
    {
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $statsMap = [];
        foreach ($leadIds as $leadId) {
            $statsMap[$leadId] = [
                'follow_count' => 0,
                'first_follow_ts' => 0,
                'last_follow_ts' => 0,
                'first_follow_time' => '',
                'last_follow_time' => '',
                'max_gap_seconds' => 0,
                'max_gap_days' => 0,
                'max_gap_prev_ts' => 0,
                'max_gap_curr_ts' => 0,
            ];
        }
        $startMap = $this->buildStartPointMap($leadIds);

        $chunks = array_chunk($leadIds, 500);
        foreach ($chunks as $chunk) {
            if (empty($chunk)) {
                continue;
            }

            $records = Db::table('crm_comment')
                ->field('leads_id,create_date')
                ->whereIn('leads_id', $chunk)
                ->order('leads_id asc, create_date asc')
                ->select();
            if (is_object($records) && method_exists($records, 'toArray')) {
                $records = $records->toArray();
            } elseif (!is_array($records)) {
                $records = [];
            }
            if (empty($records)) {
                continue;
            }

            $prevFollowTsMap = [];
            foreach ($records as $row) {
                $leadId = (int)($row['leads_id'] ?? 0);
                if ($leadId <= 0 || !isset($statsMap[$leadId])) {
                    continue;
                }

                $ts = $this->normalizeTimeToTimestamp($row['create_date'] ?? '');
                if ($ts <= 0) {
                    continue;
                }
                $startTs = (int)($startMap[$leadId]['start_ts'] ?? 0);
                $source = (string)($startMap[$leadId]['source'] ?? 'created');
                if ($source === 'picked' && $startTs > 0 && $ts < $startTs) {
                    continue;
                }

                if ($statsMap[$leadId]['follow_count'] === 0) {
                    $statsMap[$leadId]['first_follow_ts'] = $ts;
                    $statsMap[$leadId]['first_follow_time'] = date('Y-m-d H:i:s', $ts);
                } else {
                    $prevTs = isset($prevFollowTsMap[$leadId]) ? (int)$prevFollowTsMap[$leadId] : 0;
                    if ($prevTs > 0 && $ts > $prevTs) {
                        $gapSeconds = $ts - $prevTs;
                        if ($gapSeconds > (int)$statsMap[$leadId]['max_gap_seconds']) {
                            $statsMap[$leadId]['max_gap_seconds'] = $gapSeconds;
                            $statsMap[$leadId]['max_gap_days'] = (int)floor($gapSeconds / 86400);
                            $statsMap[$leadId]['max_gap_prev_ts'] = $prevTs;
                            $statsMap[$leadId]['max_gap_curr_ts'] = $ts;
                        }
                    }
                }

                $statsMap[$leadId]['follow_count']++;
                $statsMap[$leadId]['last_follow_ts'] = $ts;
                $statsMap[$leadId]['last_follow_time'] = date('Y-m-d H:i:s', $ts);
                $prevFollowTsMap[$leadId] = $ts;
            }
        }

        return $statsMap;
    }

    /**
     * 获取单个客户跟进统计（优先缓存，未命中再单查）
     *
     * @param int $leadId
     * @return array
     */
    protected function getLeadFollowStats($leadId)
    {
        $leadId = (int)$leadId;
        if ($leadId <= 0) {
            return [
                'follow_count' => 0,
                'first_follow_ts' => 0,
                'last_follow_ts' => 0,
                'first_follow_time' => '',
                'last_follow_time' => '',
                'max_gap_seconds' => 0,
                'max_gap_days' => 0,
                'max_gap_prev_ts' => 0,
                'max_gap_curr_ts' => 0,
            ];
        }

        if (isset($this->followStatsMap[$leadId])) {
            return $this->followStatsMap[$leadId];
        }

        $this->followStatsMap = $this->buildFollowStatsMap([$leadId]) + $this->followStatsMap;
        if (isset($this->followStatsMap[$leadId])) {
            return $this->followStatsMap[$leadId];
        }

        return [
            'follow_count' => 0,
            'first_follow_ts' => 0,
            'last_follow_ts' => 0,
            'first_follow_time' => '',
            'last_follow_time' => '',
            'max_gap_seconds' => 0,
            'max_gap_days' => 0,
            'max_gap_prev_ts' => 0,
            'max_gap_curr_ts' => 0,
        ];
    }

    /**
     * 兼容命名：获取单客户跟进统计
     *
     * @param int $leadId
     * @return array
     */
    protected function getFollowStats($leadId)
    {
        return $this->getLeadFollowStats($leadId);
    }

    /**
     * 构建手机号映射（复用现有公海模块口径）
     *
     * @param array $leadIds
     * @return array<int,string>
     */
    protected function buildPhoneMap($leadIds = [])
    {
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $result = [];
        $needQueryLeadIds = [];
        foreach ($leadIds as $leadId) {
            if (array_key_exists($leadId, self::$phoneMapCache)) {
                $result[$leadId] = (string)self::$phoneMapCache[$leadId];
            } else {
                $needQueryLeadIds[] = $leadId;
            }
        }

        if (empty($needQueryLeadIds)) {
            return $result;
        }

        $hasContactType = $this->hasColumnCached('crm_contacts', 'contact_type');
        $phoneType = (int)ContactMap::CONTACT_MAP['phone'];
        $firstMap = [];
        $phoneMap = [];
        $chunks = array_chunk($needQueryLeadIds, 500);
        foreach ($chunks as $chunk) {
            if (empty($chunk)) {
                continue;
            }

            $contacts = Db::table('crm_contacts')
                ->field('leads_id,contact_value' . ($hasContactType ? ',contact_type' : ''))
                ->whereIn('leads_id', $chunk)
                ->order('id asc')
                ->select();
            if (is_object($contacts) && method_exists($contacts, 'toArray')) {
                $contacts = $contacts->toArray();
            } elseif (!is_array($contacts)) {
                $contacts = [];
            }

            foreach ($contacts as $contact) {
                $leadId = (int)($contact['leads_id'] ?? 0);
                $contactValue = trim((string)($contact['contact_value'] ?? ''));
                if ($leadId <= 0 || $contactValue === '') {
                    continue;
                }
                if (!isset($firstMap[$leadId])) {
                    $firstMap[$leadId] = $contactValue;
                }
                if ($hasContactType && (int)($contact['contact_type'] ?? -1) === $phoneType && !isset($phoneMap[$leadId])) {
                    $phoneMap[$leadId] = $contactValue;
                }
            }
        }

        foreach ($needQueryLeadIds as $leadId) {
            if (isset($phoneMap[$leadId])) {
                self::$phoneMapCache[$leadId] = $phoneMap[$leadId];
            } elseif (isset($firstMap[$leadId])) {
                self::$phoneMapCache[$leadId] = $firstMap[$leadId];
            } else {
                self::$phoneMapCache[$leadId] = '';
            }
            $result[$leadId] = (string)self::$phoneMapCache[$leadId];
        }

        return $result;
    }

    /**
     * 根据手机号反查客户ID（复用现有口径）
     *
     * @param string $phone
     * @return array<int>
     */
    protected function findLeadIdsByPhone($phone = '')
    {
        $phone = strtolower(trim((string)$phone));
        if ($phone === '') {
            return [];
        }

        if (array_key_exists($phone, self::$phoneSearchCache)) {
            return self::$phoneSearchCache[$phone];
        }

        $leadIds = Db::table('crm_contacts')
            ->whereLike('contact_value', '%' . $phone . '%')
            ->column('DISTINCT leads_id');
        if (!is_array($leadIds)) {
            $leadIds = [];
        }

        $result = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));

        self::$phoneSearchCache[$phone] = $result;
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

        $ts = strtotime($str);
        return $ts === false ? 0 : (int)$ts;
    }

    /**
     * 判断 datetime 字段是否为空值（兼容历史脏值）
     *
     * @param mixed $value
     * @return bool
     */
    protected function isInvalidDateTime($value)
    {
        return !$this->isValidDateTime($value);
    }

    /**
     * 判断时间是否有效（非空/非零值/可解析）
     *
     * @param mixed $value
     * @return bool
     */
    protected function isValidDateTime($value)
    {
        $str = trim((string)$value);
        if ($str === '' || strtolower($str) === 'null' || strtolower($str) === 'undefined') {
            return false;
        }
        if ($str === '0000-00-00 00:00:00') {
            return false;
        }
        return $this->normalizeTimeToTimestamp($str) > 0;
    }

    /**
     * 构造 SQL 有效时间表达式（用于预筛选）
     *
     * @param string $field
     * @return string
     */
    protected function buildValidDateTimeSql($field)
    {
        $field = trim((string)$field);
        if ($field === '') {
            return '(1=0)';
        }
        return '(' . $field . " IS NOT NULL AND " . $field . " <> '' AND " . $field . " <> '0000-00-00 00:00:00')";
    }

    /**
     * 构造 SQL 无效时间表达式（用于预筛选）
     *
     * @param string $field
     * @return string
     */
    protected function buildInvalidDateTimeSql($field)
    {
        $field = trim((string)$field);
        if ($field === '') {
            return '(1=1)';
        }
        return '(' . $field . " IS NULL OR " . $field . " = '' OR " . $field . " = '0000-00-00 00:00:00')";
    }

    /**
     * 获取自动规则起点
     *
     * @param array $lead
     * @return array{source:string,start_time:string,start_ts:int}
     */
    protected function getRuleStartPoint($lead)
    {
        $lead = is_array($lead) ? $lead : [];
        $pickedTime = $lead['to_kh_time'] ?? '';
        if ($this->isValidDateTime($pickedTime)) {
            return [
                'source' => 'picked',
                'start_time' => date('Y-m-d H:i:s', $this->normalizeTimeToTimestamp($pickedTime)),
                'start_ts' => $this->normalizeTimeToTimestamp($pickedTime),
            ];
        }

        $createdTime = $lead['at_time'] ?? ($lead['create_time'] ?? '');
        if ($this->isValidDateTime($createdTime)) {
            return [
                'source' => 'created',
                'start_time' => date('Y-m-d H:i:s', $this->normalizeTimeToTimestamp($createdTime)),
                'start_ts' => $this->normalizeTimeToTimestamp($createdTime),
            ];
        }

        return [
            'source' => 'created',
            'start_time' => '',
            'start_ts' => 0,
        ];
    }

    /**
     * 批量构建规则起点映射
     *
     * @param array $leadIds
     * @return array<int,array>
     */
    protected function buildStartPointMap($leadIds = [])
    {
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $result = [];
        $needQueryIds = [];
        foreach ($leadIds as $leadId) {
            if (isset($this->leadMap[$leadId]) && is_array($this->leadMap[$leadId])) {
                $result[$leadId] = $this->getRuleStartPoint($this->leadMap[$leadId]);
            } else {
                $needQueryIds[] = $leadId;
            }
        }

        if (!empty($needQueryIds)) {
            $rows = Db::table('crm_leads')
                ->field('id,at_time,to_kh_time')
                ->whereIn('id', $needQueryIds)
                ->select();
            if (is_object($rows) && method_exists($rows, 'toArray')) {
                $rows = $rows->toArray();
            } elseif (!is_array($rows)) {
                $rows = [];
            }

            foreach ($rows as $row) {
                $leadId = (int)($row['id'] ?? 0);
                if ($leadId <= 0) {
                    continue;
                }
                $result[$leadId] = $this->getRuleStartPoint($row);
            }
        }

        foreach ($leadIds as $leadId) {
            if (!isset($result[$leadId])) {
                $result[$leadId] = ['source' => 'created', 'start_time' => '', 'start_ts' => 0];
            }
        }

        return $result;
    }

    /**
     * 统一评估候选规则
     *
     * @param array $lead
     * @return array{rule:string,reason:string,overdue_days:int,overdue_seconds:int}
     */
    protected function evaluateCandidateRule($lead)
    {
        $lead = is_array($lead) ? $lead : [];
        $leadId = (int)($lead['id'] ?? 0);
        if ($leadId <= 0) {
            return ['rule' => '', 'reason' => '', 'overdue_days' => 0, 'overdue_seconds' => 0];
        }
        if ((int)($lead['issuccess'] ?? 0) === 1 || (int)($lead['status'] ?? 0) !== 1) {
            return ['rule' => '', 'reason' => '', 'overdue_days' => 0, 'overdue_seconds' => 0];
        }

        $days = (int)$this->getAutoLiberumDays();
        $thresholdSeconds = $days * 86400;
        $now = time();
        $startInfo = $this->getRuleStartPoint($lead);
        $startTs = (int)($startInfo['start_ts'] ?? 0);
        $source = (string)($startInfo['source'] ?? 'created');
        if ($startTs <= 0) {
            return ['rule' => '', 'reason' => '', 'overdue_days' => 0, 'overdue_seconds' => 0];
        }

        $stats = $this->getFollowStats($leadId);
        $followCount = (int)($stats['follow_count'] ?? 0);
        $firstTs = (int)($stats['first_follow_ts'] ?? 0);
        $lastTs = (int)($stats['last_follow_ts'] ?? 0);
        $maxGapSeconds = (int)($stats['max_gap_seconds'] ?? 0);

        if ($followCount <= 0) {
            $diff = $now - $startTs;
            if ($diff > $thresholdSeconds) {
                if ($source === 'picked') {
                    return [
                        'rule' => '领取后未跟进超时',
                        'reason' => '领取后超过' . $days . '天未跟进',
                        'overdue_days' => (int)floor($diff / 86400),
                        'overdue_seconds' => $diff,
                    ];
                }

                return [
                    'rule' => '创建未跟进超时',
                    'reason' => '客户创建后超过' . $days . '天未跟进',
                    'overdue_days' => (int)floor($diff / 86400),
                    'overdue_seconds' => $diff,
                ];
            }
        }

        if ($firstTs > 0) {
            $firstGap = $firstTs - $startTs;
            if ($firstGap > $thresholdSeconds) {
                if ($source === 'picked') {
                    return [
                        'rule' => '领取后首次跟进超时',
                        'reason' => '领取后首次跟进超过' . $days . '天',
                        'overdue_days' => (int)floor($firstGap / 86400),
                        'overdue_seconds' => $firstGap,
                    ];
                }

                return [
                    'rule' => '首次跟进超时',
                    'reason' => '首次跟进距离创建时间超过' . $days . '天',
                    'overdue_days' => (int)floor($firstGap / 86400),
                    'overdue_seconds' => $firstGap,
                ];
            }
        }

        if ($maxGapSeconds > $thresholdSeconds) {
            return [
                'rule' => '跟进间隔超时',
                'reason' => '存在相邻两次跟进间隔超过' . $days . '天',
                'overdue_days' => (int)floor($maxGapSeconds / 86400),
                'overdue_seconds' => $maxGapSeconds,
            ];
        }

        if ($lastTs > 0) {
            $lastGap = $now - $lastTs;
            if ($lastGap > $thresholdSeconds) {
                return [
                    'rule' => '最后跟进超时',
                    'reason' => '最后一次跟进距今超过' . $days . '天',
                    'overdue_days' => (int)floor($lastGap / 86400),
                    'overdue_seconds' => $lastGap,
                ];
            }
        }

        return ['rule' => '', 'reason' => '', 'overdue_days' => 0, 'overdue_seconds' => 0];
    }

    /**
     * 文本包含匹配（兼容 mbstring）
     *
     * @param string $haystack
     * @param string $needle
     * @return bool
     */
    protected function containsText($haystack, $needle)
    {
        $haystack = (string)$haystack;
        $needle = (string)$needle;
        if ($needle === '') {
            return true;
        }
        if (function_exists('mb_strpos')) {
            return mb_strpos($haystack, $needle) !== false;
        }
        return strpos($haystack, $needle) !== false;
    }

    /**
     * 组装自动流入原因文本
     *
     * @param string $rule
     * @return string
     */
    protected function buildAutoInReasonText($rule = '')
    {
        $days = (int)$this->getAutoLiberumDays();
        if ($rule === '领取后未跟进超时') {
            return '自动流入公海：领取后' . $days . '天未跟进';
        }
        if ($rule === '领取后首次跟进超时') {
            return '自动流入公海：领取后首次跟进超过' . $days . '天';
        }
        if ($rule === '创建未跟进超时') {
            return '自动流入公海：创建' . $days . '天未跟进';
        }
        if ($rule === '首次跟进超时') {
            return '自动流入公海：首次跟进超过' . $days . '天';
        }
        if ($rule === '跟进间隔超时') {
            return '自动流入公海：跟进间隔超过' . $days . '天';
        }
        if ($rule === '最后跟进超时') {
            return '自动流入公海：最后跟进超过' . $days . '天';
        }

        return '自动流入公海：命中自动规则';
    }

    /**
     * 按字段存在性兼容组装流入日志数据
     *
     * @param array $lead
     * @param int $ghTypeId
     * @param string $reason
     * @param string $inTime
     * @param int $operatorId
     * @param string $operatorName
     * @return array
     */
    protected function buildInLogInsertData(array $lead, $ghTypeId, $ghTypeName, $reason, $inTime, $operatorId, $operatorName)
    {
        $leadId = (int)($lead['id'] ?? 0);
        $khName = (string)($lead['kh_name'] ?? '');
        $beforePrUser = (string)($lead['pr_user'] ?? '');
        $ghTypeName = trim((string)$ghTypeName);
        $nowTimestamp = time();

        $data = [];
        if ($this->hasColumnCached('crm_liberum_in_log', 'leads_id')) {
            $data['leads_id'] = $leadId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'kh_name')) {
            $data['kh_name'] = $khName;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'before_pr_user')) {
            $data['before_pr_user'] = $beforePrUser;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'reason')) {
            $data['reason'] = $reason;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'in_time')) {
            $data['in_time'] = $inTime;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'operator_id')) {
            $data['operator_id'] = (int)$operatorId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'operator_name')) {
            $data['operator_name'] = $operatorName;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'source_type')) {
            $data['source_type'] = 'auto_rule';
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'liberum_type')) {
            $data['liberum_type'] = (int)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'in_gh_type')) {
            $data['in_gh_type'] = (int)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'in_gh_type_name')) {
            $data['in_gh_type_name'] = $ghTypeName !== '' ? $ghTypeName : (string)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'current_gh_type')) {
            $data['current_gh_type'] = (int)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'current_gh_type_name')) {
            $data['current_gh_type_name'] = $ghTypeName !== '' ? $ghTypeName : (string)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'gh_type_id')) {
            $data['gh_type_id'] = (int)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'gh_type')) {
            $data['gh_type'] = $ghTypeName !== '' ? $ghTypeName : (string)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'pr_gh_type')) {
            $data['pr_gh_type'] = (int)$ghTypeId;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'is_recovered')) {
            $data['is_recovered'] = 0;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'is_deleted')) {
            $data['is_deleted'] = 0;
        }
        if ($this->hasColumnCached('crm_liberum_in_log', 'create_time')) {
            $data['create_time'] = $nowTimestamp;
        }

        return $data;
    }

    /**
     * 字段探测缓存包装
     *
     * @param string $table
     * @param string $column
     * @return bool
     */
    protected function hasColumnCached($table, $column)
    {
        $table = trim((string)$table);
        $column = trim((string)$column);
        if ($table === '' || $column === '') {
            return false;
        }

        if (!isset(self::$columnExistsCache[$table])) {
            self::$columnExistsCache[$table] = [];
        }

        if (!array_key_exists($column, self::$columnExistsCache[$table])) {
            self::$columnExistsCache[$table][$column] = (bool)$this->hasColumn($table, $column);
        }

        return (bool)self::$columnExistsCache[$table][$column];
    }
}
