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
     * 默认公海类型ID缓存
     *
     * @var int|null
     */
    protected static $defaultLiberumTypeIdCache = null;

    /**
     * 确认单个候选客户流入公海
     *
     * @param int $leadId
     * @param array $operatorInfo
     * @return array
     */
    public function confirmToLiberum($leadId, $operatorInfo = [])
    {
        $leadId = (int)$leadId;
        $operatorInfo = is_array($operatorInfo) ? $operatorInfo : [];
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));

        if ($leadId <= 0) {
            return ['code' => -200, 'msg' => '参数错误：客户ID无效', 'data' => []];
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

            $ghTypeId = $this->getDefaultLiberumTypeId();
            if ($ghTypeId <= 0) {
                Db::rollback();
                return ['code' => -200, 'msg' => '请先维护公海类型', 'data' => []];
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

            $insertLogData = $this->buildInLogInsertData($lead, $ghTypeId, $logReason, $now, $operatorId, $operatorName);
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
    public function batchConfirmToLiberum($leadIds = [], $operatorInfo = [])
    {
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
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
            $result = $this->confirmToLiberum($leadId, $operatorInfo);
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
        $limit = max(1, (int)($params['limit'] ?? 10));

        // 每次查询先清空批次级缓存，避免脏数据串场。
        $this->leadMap = [];
        $this->followStatsMap = [];
        $this->candidateReasonMap = [];

        try {
            $khNameKeyword = trim((string)($params['kh_name'] ?? ($params['client_keyword'] ?? '')));
            $prUserKeyword = trim((string)($params['pr_user'] ?? ($params['pr_user_keyword'] ?? '')));
            $candidateRuleKeyword = trim((string)($params['candidate_rule'] ?? ''));
            $overdueDaysMin = isset($params['overdue_days']) && $params['overdue_days'] !== ''
                ? max(0, (int)$params['overdue_days'])
                : null;

            $query = Db::table('crm_leads')->alias('l')
                ->where('l.status', 1)
                ->where('l.issuccess', '<>', 1);

            if ($khNameKeyword !== '') {
                $query->whereLike('l.kh_name', '%' . $khNameKeyword . '%');
            }
            if ($prUserKeyword !== '') {
                $query->whereLike('l.pr_user', '%' . $prUserKeyword . '%');
            }
            if (!empty($params['phone'])) {
                $matchedLeadIds = $this->findLeadIdsByPhone((string)$params['phone']);
                if (empty($matchedLeadIds)) {
                    return ['count' => 0, 'data' => []];
                }
                $query->whereIn('l.id', $matchedLeadIds);
            }

            $leads = $query
                ->field('l.id,l.kh_name,l.pr_user,l.at_time as create_time,l.status,l.issuccess')
                ->order('l.id asc')
                ->select();
            if (is_object($leads) && method_exists($leads, 'toArray')) {
                $leads = $leads->toArray();
            } elseif (!is_array($leads)) {
                $leads = [];
            }
            if (empty($leads)) {
                return ['count' => 0, 'data' => []];
            }

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
                return ['count' => 0, 'data' => []];
            }

            // 批量加载跟进统计，避免循环查库。
            $this->followStatsMap = $this->buildFollowStatsMap($leadIds);

            $candidateRows = [];
            foreach ($leads as $lead) {
                if (!$this->isCandidate($lead)) {
                    continue;
                }

                $leadId = (int)$lead['id'];
                $reasonInfo = $this->buildCandidateReason($leadId);
                $lastFollowTime = $this->getLastFollowTime($leadId);
                $overdueBaseTime = $lastFollowTime !== '' ? $lastFollowTime : (string)($lead['create_time'] ?? '');

                $candidateRows[] = [
                    'id' => $leadId,
                    'kh_name' => (string)($lead['kh_name'] ?? ''),
                    'phone' => '',
                    'pr_user' => (string)($lead['pr_user'] ?? ''),
                    'create_time' => (string)($lead['create_time'] ?? ''),
                    'last_follow_time' => $lastFollowTime,
                    'overdue_days' => $this->getOverdueDays($overdueBaseTime),
                    'candidate_rule' => (string)($reasonInfo['rule'] ?? ''),
                    'candidate_reason' => (string)($reasonInfo['reason'] ?? ''),
                    'status' => (int)($lead['status'] ?? 0),
                    'issuccess' => (int)($lead['issuccess'] ?? 0),
                ];
            }

            if ($candidateRuleKeyword !== '') {
                $candidateRows = array_values(array_filter($candidateRows, function ($row) use ($candidateRuleKeyword) {
                    $rule = (string)($row['candidate_rule'] ?? '');
                    if ($rule === '') {
                        return false;
                    }
                    if (function_exists('mb_strpos')) {
                        return mb_strpos($rule, $candidateRuleKeyword) !== false;
                    }
                    return strpos($rule, $candidateRuleKeyword) !== false;
                }));
            }
            if ($overdueDaysMin !== null) {
                $candidateRows = array_values(array_filter($candidateRows, function ($row) use ($overdueDaysMin) {
                    return (int)($row['overdue_days'] ?? 0) >= $overdueDaysMin;
                }));
            }

            if (!empty($candidateRows)) {
                usort($candidateRows, function ($a, $b) {
                    $aOver = (int)($a['overdue_days'] ?? 0);
                    $bOver = (int)($b['overdue_days'] ?? 0);
                    if ($aOver !== $bOver) {
                        return $bOver <=> $aOver;
                    }

                    return (int)($b['id'] ?? 0) <=> (int)($a['id'] ?? 0);
                });
            }

            $total = count($candidateRows);
            if ($total === 0) {
                return ['count' => 0, 'data' => []];
            }

            // 分页切片（候选规则需先完整计算后分页，保证口径正确）。
            $offset = ($page - 1) * $limit;
            $pageData = array_slice($candidateRows, $offset, $limit);

            if (!empty($pageData)) {
                $pageLeadIds = array_values(array_unique(array_map(function ($row) {
                    return (int)($row['id'] ?? 0);
                }, $pageData)));
                $phoneMap = $this->buildPhoneMap($pageLeadIds);
                foreach ($pageData as &$row) {
                    $row['phone'] = $phoneMap[(int)$row['id']] ?? '';
                }
                unset($row);
            }

            return [
                'count' => $total,
                'data' => $pageData,
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

        $createdTs = $this->normalizeTimeToTimestamp($lead['create_time'] ?? '');
        if ($createdTs <= 0) {
            return false;
        }

        $stats = $this->getLeadFollowStats($leadId);
        $thresholdSeconds = (int)$this->getAutoLiberumDays() * 86400;
        $now = time();

        $followCount = (int)($stats['follow_count'] ?? 0);
        $firstTs = (int)($stats['first_follow_ts'] ?? 0);
        $lastTs = (int)($stats['last_follow_ts'] ?? 0);
        $maxGapSeconds = (int)($stats['max_gap_seconds'] ?? 0);

        // 规则1：创建超过 N 天，无跟进。
        if ($followCount <= 0 && ($now - $createdTs) > $thresholdSeconds) {
            return true;
        }

        // 规则2：首次跟进距离创建超过 N 天。
        if ($firstTs > 0 && ($firstTs - $createdTs) > $thresholdSeconds) {
            return true;
        }

        // 规则3：任意相邻两次跟进间隔超过 N 天。
        if ($maxGapSeconds > $thresholdSeconds) {
            return true;
        }

        // 规则4：最后一次跟进距今超过 N 天。
        if ($lastTs > 0 && ($now - $lastTs) > $thresholdSeconds) {
            return true;
        }

        return false;
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
            ->field('id,at_time as create_time,status,issuccess')
            ->find();
        if (!is_array($lead) || empty($lead)) {
            return ['rule' => '', 'reason' => ''];
        }

        if ((int)($lead['issuccess'] ?? 0) === 1) {
            $this->candidateReasonMap[$leadId] = ['rule' => '', 'reason' => '成交客户不进入候选'];
            return $this->candidateReasonMap[$leadId];
        }

        $stats = $this->getLeadFollowStats($leadId);
        $createdTs = $this->normalizeTimeToTimestamp($lead['create_time'] ?? '');
        $thresholdSeconds = (int)$this->getAutoLiberumDays() * 86400;
        $daysText = (int)$this->getAutoLiberumDays();
        $now = time();

        $followCount = (int)($stats['follow_count'] ?? 0);
        $firstTs = (int)($stats['first_follow_ts'] ?? 0);
        $lastTs = (int)($stats['last_follow_ts'] ?? 0);
        $maxGapSeconds = (int)($stats['max_gap_seconds'] ?? 0);

        if ($followCount <= 0 && $createdTs > 0 && ($now - $createdTs) > $thresholdSeconds) {
            $this->candidateReasonMap[$leadId] = [
                'rule' => '创建未跟进超时',
                'reason' => '客户创建后已超过' . $daysText . '天且无任何跟进记录',
            ];
            return $this->candidateReasonMap[$leadId];
        }

        if ($firstTs > 0 && $createdTs > 0 && ($firstTs - $createdTs) > $thresholdSeconds) {
            $this->candidateReasonMap[$leadId] = [
                'rule' => '首次跟进超时',
                'reason' => '首次跟进距离客户创建时间已超过' . $daysText . '天',
            ];
            return $this->candidateReasonMap[$leadId];
        }

        if ($maxGapSeconds > $thresholdSeconds) {
            $this->candidateReasonMap[$leadId] = [
                'rule' => '跟进间隔超时',
                'reason' => '存在两次相邻跟进记录间隔超过' . $daysText . '天',
            ];
            return $this->candidateReasonMap[$leadId];
        }

        if ($lastTs > 0 && ($now - $lastTs) > $thresholdSeconds) {
            $this->candidateReasonMap[$leadId] = [
                'rule' => '最后跟进超时',
                'reason' => '最后一次跟进距离当前已经超过' . $daysText . '天',
            ];
            return $this->candidateReasonMap[$leadId];
        }

        $this->candidateReasonMap[$leadId] = ['rule' => '', 'reason' => ''];
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
            ];
        }

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
        ];
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
     * 获取默认公海类型（is_deleted=0 且 id 最小）
     *
     * @return int
     */
    protected function getDefaultLiberumTypeId()
    {
        if (self::$defaultLiberumTypeIdCache !== null) {
            return (int)self::$defaultLiberumTypeIdCache;
        }

        $row = Db::table('crm_liberum_type')
            ->where('is_deleted', 0)
            ->order('id asc')
            ->field('id')
            ->find();

        self::$defaultLiberumTypeIdCache = (int)($row['id'] ?? 0);

        return (int)self::$defaultLiberumTypeIdCache;
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
    protected function buildInLogInsertData(array $lead, $ghTypeId, $reason, $inTime, $operatorId, $operatorName)
    {
        $leadId = (int)($lead['id'] ?? 0);
        $khName = (string)($lead['kh_name'] ?? '');
        $beforePrUser = (string)($lead['pr_user'] ?? '');
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
