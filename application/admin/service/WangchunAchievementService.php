<?php

namespace app\admin\service;

use think\Db;

/**
 * 旺春业绩看板：订单聚合与 heartbeat stamp（配置见 {@see AchievementConfigService}）
 */
class WangchunAchievementService
{
    /** @var AchievementConfigService */
    private $configService;

    public function __construct(AchievementConfigService $configService = null)
    {
        $this->configService = $configService ?: new AchievementConfigService();
    }

    /**
     * 按配置 key 拉取完整配置并汇总（PK 小组 / 现有团队 的名单已在 {@see AchievementConfigService::getConfig} 中写入 pkGroups）。
     *
     * @param string $key AchievementConfigService::KEY_* 之一
     * @return array{groupAvgRankList:array, memberRankGroupList:array, globalMemberRankList:array}
     */
    public function buildAchievementData($key)
    {
        $c = $this->configService->getConfig($key);

        return $this->buildAchievementDataByGroups($c['pkGroups'], $c);
    }

    /**
     * 通用：按小组名单 + 单页配置汇总业绩（PK 小组名单、现有团队名单均由调用方通过 $groups 传入，配置来自 AchievementConfigService::getConfig）。
     *
     * @param array $groups 小组名 => 成员列表（PK 对应 pkGroupsTemporary，现有团队对应 pkGroupsPermanent）
     * @param array<string,mixed> $config {@see AchievementConfigService::getConfig} 返回结构（含 startTime、endTime、orderTimeField、排序等）
     * @return array{groupAvgRankList:array, memberRankGroupList:array, globalMemberRankList:array}
     */
    public function buildAchievementDataByGroups(array $groups, array $config)
    {
        return $this->buildAchievementDataForGroupsBody($groups, $config);
    }

    /**
     * temporary 系列页面/接口专用：stamp 仅允许 temporary 配置，与 permanent 完全隔离（不可复用对方 $config）。
     *
     * @param array<string,mixed> $config {@see AchievementConfigService::getConfig}
     * @return string
     */
    public function getTemporaryAchievementStampByConfig(array $config)
    {
        if (($config['series'] ?? '') !== AchievementConfigService::SERIES_TEMPORARY) {
            throw new \InvalidArgumentException('Temporary stamp/heartbeat must use temporary_* config (series mismatch).');
        }

        return $this->computeAchievementStampFromFullConfig($config);
    }

    /**
     * permanent 系列页面/接口专用：stamp 仅允许 permanent 配置，与 temporary 完全隔离。
     *
     * @param array<string,mixed> $config {@see AchievementConfigService::getConfig}
     * @return string
     */
    public function getPermanentAchievementStampByConfig(array $config)
    {
        if (($config['series'] ?? '') !== AchievementConfigService::SERIES_PERMANENT) {
            throw new \InvalidArgumentException('Permanent stamp/heartbeat must use permanent_* config (series mismatch).');
        }

        return $this->computeAchievementStampFromFullConfig($config);
    }

    /**
     * @param string $key AchievementConfigService::KEY_* 之一
     * @return string
     */
    public function getAchievementStamp($key)
    {
        $c = $this->configService->getConfig($key);
        if (($c['series'] ?? '') === AchievementConfigService::SERIES_TEMPORARY) {
            return $this->getTemporaryAchievementStampByConfig($c);
        }

        return $this->getPermanentAchievementStampByConfig($c);
    }

    /**
     * @param string $key AchievementConfigService::KEY_* 之一
     * @return string
     */
    public function getDashboardTitle($key)
    {
        $c = $this->configService->getConfig($key);

        return (string)$c['dashboardTitle'];
    }

    /**
     * @param string $key AchievementConfigService::KEY_* 之一
     * @return string
     */
    public function getPeriodText($key)
    {
        $c = $this->configService->getConfig($key);

        return (string)$c['periodText'];
    }

    /**
     * @param array<string,mixed> $c
     * @return array<string,mixed>
     */
    private function extractRankOptions(array $c)
    {
        return [
            'include_zero_member'        => (bool)$c['includeZeroMember'],
            'left_group_top_limit'       => (int)$c['leftGroupTopLimit'],
            'right_member_sort_desc'     => (bool)$c['rightMemberSortDesc'],
            'right_group_sort_field'     => (string)$c['rightGroupSortField'],
            'right_group_sort_direction' => (string)$c['rightGroupSortDirection'],
        ];
    }

    /**
     * @param string $field
     * @return string
     */
    private function normalizeOrderTimeField($field)
    {
        $allowedTimeFields = ['order_time', 'create_time'];

        return in_array($field, $allowedTimeFields, true) ? $field : 'order_time';
    }

    /**
     * 排行榜汇总：SQL 时间条件与排序等均来自 $config。
     *
     * @param array $groups 小组名 => 成员列表
     * @param array<string,mixed> $config {@see AchievementConfigService::getConfig}
     * @return array{groupAvgRankList:array, memberRankGroupList:array, globalMemberRankList:array}
     */
    private function buildAchievementDataForGroupsBody(array $groups, array $config)
    {
        $groupAvgRankList    = [];
        $memberRankGroupList = [];
        $globalMemberRows    = [];

        if (empty($groups) || !is_array($groups)) {
            return [
                'groupAvgRankList'     => $groupAvgRankList,
                'memberRankGroupList'  => $memberRankGroupList,
                'globalMemberRankList' => [],
            ];
        }

        $startTime      = $config['startTime'];
        $endTime        = $config['endTime'];
        $orderTimeField = $config['orderTimeField'];
        $rankOptions    = $this->extractRankOptions($config);

        $timeField = $this->normalizeOrderTimeField($orderTimeField);

        $orderRows = Db::table('crm_client_order')
            ->alias('o')
            ->field('o.id, o.order_no, o.pr_user_id, o.pr_user, o.joint_person, o.profit, o.owner_profit_rate, o.collaborator_profit_rate')
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->select();

        $achievementByUserId = [];
        $achievementByName   = [];
        if (is_array($orderRows)) {
            $allJointIds = [];
            foreach ($orderRows as $row) {
                foreach ($this->parseJointPersonIds(isset($row['joint_person']) ? $row['joint_person'] : '') as $jointId) {
                    $allJointIds[$jointId] = $jointId;
                }
            }
            $jointNameMap = $this->getAdminNameMapByIds(array_values($allJointIds));

            /**
             * 开发自测样例（旺春业绩看板分润）：
             * A: profit=10000, owner=100, collaborator=0,   joint=空      => 负责人+10000
             * B: profit=10000, owner=90,  collaborator=10,  joint=单人ID  => 负责人+9000, 协同+1000
             * C: profit=10000, owner=80,  collaborator=20,  joint=两人ID  => 负责人+8000, 协同各+1000
             */
            foreach ($orderRows as $row) {
                $uid    = isset($row['pr_user_id']) ? (int)$row['pr_user_id'] : 0;
                $name   = isset($row['pr_user']) ? trim((string)$row['pr_user']) : '';
                $profit = $this->toFloatOrNull(isset($row['profit']) ? $row['profit'] : null);
                if ($profit === null) {
                    $profit = 0.0;
                }
                $profit = round($profit, 2);

                $ownerRate = $this->normalizeProfitRate(isset($row['owner_profit_rate']) ? $row['owner_profit_rate'] : null, 100);
                $collaboratorRate = $this->normalizeProfitRate(isset($row['collaborator_profit_rate']) ? $row['collaborator_profit_rate'] : null, 0);

                $jointIds = $this->parseJointPersonIds(isset($row['joint_person']) ? $row['joint_person'] : '');
                $hasCollaborator = $collaboratorRate > 0 && !empty($jointIds);

                if (!$hasCollaborator) {
                    $this->addAchievementAmount($achievementByUserId, $achievementByName, $uid, $name, $profit);
                    continue;
                }

                // 旺春业绩看板分润统计：先按占比拆分，再做历史脏数据容错。
                $ownerAmount = round($profit * $ownerRate / 100, 2);
                $collaboratorAmount = round($profit * $collaboratorRate / 100, 2);
                $allocatedAmount = round($ownerAmount + $collaboratorAmount, 2);
                if ($profit > 0 && $allocatedAmount < $profit) {
                    // 若比例合计不足导致少算，差额补给负责人，确保总业绩不丢失。
                    $ownerAmount = round($ownerAmount + ($profit - $allocatedAmount), 2);
                }
                $this->addAchievementAmount($achievementByUserId, $achievementByName, $uid, $name, $ownerAmount);

                $jointCount = count($jointIds);
                if ($jointCount <= 0) {
                    continue;
                }
                if ($jointCount === 1) {
                    $jointId = (int)$jointIds[0];
                    $jointName = isset($jointNameMap[$jointId]) ? (string)$jointNameMap[$jointId] : '';
                    $this->addAchievementAmount($achievementByUserId, $achievementByName, $jointId, $jointName, $collaboratorAmount);
                    continue;
                }

                // 历史多人协同：协同利润均分，最后一人吃差额，避免舍入误差导致丢利润。
                $avgAmount = round($collaboratorAmount / $jointCount, 2);
                $allocated = 0.0;
                foreach ($jointIds as $idx => $jointId) {
                    $jointId = (int)$jointId;
                    $isLast = $idx === $jointCount - 1;
                    $jointShare = $isLast ? round($collaboratorAmount - $allocated, 2) : $avgAmount;
                    $allocated = round($allocated + $jointShare, 2);
                    $jointName = isset($jointNameMap[$jointId]) ? (string)$jointNameMap[$jointId] : '';
                    $this->addAchievementAmount($achievementByUserId, $achievementByName, $jointId, $jointName, $jointShare);
                }
            }
        }

        $includeZero = !empty($rankOptions['include_zero_member']);
        $rightMemberSortDesc = !empty($rankOptions['right_member_sort_desc']);

        foreach ($groups as $groupName => $memberList) {
            $groupName = (string)$groupName;
            $members   = is_array($memberList) ? $memberList : [];
            $memberCountConfig = count($members);

            $groupTotalAmount = 0.0;

            $resolvedMembers = [];
            foreach ($members as $member) {
                $name  = '';
                $uid   = null;
                if (is_array($member)) {
                    $name = isset($member['name']) ? trim((string)$member['name']) : '';
                    $uid  = isset($member['id']) ? (int)$member['id'] : null;
                } else {
                    $name = trim((string)$member);
                }
                if ($name === '' && $uid === null) {
                    continue;
                }
                if ($name === '' && $uid !== null) {
                    $name = (string)$uid;
                }

                $amount = 0.0;
                if ($uid !== null && $uid > 0 && isset($achievementByUserId[$uid])) {
                    $amount = (float)$achievementByUserId[$uid];
                } elseif ($name !== '' && isset($achievementByName[$name])) {
                    $amount = (float)$achievementByName[$name];
                }
                $amount = round($amount, 2);

                $groupTotalAmount += $amount;

                if (!$includeZero && $amount <= 0) {
                    continue;
                }
                $resolvedMembers[] = ['name' => $name, 'amount' => $amount];
            }

            foreach ($resolvedMembers as $rm) {
                $globalMemberRows[] = [
                    'name'       => $rm['name'],
                    'group_name' => $groupName,
                    'amount'     => $rm['amount'],
                ];
            }

            $groupTotalAmount = round($groupTotalAmount, 2);
            $avgAmount        = 0.0;
            if ($memberCountConfig > 0) {
                $avgAmount = round($groupTotalAmount / $memberCountConfig, 2);
            }

            if (!empty($resolvedMembers) && $rightMemberSortDesc) {
                usort($resolvedMembers, function ($a, $b) {
                    if ($a['amount'] === $b['amount']) {
                        return 0;
                    }
                    return $a['amount'] < $b['amount'] ? 1 : -1;
                });
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            } else {
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            }

            $groupAvgRankList[] = [
                'rank'          => 0,
                'group_name'    => $groupName,
                'total_amount'  => $groupTotalAmount,
                'member_count'  => $memberCountConfig,
                'avg_amount'    => $avgAmount,
            ];

            $memberRankGroupList[] = [
                'group_name'   => $groupName,
                'total_amount' => $groupTotalAmount,
                'avg_amount'   => $avgAmount,
                'members'      => $resolvedMembers,
            ];
        }

        if (!empty($groupAvgRankList)) {
            usort($groupAvgRankList, function ($a, $b) {
                $aAvg = isset($a['avg_amount']) ? (float)$a['avg_amount'] : 0.0;
                $bAvg = isset($b['avg_amount']) ? (float)$b['avg_amount'] : 0.0;
                if ($aAvg !== $bAvg) {
                    return ($aAvg < $bAvg) ? 1 : -1;
                }

                $aTotal = isset($a['total_amount']) ? (float)$a['total_amount'] : 0.0;
                $bTotal = isset($b['total_amount']) ? (float)$b['total_amount'] : 0.0;
                if ($aTotal !== $bTotal) {
                    return ($aTotal < $bTotal) ? 1 : -1;
                }

                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName);
            });
            $rank = 1;
            foreach ($groupAvgRankList as $idx => $item) {
                $groupAvgRankList[$idx]['rank'] = $rank++;
            }
        }

        if (!empty($memberRankGroupList)) {
            $sortField = isset($rankOptions['right_group_sort_field']) ? (string)$rankOptions['right_group_sort_field'] : 'total_amount';
            $allowedFields = ['total_amount', 'avg_amount'];
            if (!in_array($sortField, $allowedFields, true)) {
                $sortField = 'total_amount';
            }

            $direction = strtolower(isset($rankOptions['right_group_sort_direction']) ? (string)$rankOptions['right_group_sort_direction'] : 'desc') === 'asc' ? 'asc' : 'desc';

            usort($memberRankGroupList, function ($a, $b) use ($sortField, $direction) {
                $aPrimary = isset($a[$sortField]) ? (float)$a[$sortField] : 0.0;
                $bPrimary = isset($b[$sortField]) ? (float)$b[$sortField] : 0.0;

                if ($aPrimary !== $bPrimary) {
                    if ($direction === 'asc') {
                        return ($aPrimary < $bPrimary) ? -1 : 1;
                    }
                    return ($aPrimary < $bPrimary) ? 1 : -1;
                }

                $secondaryField = ($sortField === 'total_amount') ? 'avg_amount' : 'total_amount';
                $aSecondary = isset($a[$secondaryField]) ? (float)$a[$secondaryField] : 0.0;
                $bSecondary = isset($b[$secondaryField]) ? (float)$b[$secondaryField] : 0.0;

                if ($aSecondary !== $bSecondary) {
                    return ($aSecondary < $bSecondary) ? 1 : -1;
                }

                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName);
            });
        }

        $globalMemberRankList = [];
        if (!empty($globalMemberRows)) {
            usort($globalMemberRows, function ($a, $b) {
                $aAmt = isset($a['amount']) ? (float)$a['amount'] : 0.0;
                $bAmt = isset($b['amount']) ? (float)$b['amount'] : 0.0;

                if ($aAmt !== $bAmt) {
                    return ($aAmt < $bAmt) ? 1 : -1;
                }

                $aGroup = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bGroup = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aGroup !== $bGroup) {
                    return strcmp($aGroup, $bGroup);
                }

                $aName = isset($a['name']) ? (string)$a['name'] : '';
                $bName = isset($b['name']) ? (string)$b['name'] : '';
                return strcmp($aName, $bName);
            });

            $rank = 1;
            foreach ($globalMemberRows as $item) {
                $globalMemberRankList[] = [
                    'rank'       => $rank++,
                    'name'       => isset($item['name']) ? (string)$item['name'] : '',
                    'group_name' => isset($item['group_name']) ? (string)$item['group_name'] : '',
                    'amount'     => isset($item['amount']) ? (float)$item['amount'] : 0.0,
                ];
            }
        }

        $leftLimit = (int)(isset($rankOptions['left_group_top_limit']) ? $rankOptions['left_group_top_limit'] : 0);
        if ($leftLimit > 0) {
            $groupAvgRankList = array_slice($groupAvgRankList, 0, $leftLimit);
        }

        return [
            'groupAvgRankList'     => $groupAvgRankList,
            'memberRankGroupList'  => $memberRankGroupList,
            'globalMemberRankList' => $globalMemberRankList,
        ];
    }

    /**
     * 签名载荷：必须带 series + configKey，保证 temporary / permanent 两套 stamp 永不混算。
     *
     * @param array<string,mixed> $config
     * @return array
     */
    private function buildAchievementSignatureDataFromConfig(array $config)
    {
        $stampScope     = $config['stampScope'];
        $startTime      = $config['startTime'];
        $endTime        = $config['endTime'];
        $orderTimeField = $config['orderTimeField'];
        $recentDays     = (int)$config['recentCaptureDays'];

        $timeField = $this->normalizeOrderTimeField($orderTimeField);

        $periodRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS cnt',
                'SUM(COALESCE(o.profit,0)) AS sum_profit',
                'SUM(COALESCE(o.owner_profit_rate,0)) AS sum_owner_profit_rate',
                'SUM(COALESCE(o.collaborator_profit_rate,0)) AS sum_collaborator_profit_rate',
                'SUM(COALESCE(CRC32(COALESCE(o.joint_person,\'\')),0)) AS sum_joint_person_crc',
                'MAX(o.id) AS max_id',
                'MAX(o.' . $timeField . ') AS max_order_time',
                'MAX(o.create_time) AS max_create_time',
                'MAX(o.ut_time) AS max_ut_time',
                'MAX(o.audit_time) AS max_audit_time',
            ])
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->find();

        $periodSummary = [
            'cnt'             => isset($periodRow['cnt']) ? (int)$periodRow['cnt'] : 0,
            'sum_profit'      => isset($periodRow['sum_profit']) ? (float)$periodRow['sum_profit'] : 0.0,
            'sum_owner_profit_rate'        => isset($periodRow['sum_owner_profit_rate']) ? (float)$periodRow['sum_owner_profit_rate'] : 0.0,
            'sum_collaborator_profit_rate' => isset($periodRow['sum_collaborator_profit_rate']) ? (float)$periodRow['sum_collaborator_profit_rate'] : 0.0,
            'sum_joint_person_crc'         => isset($periodRow['sum_joint_person_crc']) ? (string)$periodRow['sum_joint_person_crc'] : '0',
            'max_id'          => isset($periodRow['max_id']) ? (int)$periodRow['max_id'] : 0,
            'max_order_time'  => isset($periodRow['max_order_time']) ? (string)$periodRow['max_order_time'] : '',
            'max_create_time' => isset($periodRow['max_create_time']) ? (string)$periodRow['max_create_time'] : '',
            'max_ut_time'     => isset($periodRow['max_ut_time']) ? (string)$periodRow['max_ut_time'] : '',
            'max_audit_time'  => isset($periodRow['max_audit_time']) ? (string)$periodRow['max_audit_time'] : '',
        ];

        if ($recentDays < 0) {
            $recentDays = 0;
        }
        $recentStart = date('Y-m-d H:i:s', strtotime('-' . $recentDays . ' days'));

        $recentRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS recent_cnt',
                'SUM(COALESCE(o.profit,0)) AS recent_sum_profit',
                'SUM(COALESCE(o.owner_profit_rate,0)) AS recent_sum_owner_profit_rate',
                'SUM(COALESCE(o.collaborator_profit_rate,0)) AS recent_sum_collaborator_profit_rate',
                'SUM(COALESCE(CRC32(COALESCE(o.joint_person,\'\')),0)) AS recent_sum_joint_person_crc',
                'MAX(o.id) AS recent_max_id',
                'MAX(o.create_time) AS recent_max_create_time',
                'MAX(o.ut_time) AS recent_max_ut_time',
                'MAX(o.audit_time) AS recent_max_audit_time',
            ])
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->where(function ($query) use ($recentStart) {
                $query->where('o.create_time', '>=', $recentStart)
                      ->whereOr('o.ut_time', '>=', $recentStart)
                      ->whereOr('o.audit_time', '>=', $recentStart);
            })
            ->find();

        $recentSummary = [
            'recent_cnt'             => isset($recentRow['recent_cnt']) ? (int)$recentRow['recent_cnt'] : 0,
            'recent_sum_profit'                   => isset($recentRow['recent_sum_profit']) ? (float)$recentRow['recent_sum_profit'] : 0.0,
            'recent_sum_owner_profit_rate'        => isset($recentRow['recent_sum_owner_profit_rate']) ? (float)$recentRow['recent_sum_owner_profit_rate'] : 0.0,
            'recent_sum_collaborator_profit_rate' => isset($recentRow['recent_sum_collaborator_profit_rate']) ? (float)$recentRow['recent_sum_collaborator_profit_rate'] : 0.0,
            'recent_sum_joint_person_crc'         => isset($recentRow['recent_sum_joint_person_crc']) ? (string)$recentRow['recent_sum_joint_person_crc'] : '0',
            'recent_max_id'          => isset($recentRow['recent_max_id']) ? (int)$recentRow['recent_max_id'] : 0,
            'recent_max_create_time' => isset($recentRow['recent_max_create_time']) ? (string)$recentRow['recent_max_create_time'] : '',
            'recent_max_ut_time'     => isset($recentRow['recent_max_ut_time']) ? (string)$recentRow['recent_max_ut_time'] : '',
            'recent_max_audit_time'  => isset($recentRow['recent_max_audit_time']) ? (string)$recentRow['recent_max_audit_time'] : '',
        ];

        return [
            'series'         => (string)$config['series'],
            'configKey'      => (string)$config['configKey'],
            'stamp_scope'    => $stampScope,
            'period_summary' => $periodSummary,
            'recent_changes' => $recentSummary,
        ];
    }

    /**
     * @param array<string,mixed> $config
     * @return string
     */
    private function computeAchievementStampFromFullConfig(array $config)
    {
        $signatureData = $this->buildAchievementSignatureDataFromConfig($config);

        return md5(json_encode($signatureData, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * @param mixed $jointPerson
     * @return int[]
     */
    private function parseJointPersonIds($jointPerson)
    {
        if ($jointPerson === null) {
            return [];
        }

        $rawItems = [];
        if (is_array($jointPerson)) {
            $rawItems = $jointPerson;
        } else {
            $raw = trim((string)$jointPerson);
            if ($raw === '') {
                return [];
            }

            if ($raw[0] === '[' || $raw[0] === '{') {
                $decoded = json_decode($raw, true);
                if (is_array($decoded)) {
                    $rawItems = $decoded;
                }
            }
            if (empty($rawItems)) {
                $normalized = str_replace(['，', '、', ';', '|'], ',', $raw);
                $rawItems = explode(',', $normalized);
            }
        }

        $ids = [];
        foreach ($rawItems as $item) {
            if (is_array($item)) {
                foreach (['admin_id', 'id', 'user_id', 'uid'] as $key) {
                    if (!isset($item[$key])) {
                        continue;
                    }
                    $candidate = trim((string)$item[$key]);
                    if ($candidate !== '' && preg_match('/^\d+$/', $candidate)) {
                        $id = (int)$candidate;
                        if ($id > 0) {
                            $ids[$id] = $id;
                        }
                        break;
                    }
                }
                continue;
            }

            $candidate = trim((string)$item);
            if ($candidate === '' || !preg_match('/^\d+$/', $candidate)) {
                continue;
            }
            $id = (int)$candidate;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }

        return array_values($ids);
    }

    /**
     * @param int[] $userIds
     * @return array<int,string>
     */
    private function getAdminNameMapByIds(array $userIds)
    {
        $ids = [];
        foreach ($userIds as $userId) {
            $id = (int)$userId;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }
        if (empty($ids)) {
            return [];
        }

        $rows = Db::name('admin')
            ->field('admin_id, username')
            ->where('admin_id', 'in', array_values($ids))
            ->select();

        $map = [];
        if (!is_array($rows)) {
            return $map;
        }
        foreach ($rows as $row) {
            $id = isset($row['admin_id']) ? (int)$row['admin_id'] : 0;
            if ($id <= 0) {
                continue;
            }
            $name = isset($row['username']) ? trim((string)$row['username']) : '';
            if ($name !== '') {
                $map[$id] = $name;
            }
        }

        return $map;
    }

    /**
     * @param array<int,float> $achievementByUserId
     * @param array<string,float> $achievementByName
     * @param int $uid
     * @param string $name
     * @param float $amount
     * @return void
     */
    private function addAchievementAmount(array &$achievementByUserId, array &$achievementByName, $userId, $userName, $amount)
    {
        $uid = (int)$userId;
        $name = trim((string)$userName);
        $amount = round((float)$amount, 2);
        if ($amount <= 0) {
            return;
        }

        if ($uid > 0) {
            if (!isset($achievementByUserId[$uid])) {
                $achievementByUserId[$uid] = 0.0;
            }
            $achievementByUserId[$uid] = round((float)$achievementByUserId[$uid] + $amount, 2);
        }

        if ($name !== '') {
            if (!isset($achievementByName[$name])) {
                $achievementByName[$name] = 0.0;
            }
            $achievementByName[$name] = round((float)$achievementByName[$name] + $amount, 2);
        }
    }

    /**
     * @param mixed $value
     * @param mixed $default
     * @return float
     */
    private function normalizeProfitRate($value, $default)
    {
        $defaultRate = $this->toFloatOrNull($default);
        if ($defaultRate === null || $defaultRate < 0) {
            $defaultRate = 0.0;
        }
        if ($defaultRate > 100) {
            $defaultRate = 100.0;
        }

        $rate = $this->toFloatOrNull($value);
        if ($rate === null || $rate < 0) {
            $rate = $defaultRate;
        }
        if ($rate > 100) {
            $rate = 100.0;
        }

        return (float)$rate;
    }

    /**
     * @param mixed $value
     * @return float|null
     */
    private function toFloatOrNull($value)
    {
        if ($value === null) {
            return null;
        }
        if (is_int($value) || is_float($value)) {
            return (float)$value;
        }
        $raw = trim((string)$value);
        if ($raw === '' || !is_numeric($raw)) {
            return null;
        }

        return (float)$raw;
    }
}
