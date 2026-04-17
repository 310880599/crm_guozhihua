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
            ->field('o.pr_user_id, o.pr_user, SUM(COALESCE(o.profit, 0)) AS total_profit')
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->group('o.pr_user_id, o.pr_user')
            ->select();

        $achievementByUserId = [];
        $achievementByName   = [];
        if (is_array($orderRows)) {
            foreach ($orderRows as $row) {
                $uid    = isset($row['pr_user_id']) ? (int)$row['pr_user_id'] : 0;
                $name   = isset($row['pr_user']) ? trim((string)$row['pr_user']) : '';
                $amount = isset($row['total_profit']) ? (float)$row['total_profit'] : 0.0;
                $amount = round($amount, 2);
                if ($uid > 0) {
                    if (!isset($achievementByUserId[$uid])) {
                        $achievementByUserId[$uid] = 0.0;
                    }
                    $achievementByUserId[$uid] += $amount;
                }
                if ($name !== '') {
                    if (!isset($achievementByName[$name])) {
                        $achievementByName[$name] = 0.0;
                    }
                    $achievementByName[$name] += $amount;
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
                'MAX(o.id) AS recent_max_id',
                'MAX(o.create_time) AS recent_max_create_time',
                'MAX(o.ut_time) AS recent_max_ut_time',
                'MAX(o.audit_time) AS recent_max_audit_time',
            ])
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
}
