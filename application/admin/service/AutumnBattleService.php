<?php

namespace app\admin\service;

use think\Db;

/**
 * 金秋大战业务：挑战人榜 / 押注资金榜 / stamp
 */
class AutumnBattleService
{
    /** @var AutumnBattleConfigService */
    private $configService;

    /** @var OrderProfitAchievementService */
    private $orderProfitService;

    public function __construct(
        AutumnBattleConfigService $configService = null,
        OrderProfitAchievementService $orderProfitService = null
    ) {
        $this->configService = $configService ?: new AutumnBattleConfigService();
        $this->orderProfitService = $orderProfitService ?: new OrderProfitAchievementService();
    }

    /**
     * @return array<string,mixed>
     */
    public function buildPersonPageData()
    {
        $config = $this->configService->getPersonConfig();
        $challengerRankList = $this->buildChallengerRankList($config);
        $bettorRankList = $this->buildBettorRankList($config, $challengerRankList);
        $summary = $this->buildSummary($config, $challengerRankList, $bettorRankList);
        $stamp = $this->getPersonStampByConfig($config);

        return [
            'dashboardTitle'      => $config['dashboardTitle'],
            'periodText'          => $config['periodText'],
            'challengerRankList'  => $challengerRankList,
            'bettorRankList'      => $bettorRankList,
            'summary'             => $summary,
            'stamp'               => $stamp,
            'fundStatusRule'      => null,
            'winProbabilityRule'  => null,
        ];
    }

    /**
     * @return string
     */
    public function getPersonStamp()
    {
        return $this->getPersonStampByConfig($this->configService->getPersonConfig());
    }

    /**
     * @param array<string,mixed> $config
     * @return string
     */
    public function getPersonStampByConfig(array $config)
    {
        if (($config['series'] ?? '') !== AutumnBattleConfigService::SERIES_AUTUMN) {
            throw new \InvalidArgumentException('Autumn stamp must use autumn series config.');
        }

        $signature = $this->buildStampSignature($config);

        return md5(json_encode($signature, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * @param array<string,mixed> $config
     * @return array<string,mixed>
     */
    private function buildStampSignature(array $config)
    {
        $timeField = $this->orderProfitService->normalizeOrderTimeField($config['orderTimeField']);
        $startTime = (string)$config['startTime'];
        $endExclusive = (string)$config['endTimeExclusive'];
        $recentDays = (int)$config['recentCaptureDays'];
        if ($recentDays < 0) {
            $recentDays = 0;
        }

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
            ->where('o.' . $timeField, '<', $endExclusive)
            ->find();

        $periodSummary = [
            'cnt'                         => isset($periodRow['cnt']) ? (int)$periodRow['cnt'] : 0,
            'sum_profit'                  => isset($periodRow['sum_profit']) ? (float)$periodRow['sum_profit'] : 0.0,
            'sum_owner_profit_rate'       => isset($periodRow['sum_owner_profit_rate']) ? (float)$periodRow['sum_owner_profit_rate'] : 0.0,
            'sum_collaborator_profit_rate'=> isset($periodRow['sum_collaborator_profit_rate']) ? (float)$periodRow['sum_collaborator_profit_rate'] : 0.0,
            'sum_joint_person_crc'        => isset($periodRow['sum_joint_person_crc']) ? (string)$periodRow['sum_joint_person_crc'] : '0',
            'max_id'                      => isset($periodRow['max_id']) ? (int)$periodRow['max_id'] : 0,
            'max_order_time'              => isset($periodRow['max_order_time']) ? (string)$periodRow['max_order_time'] : '',
            'max_create_time'             => isset($periodRow['max_create_time']) ? (string)$periodRow['max_create_time'] : '',
            'max_ut_time'                 => isset($periodRow['max_ut_time']) ? (string)$periodRow['max_ut_time'] : '',
            'max_audit_time'              => isset($periodRow['max_audit_time']) ? (string)$periodRow['max_audit_time'] : '',
        ];

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
            ->where('o.' . $timeField, '<', $endExclusive)
            ->where(function ($query) use ($recentStart) {
                $query->where('o.create_time', '>=', $recentStart)
                      ->whereOr('o.ut_time', '>=', $recentStart)
                      ->whereOr('o.audit_time', '>=', $recentStart);
            })
            ->find();

        $recentSummary = [
            'recent_cnt'                          => isset($recentRow['recent_cnt']) ? (int)$recentRow['recent_cnt'] : 0,
            'recent_sum_profit'                   => isset($recentRow['recent_sum_profit']) ? (float)$recentRow['recent_sum_profit'] : 0.0,
            'recent_sum_owner_profit_rate'        => isset($recentRow['recent_sum_owner_profit_rate']) ? (float)$recentRow['recent_sum_owner_profit_rate'] : 0.0,
            'recent_sum_collaborator_profit_rate' => isset($recentRow['recent_sum_collaborator_profit_rate']) ? (float)$recentRow['recent_sum_collaborator_profit_rate'] : 0.0,
            'recent_sum_joint_person_crc'         => isset($recentRow['recent_sum_joint_person_crc']) ? (string)$recentRow['recent_sum_joint_person_crc'] : '0',
            'recent_max_id'                       => isset($recentRow['recent_max_id']) ? (int)$recentRow['recent_max_id'] : 0,
            'recent_max_create_time'              => isset($recentRow['recent_max_create_time']) ? (string)$recentRow['recent_max_create_time'] : '',
            'recent_max_ut_time'                  => isset($recentRow['recent_max_ut_time']) ? (string)$recentRow['recent_max_ut_time'] : '',
            'recent_max_audit_time'               => isset($recentRow['recent_max_audit_time']) ? (string)$recentRow['recent_max_audit_time'] : '',
        ];

        return [
            'series'              => (string)$config['series'],
            'configKey'           => (string)$config['configKey'],
            'stamp_scope'         => (string)$config['stampScope'],
            'config_version'      => (string)$config['configVersion'],
            'config_fingerprint'  => (string)$config['configFingerprint'],
            'stat_start'          => $startTime,
            'stat_end_exclusive'  => $endExclusive,
            'order_time_field'    => $timeField,
            'challenger_count'    => count($config['challengers']),
            'bettor_count'        => count($config['bettors']),
            'period_summary'      => $periodSummary,
            'recent_changes'      => $recentSummary,
        ];
    }

    /**
     * @param array<string,mixed> $config
     * @return array
     */
    private function buildChallengerRankList(array $config)
    {
        $aggregated = $this->orderProfitService->aggregateProfitByUser(
            $config['startTime'],
            $config['endTimeExclusive'],
            $config['orderTimeField'],
            '<'
        );
        $byUserId = $aggregated['byUserId'];
        $byName = $aggregated['byName'];

        $betByChallenger = [];
        $bettorSetByChallenger = [];
        foreach ($config['betDetails'] as $detail) {
            $cname = isset($detail['challenger_display_name']) ? (string)$detail['challenger_display_name'] : '';
            if ($cname === '') {
                continue;
            }
            if (!isset($betByChallenger[$cname])) {
                $betByChallenger[$cname] = 0.0;
                $bettorSetByChallenger[$cname] = [];
            }
            $betByChallenger[$cname] = round($betByChallenger[$cname] + (float)$detail['amount'], 2);
            $bettorKey = isset($detail['bettor_excel_name']) ? (string)$detail['bettor_excel_name'] : '';
            if ($bettorKey !== '') {
                $bettorSetByChallenger[$cname][$bettorKey] = true;
            }
        }

        $rows = [];
        foreach ($config['challengers'] as $idx => $item) {
            $displayName = isset($item['display_name']) ? trim((string)$item['display_name']) : '';
            $excelName = isset($item['excel_name']) ? trim((string)$item['excel_name']) : $displayName;
            $adminId = array_key_exists('admin_id', $item) ? $item['admin_id'] : null;
            $adminId = ($adminId === null || $adminId === '') ? null : (int)$adminId;
            $target = array_key_exists('target_amount', $item) ? $item['target_amount'] : null;
            $targetAmount = ($target === null || $target === '') ? null : round((float)$target, 2);

            $achievementStatus = 'ok';
            $actualAmount = null;
            if ($adminId === null || $adminId <= 0) {
                $achievementStatus = 'pending_match';
                $actualAmount = null;
            } else {
                if (isset($byUserId[$adminId])) {
                    $actualAmount = round((float)$byUserId[$adminId], 2);
                } elseif ($displayName !== '' && isset($byName[$displayName])) {
                    $actualAmount = round((float)$byName[$displayName], 2);
                } else {
                    $actualAmount = 0.0;
                }
            }

            $completionRate = null;
            $completionRateText = '待配置';
            if ($targetAmount === null || $targetAmount <= 0) {
                $completionRate = null;
                $completionRateText = '待配置';
            } elseif ($achievementStatus === 'pending_match') {
                $completionRate = null;
                $completionRateText = '待核对';
            } else {
                $completionRate = round(((float)$actualAmount / $targetAmount) * 100, 2);
                $completionRateText = number_format($completionRate, 2, '.', '') . '%';
            }

            $betAmount = isset($betByChallenger[$displayName])
                ? round((float)$betByChallenger[$displayName], 2)
                : round((float)(isset($item['excel_bet_total']) ? $item['excel_bet_total'] : 0), 2);
            $betCount = isset($bettorSetByChallenger[$displayName])
                ? count($bettorSetByChallenger[$displayName])
                : (int)(isset($item['excel_bettor_count']) ? $item['excel_bettor_count'] : 0);

            $rows[] = [
                'rank'                 => 0,
                'excel_name'           => $excelName,
                'display_name'         => $displayName,
                'admin_id'             => $adminId,
                'target_amount'        => $targetAmount,
                'actual_amount'        => $actualAmount,
                'achievement_status'   => $achievementStatus,
                'completion_rate'      => $completionRate,
                'completion_rate_text' => $completionRateText,
                'bet_amount'           => $betAmount,
                'bet_count'            => $betCount,
                'config_index'         => (int)$idx,
            ];
        }

        usort($rows, function ($a, $b) {
            $aPending = ($a['achievement_status'] === 'pending_match') ? 1 : 0;
            $bPending = ($b['achievement_status'] === 'pending_match') ? 1 : 0;
            if ($aPending !== $bPending) {
                // 待核对排在真实业绩之后，避免与 0 混排误导
                return $aPending - $bPending;
            }

            $aAmt = $a['actual_amount'];
            $bAmt = $b['actual_amount'];
            $aVal = ($aAmt === null) ? -1.0 : (float)$aAmt;
            $bVal = ($bAmt === null) ? -1.0 : (float)$bAmt;
            if ($aVal !== $bVal) {
                return ($aVal < $bVal) ? 1 : -1;
            }

            $aName = (string)$a['display_name'];
            $bName = (string)$b['display_name'];
            if ($aName !== $bName) {
                return strcmp($aName, $bName);
            }

            return ((int)$a['config_index'] < (int)$b['config_index']) ? -1 : 1;
        });

        $rank = 1;
        foreach ($rows as $i => $row) {
            $rows[$i]['rank'] = $rank++;
            unset($rows[$i]['config_index']);
        }

        return $rows;
    }

    /**
     * @param array<string,mixed> $config
     * @param array $challengerRankList
     * @return array
     */
    private function buildBettorRankList(array $config, array $challengerRankList = [])
    {
        $challengerMap = [];
        foreach ($challengerRankList as $crow) {
            $cname = isset($crow['display_name']) ? trim((string)$crow['display_name']) : '';
            if ($cname !== '') {
                $challengerMap[$cname] = $crow;
            }
        }

        $detailsByBettor = [];
        foreach ($config['betDetails'] as $detail) {
            $bettorKey = isset($detail['bettor_excel_name']) ? trim((string)$detail['bettor_excel_name']) : '';
            if ($bettorKey === '') {
                continue;
            }
            if (!isset($detailsByBettor[$bettorKey])) {
                $detailsByBettor[$bettorKey] = [];
            }
            $detailsByBettor[$bettorKey][] = $detail;
        }

        $rows = [];
        foreach ($config['bettors'] as $idx => $item) {
            $excelName = isset($item['excel_name']) ? trim((string)$item['excel_name']) : '';
            $displayName = isset($item['display_name']) ? trim((string)$item['display_name']) : $excelName;
            $adminId = array_key_exists('admin_id', $item) ? $item['admin_id'] : null;
            $adminId = ($adminId === null || $adminId === '') ? null : (int)$adminId;
            $betAmount = round((float)(isset($item['bet_amount']) ? $item['bet_amount'] : 0), 2);
            $targets = isset($item['target_display_names']) && is_array($item['target_display_names'])
                ? array_values($item['target_display_names'])
                : [];
            $targetsText = implode('、', $targets);

            $rewardPack = $this->buildBettorRewardPack(
                isset($detailsByBettor[$excelName]) ? $detailsByBettor[$excelName] : [],
                $challengerMap,
                $betAmount,
                $displayName !== '' ? $displayName : $excelName
            );

            $rows[] = [
                'rank'                   => 0,
                'excel_name'             => $excelName,
                'display_name'           => $displayName,
                'admin_id'               => $adminId,
                'bet_amount'             => $betAmount,
                'principal_amount'       => $betAmount,
                'target_names'           => $targets,
                'target_names_text'      => $targetsText,
                'reward_amount'          => $rewardPack['reward_amount'],
                'expected_return_amount' => $rewardPack['expected_return_amount'],
                'reward_tier_summary'    => $rewardPack['reward_tier_summary'],
                'reward_tier_code'       => $rewardPack['reward_tier_code'],
                'reward_details'         => $rewardPack['reward_details'],
                'has_pending_reward'     => $rewardPack['has_pending_reward'],
                'has_data_error'         => $rewardPack['has_data_error'],
                // 兼容旧字段：View 不再展示
                'fund_status'            => null,
                'fund_status_text'       => '',
                'win_probability'        => null,
                'win_probability_text'   => '',
                'config_index'           => (int)$idx,
            ];
        }

        usort($rows, function ($a, $b) {
            // 待核对 / 数据异常 / reward_amount=null 排在可计算人员之后，不得与真实 0 元混排
            $aPending = (
                $a['reward_amount'] === null
                || !empty($a['has_pending_reward'])
                || !empty($a['has_data_error'])
            ) ? 1 : 0;
            $bPending = (
                $b['reward_amount'] === null
                || !empty($b['has_pending_reward'])
                || !empty($b['has_data_error'])
            ) ? 1 : 0;
            if ($aPending !== $bPending) {
                return $aPending - $bPending;
            }

            if ($aPending === 0) {
                // 可计算：按当前奖励金额（分）降序；真实 0 元排在正数之后
                $aCents = (int)round(((float)$a['reward_amount']) * 100);
                $bCents = (int)round(((float)$b['reward_amount']) * 100);
                if ($aCents !== $bCents) {
                    return ($aCents < $bCents) ? 1 : -1;
                }
            }

            $aName = (string)$a['display_name'];
            $bName = (string)$b['display_name'];
            if ($aName !== $bName) {
                return strcmp($aName, $bName);
            }

            $aIdx = (int)$a['config_index'];
            $bIdx = (int)$b['config_index'];
            if ($aIdx !== $bIdx) {
                return ($aIdx < $bIdx) ? -1 : 1;
            }
            return 0;
        });

        $rank = 1;
        foreach ($rows as $i => $row) {
            $rows[$i]['rank'] = $rank++;
            unset($rows[$i]['config_index']);
        }

        return $rows;
    }

    /**
     * 按 bet_details 逐笔计算押注人奖励汇总。
     *
     * @param array $details
     * @param array $challengerMap display_name => challenger row
     * @param float $betAmount
     * @param string $bettorLabel
     * @return array<string,mixed>
     */
    private function buildBettorRewardPack(array $details, array $challengerMap, $betAmount, $bettorLabel)
    {
        $rewardDetails = [];
        $principalCentsSum = 0;
        $rewardCentsSum = 0;
        $returnCentsSum = 0;
        $hasPending = false;
        $tierCodes = [];

        foreach ($details as $detail) {
            $principalAmount = round((float)(isset($detail['amount']) ? $detail['amount'] : 0), 2);
            $targetName = isset($detail['challenger_display_name'])
                ? trim((string)$detail['challenger_display_name'])
                : '';
            $challenger = ($targetName !== '' && isset($challengerMap[$targetName]))
                ? $challengerMap[$targetName]
                : null;

            $completionRate = null;
            $achievementStatus = 'pending_match';
            $targetAmount = null;
            if ($challenger !== null) {
                $completionRate = array_key_exists('completion_rate', $challenger)
                    ? $challenger['completion_rate']
                    : null;
                $achievementStatus = isset($challenger['achievement_status'])
                    ? (string)$challenger['achievement_status']
                    : 'ok';
                $targetAmount = array_key_exists('target_amount', $challenger)
                    ? $challenger['target_amount']
                    : null;
            }

            $needPending = (
                $challenger === null
                || $completionRate === null
                || $achievementStatus === 'pending_match'
                || $targetAmount === null
                || $targetAmount === ''
                || (float)$targetAmount <= 0
            );

            if ($needPending) {
                $tier = $this->resolveRewardTier(null, 'pending_match');
                $hasPending = true;
                $rewardDetails[] = [
                    'target_name'      => $targetName !== '' ? $targetName : '未知对象',
                    'principal_amount' => $principalAmount,
                    'completion_rate'  => null,
                    'tier_code'        => $tier['tier_code'],
                    'tier_text'        => $tier['tier_text'],
                    'reward_amount'    => null,
                    'return_amount'    => null,
                    'calculable'       => false,
                ];
                $tierCodes[] = $tier['tier_code'];
                $principalCentsSum += $this->toCents($principalAmount);
                continue;
            }

            $tier = $this->resolveRewardTier((float)$completionRate, $achievementStatus);
            $principalCents = $this->toCents($principalAmount);
            $rewardCents = (int)round($principalCents * (float)$tier['reward_multiplier']);
            $returnCents = $principalCents + $rewardCents;
            $rewardAmount = $this->fromCents($rewardCents);
            $returnAmount = $this->fromCents($returnCents);

            $rewardDetails[] = [
                'target_name'      => $targetName,
                'principal_amount' => $principalAmount,
                'completion_rate'  => round((float)$completionRate, 2),
                'tier_code'        => $tier['tier_code'],
                'tier_text'        => $tier['tier_text'],
                'reward_amount'    => $rewardAmount,
                'return_amount'    => $returnAmount,
                'calculable'       => true,
            ];
            $tierCodes[] = $tier['tier_code'];
            $principalCentsSum += $principalCents;
            $rewardCentsSum += $rewardCents;
            $returnCentsSum += $returnCents;
        }

        $principalFromDetails = $this->fromCents($principalCentsSum);
        $betAmountRounded = round((float)$betAmount, 2);
        $hasDataError = (abs($principalFromDetails - $betAmountRounded) > 0.009);
        if ($hasDataError) {
            // 本金明细与 bet_amount 不一致时不静默汇总
            error_log(sprintf(
                '[AutumnBattle] bettor principal mismatch: %s details=%.2f bet_amount=%.2f',
                (string)$bettorLabel,
                $principalFromDetails,
                $betAmountRounded
            ));
        }

        if (empty($rewardDetails) && !$hasDataError) {
            // 无押注明细且本金一致（通常为 0）：视为无奖励返本金
            $tierCodes = ['no_reward'];
            $hasPending = false;
        }

        $summaryPack = $this->resolveRewardTierSummary($tierCodes, $hasPending);
        $rewardTotal = null;
        $expectedReturn = null;
        if (!$hasDataError && !$hasPending) {
            if (!empty($rewardDetails)) {
                $rewardTotal = $this->fromCents($rewardCentsSum);
                $expectedReturn = $this->fromCents($returnCentsSum);
            } else {
                $rewardTotal = 0.0;
                $expectedReturn = $betAmountRounded;
            }
        }

        return [
            'reward_amount'          => $hasDataError ? null : $rewardTotal,
            'expected_return_amount' => $hasDataError ? null : ($hasPending ? null : $expectedReturn),
            'reward_tier_summary'    => $hasDataError ? '待核对' : $summaryPack['summary'],
            'reward_tier_code'       => $hasDataError ? 'pending' : $summaryPack['code'],
            'reward_details'         => $rewardDetails,
            'has_pending_reward'     => $hasPending || $hasDataError,
            'has_data_error'         => $hasDataError,
        ];
    }

    /**
     * 根据完成率解析奖励档位（边界严格半开区间）。
     *
     * @param float|null $completionRate
     * @param string $achievementStatus
     * @return array<string,mixed>
     */
    private function resolveRewardTier($completionRate, $achievementStatus = 'ok')
    {
        if ($achievementStatus === 'pending_match' || $completionRate === null) {
            return [
                'calculable'         => false,
                'tier_code'          => 'pending',
                'tier_text'          => '待核对',
                'reward_multiplier'  => null,
                'return_multiplier'  => null,
            ];
        }

        $rate = (float)$completionRate;
        if ($rate < 100) {
            return [
                'calculable'         => true,
                'tier_code'          => 'no_reward',
                'tier_text'          => '无奖励',
                'reward_multiplier'  => 0.0,
                'return_multiplier'  => 1.0,
            ];
        }
        if ($rate <= 130) {
            return [
                'calculable'         => true,
                'tier_code'          => 'win_1',
                'tier_text'          => '赢1倍',
                'reward_multiplier'  => 1.0,
                'return_multiplier'  => 2.0,
            ];
        }
        if ($rate <= 160) {
            return [
                'calculable'         => true,
                'tier_code'          => 'win_1_5',
                'tier_text'          => '赢1.5倍',
                'reward_multiplier'  => 1.5,
                'return_multiplier'  => 2.5,
            ];
        }
        if ($rate <= 200) {
            return [
                'calculable'         => true,
                'tier_code'          => 'win_2',
                'tier_text'          => '赢2倍',
                'reward_multiplier'  => 2.0,
                'return_multiplier'  => 3.0,
            ];
        }

        return [
            'calculable'         => true,
            'tier_code'          => 'win_3',
            'tier_text'          => '赢3倍',
            'reward_multiplier'  => 3.0,
            'return_multiplier'  => 4.0,
        ];
    }

    /**
     * @param string[] $tierCodes
     * @param bool $hasPending
     * @return array{summary:string,code:string}
     */
    private function resolveRewardTierSummary(array $tierCodes, $hasPending)
    {
        if ($hasPending || in_array('pending', $tierCodes, true) || empty($tierCodes)) {
            return ['summary' => '待核对', 'code' => 'pending'];
        }

        $unique = array_values(array_unique($tierCodes));
        if (count($unique) === 1) {
            $map = [
                'no_reward' => '无奖励',
                'win_1'     => '赢1倍',
                'win_1_5'   => '赢1.5倍',
                'win_2'     => '赢2倍',
                'win_3'     => '赢3倍',
            ];
            $code = $unique[0];
            return [
                'summary' => isset($map[$code]) ? $map[$code] : '待核对',
                'code'    => isset($map[$code]) ? $code : 'pending',
            ];
        }

        return ['summary' => '混合档位', 'code' => 'mixed'];
    }

    /**
     * @param float|int|string $amount
     * @return int
     */
    private function toCents($amount)
    {
        return (int)round(((float)$amount) * 100);
    }

    /**
     * @param int $cents
     * @return float
     */
    private function fromCents($cents)
    {
        return round(((int)$cents) / 100, 2);
    }

    /**
     * @param array<string,mixed> $config
     * @param array $challengerRankList
     * @param array $bettorRankList
     * @return array<string,mixed>
     */
    private function buildSummary(array $config, array $challengerRankList, array $bettorRankList)
    {
        $checksums = isset($config['checksums']) && is_array($config['checksums']) ? $config['checksums'] : [];
        $actualTotal = 0.0;
        $targetTotal = 0.0;
        $pendingMatchCount = 0;
        foreach ($challengerRankList as $row) {
            if (($row['achievement_status'] ?? '') === 'pending_match') {
                $pendingMatchCount++;
            } elseif ($row['actual_amount'] !== null) {
                $actualTotal = round($actualTotal + (float)$row['actual_amount'], 2);
            }
            if ($row['target_amount'] !== null) {
                $targetTotal = round($targetTotal + (float)$row['target_amount'], 2);
            }
        }

        $betTotal = 0.0;
        foreach ($bettorRankList as $row) {
            $betTotal = round($betTotal + (float)$row['bet_amount'], 2);
        }

        return [
            'challenger_count'     => count($challengerRankList),
            'bettor_count'         => count($bettorRankList),
            'bet_total'            => $betTotal,
            'actual_total'        => $actualTotal,
            'target_total'         => $targetTotal,
            'pending_match_count'  => $pendingMatchCount,
            'config_bet_total'     => isset($checksums['bet_total']) ? (float)$checksums['bet_total'] : $betTotal,
            'config_goal_total_all'=> isset($checksums['goal_total_all']) ? (float)$checksums['goal_total_all'] : null,
        ];
    }
}
