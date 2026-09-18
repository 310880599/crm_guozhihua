<?php

namespace app\admin\service;

use think\Db;

/**
 * 金秋大战配置写入服务：校验、派生、安全写 PHP 文件。
 * 正式数据源：application/admin/data/autumn_battle_person.php（禁止双数据源）。
 */
class AutumnBattleConfigWriteService
{
    public const AUTH_RULE_ID = 384;
    public const BACKUP_KEEP = 20;

    /** @var AutumnBattleConfigService */
    private $configService;

    public function __construct(AutumnBattleConfigService $configService = null)
    {
        $this->configService = $configService ?: new AutumnBattleConfigService();
    }

    /**
     * 当前登录用户是否具备金秋配置权限（aid=1 或拥有 auth_rule 384）。
     *
     * @param int $adminId
     * @return bool
     */
    public function canAccessConfig($adminId = 0)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            $adminId = (int)session('aid');
        }
        if ($adminId <= 0) {
            return false;
        }
        if ($adminId === 1) {
            return true;
        }

        $ruleId = $this->resolveConfigAuthRuleId();
        if ($ruleId <= 0) {
            return false;
        }

        $rules = Db::table(config('database.prefix') . 'admin')->alias('a')
            ->join(config('database.prefix') . 'auth_group ag', 'a.group_id = ag.group_id', 'left')
            ->where('a.admin_id', $adminId)
            ->value('ag.rules');
        if (!is_string($rules) || trim($rules) === '') {
            return false;
        }

        $ruleList = array_map('strval', array_filter(array_map('trim', explode(',', $rules)), function ($item) {
            return $item !== '';
        }));

        return in_array((string)$ruleId, $ruleList, true);
    }

    /**
     * @return int
     */
    public function resolveConfigAuthRuleId()
    {
        $id = (int)db('auth_rule')->where('href', 'Achievement/autumnAchievementConfig')->value('id');
        if ($id > 0) {
            return $id;
        }
        $id = (int)db('auth_rule')->where('href', 'achievement/autumnachievementconfig')->value('id');
        if ($id > 0) {
            return $id;
        }

        return self::AUTH_RULE_ID;
    }

    /**
     * @return string
     */
    public function getOfficialPath()
    {
        return $this->configService->getConfigPath();
    }

    /**
     * @return string
     */
    public function getDataDir()
    {
        return dirname($this->getOfficialPath());
    }

    /**
     * @return string
     */
    public function getBackupDir()
    {
        $runtime = '';
        if (class_exists('\\think\\facade\\Env')) {
            $runtime = (string)\think\facade\Env::get('runtime_path');
        }
        if ($runtime === '' && defined('ROOT_PATH')) {
            $runtime = rtrim(ROOT_PATH, '\\/') . DIRECTORY_SEPARATOR . 'runtime' . DIRECTORY_SEPARATOR;
        }
        if ($runtime === '') {
            $runtime = dirname(dirname(dirname(__DIR__))) . DIRECTORY_SEPARATOR . 'runtime' . DIRECTORY_SEPARATOR;
        }

        return rtrim($runtime, '\\/') . DIRECTORY_SEPARATOR . 'autumn_config_backup';
    }

    /**
     * 保存配置。成功返回 code=1；失败返回 code=0。
     *
     * @param array<string,mixed> $input
     * @param array{admin_id?:int,username?:string} $operator
     * @return array{code:int,msg:string,data:array}
     */
    public function save(array $input, array $operator = [])
    {
        $tmpPath = '';
        try {
            $officialPath = $this->getOfficialPath();
            $dataDir = $this->getDataDir();

            if (!is_dir($dataDir) || !is_writable($dataDir)) {
                return $this->fail('金秋大战配置目录不可写，请联系管理员检查 PHP 运行用户权限。');
            }
            if (!is_file($officialPath)) {
                return $this->fail('金秋大战正式配置文件不存在，禁止保存。');
            }

            $loadedFingerprint = isset($input['loaded_fingerprint']) ? trim((string)$input['loaded_fingerprint']) : '';
            if ($loadedFingerprint === '') {
                return $this->fail('缺少配置指纹，请刷新页面后重新编辑。');
            }

            // 直接重新 include 正式文件，不依赖 ConfigService 实例缓存
            $currentRaw = $this->includeConfigFile($officialPath);
            $currentFingerprint = $this->configService->getFingerprintForRawConfig($currentRaw);
            if (!hash_equals($currentFingerprint, $loadedFingerprint)) {
                return $this->fail('配置已被其他人或文件修改，请刷新页面后重新编辑。');
            }

            $adminIds = $this->loadExistingAdminIds();
            $normalized = $this->normalizeInput($input, $currentRaw);
            $this->validateNormalized($normalized, $adminIds);

            $built = $this->buildFullConfig($normalized, $currentRaw, $operator);
            $this->validateBuiltConfig($built, $adminIds);

            $warnings = $this->collectWarnings($built);

            $phpContent = $this->buildPhpFileContent($built);
            $tmpPath = $this->writeTempFile($dataDir, $phpContent);

            $tmpConfig = $this->includeConfigFile($tmpPath);
            $this->validateBuiltConfig($tmpConfig, $adminIds);

            $this->ensureBackupDir();
            $this->backupOfficialFile($officialPath, isset($currentRaw['config_version']) ? (string)$currentRaw['config_version'] : 'unknown');
            $this->replaceOfficialFile($tmpPath, $officialPath);
            $tmpPath = ''; // 已替换成功，无需再删

            if (function_exists('opcache_invalidate')) {
                @opcache_invalidate($officialPath, true);
            }

            $verified = $this->includeConfigFile($officialPath);
            $this->validateBuiltConfig($verified, $adminIds);
            $newFingerprint = $this->configService->getFingerprintForRawConfig($verified);

            return [
                'code' => 1,
                'msg'  => '保存成功，新版本：' . (string)$verified['config_version'],
                'data' => [
                    'config_version'     => (string)$verified['config_version'],
                    'config_fingerprint' => $newFingerprint,
                    'checksums'          => isset($verified['checksums']) ? $verified['checksums'] : [],
                    'warnings'           => $warnings,
                ],
            ];
        } catch (\Throwable $e) {
            return $this->fail($e->getMessage());
        } finally {
            if ($tmpPath !== '' && is_file($tmpPath)) {
                @unlink($tmpPath);
            }
        }
    }

    /**
     * @param string $msg
     * @return array{code:int,msg:string,data:array}
     */
    private function fail($msg)
    {
        return [
            'code' => 0,
            'msg'  => (string)$msg,
            'data' => [],
        ];
    }

    /**
     * @return array<int,int> admin_id => admin_id
     */
    private function loadExistingAdminIds()
    {
        $ids = Db::name('admin')->column('admin_id');
        $map = [];
        foreach ((array)$ids as $id) {
            $id = (int)$id;
            if ($id > 0) {
                $map[$id] = $id;
            }
        }

        return $map;
    }

    /**
     * @param array<string,mixed> $input
     * @param array<string,mixed> $currentRaw
     * @return array<string,mixed>
     */
    private function normalizeInput(array $input, array $currentRaw)
    {
        $activityIn = isset($input['activity']) && is_array($input['activity']) ? $input['activity'] : [];
        $startDate = trim((string)(isset($activityIn['start_date']) ? $activityIn['start_date'] : ''));
        $endDate = trim((string)(isset($activityIn['end_date']) ? $activityIn['end_date'] : ''));

        // 兼容直接传 start_time / end_time_exclusive
        if ($startDate === '' && !empty($activityIn['start_time'])) {
            $startDate = substr(trim((string)$activityIn['start_time']), 0, 10);
        }
        if ($endDate === '' && !empty($activityIn['end_time_exclusive'])) {
            $endExclusive = trim((string)$activityIn['end_time_exclusive']);
            $ts = strtotime($endExclusive);
            if ($ts !== false) {
                $endDate = date('Y-m-d', $ts - 86400);
            }
        }

        $title = trim((string)(isset($activityIn['title']) ? $activityIn['title'] : ''));
        $recentCaptureDays = isset($activityIn['recent_capture_days']) ? (int)$activityIn['recent_capture_days'] : 3;

        $oldActivity = isset($currentRaw['activity']) && is_array($currentRaw['activity']) ? $currentRaw['activity'] : [];
        $activity = [
            'title'                => $title,
            'period_text'          => '',
            'start_time'           => $startDate !== '' ? ($startDate . ' 00:00:00') : '',
            'end_time_exclusive'   => '',
            'order_time_field'     => 'order_time',
            'check_status'         => 2,
            'recent_capture_days'  => $recentCaptureDays,
            'stamp_scope'          => 'jq_person',
        ];
        if ($endDate !== '') {
            $endTs = strtotime($endDate . ' 00:00:00');
            if ($endTs === false) {
                throw new \InvalidArgumentException('活动结束日期格式不正确。');
            }
            $activity['end_time_exclusive'] = date('Y-m-d', $endTs + 86400) . ' 00:00:00';
            $activity['period_text'] = date('Y.m.d', strtotime($startDate . ' 00:00:00'))
                . ' - '
                . date('Y.m.d', $endTs);
        }
        // 保留未知旧字段以外的固定技术字段已强制覆盖
        unset($oldActivity);

        $challengers = [];
        $challengerRows = isset($input['challengers']) && is_array($input['challengers']) ? $input['challengers'] : [];
        foreach ($challengerRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $displayName = trim((string)(isset($row['display_name']) ? $row['display_name'] : ''));
            $excelName = trim((string)(isset($row['excel_name']) ? $row['excel_name'] : ''));
            if ($excelName === '') {
                $excelName = $displayName;
            }
            $adminIdRaw = isset($row['admin_id']) ? $row['admin_id'] : null;
            $adminId = ($adminIdRaw === '' || $adminIdRaw === null) ? null : (int)$adminIdRaw;
            $targetAmount = $this->normalizeMoney(isset($row['target_amount']) ? $row['target_amount'] : 0);
            $challengers[] = [
                'excel_name'         => $excelName,
                'display_name'       => $displayName,
                'admin_id'           => $adminId,
                'target_amount'      => $targetAmount,
                'excel_bet_total'    => 0,
                'excel_bettor_count' => 0,
            ];
        }

        $bettors = [];
        $bettorRows = isset($input['bettors']) && is_array($input['bettors']) ? $input['bettors'] : [];
        foreach ($bettorRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $displayName = trim((string)(isset($row['display_name']) ? $row['display_name'] : ''));
            $excelName = trim((string)(isset($row['excel_name']) ? $row['excel_name'] : ''));
            if ($excelName === '') {
                $excelName = $displayName;
            }
            $adminIdRaw = isset($row['admin_id']) ? $row['admin_id'] : null;
            $adminId = ($adminIdRaw === '' || $adminIdRaw === null) ? null : (int)$adminIdRaw;
            $betAmount = $this->normalizeMoney(isset($row['bet_amount']) ? $row['bet_amount'] : 0);
            $targets = [];
            if (isset($row['target_display_names']) && is_array($row['target_display_names'])) {
                foreach ($row['target_display_names'] as $t) {
                    $t = trim((string)$t);
                    if ($t !== '') {
                        $targets[] = $t;
                    }
                }
            }
            $bettors[] = [
                'excel_name'            => $excelName,
                'display_name'          => $displayName,
                'admin_id'              => $adminId,
                'bet_amount'            => $betAmount,
                'target_display_names'  => $targets,
                'targets_raw'           => '',
            ];
        }

        $nameAliases = [];
        $aliasRows = isset($input['name_aliases']) && is_array($input['name_aliases']) ? $input['name_aliases'] : [];
        // 支持 map 或 [{from,to}] / [{key,value}]
        $isList = array_keys($aliasRows) === range(0, count($aliasRows) - 1);
        if ($isList) {
            foreach ($aliasRows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $key = trim((string)(isset($row['from']) ? $row['from'] : (isset($row['key']) ? $row['key'] : '')));
                $value = trim((string)(isset($row['to']) ? $row['to'] : (isset($row['value']) ? $row['value'] : '')));
                if ($key === '' && $value === '') {
                    continue;
                }
                $nameAliases[$key] = $value;
            }
        } else {
            foreach ($aliasRows as $key => $value) {
                $nameAliases[trim((string)$key)] = trim((string)$value);
            }
        }

        $goals = [];
        $goalRows = isset($input['goals_all_by_display']) ? $input['goals_all_by_display'] : [];
        if (is_array($goalRows)) {
            $isGoalList = array_keys($goalRows) === range(0, count($goalRows) - 1);
            if ($isGoalList) {
                foreach ($goalRows as $row) {
                    if (!is_array($row)) {
                        continue;
                    }
                    $name = trim((string)(isset($row['display_name']) ? $row['display_name'] : (isset($row['name']) ? $row['name'] : '')));
                    if ($name === '') {
                        continue;
                    }
                    $goals[$name] = $this->normalizeMoney(isset($row['target_amount']) ? $row['target_amount'] : (isset($row['amount']) ? $row['amount'] : 0));
                }
            } else {
                foreach ($goalRows as $name => $amount) {
                    $name = trim((string)$name);
                    if ($name === '') {
                        continue;
                    }
                    $goals[$name] = $this->normalizeMoney($amount);
                }
            }
        }

        return [
            'activity'             => $activity,
            'challengers'          => $challengers,
            'bettors'              => $bettors,
            'name_aliases'         => $nameAliases,
            'goals_all_by_display' => $goals,
        ];
    }

    /**
     * @param array<string,mixed> $normalized
     * @param array<int,int> $adminIds
     * @return void
     */
    private function validateNormalized(array $normalized, array $adminIds)
    {
        $activity = $normalized['activity'];
        if ($activity['title'] === '') {
            throw new \InvalidArgumentException('活动标题不能为空。');
        }
        if ($activity['start_time'] === '' || $activity['end_time_exclusive'] === '') {
            throw new \InvalidArgumentException('活动开始/结束日期不能为空。');
        }
        $startTs = strtotime($activity['start_time']);
        $endTs = strtotime($activity['end_time_exclusive']);
        if ($startTs === false || $endTs === false) {
            throw new \InvalidArgumentException('活动时间格式不正确。');
        }
        if ($endTs <= $startTs) {
            throw new \InvalidArgumentException('活动结束时间必须晚于开始时间。');
        }
        if ((int)$activity['recent_capture_days'] < 0) {
            throw new \InvalidArgumentException('recent_capture_days 不能为负数。');
        }

        $challengers = $normalized['challengers'];
        if (count($challengers) < 1) {
            throw new \InvalidArgumentException('至少需要配置 1 名挑战人。');
        }

        $displaySeen = [];
        $adminSeen = [];
        foreach ($challengers as $idx => $c) {
            $n = $idx + 1;
            if ($c['display_name'] === '') {
                throw new \InvalidArgumentException("挑战人第{$n}行：展示姓名不能为空。");
            }
            if ($c['excel_name'] === '') {
                throw new \InvalidArgumentException("挑战人「{$c['display_name']}」：excel_name 不能为空。");
            }
            if (isset($displaySeen[$c['display_name']])) {
                throw new \InvalidArgumentException("挑战人展示姓名重复：{$c['display_name']}");
            }
            $displaySeen[$c['display_name']] = true;

            if ($c['admin_id'] === null || (int)$c['admin_id'] <= 0) {
                throw new \InvalidArgumentException("挑战人「{$c['display_name']}」必须绑定有效 CRM 账号（admin_id）。");
            }
            $aid = (int)$c['admin_id'];
            if (!isset($adminIds[$aid])) {
                throw new \InvalidArgumentException("挑战人「{$c['display_name']}」的 admin_id={$aid} 在 admin 表中不存在，禁止保存。");
            }
            if (isset($adminSeen[$aid])) {
                throw new \InvalidArgumentException("多个挑战人绑定了同一个 admin_id={$aid}，禁止保存。");
            }
            $adminSeen[$aid] = true;

            if (!$this->isValidNonNegativeMoney($c['target_amount'])) {
                throw new \InvalidArgumentException("挑战人「{$c['display_name']}」目标业绩必须为 >=0 的数字（最多两位小数）。");
            }
        }

        $challengerNames = $displaySeen;
        $bettors = $normalized['bettors'];
        if (count($bettors) < 1) {
            throw new \InvalidArgumentException('至少需要配置 1 名押注人。');
        }

        foreach ($bettors as $idx => $b) {
            $n = $idx + 1;
            if ($b['display_name'] === '') {
                throw new \InvalidArgumentException("押注人第{$n}行：展示姓名不能为空。");
            }
            if ($b['excel_name'] === '') {
                throw new \InvalidArgumentException("押注人「{$b['display_name']}」：excel_name 不能为空。");
            }
            if (!$this->isValidPositiveMoney($b['bet_amount'])) {
                throw new \InvalidArgumentException("押注人「{$b['display_name']}」押注金额必须 > 0（最多两位小数）。");
            }
            if (empty($b['target_display_names']) || !is_array($b['target_display_names'])) {
                throw new \InvalidArgumentException("押注人「{$b['display_name']}」至少选择 1 个押注对象。");
            }
            $targetSeen = [];
            foreach ($b['target_display_names'] as $t) {
                if (!isset($challengerNames[$t])) {
                    throw new \InvalidArgumentException("押注人「{$b['display_name']}」仍押注已删除挑战人「{$t}」，请先调整押注对象。");
                }
                if (isset($targetSeen[$t])) {
                    throw new \InvalidArgumentException("押注人「{$b['display_name']}」的押注对象「{$t}」重复。");
                }
                $targetSeen[$t] = true;
            }
            if ($b['admin_id'] !== null) {
                $aid = (int)$b['admin_id'];
                if ($aid <= 0 || !isset($adminIds[$aid])) {
                    throw new \InvalidArgumentException("押注人「{$b['display_name']}」的 admin_id 无效或不存在。");
                }
            }
        }

        $aliases = $normalized['name_aliases'];
        $aliasKeys = [];
        foreach ($aliases as $key => $value) {
            $key = (string)$key;
            $value = (string)$value;
            if (trim($key) === '') {
                throw new \InvalidArgumentException('姓名别名 key 不能为空。');
            }
            if (trim($value) === '') {
                throw new \InvalidArgumentException("姓名别名「{$key}」的规范姓名不能为空。");
            }
            if (isset($aliasKeys[$key])) {
                throw new \InvalidArgumentException("姓名别名 key 重复：{$key}");
            }
            $aliasKeys[$key] = true;
            if ($key === $value) {
                throw new \InvalidArgumentException("姓名别名禁止 A→A：{$key}");
            }
        }
        foreach ($aliases as $key => $value) {
            if (isset($aliases[$value]) && (string)$aliases[$value] === (string)$key) {
                throw new \InvalidArgumentException("姓名别名存在双向循环：{$key} ↔ {$value}");
            }
        }

        $goals = $normalized['goals_all_by_display'];
        if (!is_array($goals) || count($goals) < 1) {
            throw new \InvalidArgumentException('全员目标配置不能为空。');
        }
        $goalNames = [];
        foreach ($goals as $name => $amount) {
            $name = trim((string)$name);
            if ($name === '') {
                throw new \InvalidArgumentException('全员目标人员姓名不能为空。');
            }
            if (isset($goalNames[$name])) {
                throw new \InvalidArgumentException("全员目标姓名重复：{$name}");
            }
            $goalNames[$name] = true;
            if (!$this->isValidNonNegativeMoney($amount)) {
                throw new \InvalidArgumentException("全员目标「{$name}」金额必须为 >=0 的数字（最多两位小数）。");
            }
        }
    }

    /**
     * @param array<string,mixed> $normalized
     * @param array<string,mixed> $currentRaw
     * @param array{admin_id?:int,username?:string} $operator
     * @return array<string,mixed>
     */
    private function buildFullConfig(array $normalized, array $currentRaw, array $operator)
    {
        $challengerByDisplay = [];
        foreach ($normalized['challengers'] as $c) {
            $challengerByDisplay[$c['display_name']] = $c;
        }

        $bettors = [];
        foreach ($normalized['bettors'] as $b) {
            $b['targets_raw'] = implode(' ', $b['target_display_names']);
            $bettors[] = $b;
        }

        $betDetails = [];
        foreach ($bettors as $b) {
            $targets = $b['target_display_names'];
            $shares = $this->splitAmount((float)$b['bet_amount'], count($targets));
            foreach ($targets as $i => $targetName) {
                $ch = $challengerByDisplay[$targetName];
                $betDetails[] = [
                    'bettor_excel_name'       => $b['excel_name'],
                    'bettor_display_name'     => $b['display_name'],
                    'bettor_admin_id'         => $b['admin_id'],
                    'challenger_excel_name'   => $ch['excel_name'],
                    'challenger_display_name' => $ch['display_name'],
                    'amount'                  => $shares[$i],
                ];
            }
        }

        $challengers = $this->refillChallengerBetSummary($normalized['challengers'], $betDetails);

        $goalTotalAll = 0;
        foreach ($normalized['goals_all_by_display'] as $amount) {
            $goalTotalAll += $this->toCents($amount);
        }
        $challengerGoalTotal = 0;
        foreach ($challengers as $c) {
            $challengerGoalTotal += $this->toCents($c['target_amount']);
        }
        $betTotalCents = 0;
        foreach ($bettors as $b) {
            $betTotalCents += $this->toCents($b['bet_amount']);
        }
        $detailTotalCents = 0;
        foreach ($betDetails as $d) {
            $detailTotalCents += $this->toCents($d['amount']);
        }
        if ($betTotalCents !== $detailTotalCents) {
            throw new \RuntimeException('押注明细合计与押注人总金额不一致，已中止保存。');
        }

        $checksums = [
            'challenger_count'      => count($challengers),
            'bettor_count'          => count($bettors),
            'bet_total'             => $this->fromCents($betTotalCents),
            'goal_total_all'        => $this->fromCents($goalTotalAll),
            'challenger_goal_total' => $this->fromCents($challengerGoalTotal),
            'bet_detail_count'      => count($betDetails),
        ];

        $oldSource = isset($currentRaw['source_files']) && is_array($currentRaw['source_files'])
            ? $currentRaw['source_files']
            : [];
        $sourceFiles = [
            'bet_excel'  => isset($oldSource['bet_excel']) ? $oldSource['bet_excel'] : '',
            'goal_excel' => isset($oldSource['goal_excel']) ? $oldSource['goal_excel'] : '',
            'last_edit_source' => 'crm_config_page',
            'last_saved_at'    => date('Y-m-d H:i:s'),
            'last_saved_by'    => [
                'admin_id' => isset($operator['admin_id']) ? (int)$operator['admin_id'] : (int)session('aid'),
                'username' => isset($operator['username'])
                    ? (string)$operator['username']
                    : (string)session('username'),
            ],
        ];

        $oldVersion = isset($currentRaw['config_version']) ? (string)$currentRaw['config_version'] : '';
        $configVersion = $this->nextConfigVersion($oldVersion);

        return [
            'config_version'       => $configVersion,
            'source_files'         => $sourceFiles,
            'activity'             => $normalized['activity'],
            'name_aliases'         => $normalized['name_aliases'],
            'challengers'          => $challengers,
            'bettors'              => $bettors,
            'bet_details'          => $betDetails,
            'goals_all_by_display' => $normalized['goals_all_by_display'],
            'checksums'            => $checksums,
        ];
    }

    /**
     * @param array<int,array<string,mixed>> $challengers
     * @param array<int,array<string,mixed>> $betDetails
     * @return array<int,array<string,mixed>>
     */
    private function refillChallengerBetSummary(array $challengers, array $betDetails)
    {
        $totals = [];
        $bettors = [];
        foreach ($betDetails as $d) {
            $name = (string)$d['challenger_display_name'];
            if (!isset($totals[$name])) {
                $totals[$name] = 0;
                $bettors[$name] = [];
            }
            $totals[$name] += $this->toCents($d['amount']);

            $bettorKey = '';
            if ($d['bettor_admin_id'] !== null && $d['bettor_admin_id'] !== '') {
                $bettorKey = 'id:' . (int)$d['bettor_admin_id'];
            } else {
                $bettorKey = 'name:' . (string)$d['bettor_display_name'] . '|' . (string)$d['bettor_excel_name'];
            }
            $bettors[$name][$bettorKey] = true;
        }

        foreach ($challengers as &$c) {
            $name = $c['display_name'];
            $c['excel_bet_total'] = isset($totals[$name]) ? $this->fromCents($totals[$name]) : 0;
            $c['excel_bettor_count'] = isset($bettors[$name]) ? count($bettors[$name]) : 0;
        }
        unset($c);

        return $challengers;
    }

    /**
     * @param float|int|string $total
     * @param int $n
     * @return array<int,int|float>
     */
    public function splitAmount($total, $n)
    {
        $n = (int)$n;
        if ($n <= 0) {
            throw new \InvalidArgumentException('均分对象数量必须 > 0。');
        }
        $totalCents = $this->toCents($total);
        $base = intdiv($totalCents, $n);
        $shares = [];
        $allocated = 0;
        for ($i = 0; $i < $n; $i++) {
            if ($i === $n - 1) {
                $cents = $totalCents - $allocated;
            } else {
                $cents = $base;
                $allocated += $cents;
            }
            $shares[] = $this->fromCents($cents);
        }

        return $shares;
    }

    /**
     * @param string $oldVersion
     * @return string
     */
    public function nextConfigVersion($oldVersion)
    {
        $today = date('Y.m.d');
        $oldVersion = trim((string)$oldVersion);
        if (preg_match('/^(\d{4}\.\d{2}\.\d{2})\.(\d+)$/', $oldVersion, $m)) {
            if ($m[1] === $today) {
                return $today . '.' . ((int)$m[2] + 1);
            }
        }

        return $today . '.1';
    }

    /**
     * @param array<string,mixed> $config
     * @param array<int,int> $adminIds
     * @return void
     */
    private function validateBuiltConfig(array $config, array $adminIds)
    {
        $requiredKeys = [
            'config_version', 'source_files', 'activity', 'name_aliases',
            'challengers', 'bettors', 'bet_details', 'goals_all_by_display', 'checksums',
        ];
        foreach ($requiredKeys as $key) {
            if (!array_key_exists($key, $config)) {
                throw new \RuntimeException("生成配置缺少字段：{$key}");
            }
        }
        if (!is_array($config['challengers']) || count($config['challengers']) < 1) {
            throw new \RuntimeException('生成配置挑战人无效。');
        }
        if (!is_array($config['bettors']) || count($config['bettors']) < 1) {
            throw new \RuntimeException('生成配置押注人无效。');
        }
        if (!is_array($config['bet_details']) || count($config['bet_details']) < 1) {
            throw new \RuntimeException('生成配置押注明细无效。');
        }

        $betCents = 0;
        foreach ($config['bettors'] as $b) {
            $betCents += $this->toCents($b['bet_amount']);
        }
        $detailCents = 0;
        foreach ($config['bet_details'] as $d) {
            $detailCents += $this->toCents($d['amount']);
        }
        if ($betCents !== $detailCents) {
            throw new \RuntimeException('校验失败：押注人金额合计与明细合计不一致。');
        }

        $cs = $config['checksums'];
        if ((int)$cs['challenger_count'] !== count($config['challengers'])
            || (int)$cs['bettor_count'] !== count($config['bettors'])
            || (int)$cs['bet_detail_count'] !== count($config['bet_details'])
            || $this->toCents($cs['bet_total']) !== $betCents
        ) {
            throw new \RuntimeException('校验失败：checksums 与实际数据不一致。');
        }

        // 再确认挑战人 admin
        foreach ($config['challengers'] as $c) {
            $aid = isset($c['admin_id']) ? (int)$c['admin_id'] : 0;
            if ($aid <= 0 || !isset($adminIds[$aid])) {
                throw new \RuntimeException('校验失败：存在无效挑战人 admin_id。');
            }
        }
    }

    /**
     * @param array<string,mixed> $config
     * @return array<int,string>
     */
    private function collectWarnings(array $config)
    {
        $warnings = [];
        $goals = isset($config['goals_all_by_display']) && is_array($config['goals_all_by_display'])
            ? $config['goals_all_by_display']
            : [];
        foreach ($config['challengers'] as $c) {
            $name = $c['display_name'];
            if (!array_key_exists($name, $goals)) {
                continue;
            }
            if ($this->toCents($c['target_amount']) !== $this->toCents($goals[$name])) {
                $warnings[] = "挑战人目标与全员目标中的同名人员不一致：{$name}";
            }
        }

        return $warnings;
    }

    /**
     * @param array<string,mixed> $config
     * @return string
     */
    private function buildPhpFileContent(array $config)
    {
        $exported = var_export($config, true);

        return "<?php\n"
            . "/**\n"
            . " * 金秋大战配置\n"
            . " * 自动由 CRM 金秋大战配置页生成\n"
            . " */\n"
            . 'return ' . $exported . ";\n";
    }

    /**
     * @param string $dataDir
     * @param string $content
     * @return string temp path
     */
    private function writeTempFile($dataDir, $content)
    {
        $unique = str_replace('.', '', uniqid('', true));
        $tmpPath = rtrim($dataDir, '\\/') . DIRECTORY_SEPARATOR . 'autumn_battle_person.php.tmp.' . $unique;
        $written = @file_put_contents($tmpPath, $content, LOCK_EX);
        if ($written === false) {
            throw new \RuntimeException('写入临时配置文件失败。');
        }
        if ((int)$written !== strlen($content)) {
            @unlink($tmpPath);
            throw new \RuntimeException('临时配置文件写入长度校验失败。');
        }

        return $tmpPath;
    }

    /**
     * @param string $path
     * @return array<string,mixed>
     */
    private function includeConfigFile($path)
    {
        if (!is_file($path)) {
            throw new \RuntimeException('配置文件不存在：' . $path);
        }
        /** @noinspection PhpIncludeInspection */
        $data = include $path;
        if (!is_array($data)) {
            throw new \RuntimeException('配置文件未返回有效数组。');
        }

        return $data;
    }

    /**
     * @return void
     */
    private function ensureBackupDir()
    {
        $dir = $this->getBackupDir();
        if (!is_dir($dir)) {
            if (!@mkdir($dir, 0755, true) && !is_dir($dir)) {
                throw new \RuntimeException('无法创建配置备份目录：runtime/autumn_config_backup/');
            }
        }
        if (!is_writable($dir)) {
            throw new \RuntimeException('配置备份目录不可写：runtime/autumn_config_backup/');
        }
    }

    /**
     * @param string $officialPath
     * @param string $oldVersion
     * @return void
     */
    private function backupOfficialFile($officialPath, $oldVersion)
    {
        $safeVersion = preg_replace('/[^0-9A-Za-z._-]/', '_', (string)$oldVersion);
        $backupName = 'autumn_battle_person_' . date('Ymd_His') . '_' . $safeVersion . '.php';
        $backupPath = $this->getBackupDir() . DIRECTORY_SEPARATOR . $backupName;
        if (!@copy($officialPath, $backupPath)) {
            throw new \RuntimeException('备份正式配置失败，已中止覆盖。');
        }
        $this->pruneBackups();
    }

    /**
     * 保留最近 BACKUP_KEEP 个备份。
     *
     * @return void
     */
    private function pruneBackups()
    {
        $dir = $this->getBackupDir();
        $files = glob($dir . DIRECTORY_SEPARATOR . 'autumn_battle_person_*.php');
        if (!is_array($files) || count($files) <= self::BACKUP_KEEP) {
            return;
        }
        usort($files, function ($a, $b) {
            return filemtime($a) - filemtime($b);
        });
        $overflow = count($files) - self::BACKUP_KEEP;
        for ($i = 0; $i < $overflow; $i++) {
            @unlink($files[$i]);
        }
    }

    /**
     * 安全替换正式文件。
     *
     * Linux：rename(tmp, official) 通常可原子覆盖。
     * Windows：rename 不能覆盖已存在目标，故：
     *   1) official → sideBackup（同目录旁路）
     *   2) tmp → official
     *   若步骤2失败：sideBackup → official 回滚
     * 禁止：先 unlink(official) 再 rename（中间窗口正式文件不存在）。
     *
     * @param string $tmpPath
     * @param string $officialPath
     * @return void
     */
    private function replaceOfficialFile($tmpPath, $officialPath)
    {
        if (!is_file($tmpPath)) {
            throw new \RuntimeException('临时配置文件不存在，无法替换。');
        }

        $isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
        if (!$isWindows) {
            if (!@rename($tmpPath, $officialPath)) {
                throw new \RuntimeException('无法原子替换正式配置文件。');
            }

            return;
        }

        // Windows 安全替换（带回滚）
        $sideBackup = $officialPath . '.replace_bak.' . str_replace('.', '', uniqid('', true));
        $movedOfficial = false;
        if (is_file($officialPath)) {
            if (!@rename($officialPath, $sideBackup)) {
                throw new \RuntimeException('无法移开旧正式配置文件（Windows）。');
            }
            $movedOfficial = true;
        }

        if (!@rename($tmpPath, $officialPath)) {
            // 回滚正式文件
            if ($movedOfficial && is_file($sideBackup)) {
                @rename($sideBackup, $officialPath);
            }
            throw new \RuntimeException('无法将临时文件替换为正式配置（Windows）。');
        }

        if (is_file($sideBackup)) {
            @unlink($sideBackup);
        }
    }

    /**
     * @param mixed $amount
     * @return int|float
     */
    private function normalizeMoney($amount)
    {
        if (is_string($amount)) {
            $amount = trim($amount);
        }
        if ($amount === '' || $amount === null) {
            return 0;
        }
        if (!is_numeric($amount)) {
            throw new \InvalidArgumentException('金额必须为数字。');
        }

        return $this->fromCents($this->toCents($amount));
    }

    /**
     * @param mixed $amount
     * @return bool
     */
    private function isValidNonNegativeMoney($amount)
    {
        if (!is_numeric($amount)) {
            return false;
        }
        if ((float)$amount < 0) {
            return false;
        }

        return $this->hasAtMostTwoDecimals($amount);
    }

    /**
     * @param mixed $amount
     * @return bool
     */
    private function isValidPositiveMoney($amount)
    {
        if (!$this->isValidNonNegativeMoney($amount)) {
            return false;
        }

        return $this->toCents($amount) > 0;
    }

    /**
     * @param mixed $amount
     * @return bool
     */
    private function hasAtMostTwoDecimals($amount)
    {
        $s = trim((string)$amount);
        if (!preg_match('/^-?\d+(\.\d{1,2})?$/', $s)) {
            // 允许科学计数法等经 toCents 可表达的值：再比较分
            $cents = $this->toCents($amount);
            $back = $this->fromCents($cents);

            return abs(((float)$amount) - ((float)$back)) < 0.00001;
        }

        return true;
    }

    /**
     * @param mixed $amount
     * @return int
     */
    private function toCents($amount)
    {
        return (int)round(((float)$amount) * 100);
    }

    /**
     * @param int $cents
     * @return int|float
     */
    private function fromCents($cents)
    {
        $cents = (int)$cents;
        $val = round($cents / 100, 2);
        if (abs($val - (int)$val) < 0.00001) {
            return (int)$val;
        }

        return $val;
    }
}
