<?php
// ===== 0. 初始化 =====
require __DIR__ . '/thinkphp/base.php';
\think\Container::get('app')->path(__DIR__ . '/application/')->initialize();

/**
 * LIKE 通配符转义（保留模糊查询的首尾 %，仅字面化用户输入中的 % _ \）
 */
function conflict_escape_like($value)
{
    return strtr((string)$value, [
        '\\' => '\\\\',
        '%'  => '\%',
        '_'  => '\_',
    ]);
}

/**
 * 连接 / 重连 Redis（有限重试；失败则抛出由外层 sleep 后继续）
 */
function conflict_redis_connect(\Redis $redis = null)
{
    if (!$redis) {
        $redis = new \Redis();
    }
    if (!$redis->connect('127.0.0.1', 26739, 2.0)) {
        throw new \RuntimeException('redis_connect_failed');
    }
    $redis->auth('csE88ifakDGC8PfH');
    return $redis;
}

/**
 * 仅允许 pending/processing → failed；禁止覆盖 done，禁止状态缺失时复活
 */
function conflict_mark_failed(\Redis $redis, $statusKey, $errorKey, $ttl, $reason)
{
    $markFailedLua = <<<'LUA'
local st = redis.call('GET', KEYS[1])
if st ~= 'pending' and st ~= 'processing' then
  return 0
end
redis.call('SET', KEYS[1], 'failed', 'EX', tonumber(ARGV[1]))
redis.call('SET', KEYS[2], ARGV[2], 'EX', tonumber(ARGV[1]))
return 1
LUA;
    try {
        if (method_exists($redis, 'eval')) {
            return ((int)$redis->eval($markFailedLua, [$statusKey, $errorKey, (string)$ttl, (string)$reason], 2)) === 1;
        }
        $cur = $redis->get($statusKey);
        if ($cur !== 'pending' && $cur !== 'processing') {
            return false;
        }
        $redis->set($statusKey, 'failed', ['ex' => $ttl]);
        $redis->set($errorKey, (string)$reason, ['ex' => $ttl]);
        return true;
    } catch (\Throwable $e) {
        echo '[' . date('Y-m-d H:i:s') . "] mark failed error reason={$reason}\n";
        return false;
    }
}

/**
 * 计算自 $start 起的毫秒耗时（PHP 7.4 兼容）
 */
function conflict_perf_ms($start)
{
    return round((microtime(true) - (float)$start) * 1000, 2);
}

/**
 * 是否输出性能摘要：失败全量 / 慢任务(>1000ms)全量 / 快速成功约 10% 抽样
 * 抽样仅影响日志，不影响任务结果与 Redis 状态
 */
function conflict_perf_should_log($workerTotalMs, $isFailed)
{
    if ($isFailed) {
        return true;
    }
    if ((float)$workerTotalMs > 1000) {
        return true;
    }
    try {
        return mt_rand(1, 100) <= 10;
    } catch (\Throwable $e) {
        return false;
    }
}

/**
 * 单行结构化性能日志；输出失败不得影响业务
 * 严禁写入手机号/客户名/联系方式/密码/payload/SQL 参数
 */
function conflict_perf_log($taskId, array $metrics)
{
    try {
        $line = '[conflict_perf]'
            . ' task_id=' . (string)$taskId
            . ' sql_name_ms=' . round((float)($metrics['sql_name_ms'] ?? 0), 2)
            . ' sql_contact_ms=' . round((float)($metrics['sql_contact_ms'] ?? 0), 2)
            . ' sql_map_ms=' . round((float)($metrics['sql_map_ms'] ?? 0), 2)
            . ' aggregate_ms=' . round((float)($metrics['aggregate_ms'] ?? 0), 2)
            . ' redis_save_ms=' . round((float)($metrics['redis_save_ms'] ?? 0), 2)
            . ' worker_total_ms=' . round((float)($metrics['worker_total_ms'] ?? 0), 2)
            . ' hit_count=' . (int)($metrics['hit_count'] ?? 0)
            . ' status=' . (string)($metrics['status'] ?? '');
        if (!empty($metrics['fail_code'])) {
            $line .= ' fail_code=' . (string)$metrics['fail_code'];
        }
        if (array_key_exists('fail_saved', $metrics)) {
            $line .= ' fail_saved=' . (!empty($metrics['fail_saved']) ? '1' : '0');
        }
        echo $line . "\n";
    } catch (\Throwable $e) {
        // ignore log errors
    }
}

// ===== 1. 连接 Redis 队列 =====
$redis = null;
$reconnectAttempts = 0;
try {
    $redis = conflict_redis_connect();
} catch (\Throwable $e) {
    fwrite(STDERR, "Redis connect failed on startup\n");
    exit(1); // 交给 Supervisor 重启
}

echo "Starting conflict check queue worker for 外贸CRM...\n";

while (true) {

    try {
        /* ---------- 2. 阻塞取任务 ---------- */
        $job = $redis->blPop(['waimao_conflict_queue'], 5);  // 短阻塞，便于感知断线
        if (!$job) {
            $reconnectAttempts = 0;
            continue;
        }

        $payload = json_decode($job[1], true);
        if (!$payload) continue;

        $taskId        = $payload['id']      ?? '';
        $keywordOrigin = $payload['keyword'] ?? '';

        if (!$taskId || $keywordOrigin === '') continue;

        $resultKey = 'waimao_conflict_result:' . $taskId;
        $statusKey = 'waimao_conflict_status:' . $taskId;
        $errorKey  = 'waimao_conflict_error:' . $taskId;
        $ttl       = 300;

        // 仅 pending（兼容旧 Worker 已写 processing）可领取；状态缺失/done/failed 一律跳过，禁止复活过期任务
        $markProcessingLua = <<<'LUA'
local st = redis.call('GET', KEYS[1])
if st ~= 'pending' and st ~= 'processing' then
  return 0
end
redis.call('SET', KEYS[1], 'processing', 'EX', tonumber(ARGV[1]))
return 1
LUA;
        $claimed = false;
        try {
            if (method_exists($redis, 'eval')) {
                $ret = $redis->eval($markProcessingLua, [$statusKey, (string)$ttl], 1);
                $claimed = ((int)$ret) === 1;
            } else {
                $cur = $redis->get($statusKey);
                if ($cur === 'pending' || $cur === 'processing') {
                    $redis->set($statusKey, 'processing', ['ex' => $ttl]);
                    $claimed = true;
                }
            }
        } catch (\Throwable $e) {
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} mark processing failed\n";
            $claimed = false;
        }

        if (!$claimed) {
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} skip claim (status missing/terminal)\n";
            continue;
        }

        // 性能监控：自领取成功后开始计时（不含队列等待）
        $workerStart = microtime(true);
        $perf = [
            'sql_name_ms' => 0.0,
            'sql_contact_ms' => 0.0,
            'sql_map_ms' => 0.0,
            'aggregate_ms' => 0.0,
            'redis_save_ms' => 0.0,
            'worker_total_ms' => 0.0,
            'hit_count' => 0,
            'status' => '',
            'fail_code' => '',
        ];

        /* ---------- 3. 清洗关键词 ---------- */
        $keyword = trim(preg_replace('/[+\-\s]/', '', $keywordOrigin));
        if ($keyword === '') {
            $failSaved = conflict_mark_failed($redis, $statusKey, $errorKey, $ttl, 'empty_keyword');
            $perf['worker_total_ms'] = conflict_perf_ms($workerStart);
            $perf['status'] = 'failed';
            $perf['fail_code'] = 'empty_keyword';
            $perf['fail_saved'] = $failSaved;
            if (conflict_perf_should_log($perf['worker_total_ms'], true)) {
                conflict_perf_log($taskId, $perf);
            }
            continue;
        }
        $likeKeyword = '%' . conflict_escape_like($keyword) . '%';
        $likeOrigin  = '%' . conflict_escape_like($keywordOrigin) . '%';

        /* ---------- 4. 查询数据库 ---------- */
        $rawList = [];
        $leadsIds = []; // 收集所有匹配的客户ID
        $queryFailed = false;
        $perfSqlStage = '';
        $perfSqlStageStart = 0.0;

        try {
            // 4-1 客户名称匹配
            $leadsQuery = \think\Db::name('crm_leads')
                ->alias('l')
                ->field([
                    'l.id', 'l.kh_name', 'l.kh_status', 'l.inquiry_id', 'l.port_id',
                    'l.at_user', 'l.at_time', 'l.pr_gh_type', 'l.pr_user',
                    \think\Db::raw('NULL AS contact_type'),
                    \think\Db::raw('NULL AS contact_value')
                ])
                ->whereLike('l.kh_name', $likeKeyword);
            $perfSqlStage = 'name';
            $perfSqlStageStart = microtime(true);
            $leadsList = $leadsQuery->select();
            $perf['sql_name_ms'] = conflict_perf_ms($perfSqlStageStart);

            // 4-2 联系方式匹配（参数绑定，防 SQL 注入）
            // TP5.1 闭包 where 会 newQuery()；whereOrRaw(?, [$bind]) 的 bindParams 落在临时
            // newQuery，占位符写入 Expression 后进入父 SQL，绑定值却随 newQuery 丢弃 → HY093。
            // 父查询预绑定唯一命名参数，闭包内 whereOrRaw 只引用该名、不传 bind 数组。
            $contactsQuery = \think\Db::name('crm_contacts')
                ->alias('c')
                ->leftJoin('crm_leads l', 'l.id = c.leads_id')
                ->where('c.is_delete', 0)
                ->bind([
                    'cf_concat_like' => [$likeKeyword, \PDO::PARAM_STR],
                ])
                ->where(function ($q) use ($likeKeyword, $likeOrigin, $keyword, $keywordOrigin) {
                    $q->whereLike('c.contact_value', $likeKeyword)
                      ->whereOr('c.vdigits', 'like', $likeKeyword)
                      ->whereOrRaw(
                          'CONCAT(c.contact_extra, c.contact_value) LIKE :cf_concat_like'
                      );
                    if ($keywordOrigin !== $keyword) {
                        // 原串（含空格 / + -）再查一次，避免漏匹配
                        $q->whereOr('c.contact_value', 'like', $likeOrigin);
                    }
                })
                ->field([
                    'l.id', 'l.kh_name', 'l.kh_status', 'l.inquiry_id', 'l.port_id',
                    'l.at_user', 'l.at_time', 'l.pr_gh_type', 'l.pr_user',
                    'c.contact_type', 'c.contact_value', 'c.vdigits', 'c.id AS contact_id'
                ]);
            $perfSqlStage = 'contact';
            $perfSqlStageStart = microtime(true);
            $contactsList = $contactsQuery->select();
            $perf['sql_contact_ms'] = conflict_perf_ms($perfSqlStageStart);

            // 合并结果（计入后续 aggregate_ms）
            $perfAggMergeStart = microtime(true);
            foreach ($leadsList as $row) {
                $rawList[] = $row;
                $leadsIds[$row['id']] = $row['id'];
            }
            foreach ($contactsList as $row) {
                if (!isset($leadsIds[$row['id']])) {
                    $rawList[] = $row;
                    $leadsIds[$row['id']] = $row['id'];
                } else {
                    // 如果客户已存在，添加联系方式信息以便后续匹配
                    $rawList[] = $row;
                }
            }
            $perf['aggregate_ms'] = conflict_perf_ms($perfAggMergeStart);

        } catch (\Throwable $e) {
            // SQL / Error 异常不得伪装成“查重成功无数据”
            // 保留已完成阶段耗时；对当前失败阶段补记耗时
            if ($perfSqlStage === 'name' && $perfSqlStageStart > 0) {
                $perf['sql_name_ms'] = conflict_perf_ms($perfSqlStageStart);
            } elseif ($perfSqlStage === 'contact' && $perfSqlStageStart > 0) {
                $perf['sql_contact_ms'] = conflict_perf_ms($perfSqlStageStart);
            }
            $queryFailed = true;
            $rawList = [];
            $leadsIds = [];
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} db_query_exception\n";

            // 临时诊断：仅记录异常类型与安全错误码（不输出 message/SQL/参数/用户输入）
            try {
                $dbgStage = ($perfSqlStage === 'name' || $perfSqlStage === 'contact') ? $perfSqlStage : 'unknown';
                $dbgClass = get_class($e);
                if (!is_string($dbgClass) || !preg_match('/^[A-Za-z0-9_\\\\]+$/', $dbgClass)) {
                    $dbgClass = 'unknown';
                }
                $dbgCode = 'unknown';
                $dbgCodeRaw = $e->getCode();
                if (is_int($dbgCodeRaw)) {
                    $dbgCode = (string)$dbgCodeRaw;
                } elseif (is_string($dbgCodeRaw) && preg_match('/^[A-Za-z0-9_\-]{1,32}$/', $dbgCodeRaw)) {
                    $dbgCode = $dbgCodeRaw;
                }
                $dbgSqlState = 'unknown';
                $dbgDriver = 'unknown';
                if ($e instanceof \PDOException && isset($e->errorInfo) && is_array($e->errorInfo)) {
                    if (isset($e->errorInfo[0]) && is_string($e->errorInfo[0])
                        && preg_match('/^[A-Za-z0-9]{1,8}$/', $e->errorInfo[0])) {
                        $dbgSqlState = $e->errorInfo[0];
                    }
                    if (isset($e->errorInfo[1]) && (is_int($e->errorInfo[1])
                        || (is_string($e->errorInfo[1]) && preg_match('/^-?[0-9]{1,10}$/', $e->errorInfo[1])))) {
                        $dbgDriver = (string)$e->errorInfo[1];
                    }
                }
                if (method_exists($e, 'getData')) {
                    $dbgData = $e->getData();
                    if (is_array($dbgData) && isset($dbgData['PDO Error Info']) && is_array($dbgData['PDO Error Info'])) {
                        $pdoInfo = $dbgData['PDO Error Info'];
                        if ($dbgSqlState === 'unknown' && isset($pdoInfo['SQLSTATE'])
                            && is_string($pdoInfo['SQLSTATE'])
                            && preg_match('/^[A-Za-z0-9]{1,8}$/', $pdoInfo['SQLSTATE'])) {
                            $dbgSqlState = $pdoInfo['SQLSTATE'];
                        }
                        if ($dbgDriver === 'unknown' && isset($pdoInfo['Driver Error Code'])) {
                            $drv = $pdoInfo['Driver Error Code'];
                            if (is_int($drv) || (is_string($drv) && preg_match('/^-?[0-9]{1,10}$/', $drv))) {
                                $dbgDriver = (string)$drv;
                            }
                        }
                    }
                }
                // uniqid('', true) 含小数点，白名单纳入 `.` 仅用于安全关联日志
                $dbgTaskId = (is_string($taskId) && preg_match('/^[A-Za-z0-9_\-.]{1,128}$/', $taskId))
                    ? $taskId : 'unknown';
                echo '[conflict_db_debug] task_id=' . $dbgTaskId
                    . ' stage=' . $dbgStage
                    . ' exception_class=' . $dbgClass
                    . ' exception_code=' . $dbgCode
                    . ' sqlstate=' . $dbgSqlState
                    . ' driver_code=' . $dbgDriver
                    . "\n";
            } catch (\Throwable $dbgIgnore) {
                // 诊断失败不得影响原有 failed 处理
            }
        }

        if ($queryFailed) {
            $failSaved = conflict_mark_failed($redis, $statusKey, $errorKey, $ttl, 'db_query_exception');
            $perf['worker_total_ms'] = conflict_perf_ms($workerStart);
            $perf['status'] = 'failed';
            $perf['fail_code'] = 'db_query_exception';
            $perf['fail_saved'] = $failSaved;
            if (conflict_perf_should_log($perf['worker_total_ms'], true)) {
                conflict_perf_log($taskId, $perf);
            }
            continue;
        }

        /* ---------- 5~8. 聚合 / 映射 / 排序（捕获 Throwable，避免单任务打崩 Worker） ---------- */
        $finalList = [];
        try {
            $perfAggStart = microtime(true);
            $leadsMap = [];
            $contactMap = [];

            foreach ($rawList as $row) {
                $leadsId = $row['id'];

                if (!isset($leadsMap[$leadsId])) {
                    $leadsMap[$leadsId] = [
                        'id' => $row['id'],
                        'kh_name' => $row['kh_name'],
                        'kh_status' => $row['kh_status'] ?? '',
                        'inquiry_id' => $row['inquiry_id'] ?? '',
                        'port_id' => $row['port_id'] ?? '',
                        'at_user' => $row['at_user'] ?? '',
                        'at_time' => $row['at_time'] ?? '',
                        'pr_gh_type' => $row['pr_gh_type'] ?? '',
                        'pr_user' => $row['pr_user'] ?? '',
                        'repeat_info' => '客户名称重复',
                        'main_phone' => '',
                        'assist_phone' => '',
                        'matched_contact' => null
                    ];
                }

                if (isset($row['contact_type']) && $row['contact_type'] !== null) {
                    $contactType = (int)$row['contact_type'];
                    $contactValue = $row['contact_value'] ?? '';

                    $vdigits = $row['vdigits'] ?? '';
                    if ($contactValue && (
                        stripos($contactValue, $keyword) !== false ||
                        stripos($contactValue, $keywordOrigin) !== false ||
                        ($vdigits && stripos($vdigits, $keyword) !== false)
                    )) {
                        $leadsMap[$leadsId]['matched_contact'] = [
                            'type' => $contactType,
                            'value' => $contactValue
                        ];
                    }

                    if (!isset($contactMap[$leadsId])) {
                        $contactMap[$leadsId] = [
                            'main' => [],
                            'assist' => []
                        ];
                    }

                    if ($contactType == 1) {
                        $contactMap[$leadsId]['main'][] = [
                            'value' => $contactValue,
                            'id' => $row['contact_id'] ?? 0
                        ];
                    } elseif ($contactType == 3) {
                        $contactMap[$leadsId]['assist'][] = [
                            'value' => $contactValue,
                            'id' => $row['contact_id'] ?? 0
                        ];
                    }
                }
            }

            foreach ($leadsMap as $leadsId => &$lead) {
                if (!isset($lead['main_phone'])) {
                    $lead['main_phone'] = '';
                }
                if (!isset($lead['assist_phone'])) {
                    $lead['assist_phone'] = '';
                }

                if (isset($contactMap[$leadsId])) {
                    if (!empty($contactMap[$leadsId]['main'])) {
                        $matchedMain = null;
                        foreach ($contactMap[$leadsId]['main'] as $phone) {
                            $phoneValue = $phone['value'] ?? '';
                            if ($phoneValue && (
                                stripos($phoneValue, $keyword) !== false ||
                                stripos($phoneValue, $keywordOrigin) !== false
                            )) {
                                $matchedMain = $phoneValue;
                                break;
                            }
                        }
                        $lead['main_phone'] = $matchedMain ?: ($contactMap[$leadsId]['main'][0]['value'] ?? '');
                    }

                    if (!empty($contactMap[$leadsId]['assist'])) {
                        $matchedAssist = null;
                        foreach ($contactMap[$leadsId]['assist'] as $phone) {
                            $phoneValue = $phone['value'] ?? '';
                            if ($phoneValue && (
                                stripos($phoneValue, $keyword) !== false ||
                                stripos($phoneValue, $keywordOrigin) !== false
                            )) {
                                $matchedAssist = $phoneValue;
                                break;
                            }
                        }
                        $lead['assist_phone'] = $matchedAssist ?: ($contactMap[$leadsId]['assist'][0]['value'] ?? '');
                    }
                }

                if ($lead['matched_contact']) {
                    $mc = $lead['matched_contact'];
                    switch ($mc['type']) {
                        case 1: $lead['repeat_info'] = '主电话：' . $mc['value']; break;
                        case 2: $lead['repeat_info'] = '邮箱：' . $mc['value']; break;
                        case 3: $lead['repeat_info'] = '辅助电话：' . $mc['value']; break;
                        case 4: $lead['repeat_info'] = '阿里ID：' . $mc['value']; break;
                        case 5: $lead['repeat_info'] = '微信：' . $mc['value']; break;
                        default: $lead['repeat_info'] = '未知类型(' . $mc['type'] . ')：' . $mc['value'];
                    }
                }
            }
            unset($lead);

            $inquiryMap = [];
            $portMap = [];
            $mapSqlPendingStart = null;
            try {
                $inquiryIds = array_filter(array_unique(array_column($leadsMap, 'inquiry_id')));
                if (!empty($inquiryIds)) {
                    $mapSqlPendingStart = microtime(true);
                    $inquiryList = \think\Db::table('crm_inquiry')->whereIn('id', $inquiryIds)->select();
                    $perf['sql_map_ms'] += conflict_perf_ms($mapSqlPendingStart);
                    $mapSqlPendingStart = null;
                    foreach ($inquiryList as $inq) {
                        $inquiryMap[$inq['id']] = $inq['inquiry_name'] ?? '';
                    }
                }

                $portIds = [];
                foreach ($leadsMap as $lead) {
                    if (!empty($lead['port_id'])) {
                        $ports = explode(',', $lead['port_id']);
                        foreach ($ports as $pid) {
                            $pid = trim($pid);
                            if ($pid && is_numeric($pid)) {
                                $portIds[] = (int)$pid;
                            }
                        }
                    }
                }
                $portIds = array_unique($portIds);
                if (!empty($portIds)) {
                    $mapSqlPendingStart = microtime(true);
                    $portList = \think\Db::table('crm_inquiry_port')->whereIn('id', $portIds)->select();
                    $perf['sql_map_ms'] += conflict_perf_ms($mapSqlPendingStart);
                    $mapSqlPendingStart = null;
                    foreach ($portList as $port) {
                        $portMap[$port['id']] = $port['port_name'] ?? '';
                    }
                }
            } catch (\Throwable $e) {
                // 映射失败不阻断主流程，渠道/端口显示为空
                if ($mapSqlPendingStart !== null) {
                    $perf['sql_map_ms'] += conflict_perf_ms($mapSqlPendingStart);
                    $mapSqlPendingStart = null;
                }
            }
            $perf['sql_map_ms'] = round((float)$perf['sql_map_ms'], 2);

            foreach ($leadsMap as $lead) {
                $lead['channel_name'] = '';
                if (!empty($lead['inquiry_id'])) {
                    $lead['channel_name'] = $inquiryMap[$lead['inquiry_id']] ?? '';
                }

                $lead['port_name'] = '';
                if (!empty($lead['port_id'])) {
                    $ports = explode(',', $lead['port_id']);
                    $portNames = [];
                    foreach ($ports as $pid) {
                        $pid = trim($pid);
                        if ($pid && is_numeric($pid) && isset($portMap[(int)$pid])) {
                            $portNames[] = $portMap[(int)$pid];
                        }
                    }
                    $lead['port_name'] = implode(',', $portNames);
                }

                unset($lead['matched_contact'], $lead['inquiry_id'], $lead['port_id']);
                $finalList[] = $lead;
            }

            usort($finalList, function ($a, $b) {
                return strtotime($b['at_time']) <=> strtotime($a['at_time']);
            });

            // aggregate_ms = 前置合并 + 本段总耗时 - 内部映射 SQL 耗时（保证非负）
            $aggBlockMs = conflict_perf_ms($perfAggStart);
            $perf['aggregate_ms'] = round(max(0, (float)$perf['aggregate_ms'] + $aggBlockMs - (float)$perf['sql_map_ms']), 2);
        } catch (\Throwable $e) {
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} aggregate_exception\n";
            if (isset($perfAggStart)) {
                $aggBlockMs = conflict_perf_ms($perfAggStart);
                $perf['aggregate_ms'] = round(max(0, (float)$perf['aggregate_ms'] + $aggBlockMs - (float)$perf['sql_map_ms']), 2);
            }
            $failSaved = conflict_mark_failed($redis, $statusKey, $errorKey, $ttl, 'aggregate_exception');
            $perf['worker_total_ms'] = conflict_perf_ms($workerStart);
            $perf['status'] = 'failed';
            $perf['fail_code'] = 'aggregate_exception';
            $perf['fail_saved'] = $failSaved;
            if (conflict_perf_should_log($perf['worker_total_ms'], true)) {
                conflict_perf_log($taskId, $perf);
            }
            continue;
        }

        /* ---------- 9. 写入 Redis 结果 & 状态 ---------- */
        // 空结果也视为成功 done（与“查询失败”严格区分）
        $resultJson = json_encode($finalList, JSON_UNESCAPED_UNICODE);
        if ($resultJson === false) {
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} json_encode_failed\n";
            $failSaved = conflict_mark_failed($redis, $statusKey, $errorKey, $ttl, 'json_encode_failed');
            $perf['hit_count'] = is_array($finalList) ? count($finalList) : 0;
            $perf['worker_total_ms'] = conflict_perf_ms($workerStart);
            $perf['status'] = 'failed';
            $perf['fail_code'] = 'json_encode_failed';
            $perf['fail_saved'] = $failSaved;
            if (conflict_perf_should_log($perf['worker_total_ms'], true)) {
                conflict_perf_log($taskId, $perf);
            }
            continue;
        }

        // 仅 processing/pending 可写 done；状态缺失/failed/done 禁止覆盖（防旧 Worker / 过期复活）
        $markDoneLua = <<<'LUA'
local st = redis.call('GET', KEYS[2])
if st ~= 'processing' and st ~= 'pending' then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SET', KEYS[2], 'done', 'EX', tonumber(ARGV[2]))
return 1
LUA;
        $saved = false;
        $tRedisSave = microtime(true);
        try {
            if (method_exists($redis, 'eval')) {
                $ret = $redis->eval($markDoneLua, [$resultKey, $statusKey, $resultJson, (string)$ttl], 2);
                $saved = ((int)$ret) === 1;
            } else {
                $cur = $redis->get($statusKey);
                if ($cur === 'processing' || $cur === 'pending') {
                    $ok1 = $redis->set($resultKey, $resultJson, ['ex' => $ttl]);
                    $ok2 = $redis->set($statusKey, 'done', ['ex' => $ttl]);
                    $saved = ($ok1 !== false && $ok2 !== false);
                }
            }
        } catch (\Throwable $e) {
            $saved = false;
            echo '[' . date('Y-m-d H:i:s') . "] task={$taskId} save_result_exception\n";
        }
        $perf['redis_save_ms'] = conflict_perf_ms($tRedisSave);
        $perf['hit_count'] = is_array($finalList) ? count($finalList) : 0;
        $perf['worker_total_ms'] = conflict_perf_ms($workerStart);

        if (!$saved) {
            // 结果保存失败：仅在仍为 processing/pending 时标记 failed
            $failSaved = conflict_mark_failed($redis, $statusKey, $errorKey, $ttl, 'save_result_failed');
            $perf['status'] = 'failed';
            $perf['fail_code'] = 'save_result_failed';
            $perf['fail_saved'] = $failSaved;
            // 失败写入后刷新总耗时（含 mark_failed）
            $perf['worker_total_ms'] = conflict_perf_ms($workerStart);
            if (conflict_perf_should_log($perf['worker_total_ms'], true)) {
                conflict_perf_log($taskId, $perf);
            }
        } else {
            $perf['status'] = 'done';
            if (conflict_perf_should_log($perf['worker_total_ms'], false)) {
                conflict_perf_log($taskId, $perf);
            }
        }

        $reconnectAttempts = 0;
        // —— 循环下一任务 ——
    } catch (\Throwable $e) {
        // Redis 断线等：有限重连；持续失败交给 Supervisor 重启
        $reconnectAttempts++;
        echo '[' . date('Y-m-d H:i:s') . "] worker loop exception attempt={$reconnectAttempts}\n";
        if ($reconnectAttempts > 10) {
            fwrite(STDERR, "Redis reconnect exceeded, exit for supervisor restart\n");
            exit(1);
        }
        sleep(min(5, $reconnectAttempts));
        try {
            $redis = conflict_redis_connect($redis);
        } catch (\Throwable $re) {
            // 下一轮继续重试 / 最终 exit
        }
    }
}
