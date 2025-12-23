<?php
// ===== 0. 初始化 =====
require __DIR__ . '/thinkphp/base.php';
\think\Container::get('app')->path(__DIR__ . '/application/')->initialize();

// ===== 1. 连接 Redis 队列 =====
$redis = new \Redis();
$redis->connect('127.0.0.1', 26739);
$redis->auth('csE88ifakDGC8PfH');   // 如有密码请取消注释

echo "Starting conflict check queue worker for 外贸CRM...\n";

while (true) {

    /* ---------- 2. 阻塞取任务 ---------- */
    $job = $redis->blPop(['waimao_conflict_queue'], 0);  // 使用外贸前缀的队列键
    if (!$job) continue;

    $payload = json_decode($job[1], true);
    if (!$payload) continue;

    $taskId        = $payload['id']      ?? '';
    $keywordOrigin = $payload['keyword'] ?? '';

    if (!$taskId || $keywordOrigin === '') continue;

    /* ---------- 3. 清洗关键词 ---------- */
    $keyword = trim(preg_replace('/[+\-\s]/', '', $keywordOrigin));

    /* ---------- 4. 查询数据库 ---------- */
    $rawList = [];
    $leadsIds = []; // 收集所有匹配的客户ID

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
            ->whereLike('l.kh_name', "%{$keyword}%");
        $leadsList = $leadsQuery->select();
        
        // 4-2 联系方式匹配
        $contactsQuery = \think\Db::name('crm_contacts')
            ->alias('c')
            ->leftJoin('crm_leads l', 'l.id = c.leads_id')
            ->where('c.is_delete', 0)
            ->where(function ($q) use ($keyword, $keywordOrigin) {
                $q->whereLike('c.contact_value', "%{$keyword}%")
                ->whereOr('c.vdigits', 'like',"%{$keyword}%")
                  ->whereOrRaw("CONCAT(c.contact_extra, c.contact_value) LIKE '%{$keyword}%'");
                if ($keywordOrigin !== $keyword) {
                    // 原串（含空格 / + -）再查一次，避免漏匹配
                    $q->whereOr('c.contact_value', 'like',"%{$keywordOrigin}%");
                }
            })
            ->field([
                'l.id', 'l.kh_name', 'l.kh_status', 'l.inquiry_id', 'l.port_id',
                'l.at_user', 'l.at_time', 'l.pr_gh_type', 'l.pr_user',
                'c.contact_type', 'c.contact_value', 'c.vdigits', 'c.id AS contact_id'
            ]);
        $contactsList = $contactsQuery->select();

        // 合并结果
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

    } catch (\Exception $e) {
        // 记录或忽略异常
        $rawList = [];
        $leadsIds = [];
    }

    /* ---------- 5. 按 leads_id 聚合数据，生成 repeat_info，收集电话信息 ---------- */
    $leadsMap = []; // 以 leads_id 为 key 聚合数据
    $contactMap = []; // 收集联系方式信息，用于匹配关键词选择电话

    foreach ($rawList as $row) {
        $leadsId = $row['id'];
        
        // 如果该客户还未初始化，初始化基础信息
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
                'repeat_info' => '客户名称重复', // 默认
                'main_phone' => '',
                'assist_phone' => '',
                'matched_contact' => null // 匹配到的联系方式（用于生成 repeat_info）
            ];
        }
        
        // 如果有联系方式信息，记录匹配的联系方式
        if (isset($row['contact_type']) && $row['contact_type'] !== null) {
            $contactType = (int)$row['contact_type'];
            $contactValue = $row['contact_value'] ?? '';
            
            // 如果这个联系方式匹配了关键词，更新 repeat_info
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
            
            // 收集联系方式信息
            if (!isset($contactMap[$leadsId])) {
                $contactMap[$leadsId] = [
                    'main' => [],
                    'assist' => []
                ];
            }
            
            if ($contactType == 1) {
                // 主电话
                $contactMap[$leadsId]['main'][] = [
                    'value' => $contactValue,
                    'id' => $row['contact_id'] ?? 0
                ];
            } elseif ($contactType == 3) {
                // 辅助电话
                $contactMap[$leadsId]['assist'][] = [
                    'value' => $contactValue,
                    'id' => $row['contact_id'] ?? 0
                ];
            }
        }
    }

    /* ---------- 6. 填充电话信息（优先匹配关键词的电话） ---------- */
    foreach ($leadsMap as $leadsId => &$lead) {
        // 确保 main_phone 和 assist_phone 字段始终存在，初始化为空字符串
        if (!isset($lead['main_phone'])) {
            $lead['main_phone'] = '';
        }
        if (!isset($lead['assist_phone'])) {
            $lead['assist_phone'] = '';
        }
        
        if (isset($contactMap[$leadsId])) {
            // 主电话：优先选择匹配关键词的，否则选第一个
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
            
            // 辅助电话：优先选择匹配关键词的，否则选第一个
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
        
        // 生成 repeat_info
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

    /* ---------- 7. 查询渠道和端口名称映射 ---------- */
    $inquiryMap = [];
    $portMap = [];
    try {
        $inquiryIds = array_filter(array_unique(array_column($leadsMap, 'inquiry_id')));
        if (!empty($inquiryIds)) {
            $inquiryList = \think\Db::table('crm_inquiry')->whereIn('id', $inquiryIds)->select();
            foreach ($inquiryList as $inq) {
                $inquiryMap[$inq['id']] = $inq['inquiry_name'] ?? '';
            }
        }
        
        // 处理 port_id（可能是逗号分隔的多选值）
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
            $portList = \think\Db::table('crm_inquiry_port')->whereIn('id', $portIds)->select();
            foreach ($portList as $port) {
                $portMap[$port['id']] = $port['port_name'] ?? '';
            }
        }
    } catch (\Exception $e) {
        // 忽略映射查询异常
    }

    /* ---------- 8. 填充渠道和端口名称 ---------- */
    $finalList = [];
    foreach ($leadsMap as $lead) {
        // 所属渠道名称
        $lead['channel_name'] = '';
        if (!empty($lead['inquiry_id'])) {
            $lead['channel_name'] = $inquiryMap[$lead['inquiry_id']] ?? '';
        }
        
        // 运营端口名称（处理多选情况）
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
        
        // 清理不需要的字段
        unset($lead['matched_contact']);
        unset($lead['inquiry_id']); // 前端不需要ID，只要名称
        unset($lead['port_id']); // 前端不需要ID，只要名称
        
        $finalList[] = $lead;
    }

    // 时间倒序
    usort($finalList, function ($a, $b) {
        return strtotime($b['at_time']) <=> strtotime($a['at_time']);
    });

    /* ---------- 6. 写入 Redis 结果 & 状态 ---------- */
    $resultKey = 'waimao_conflict_result:' . $taskId;
    $statusKey = 'waimao_conflict_status:' . $taskId;

    $redis->set($resultKey, json_encode($finalList, JSON_UNESCAPED_UNICODE));
    $redis->expire($resultKey, 300);          // 结果 5 分钟有效

    // 设置状态为 done
    $redis->set($statusKey, 'done');
    $redis->expire($statusKey, 300);         // 状态 5 分钟有效

    // —— 循环下一任务 ——
}
