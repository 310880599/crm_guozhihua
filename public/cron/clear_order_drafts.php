<?php
/**
 * 订单草稿每日自动清理脚本（供宝塔计划任务调用）
 * 删除 crm_client_order 中 check_status=0 且 COALESCE(ut_time, create_time) < cutoff 的草稿，
 * 并同时删除 crm_order_item 中对应 order_id 的明细。
 * 不修改现有业务逻辑，仅新增本脚本。
 */

// 保留天数：1=只保留今天草稿清理昨天及更早；3/7 等=保留最近 N 天的草稿
$keepDays = 1;

$batchSize = 500;
$logFile   = __DIR__ . '/clear_order_drafts.log';

define('APP_PATH', __DIR__ . '/../../application/');
require __DIR__ . '/../../thinkphp/base.php';
\think\Container::get('app')->path(APP_PATH)->initialize();

use think\Db;

function logLine($msg, $logFile) {
    $line = '[' . date('Y-m-d H:i:s') . '] ' . $msg . "\n";
    @file_put_contents($logFile, $line, FILE_APPEND | LOCK_EX);
}

$startTime = date('Y-m-d H:i:s');
if ($keepDays <= 1) {
    $cutoff = date('Y-m-d 00:00:00');
} else {
    $cutoff = date('Y-m-d 00:00:00', strtotime("-{$keepDays} days"));
}

logLine("======== 开始清理 ========", $logFile);
logLine("开始时间: {$startTime}, cutoff: {$cutoff}, keepDays: {$keepDays}", $logFile);

$totalDeleted = 0;

try {
    while (true) {
        $ids = Db::table('crm_client_order')
            ->where('check_status', 0)
            ->whereRaw('COALESCE(ut_time, create_time) < ?', [$cutoff])
            ->limit($batchSize)
            ->column('id');

        if (empty($ids)) {
            break;
        }

        Db::startTrans();
        try {
            $n = count($ids);
            Db::table('crm_order_item')->whereIn('order_id', $ids)->delete();
            Db::table('crm_client_order')->whereIn('id', $ids)->delete();
            Db::commit();
            $totalDeleted += $n;
            logLine("本批删除: {$n} 条订单", $logFile);
        } catch (\Exception $e) {
            Db::rollback();
            logLine("本批异常: " . $e->getMessage(), $logFile);
            throw $e;
        }
    }

    logLine("总删除数量: {$totalDeleted}", $logFile);
    logLine("======== 清理结束 ========" . "\n", $logFile);
} catch (\Exception $e) {
    logLine("异常: " . $e->getMessage(), $logFile);
    logLine("======== 清理异常结束 ========" . "\n", $logFile);
}
