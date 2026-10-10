<?php

namespace app\admin\service;

use app\admin\model\Admin;
use app\admin\model\ClientOrder;
use app\admin\model\CrmInquiry;
use app\admin\model\CrmInquiryPort;
use app\admin\model\Lead;
use app\admin\model\OrderItem;
use think\Db;
use think\facade\Env;

/**
 * 客户管理 - 检查订单（业务编排：筛选、权限范围、统计、凭证图字段）
 */
class CheckOrderService
{
    /** 检查订单列表单页最大条数（仅本接口） */
    const LIST_MAX_LIMIT = 100;

    /** 检查订单列表非法 limit 时的安全默认值（与前端默认一致） */
    const LIST_DEFAULT_LIMIT = 100;

    /** 性能诊断日志保留天数 */
    const PERF_DIAG_KEEP_DAYS = 7;

    /** 单日诊断日志文件大小上限（字节） */
    const PERF_DIAG_MAX_FILE_BYTES = 20971520;
    /**
     * 检查订单页 GET 所需 assign 数据（不含 customer_type 常量，由 Controller 赋值）
     *
     * @param string[] $allowedUsernames getCheckClientAllowedUsernames
     * @param callable(): array $teamListProvider Controller->getTeamList
     * @return array<string, mixed>
     */
    public function getPageAssignData(array $allowedUsernames, callable $teamListProvider): array
    {
        return [
            'channelList' => CrmInquiry::listEnabledNames(),
            'portList'      => CrmInquiryPort::listEnabledNames(),
            'adminResult'   => Admin::listByUsernamesForSelect($allowedUsernames),
            'teamList'      => $teamListProvider(),
        ];
    }

    /**
     * 检查订单列表 JSON（原 checkOrderSearch）
     *
     * @param array $keyword
     * @param int|string $page
     * @param int|string $limit
     * @param array $visibleUsers getCheckClientAllowedUsernames
     * @param string $currentUsername
     * @param callable(string $timeCondition, string $field): array $buildTimeWhere Controller->buildTimeWhere
     * @param callable(string $org, string $alias = ''): \Closure $getOrgWhere Controller->getOrgWhere
     * @param array $operatorInfo ['admin_id'=>int,'username'=>string,'group_id'=>int,'team_name'=>string] 跟进只读权限上下文
     * @return array{code:int,msg:string,data:array,count:int,rel:int,totalInquiries:int,successRate:string,totalMoney:string,totalProfit:string}
     */
    public function search(
        array $keyword,
        $page,
        $limit,
        array $visibleUsers,
        string $currentUsername,
        callable $buildTimeWhere,
        callable $getOrgWhere,
        array $operatorInfo = []
    ): array {
        $where = [];
        $client_where = [];

        $where[] = ['o.check_status', '=', 2];

        $visibleUsers = array_values(array_unique(array_filter(array_map('trim', $visibleUsers))));
        $currentUsername = trim($currentUsername);
        if (empty($visibleUsers) && $currentUsername !== '') {
            $visibleUsers = [$currentUsername];
        }

        if ($keyword) {
            $keyword = array_filter($keyword);
        }

        if (isset($keyword['order_no'])) {
            $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        }

        $timeCondition = null;
        if (isset($keyword['timebucket']) && $keyword['timebucket'] !== '') {
            $timeCondition = $keyword['timebucket'];
        } elseif (isset($keyword['at_time']) && $keyword['at_time'] !== '') {
            $timeCondition = $keyword['at_time'];
        }

        if ($timeCondition) {
            $where[] = $buildTimeWhere($timeCondition, 'order_time');
            $timeWhere = [];
            $timeWhere['at_time'] = $buildTimeWhere($timeCondition, 'at_time');
            $timeWhere['to_kh_time'] = $buildTimeWhere($timeCondition, 'to_kh_time');
            $client_where[] = function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }

        if (isset($keyword['min_money'])) {
            $where[] = ['money', '>', $keyword['min_money']];
        }
        if (isset($keyword['max_money'])) {
            $where[] = ['money', '<', $keyword['max_money']];
        }
        if (isset($keyword['min_profit'])) {
            $where[] = ['profit', '>', $keyword['min_profit']];
        }
        if (isset($keyword['max_profit'])) {
            $where[] = ['profit', '<', $keyword['max_profit']];
        }
        if (isset($keyword['min_margin_rate'])) {
            $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        }
        if (isset($keyword['max_margin_rate'])) {
            $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        }
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }

        $filter_team_name = '';
        if (isset($keyword['team_name']) && $keyword['team_name'] !== '') {
            $where[] = ['team_name', '=', $keyword['team_name']];
            $filter_team_name = $keyword['team_name'];
        }

        $org_where = [];
        if (!empty($keyword['org'])) {
            $org_where[] = $getOrgWhere($keyword['org']);
        }

        $scopedUsers = $visibleUsers;
        if ($filter_team_name || !empty($org_where)) {
            $filteredUsernames = Admin::columnUsernamesAfterTeamOrgFilter(
                $visibleUsers,
                $filter_team_name !== '' ? $filter_team_name : null,
                $org_where
            );

            if (empty($filteredUsernames)) {
                return $this->emptyGridResponse();
            }

            $scopedUsers = $filteredUsernames;
        }

        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        if (isset($keyword['source_port'])) {
            $where[] = ['source_port', '=', $keyword['source_port']];
        }

        $selectedPrUser = '';
        if (isset($keyword['pr_user'])) {
            $selectedPrUser = trim((string) $keyword['pr_user']);
        }

        if ($selectedPrUser !== '') {
            if (empty($scopedUsers) || !in_array($selectedPrUser, $scopedUsers, true)) {
                return $this->emptyGridResponse();
            }

            $where[] = ['pr_user', '=', $selectedPrUser];
            $client_where[] = ['pr_user', '=', $selectedPrUser];
            $scopedUsers = [$selectedPrUser];
        }

        if (!empty($scopedUsers)) {
            $where[] = ['pr_user', 'in', $scopedUsers];
            $client_where[] = ['pr_user', 'in', $scopedUsers];
        } elseif ($currentUsername !== '') {
            $where[] = ['pr_user', '=', $currentUsername];
            $client_where[] = ['pr_user', '=', $currentUsername];
        }

        $client_where[] = ['l.status', '=', 1];

        list($page, $limit) = $this->normalizePageLimit($page, $limit);

        $perfEnabled = $this->isPerfDiagEnabled();
        $perf = [
            'request_id' => $perfEnabled ? $this->newPerfRequestId() : '',
            't0' => microtime(true),
            'agg_ms' => null,
            'page_ms' => null,
            'items_ms' => null,
            'follow_ms' => null,
            'inquiry_ms' => null,
            'fail_stage' => '',
            'err' => '',
        ];
        $timeRangeType = $this->resolveTimeRangeType($keyword, $timeCondition);

        $orderModel = new ClientOrder();

        // 一次聚合替代：分页 COUNT + SUM(money) + SUM(profit)
        try {
            $tAgg = microtime(true);
            $stats = $orderModel->aggregateStatsByWhere($where);
            $perf['agg_ms'] = round((microtime(true) - $tAgg) * 1000, 2);
        } catch (\Throwable $e) {
            $perf['fail_stage'] = 'order_aggregate';
            $perf['err'] = $this->safeErrorToken($e);
            $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);
            throw $e;
        }

        $successOrders = (int)($stats['count'] ?? 0);
        $totalMoney = $stats['money'] ?? null;
        $totalProfit = $stats['profit'] ?? null;

        try {
            $tPage = microtime(true);
            $list = $orderModel->paginatePersonSearch($where, $page, $limit, $successOrders);
            $perf['page_ms'] = round((microtime(true) - $tPage) * 1000, 2);
        } catch (\Throwable $e) {
            $perf['fail_stage'] = 'order_paginate';
            $perf['err'] = $this->safeErrorToken($e);
            $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);
            throw $e;
        }

        try {
            $tItems = microtime(true);
            $orderIds = array_column($list['data'], 'id');
            $orderItemsMap = OrderItem::getProductNamesGroupedByOrderIds($orderIds);

            foreach ($list['data'] as &$order) {
                $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            }
            unset($order);

            $list['data'] = OrderImageService::appendOrderImageFields($list['data'] ?? []);
            $perf['items_ms'] = round((microtime(true) - $tItems) * 1000, 2);
        } catch (\Throwable $e) {
            $perf['fail_stage'] = 'order_products_images';
            $perf['err'] = $this->safeErrorToken($e);
            $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);
            throw $e;
        }

        // 分页结果之后补充客户最新跟进（不 JOIN 主查询，不影响 count/统计/排序）
        try {
            $tFollow = microtime(true);
            $list['data'] = $this->appendLatestFollowFields($list['data'] ?? [], $operatorInfo);
            $perf['follow_ms'] = round((microtime(true) - $tFollow) * 1000, 2);
        } catch (\Throwable $e) {
            $perf['fail_stage'] = 'latest_follow';
            $perf['err'] = $this->safeErrorToken($e);
            $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);
            throw $e;
        }

        try {
            $tInquiry = microtime(true);
            $leadModel = new Lead();
            $totalInquiries = $leadModel->countWithAlias($client_where);
            $perf['inquiry_ms'] = round((microtime(true) - $tInquiry) * 1000, 2);
        } catch (\Throwable $e) {
            $perf['fail_stage'] = 'inquiry_count';
            $perf['err'] = $this->safeErrorToken($e);
            $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);
            throw $e;
        }

        // 分页 total 以聚合 COUNT 为准（与原 paginate COUNT 同筛选）
        $list['total'] = $successOrders;
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;

        $this->writePerfDiagLog($perf, $operatorInfo, $page, $limit, $keyword, $timeRangeType);

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $successOrders,
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }

    /**
     * 检查订单专用：规范化 page / limit（不影响其它页面）
     *
     * @param mixed $page
     * @param mixed $limit
     * @return array{0:int,1:int}
     */
    private function normalizePageLimit($page, $limit): array
    {
        $pageInt = filter_var($page, FILTER_VALIDATE_INT);
        if ($pageInt === false || $pageInt < 1) {
            $pageInt = 1;
        }

        $limitInt = filter_var($limit, FILTER_VALIDATE_INT);
        if ($limitInt === false || $limitInt < 1) {
            $limitInt = self::LIST_DEFAULT_LIMIT;
        }
        if ($limitInt > self::LIST_MAX_LIMIT) {
            $limitInt = self::LIST_MAX_LIMIT;
        }

        return [$pageInt, $limitInt];
    }

    /**
     * 是否开启检查订单性能诊断（默认关闭）
     * 开启方式：config/app.php 中 check_order_perf_diag=true，
     * 或 .env / 环境变量 CHECK_ORDER_PERF_DIAG=true
     */
    private function isPerfDiagEnabled(): bool
    {
        if ((bool)config('check_order_perf_diag')) {
            return true;
        }
        $env = Env::get('CHECK_ORDER_PERF_DIAG', null);
        if ($env === null || $env === '') {
            return false;
        }
        $normalized = strtolower(trim((string)$env));
        return in_array($normalized, ['1', 'true', 'on', 'yes'], true);
    }

    private function newPerfRequestId(): string
    {
        try {
            return bin2hex(random_bytes(8));
        } catch (\Throwable $e) {
            return substr(md5(uniqid((string)mt_rand(), true)), 0, 16);
        }
    }

    /**
     * @param array $keyword
     * @param mixed $timeCondition
     */
    private function resolveTimeRangeType(array $keyword, $timeCondition): string
    {
        if (is_string($timeCondition) && $timeCondition !== '') {
            return $timeCondition;
        }
        if (!empty($keyword['timebucket'])) {
            return (string)$keyword['timebucket'];
        }
        if (!empty($keyword['at_time'])) {
            return (string)$keyword['at_time'];
        }
        return 'none';
    }

    /**
     * 筛选条件指纹（不含手机号/姓名/完整参数）
     */
    private function buildFilterFingerprint(array $keyword): string
    {
        $safe = [];
        $allowKeys = [
            'timebucket', 'at_time', 'team_name', 'org', 'pr_user',
            'min_money', 'max_money', 'min_profit', 'max_profit',
            'min_margin_rate', 'max_margin_rate', 'source', 'source_port',
            'customer_type', 'product_name',
        ];
        foreach ($allowKeys as $key) {
            if (!array_key_exists($key, $keyword)) {
                continue;
            }
            $val = $keyword[$key];
            if (is_scalar($val) || $val === null) {
                $safe[$key] = $val;
            } else {
                $safe[$key] = gettype($val);
            }
        }
        // 仅记录敏感筛选是否存在，不记录原文
        $safe['has_order_no'] = isset($keyword['order_no']) && $keyword['order_no'] !== '' ? 1 : 0;
        $safe['has_cname'] = isset($keyword['cname']) && $keyword['cname'] !== '' ? 1 : 0;
        $safe['has_contact'] = isset($keyword['contact']) && $keyword['contact'] !== '' ? 1 : 0;

        ksort($safe);
        return substr(hash('sha256', json_encode($safe, JSON_UNESCAPED_UNICODE)), 0, 16);
    }

    private function safeErrorToken(\Throwable $e): string
    {
        $class = (new \ReflectionClass($e))->getShortName();
        return $class . ':' . substr(hash('sha256', $class . '|' . $e->getFile() . '|' . $e->getLine()), 0, 12);
    }

    /**
     * 写入可关闭的检查订单性能诊断日志（单请求一行，无敏感字段）
     */
    private function writePerfDiagLog(
        array $perf,
        array $operatorInfo,
        $page,
        $limit,
        array $keyword,
        string $timeRangeType
    ): void {
        if (empty($perf['request_id']) || !$this->isPerfDiagEnabled()) {
            return;
        }

        try {
            $runtime = (string)Env::get('runtime_path');
            if ($runtime === '') {
                $runtime = dirname(__DIR__, 3) . DIRECTORY_SEPARATOR . 'runtime' . DIRECTORY_SEPARATOR;
            }
            $dir = rtrim($runtime, '\\/') . DIRECTORY_SEPARATOR . 'log' . DIRECTORY_SEPARATOR . 'check_order_perf';
            if (!is_dir($dir) && !@mkdir($dir, 0755, true) && !is_dir($dir)) {
                return;
            }

            $this->prunePerfDiagLogs($dir);

            $file = $dir . DIRECTORY_SEPARATOR . date('Ymd') . '.log';
            if (is_file($file) && filesize($file) >= self::PERF_DIAG_MAX_FILE_BYTES) {
                return;
            }

            $adminId = (int)($operatorInfo['admin_id'] ?? 0);
            $operatorHash = $adminId > 0
                ? substr(hash('sha256', 'check_order_perf|' . $adminId), 0, 16)
                : 'anonymous';

            $totalMs = isset($perf['t0'])
                ? round((microtime(true) - (float)$perf['t0']) * 1000, 2)
                : null;

            $line = json_encode([
                'ts' => date('Y-m-d H:i:s'),
                'request_id' => (string)$perf['request_id'],
                'operator_hash' => $operatorHash,
                'time_range' => $timeRangeType,
                'page' => (int)$page,
                'limit' => (int)$limit,
                'filter_fp' => $this->buildFilterFingerprint($keyword),
                'agg_ms' => $perf['agg_ms'],
                'page_ms' => $perf['page_ms'],
                'items_ms' => $perf['items_ms'],
                'follow_ms' => $perf['follow_ms'],
                'inquiry_ms' => $perf['inquiry_ms'],
                'total_ms' => $totalMs,
                'fail_stage' => (string)($perf['fail_stage'] ?? ''),
                'err' => (string)($perf['err'] ?? ''),
            ], JSON_UNESCAPED_UNICODE);

            if ($line === false) {
                return;
            }

            @file_put_contents($file, $line . PHP_EOL, FILE_APPEND | LOCK_EX);
        } catch (\Throwable $e) {
            // 诊断失败不影响业务
        }
    }

    private function prunePerfDiagLogs(string $dir): void
    {
        $expireBefore = time() - (self::PERF_DIAG_KEEP_DAYS * 86400);
        $files = @scandir($dir);
        if (!is_array($files)) {
            return;
        }
        foreach ($files as $name) {
            if (!preg_match('/^\d{8}\.log$/', $name)) {
                continue;
            }
            $path = $dir . DIRECTORY_SEPARATOR . $name;
            if (!is_file($path)) {
                continue;
            }
            $mtime = @filemtime($path);
            if ($mtime !== false && $mtime < $expireBefore) {
                @unlink($path);
            }
        }
    }

    /**
     * 为当前页检查订单批量回填最新跟进展示字段
     *
     * follow_status: ok|empty|unresolved|forbidden
     * last_up_records: 表格展示文案（有跟进时为最新内容）
     * follow_leads_id: 仅 ok/empty 时返回已授权客户 ID
     *
     * @param array $orders
     * @param array $operatorInfo
     * @return array
     */
    public function appendLatestFollowFields(array $orders, array $operatorInfo = []): array
    {
        if (empty($orders)) {
            return $orders;
        }

        $contacts = [];
        foreach ($orders as $order) {
            $raw = trim((string)($order['contact'] ?? ''));
            if ($raw !== '') {
                $contacts[] = $raw;
            }
        }

        $contactMap = OrderService::batchResolveUniqueLeadsIdByContacts($contacts);

        $resolvedLeadsIds = [];
        foreach ($contactMap as $match) {
            if (!empty($match['ok']) && (int)($match['leads_id'] ?? 0) > 0) {
                $resolvedLeadsIds[] = (int)$match['leads_id'];
            }
        }
        $resolvedLeadsIds = array_values(array_unique($resolvedLeadsIds));

        $clientsById = [];
        if (!empty($resolvedLeadsIds)) {
            $clientRows = Db::table('crm_leads')->whereIn('id', $resolvedLeadsIds)->select();
            if (is_array($clientRows)) {
                foreach ($clientRows as $client) {
                    $cid = (int)($client['id'] ?? 0);
                    if ($cid > 0) {
                        $clientsById[$cid] = $client;
                    }
                }
            }
        }

        $followService = new ClientFollowService();
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));
        $adminContext = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($operatorInfo['group_id'] ?? 0),
            'team_name' => (string)($operatorInfo['team_name'] ?? ''),
        ];

        // 请求内按 leads_id 缓存跟进读取权限，避免同客户多订单重复校验
        $canReadCache = [];
        $allowedLeadsIds = [];
        foreach ($resolvedLeadsIds as $leadsId) {
            if (!isset($clientsById[$leadsId])) {
                $canReadCache[$leadsId] = false;
                continue;
            }
            $canRead = $operatorId > 0
                && $followService->canReadClientFollow($clientsById[$leadsId], $operatorId, $operatorName, $adminContext);
            $canReadCache[$leadsId] = $canRead;
            if ($canRead) {
                $allowedLeadsIds[] = $leadsId;
            }
        }

        $latestMap = $followService->batchGetLatestValidComments($allowedLeadsIds);

        foreach ($orders as &$order) {
            $raw = trim((string)($order['contact'] ?? ''));
            if ($raw === '') {
                $order['follow_status'] = 'unresolved';
                $order['last_up_records'] = '客户关联待确认';
                $order['follow_leads_id'] = 0;
                continue;
            }

            $match = $contactMap[$raw] ?? null;
            if (empty($match) || empty($match['ok']) || (int)($match['leads_id'] ?? 0) <= 0) {
                $order['follow_status'] = 'unresolved';
                $order['last_up_records'] = '客户关联待确认';
                $order['follow_leads_id'] = 0;
                continue;
            }

            $leadsId = (int)$match['leads_id'];
            if (!isset($clientsById[$leadsId])) {
                $order['follow_status'] = 'unresolved';
                $order['last_up_records'] = '客户关联待确认';
                $order['follow_leads_id'] = 0;
                continue;
            }

            if (empty($canReadCache[$leadsId])) {
                $order['follow_status'] = 'forbidden';
                $order['last_up_records'] = '无权限查看';
                $order['follow_leads_id'] = 0;
                continue;
            }

            $latest = $latestMap[$leadsId] ?? null;
            if (empty($latest)) {
                $order['follow_status'] = 'empty';
                $order['last_up_records'] = '暂无跟进记录';
                $order['follow_leads_id'] = $leadsId;
                continue;
            }

            $order['follow_status'] = 'ok';
            $order['last_up_records'] = (string)($latest['reply_msg'] ?? '');
            $order['follow_leads_id'] = $leadsId;
        }
        unset($order);

        return $orders;
    }

    /**
     * 检查订单：按 order_id 获取关联客户最近最多 10 条有效跟进（只读）
     *
     * @param int|string $orderId
     * @param string[] $allowedUsernames
     * @param string $currentUsername
     * @param array $operatorInfo
     * @return array{code:int,msg:string,data:array}
     */
    public function getRecentFollowsByOrderId($orderId, array $allowedUsernames, $currentUsername, array $operatorInfo = []): array
    {
        $resolved = $this->resolveOrderClientForFollow($orderId, $allowedUsernames, $currentUsername, $operatorInfo);
        if ((int)($resolved['code'] ?? 1) !== 0) {
            return [
                'code' => (int)($resolved['code'] ?? 1),
                'msg' => (string)($resolved['msg'] ?? '无权查看'),
                'data' => [],
            ];
        }

        $leadsId = (int)($resolved['data']['leads_id'] ?? 0);
        if ($leadsId <= 0) {
            return ['code' => 1, 'msg' => '无法定位订单对应客户', 'data' => []];
        }

        try {
            $followService = new ClientFollowService();
            $list = $followService->getRecentValidFollowComments($leadsId, 10);
        } catch (\Throwable $e) {
            return ['code' => 1, 'msg' => '获取跟进记录失败，请稍后重试', 'data' => []];
        }

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'order_id' => (int)$orderId,
                'leads_id' => $leadsId,
                'list' => $list,
            ],
        ];
    }

    /**
     * @return array{code:int,msg:string,data:array,count:int,rel:int,totalInquiries:int,successRate:string,totalMoney:string,totalProfit:string}
     */
    private function emptyGridResponse(): array
    {
        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => [],
            'count' => 0,
            'rel' => 1,
            'totalInquiries' => 0,
            'successRate' => number_format(0, 2),
            'totalMoney' => number_format(0, 2),
            'totalProfit' => number_format(0, 2),
        ];
    }

    /**
     * 检查订单可见业务员用户名（单一口径，供 Controller / 跟进只读桥接共用）
     * 特殊账号名单只维护在此，不在 ClientFollowService 再写一份。
     *
     * @param array $adminContext ['admin_id'=>int,'username'=>string,'group_id'=>int,'team_name'=>string]
     * @return string[]
     */
    public function getAllowedUsernames(array $adminContext = []): array
    {
        return $this->resolveAllowedUsernamesBySpecialAdminIds(
            [1, 395, 350, 375, 387, 391, 405, 407],
            $adminContext
        );
    }

    /**
     * 按特殊 admin 名单计算可见业务员用户名（原 Client 控制器私有逻辑，检查客户 / 检查订单共用算法）
     *
     * @param int[] $specialAdminIds
     * @param array $adminContext
     * @return string[]
     */
    public function resolveAllowedUsernamesBySpecialAdminIds(array $specialAdminIds, array $adminContext = []): array
    {
        $currentAdminId  = (int)($adminContext['admin_id'] ?? 0);
        $currentUsername = trim((string)($adminContext['username'] ?? ''));
        $currentGroupId  = (int)($adminContext['group_id'] ?? 0);
        $currentTeamName = trim((string)($adminContext['team_name'] ?? ''));

        if (!$currentAdminId && $currentUsername === '') {
            return [];
        }

        if (!$currentGroupId || $currentTeamName === '' || $currentUsername === '') {
            if ($currentAdminId) {
                $currentAdmin = Db::name('admin')
                    ->where('admin_id', $currentAdminId)
                    ->field('admin_id,username,group_id,team_name')
                    ->find();
                if ($currentAdmin) {
                    $currentUsername = trim((string)$currentAdmin['username']);
                    $currentGroupId  = (int)$currentAdmin['group_id'];
                    $currentTeamName = trim((string)$currentAdmin['team_name']);
                }
            }
        }

        $allVisibleGroupIds  = [10, 11, 14, 17, 18, 19, 21, 22];
        $teamVisibleGroupIds = [17, 18];
        $selfVisibleGroupIds = [10, 11, 14, 19, 21, 22];

        $allowed = [];

        if (in_array($currentAdminId, $specialAdminIds, true)) {
            $allowed = Db::name('admin')
                ->where('group_id', 'in', $allVisibleGroupIds)
                ->where('username', '<>', '')
                ->column('username');
        } elseif (in_array($currentGroupId, $teamVisibleGroupIds, true) && $currentTeamName !== '') {
            $allowed = Db::name('admin')
                ->where('team_name', $currentTeamName)
                ->where('username', '<>', '')
                ->column('username');
        } elseif (in_array($currentGroupId, $selfVisibleGroupIds, true) && $currentUsername !== '') {
            $allowed = [$currentUsername];
        } elseif ($currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        $allowed = array_values(array_unique(array_filter(array_map('trim', (array)$allowed))));
        if (empty($allowed) && $currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        return $allowed;
    }

    /**
     * 检查订单入口：按 order_id 解析可只读查看跟进的唯一客户
     * 权限仅来自检查订单可见用户名，不复用我的订单本人订单规则。
     *
     * @param int|string $orderId
     * @param string[] $allowedUsernames
     * @param string $currentUsername
     * @param array $operatorInfo
     * @return array{code:int,msg:string,data:array}
     */
    public function resolveOrderClientForFollow($orderId, array $allowedUsernames, $currentUsername, array $operatorInfo = []): array
    {
        $orderId = (int)$orderId;
        if ($orderId <= 0) {
            return ['code' => 1, 'msg' => '参数错误', 'data' => []];
        }

        $allowedUsernames = array_values(array_unique(array_filter(array_map('trim', $allowedUsernames))));
        $currentUsername = trim((string)$currentUsername);
        if (empty($allowedUsernames) && $currentUsername !== '') {
            $allowedUsernames = [$currentUsername];
        }

        $order = Db::table('crm_client_order')
            ->where('id', $orderId)
            ->where('check_status', 2)
            ->field('id,contact,pr_user')
            ->find();

        if (empty($order)) {
            return ['code' => 1, 'msg' => '订单不存在或无权查看', 'data' => []];
        }

        $prUser = trim((string)($order['pr_user'] ?? ''));
        if ($prUser === '' || empty($allowedUsernames) || !in_array($prUser, $allowedUsernames, true)) {
            return ['code' => 1, 'msg' => '订单不存在或无权查看', 'data' => []];
        }

        $contactMatch = OrderService::resolveUniqueLeadsIdByContact($order['contact'] ?? '');
        if (empty($contactMatch['ok'])) {
            return [
                'code' => 1,
                'msg' => (string)($contactMatch['msg'] ?? '无法定位订单对应客户'),
                'data' => [],
            ];
        }

        $leadsId = (int)$contactMatch['leads_id'];
        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (empty($client)) {
            return ['code' => 1, 'msg' => '无法定位订单对应客户', 'data' => []];
        }

        $followService = new ClientFollowService();
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? $currentUsername));
        $adminContext = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($operatorInfo['group_id'] ?? 0),
            'team_name' => (string)($operatorInfo['team_name'] ?? ''),
        ];

        if (!$followService->canReadClientFollow($client, $operatorId, $operatorName, $adminContext)) {
            return ['code' => 1, 'msg' => '无权查看客户跟进', 'data' => []];
        }

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'leads_id' => $leadsId,
            ],
        ];
    }
}
