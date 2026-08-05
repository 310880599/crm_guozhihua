<?php

namespace app\admin\service;

use think\Db;

/**
 * 订单产品利润分摊服务。
 *
 * 统一口径（唯一利润来源）：
 *   订单利润 = crm_client_order.profit
 *   产品利润 = 订单profit × 该产品明细total_price / 订单全部明细total_price合计
 *
 * 明确禁止：SUM(crm_order_item.sub_profit) 作为产品利润来源（口径不一致，与订单利润不对账）。
 *
 * 空产品名处理：明细 product_name 为空时不丢弃其利润，统一归类到"未分类产品"，
 * 从而保证：任意筛选范围内，产品利润合计 严格等于 该范围订单profit合计。
 *
 * 组织范围：$orgUsernames 传空数组表示不限制业务员范围（控制面板口径）；
 * 传非空数组表示仅统计名单内业务员的订单（运营页面按当前组织收口口径）。
 * 调用方若需要“无权限=不返回任何数据”，应在调用前自行判断并直接返回空结果，
 * 不要把“空名单”误传给本服务（本服务的空数组语义是“不限制”）。
 */
class OrderProductProfitAllocationService
{
    /** 明细 product_name 为空时的统一归类名称 */
    const UNCLASSIFIED_PRODUCT_NAME = '未分类产品';

    /** 团队名称为空时的统一归类名称 */
    const UNGROUPED_TEAM_NAME = '未分组';

    /**
     * 产品利润分摊：按 product_name 聚合（sale_qty、total_profit）。
     *
     * @param string $timebucket 时间快捷方式（today/yesterday/week/month/year/last_month）
     * @param string $at_time 自定义日期区间，"YYYY-MM-DD,YYYY-MM-DD" 或 "YYYY-MM-DD - YYYY-MM-DD"
     * @param string $month_keys 月份多选，逗号分隔的 "YYYY-MM" 列表，优先级最高
     * @param array $orgUsernames 组织范围限制（业务员 username 列表），空数组表示不限制
     * @param string $teamName 团队过滤（可选），支持"未分组"
     * @param string $username 业务员过滤（可选）
     * @return array<int, array{product_name:string, sale_qty:float, total_profit:float}>
     */
    public function getOrderProductProfitAllocation(
        string $timebucket = '',
        string $at_time = '',
        string $month_keys = '',
        array $orgUsernames = [],
        string $teamName = '',
        string $username = ''
    ): array {
        $rows = $this->getOrderProductProfitAllocationRows($timebucket, $at_time, $month_keys, $orgUsernames, $teamName, $username);

        $aggregated = [];
        foreach ($rows as $row) {
            $name = (string)$row['product_name'];
            if (!isset($aggregated[$name])) {
                $aggregated[$name] = ['product_name' => $name, 'sale_qty' => 0.0, 'total_profit' => 0.0];
            }
            $aggregated[$name]['sale_qty'] += (float)$row['qty'];
            $aggregated[$name]['total_profit'] += (float)$row['profit'];
        }

        return array_values($aggregated);
    }

    /**
     * 产品利润分摊：按"订单+产品"粒度展开的分摊明细（不做聚合）。
     * 供需要多维度（团队/业务员/团队+产品等）自定义聚合的场景使用（如导出报表）。
     *
     * @return array<int, array{order_id:int, order_no:string, order_time:string, username:string, team_name:string, product_name:string, qty:float, profit:float}>
     */
    public function getOrderProductProfitAllocationRows(
        string $timebucket = '',
        string $at_time = '',
        string $month_keys = '',
        array $orgUsernames = [],
        string $teamName = '',
        string $username = ''
    ): array {
        $query = $this->buildBaseQuery($timebucket, $at_time, $month_keys, $orgUsernames);

        if (trim($teamName) !== '') {
            $this->applyTeamFilter($query, trim($teamName));
        }
        if (trim($username) !== '') {
            $query->where('o.pr_user', '=', trim($username));
        }

        $teamExpr = $this->getTeamExpr();
        $rows = $query
            ->field(
                "o.id as order_id, o.profit as order_profit, o.order_no, o.order_time, " .
                "TRIM(o.pr_user) as username, " . $teamExpr . " as team_name, " .
                "oi.product_name, IFNULL(oi.total_price,0) as total_price, IFNULL(oi.qty,0) as qty"
            )
            ->order('o.id asc, oi.line_no asc')
            ->select();

        // 按订单分组，组内保留全部明细（product_name 为空的行归一化为"未分类产品"，不丢弃利润）
        $orders = [];
        foreach ((array)$rows as $row) {
            $orderId = (int)($row['order_id'] ?? 0);
            if (!isset($orders[$orderId])) {
                $orders[$orderId] = [
                    'profit' => (float)($row['order_profit'] ?? 0),
                    'order_no' => (string)($row['order_no'] ?? ''),
                    'order_time' => (string)($row['order_time'] ?? ''),
                    'username' => (string)($row['username'] ?? ''),
                    'team_name' => $this->normalizeTeamName((string)($row['team_name'] ?? '')),
                    'items' => [],
                ];
            }
            $orders[$orderId]['items'][] = [
                'product_name' => $this->normalizeProductName((string)($row['product_name'] ?? '')),
                'total_price' => (float)($row['total_price'] ?? 0),
                'qty' => (float)($row['qty'] ?? 0),
            ];
        }

        $allocatedRows = [];
        foreach ($orders as $orderId => $order) {
            foreach ($this->allocateOrderProfitByItems($orderId, $order) as $allocatedRow) {
                $allocatedRows[] = $allocatedRow;
            }
        }

        return $allocatedRows;
    }

    /**
     * 单个订单内，按明细销售金额占比将 crm_client_order.profit 分摊到每一条明细。
     *
     * 分摊规则：
     * 1. 占比 = 明细 total_price / 订单内全部明细 total_price 合计（金额全为 0 时按 qty 占比，
     *    再退化为按明细条数平均分摊，避免利润无处分配）；
     * 2. 明细分摊利润 = 订单 profit × 占比，四舍五入到 2 位小数；
     * 3. 最后一条明细补齐四舍五入产生的尾差，保证：订单内全部明细分摊利润合计 === 订单 profit。
     *
     * 明细已在上一步归一化 product_name（空值 -> "未分类产品"），因此这里不会丢弃任何明细，
     * 从而保证聚合后的产品利润合计恒等于订单利润合计。
     */
    private function allocateOrderProfitByItems(int $orderId, array $order): array
    {
        $profit = (float)($order['profit'] ?? 0);
        $items = (array)($order['items'] ?? []);
        if (empty($items)) {
            return [];
        }

        $priceDenominator = 0.0;
        $qtyDenominator = 0.0;
        foreach ($items as $item) {
            $priceDenominator += (float)$item['total_price'];
            $qtyDenominator += (float)$item['qty'];
        }

        $itemCount = count($items);
        $ideal = [];
        foreach ($items as $item) {
            if ($priceDenominator > 0.0) {
                $ratio = (float)$item['total_price'] / $priceDenominator;
            } elseif ($qtyDenominator > 0.0) {
                // 兜底：订单金额合计为 0（异常数据）时按数量占比分摊
                $ratio = (float)$item['qty'] / $qtyDenominator;
            } else {
                // 兜底：金额、数量均为 0 时按明细条数平均分摊
                $ratio = 1.0 / $itemCount;
            }
            $ideal[] = $profit * $ratio;
        }

        $idealSum = array_sum($ideal);
        $runningRounded = 0.0;
        $result = [];
        foreach ($items as $idx => $item) {
            if ($idx < $itemCount - 1) {
                $allocatedProfit = round($ideal[$idx], 2);
                $runningRounded += $allocatedProfit;
            } else {
                // 最后一条明细补齐尾差，保证订单内分摊利润合计 = 订单 profit（理论值）
                $allocatedProfit = round($idealSum - $runningRounded, 2);
            }
            $result[] = [
                'order_id' => $orderId,
                'order_no' => (string)($order['order_no'] ?? ''),
                'order_time' => (string)($order['order_time'] ?? ''),
                'username' => (string)($order['username'] ?? ''),
                'team_name' => (string)($order['team_name'] ?? ''),
                'product_name' => (string)$item['product_name'],
                'qty' => (float)$item['qty'],
                'profit' => $allocatedProfit,
            ];
        }

        return $result;
    }

    /**
     * 基础查询：crm_order_item 关联 crm_client_order（审核通过）+ admin（团队名兜底）。
     * 不按 product_name 过滤（分摊占比分母需要订单内全部明细）。
     */
    private function buildBaseQuery(string $timebucket, string $at_time, string $month_keys, array $orgUsernames)
    {
        $query = Db::table('crm_order_item')->alias('oi')
            ->join('crm_client_order o', 'oi.order_id = o.id', 'INNER')
            ->leftJoin('admin a', 'o.pr_user = a.username');

        $query->where('o.check_status', '=', 2);
        $this->applyTimeFilter($query, $timebucket, $at_time, $month_keys);

        if (!empty($orgUsernames)) {
            $query->whereIn('o.pr_user', $orgUsernames);
        }

        return $query;
    }

    /**
     * 团队名称归一化：空值统一为"未分组"。
     */
    private function normalizeTeamName(string $teamName): string
    {
        $teamName = trim($teamName);
        return $teamName === '' ? self::UNGROUPED_TEAM_NAME : $teamName;
    }

    /**
     * 产品名称归一化：空值统一为"未分类产品"（不丢弃利润）。
     */
    private function normalizeProductName(string $productName): string
    {
        $productName = trim($productName);
        return $productName === '' ? self::UNCLASSIFIED_PRODUCT_NAME : $productName;
    }

    /**
     * 团队名称口径：优先订单快照 o.team_name，其次 admin.team_name，最后"未分组"。
     */
    private function getTeamExpr(): string
    {
        return "IFNULL(NULLIF(TRIM(o.team_name),''), IFNULL(NULLIF(TRIM(a.team_name),''), '" . self::UNGROUPED_TEAM_NAME . "'))";
    }

    /**
     * 按团队过滤（支持"未分组"）。
     */
    private function applyTeamFilter($query, string $teamName)
    {
        $teamName = $this->normalizeTeamName($teamName);
        $teamExpr = $this->getTeamExpr();
        $query->whereRaw($teamExpr . " = :team_name", ['team_name' => $teamName]);
        return $query;
    }

    /**
     * 时间筛选：优先级 month_keys > at_time > timebucket > 默认当月。
     * 固定作用于 o.order_time（订单产品汇总口径专用），仅使用 where + 参数绑定。
     */
    private function applyTimeFilter($query, string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        $fieldExpr = 'o.order_time';

        $monthRanges = $this->parseMonthKeysToRanges($month_keys);
        if (!empty($monthRanges)) {
            $query->where(function ($monthOrQuery) use ($monthRanges, $fieldExpr) {
                foreach ($monthRanges as $idx => $range) {
                    if ($idx === 0) {
                        $monthOrQuery->where(function ($subQuery) use ($fieldExpr, $range) {
                            $subQuery->where($fieldExpr, '>=', $range['start'])->where($fieldExpr, '<=', $range['end']);
                        });
                    } else {
                        $monthOrQuery->whereOr(function ($subQuery) use ($fieldExpr, $range) {
                            $subQuery->where($fieldExpr, '>=', $range['start'])->where($fieldExpr, '<=', $range['end']);
                        });
                    }
                }
            });
            return $query;
        }

        $dateRange = $this->parseCustomDateRange($at_time);
        if (!empty($dateRange)) {
            return $query->where($fieldExpr, '>=', $dateRange[0] . ' 00:00:00')->where($fieldExpr, '<=', $dateRange[1] . ' 23:59:59');
        }

        $bucketRange = $this->resolveTimebucketDateRange($timebucket);
        return $query->where($fieldExpr, '>=', $bucketRange['start'])->where($fieldExpr, '<=', $bucketRange['end']);
    }

    /**
     * 解析 month_keys（逗号分隔的 "YYYY-MM" 列表）为日期区间数组。
     */
    private function parseMonthKeysToRanges(string $month_keys = ''): array
    {
        $month_keys = trim($month_keys);
        if ($month_keys === '') {
            return [];
        }

        $items = explode(',', $month_keys);
        $ranges = [];
        $seen = [];
        foreach ($items as $item) {
            $monthKey = trim((string)$item);
            if ($monthKey === '' || isset($seen[$monthKey])) {
                continue;
            }
            if (!preg_match('/^\d{4}-(0[1-9]|1[0-2])$/', $monthKey)) {
                continue;
            }
            $monthStartTs = strtotime($monthKey . '-01 00:00:00');
            if ($monthStartTs === false) {
                continue;
            }
            $ranges[] = [
                'key' => $monthKey,
                'start' => date('Y-m-01 00:00:00', $monthStartTs),
                'end' => date('Y-m-t 23:59:59', $monthStartTs),
            ];
            $seen[$monthKey] = true;
        }

        return $ranges;
    }

    /**
     * 解析 at_time 为 [startDate, endDate]，支持 "YYYY-MM-DD,YYYY-MM-DD" 与 "YYYY-MM-DD - YYYY-MM-DD"。
     */
    private function parseCustomDateRange(string $at_time = ''): array
    {
        $at_time = trim((string)$at_time);
        if ($at_time === '') {
            return [];
        }

        $startDate = '';
        $endDate = '';
        if (strpos($at_time, ',') !== false) {
            $dateParts = explode(',', $at_time, 2);
            $startDate = trim((string)($dateParts[0] ?? ''));
            $endDate = trim((string)($dateParts[1] ?? ''));
        } elseif (strpos($at_time, ' - ') !== false) {
            $dateParts = explode(' - ', $at_time, 2);
            $startDate = trim((string)($dateParts[0] ?? ''));
            $endDate = trim((string)($dateParts[1] ?? ''));
        }

        $isValidDate = function ($date) {
            $dt = \DateTime::createFromFormat('Y-m-d', (string)$date);
            return $dt && $dt->format('Y-m-d') === $date;
        };

        if ($startDate === '' || $endDate === '' || !$isValidDate($startDate) || !$isValidDate($endDate)) {
            return [];
        }
        if ($startDate > $endDate) {
            $tmp = $startDate;
            $startDate = $endDate;
            $endDate = $tmp;
        }

        return [$startDate, $endDate];
    }

    /**
     * timebucket 转换为 [startDatetime, endDatetime]，统一补齐 00:00:00 / 23:59:59。
     */
    private function resolveTimebucketDateRange(string $timebucket = ''): array
    {
        $bucket = strtolower(trim((string)$timebucket));
        if ($bucket === '' || $bucket === 'custom') {
            $bucket = 'month';
        }

        switch ($bucket) {
            case 'today':
                $startTs = strtotime(date('Y-m-d') . ' 00:00:00');
                $endTs = strtotime(date('Y-m-d') . ' 23:59:59');
                break;
            case 'yesterday':
                $startTs = strtotime(date('Y-m-d', strtotime('-1 day')) . ' 00:00:00');
                $endTs = strtotime(date('Y-m-d', strtotime('-1 day')) . ' 23:59:59');
                break;
            case 'week':
                $startTs = strtotime('monday this week');
                $endTs = strtotime('sunday this week');
                if ($startTs !== false) {
                    $startTs = strtotime(date('Y-m-d', $startTs) . ' 00:00:00');
                }
                if ($endTs !== false) {
                    $endTs = strtotime(date('Y-m-d', $endTs) . ' 23:59:59');
                }
                break;
            case 'year':
                $year = date('Y');
                $startTs = strtotime($year . '-01-01 00:00:00');
                $endTs = strtotime($year . '-12-31 23:59:59');
                break;
            case 'last month':
            case 'last_month':
                $monthTs = strtotime(date('Y-m-01', strtotime('-1 month')) . ' 00:00:00');
                $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
                $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
                break;
            case 'month':
            default:
                $monthTs = strtotime(date('Y-m-01') . ' 00:00:00');
                $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
                $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
                break;
        }

        if ($startTs === false || $endTs === false) {
            $monthTs = strtotime(date('Y-m-01') . ' 00:00:00');
            $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
            $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
        }

        return [
            'start' => date('Y-m-d H:i:s', $startTs),
            'end' => date('Y-m-d H:i:s', $endTs),
        ];
    }
}
