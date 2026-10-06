<?php

namespace app\admin\service;

use think\Db;
use Throwable;

class SuccessClientOrderService
{
    /** @var array<string,array<string,mixed>> */
    private $orderColumnMeta = [];

    /**
     * 旧口径：按客户相关子查询计算审核通过订单利润。
     * 列表排序已不再调用。保留该方法，便于与一次聚合结果对照。
     *
     * @param string $leadAlias 外层客户表别名，如 crm_leads 或 l
     * @return string
     */
    public function buildApprovedProfitTotalSql($leadAlias = 'crm_leads')
    {
        $leadAlias = $this->normalizeSqlIdentifier($leadAlias, 'crm_leads');

        return 'COALESCE((SELECT SUM(IFNULL(o.profit, 0)) FROM crm_client_order o'
            . ' WHERE o.check_status = 2'
            . ' AND o.contact IN ('
            . 'SELECT c.contact_value FROM crm_contacts c'
            . ' WHERE c.leads_id = ' . $leadAlias . '.id'
            . ' AND c.is_delete = 0'
            . ' AND c.contact_type IN (1, 3)'
            . " AND c.contact_value <> ''"
            . ')), 0)';
    }

    /**
     * 审核通过订单利润一次性聚合。
     * 每个 leads_id 最多一行，供利润排序 LEFT JOIN，以及当前页批量补利润。
     *
     * 不使用 TRIM / REPLACE，精确匹配 contact。
     * 不要求 order_time 有效，不读历史订单，不统计 check_status 0/1/3。
     *
     * 本实现依赖 crm_contacts.contact_value 当前唯一约束：
     * 一个订单 contact 最多命中一条联系人，GROUP BY leads_id 后不会因一号多联系人重复 SUM。
     *
     * @return \think\db\Query
     */
    public function buildApprovedProfitAggregateQuery()
    {
        return Db::table('crm_contacts')->alias('c')
            ->join('crm_client_order o', 'o.contact = c.contact_value AND o.check_status = 2', 'INNER')
            ->where('c.is_delete', 0)
            ->where('c.contact_type', 'in', [1, 3])
            ->where('c.contact_value', '<>', '')
            ->fieldRaw('c.leads_id, SUM(IFNULL(o.profit, 0)) AS profit_total')
            ->group('c.leads_id');
    }

    /**
     * 按当前页客户批量取审核通过订单利润。没有订单的客户不在返回数组中，由调用方补 0。
     *
     * @param array $leadIds
     * @return array<int,float>
     */
    public function getApprovedProfitTotalByLeadIds(array $leadIds): array
    {
        $leadIds = $this->normalizeLeadIds($leadIds);
        if (empty($leadIds)) {
            return [];
        }

        $rows = $this->buildApprovedProfitAggregateQuery()
            ->where('c.leads_id', 'in', $leadIds)
            ->select();

        $map = [];
        foreach ($rows as $row) {
            $leadId = (int)($row['leads_id'] ?? 0);
            if ($leadId <= 0) {
                continue;
            }
            $map[$leadId] = (float)($row['profit_total'] ?? 0);
        }

        return $map;
    }

    /**
     * 成交客户列表排序入口。
     * 仅在 profit_total 排序，或传入利润区间上下限时，才 LEFT JOIN 一次全量利润聚合（profit_agg）；
     * 否则不聚合，分页后由当前页补利润。
     * 利润区间使用 COALESCE(profit_agg.profit_total, 0)，闭区间（>= / <=）；无审核通过订单视为 0。
     * field/order 非法、为空时回落 at_time desc, id desc。
     *
     * @param \think\db\Query $query
     * @param mixed $sortField
     * @param mixed $sortOrder
     * @param string $leadAlias
     * @param float|int|string|null $minProfitTotal 订单利润合计下限；null 表示不限
     * @param float|int|string|null $maxProfitTotal 订单利润合计上限；null 表示不限
     * @return void
     */
    public function applySuccessClientProfitQuery(
        $query,
        $sortField,
        $sortOrder,
        $leadAlias = 'crm_leads',
        $minProfitTotal = null,
        $maxProfitTotal = null
    ) {
        $leadAlias = $this->normalizeSqlIdentifier($leadAlias, 'crm_leads');
        $field = strtolower(trim((string)$sortField));
        $order = strtolower(trim((string)$sortOrder));
        $allowedFields = ['profit_total', 'kh_name', 'at_time', 'ut_time'];
        $allowedOrders = ['asc', 'desc'];
        $sortAllowed = in_array($field, $allowedFields, true) && in_array($order, $allowedOrders, true);
        $profitSort = $sortAllowed && $field === 'profit_total';

        $minProfitTotal = $this->normalizeProfitBound($minProfitTotal);
        $maxProfitTotal = $this->normalizeProfitBound($maxProfitTotal);
        $profitFilter = $minProfitTotal !== null || $maxProfitTotal !== null;

        // 排序与筛选共用同一次 LEFT JOIN，避免重复 JOIN
        if ($profitSort || $profitFilter) {
            $profitSubSql = $this->buildApprovedProfitAggregateQuery()->buildSql();
            $query->leftJoin(
                [$profitSubSql => 'profit_agg'],
                'profit_agg.leads_id = ' . $leadAlias . '.id'
            );
            $query->fieldRaw($leadAlias . '.*, COALESCE(profit_agg.profit_total, 0) AS profit_total');
            if ($minProfitTotal !== null) {
                $query->whereRaw('COALESCE(profit_agg.profit_total, 0) >= :min_profit_total', ['min_profit_total' => $minProfitTotal]);
            }
            if ($maxProfitTotal !== null) {
                $query->whereRaw('COALESCE(profit_agg.profit_total, 0) <= :max_profit_total', ['max_profit_total' => $maxProfitTotal]);
            }
        }

        if ($profitSort) {
            $direction = $order === 'asc' ? 'ASC' : 'DESC';
            $query->orderRaw('profit_total ' . $direction . ', ' . $leadAlias . '.at_time DESC, ' . $leadAlias . '.id DESC');
            return;
        }

        if ($sortAllowed) {
            $query->order($leadAlias . '.' . $field, $order)->order($leadAlias . '.id', 'desc');
            return;
        }

        $query->order($leadAlias . '.at_time', 'desc')->order($leadAlias . '.id', 'desc');
    }

    /**
     * 利润上下限归一化：null/空串表示不限；0 为合法值；非有限数字视为不限（调用方应已校验）。
     *
     * @param mixed $bound
     * @return float|null
     */
    private function normalizeProfitBound($bound)
    {
        if ($bound === null || is_array($bound) || trim((string)$bound) === '') {
            return null;
        }
        if (!is_numeric($bound) || !is_finite((float)$bound)) {
            return null;
        }
        return (float)$bound;
    }

    /**
     * @param array $leadIds
     * @return array<int,int>
     */
    private function normalizeLeadIds(array $leadIds): array
    {
        return array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
    }

    /**
     * @param mixed $identifier
     * @param string $fallback
     * @return string
     */
    private function normalizeSqlIdentifier($identifier, $fallback)
    {
        $identifier = trim((string)$identifier);
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $identifier)) {
            return $identifier;
        }
        return $fallback;
    }

    /**
     * 批量获取成交客户关联订单汇总（按 leads_id 返回）
     *
     * @param array $leadIds
     * @return array<int,array<string,mixed>>
     */
    public function getOrderSummaryByLeadIds(array $leadIds): array
    {
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $summaryMap = [];
        foreach ($leadIds as $leadId) {
            $summaryMap[$leadId] = $this->buildEmptySummary();
        }

        $phonesByLead = $this->getPhonesByLeadIds($leadIds);
        if (empty($phonesByLead)) {
            return $summaryMap;
        }

        $phoneToLeadIds = [];
        foreach ($phonesByLead as $leadId => $phones) {
            foreach ($phones as $phone) {
                if (!isset($phoneToLeadIds[$phone])) {
                    $phoneToLeadIds[$phone] = [];
                }
                $phoneToLeadIds[$phone][$leadId] = true;
            }
        }
        if (empty($phoneToLeadIds)) {
            return $summaryMap;
        }

        $fields = $this->resolveOrderFields();
        if ($fields['phone'] === '') {
            return $summaryMap;
        }

        $query = Db::table('crm_client_order')->alias('o')
            ->whereIn('o.' . $fields['phone'], array_keys($phoneToLeadIds));
        $this->applyOrderValidityScope($query);

        $selectFields = ['o.id', 'o.' . $fields['phone'] . ' as __phone'];
        if ($fields['amount'] !== '') {
            $selectFields[] = 'o.' . $fields['amount'] . ' as __amount';
        }
        if ($fields['profit'] !== '') {
            $selectFields[] = 'o.' . $fields['profit'] . ' as __profit';
        }

        $orders = [];
        try {
            $orders = $query->field(implode(',', $selectFields))->select();
        } catch (Throwable $e) {
            return $summaryMap;
        }

        foreach ($orders as $order) {
            $phone = $this->normalizePhone($order['__phone'] ?? '');
            if ($phone === '' || empty($phoneToLeadIds[$phone])) {
                continue;
            }

            $amount = (float)($order['__amount'] ?? 0);
            $profit = (float)($order['__profit'] ?? 0);
            foreach (array_keys($phoneToLeadIds[$phone]) as $leadId) {
                if (!isset($summaryMap[$leadId])) {
                    $summaryMap[$leadId] = $this->buildEmptySummary();
                }
                $summaryMap[$leadId]['order_count'] += 1;
                $summaryMap[$leadId]['order_amount_total'] += $amount;
                $summaryMap[$leadId]['profit_total'] += $profit;
            }
        }

        foreach ($summaryMap as &$summary) {
            $summary['order_amount_total'] = round((float)$summary['order_amount_total'], 2);
            $summary['profit_total'] = round((float)$summary['profit_total'], 2);
            $summary['order_summary_text'] = $this->buildSummaryText(
                (int)$summary['order_count'],
                (float)$summary['order_amount_total'],
                (float)$summary['profit_total']
            );
        }
        unset($summary);

        return $summaryMap;
    }

    /**
     * 批量获取成交客户成交时间列表（固定 order_time，按 leads_id 分组）
     *
     * 关联口径（须与 Model 成交日期 EXISTS 完全一致）：
     * crm_leads.id -> crm_contacts.leads_id (is_delete=0, contact_type IN (1,3))
     * -> crm_contacts.contact_value = crm_client_order.contact
     * -> crm_client_order.check_status = 2 且 order_time 有效
     *
     * @param array $leadIds 当前页客户 ID
     * @return array<int,array<int,array{order_id:int,order_no:string,order_time:string}>>
     */
    public function getDealTimesByLeadIds(array $leadIds): array
    {
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $result = [];
        foreach ($leadIds as $leadId) {
            $result[$leadId] = [];
        }

        $contactRows = [];
        try {
            $contactRows = Db::table('crm_contacts')
                ->whereIn('leads_id', $leadIds)
                ->where('is_delete', 0)
                ->whereIn('contact_type', [1, 3])
                ->field('leads_id,contact_value')
                ->select();
        } catch (Throwable $e) {
            return $result;
        }

        // contact_value => [leads_id => true]；精确匹配，仅 trim，不做模糊/相似处理
        $contactToLeadIds = [];
        foreach ($contactRows as $row) {
            $leadId = (int)($row['leads_id'] ?? 0);
            $contact = trim((string)($row['contact_value'] ?? ''));
            if ($leadId <= 0 || $contact === '') {
                continue;
            }
            if (!isset($contactToLeadIds[$contact])) {
                $contactToLeadIds[$contact] = [];
            }
            $contactToLeadIds[$contact][$leadId] = true;
        }
        if (empty($contactToLeadIds)) {
            return $result;
        }

        $orders = [];
        try {
            $contactKeys = array_keys($contactToLeadIds);
            $bind = [];
            $placeholders = [];
            foreach ($contactKeys as $i => $cv) {
                $key = 'cv' . $i;
                $placeholders[] = ':' . $key;
                $bind[$key] = $cv;
            }
            $orders = Db::table('crm_client_order')->alias('o')
                ->whereRaw('TRIM(o.contact) IN (' . implode(',', $placeholders) . ')', $bind)
                ->where('o.check_status', 2)
                ->whereNotNull('o.order_time')
                ->where('o.order_time', '<>', '0000-00-00 00:00:00')
                ->where('o.order_time', '<>', '0000-00-00')
                ->field('o.id,o.order_no,o.order_time,o.contact')
                ->select();
        } catch (Throwable $e) {
            return $result;
        }

        $byLead = [];
        foreach ($orders as $order) {
            $orderId = (int)($order['id'] ?? 0);
            $contact = trim((string)($order['contact'] ?? ''));
            if ($orderId <= 0 || $contact === '' || empty($contactToLeadIds[$contact])) {
                continue;
            }

            $mappedLeadIds = array_keys($contactToLeadIds[$contact]);
            // 与 Model EXISTS 一致：命中关联联系人的客户均装配（本页 leadIds 范围内）
            // 不在此排除“一号多客”；库表 contact_value 唯一约束下实际不会出现跨客歧义
            $orderTime = $this->normalizeValidOrderTime($order['order_time'] ?? null);
            if ($orderTime === '') {
                continue;
            }

            foreach ($mappedLeadIds as $leadId) {
                $leadId = (int)$leadId;
                if (!isset($result[$leadId])) {
                    continue;
                }
                if (!isset($byLead[$leadId])) {
                    $byLead[$leadId] = [];
                }
                // 按 order_id 去重（同时间多单保留多条）
                if (isset($byLead[$leadId][$orderId])) {
                    continue;
                }
                $byLead[$leadId][$orderId] = [
                    'order_id'   => $orderId,
                    'order_no'   => (string)($order['order_no'] ?? ''),
                    'order_time' => $orderTime,
                ];
            }
        }

        foreach ($byLead as $leadId => $orderMap) {
            $list = array_values($orderMap);
            usort($list, function ($a, $b) {
                $cmp = strcmp((string)$b['order_time'], (string)$a['order_time']);
                if ($cmp !== 0) {
                    return $cmp;
                }
                return ((int)$b['order_id'] - (int)$a['order_id']);
            });
            $result[$leadId] = $list;
        }

        return $result;
    }

    /**
     * 归一化有效成交时间；非法/零日期返回空串（禁止 create_time/audit_time 兜底）
     *
     * @param mixed $orderTime
     * @return string
     */
    private function normalizeValidOrderTime($orderTime): string
    {
        if ($orderTime === null) {
            return '';
        }
        $raw = trim((string)$orderTime);
        if ($raw === '' || $raw === '0000-00-00' || $raw === '0000-00-00 00:00:00') {
            return '';
        }
        $ts = strtotime($raw);
        if ($ts === false || $ts <= 0) {
            return '';
        }
        return date('Y-m-d H:i:s', $ts);
    }

    /**
     * 分页获取某成交客户关联订单明细
     *
     * @param int $leadId
     * @param int $page
     * @param int $limit
     * @return array{count:int,data:array}
     */
    public function getOrderDetailsByLeadId(int $leadId, int $page = 1, int $limit = 10): array
    {
        if ($leadId <= 0) {
            return ['count' => 0, 'data' => []];
        }
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);

        $phonesByLead = $this->getPhonesByLeadIds([$leadId]);
        $phones = $phonesByLead[$leadId] ?? [];
        if (empty($phones)) {
            return ['count' => 0, 'data' => []];
        }

        $fields = $this->resolveOrderFields();
        if ($fields['phone'] === '') {
            return ['count' => 0, 'data' => []];
        }

        $baseQuery = Db::table('crm_client_order')->alias('o')
            ->whereIn('o.' . $fields['phone'], $phones);
        $this->applyOrderValidityScope($baseQuery);

        try {
            $count = (int)(clone $baseQuery)->count('o.id');
        } catch (Throwable $e) {
            return ['count' => 0, 'data' => []];
        }
        if ($count <= 0) {
            return ['count' => 0, 'data' => []];
        }

        $query = clone $baseQuery;
        $fieldSql = $this->buildDetailFieldSql($fields);
        if ($fields['time'] !== '') {
            $query->order('o.' . $fields['time'], 'desc');
        } else {
            $query->order('o.id', 'desc');
        }

        try {
            $rows = $query->field($fieldSql)->page($page, $limit)->select();
        } catch (Throwable $e) {
            return ['count' => 0, 'data' => []];
        }

        foreach ($rows as &$row) {
            $row['phone'] = $this->normalizePhone($row['phone'] ?? '');
            $row['order_amount'] = round((float)($row['order_amount'] ?? 0), 2);
            $row['profit'] = round((float)($row['profit'] ?? 0), 2);
            $row['check_status_text'] = $this->mapCheckStatusText($row['check_status'] ?? '');
        }
        unset($row);

        return ['count' => $count, 'data' => $rows];
    }

    /**
     * @param array $leadIds
     * @return array<int,array<int,string>>
     */
    private function getPhonesByLeadIds(array $leadIds): array
    {
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $rows = Db::table('crm_contacts')
            ->whereIn('leads_id', $leadIds)
            ->whereIn('contact_type', [1, 3])
            ->where('is_delete', 0)
            ->field('leads_id,contact_value')
            ->select();

        $result = [];
        foreach ($rows as $row) {
            $leadId = (int)($row['leads_id'] ?? 0);
            $phone = $this->normalizePhone($row['contact_value'] ?? '');
            if ($leadId <= 0 || $phone === '') {
                continue;
            }
            if (!isset($result[$leadId])) {
                $result[$leadId] = [];
            }
            $result[$leadId][$phone] = true;
        }

        foreach ($result as $leadId => $phoneSet) {
            $result[$leadId] = array_keys($phoneSet);
        }

        return $result;
    }

    /**
     * @return array<string,string>
     */
    private function resolveOrderFields(): array
    {
        $fields = [
            'order_no' => $this->pickOrderColumn(['order_number', 'order_sn', 'order_no', 'sn']),
            'phone' => $this->pickOrderColumn(['phone', 'mobile', 'tel', 'contact_phone', 'kh_phone', 'contact', 'cphone']),
            'amount' => $this->pickOrderColumn(['order_amount', 'amount', 'total_money', 'money', 'price', 'total_price']),
            'profit' => $this->pickOrderColumn(['profit']),
            'check_status' => $this->pickOrderColumn(['check_status']),
            'time' => $this->pickOrderColumn(['create_time', 'add_time', 'at_time']),
            'sales_user' => $this->pickOrderColumn(['pr_user', 'sales_user']),
            'product_name' => $this->pickOrderColumn(['product_name']),
            'inquiry_id' => $this->pickOrderColumn(['inquiry_id']),
            'inquiry_name' => $this->pickOrderColumn(['inquiry_name', 'source']),
            'port_id' => $this->pickOrderColumn(['port_id']),
            'port_name' => $this->pickOrderColumn(['port_name', 'source_port']),
        ];

        if ($fields['order_no'] === '') {
            $fields['order_no'] = 'id';
        }
        if ($fields['time'] === '') {
            $fields['time'] = $this->pickOrderColumn(['order_time', 'ut_time']);
        }

        return $fields;
    }

    /**
     * @param array<string,string> $fields
     */
    private function buildDetailFieldSql(array $fields): string
    {
        $selects = ['o.id as id'];
        $selects[] = 'o.' . $fields['order_no'] . ' as order_no';
        $selects[] = 'o.' . $fields['phone'] . ' as phone';

        $selects[] = $fields['product_name'] !== '' ? ('o.' . $fields['product_name'] . ' as product_name') : "'' as product_name";
        $selects[] = $fields['inquiry_id'] !== '' ? ('o.' . $fields['inquiry_id'] . ' as inquiry_id') : "'' as inquiry_id";
        $selects[] = $fields['inquiry_name'] !== '' ? ('o.' . $fields['inquiry_name'] . ' as inquiry_name') : "'' as inquiry_name";
        $selects[] = $fields['port_id'] !== '' ? ('o.' . $fields['port_id'] . ' as port_id') : "'' as port_id";
        $selects[] = $fields['port_name'] !== '' ? ('o.' . $fields['port_name'] . ' as port_name') : "'' as port_name";
        $selects[] = $fields['amount'] !== '' ? ('o.' . $fields['amount'] . ' as order_amount') : "0 as order_amount";
        $selects[] = $fields['profit'] !== '' ? ('o.' . $fields['profit'] . ' as profit') : "0 as profit";
        $selects[] = $fields['check_status'] !== '' ? ('o.' . $fields['check_status'] . ' as check_status') : "'' as check_status";
        $selects[] = $fields['time'] !== '' ? ('o.' . $fields['time'] . ' as order_time') : "'' as order_time";
        $selects[] = $fields['sales_user'] !== '' ? ('o.' . $fields['sales_user'] . ' as sales_user') : "'' as sales_user";

        return implode(',', $selects);
    }

    /**
     * 订单有效性过滤：优先按软删除字段过滤，避免取到已删除订单
     *
     * @param \think\db\Query $query
     * @return void
     */
    private function applyOrderValidityScope($query): void
    {
        if ($this->hasOrderColumn('is_delete')) {
            $query->where('o.is_delete', 0);
        }
        if ($this->hasOrderColumn('delete_time')) {
            $query->where(function ($subQuery) {
                $subQuery->whereNull('o.delete_time')
                    ->whereOr('o.delete_time', '')
                    ->whereOr('o.delete_time', 0)
                    ->whereOr('o.delete_time', '0');
            });
        }
        if ($this->hasOrderColumn('check_status')) {
            $query->where('o.check_status', 2);
        }
    }

    private function mapCheckStatusText($checkStatus): string
    {
        $val = (string)$checkStatus;
        if ($val === '2') {
            return '审核通过';
        }
        if ($val === '1') {
            return '待审核';
        }
        if ($val === '0') {
            return '未审核';
        }
        return $val;
    }

    /**
     * @return array<string,mixed>
     */
    private function buildEmptySummary(): array
    {
        return [
            'order_count' => 0,
            'order_amount_total' => 0,
            'profit_total' => 0,
            'order_summary_text' => '0单 / ¥0 / 利润¥0',
        ];
    }

    private function buildSummaryText(int $orderCount, float $amountTotal, float $profitTotal): string
    {
        return $orderCount . '单 / ¥' . $this->formatMoney($amountTotal) . ' / 利润¥' . $this->formatMoney($profitTotal);
    }

    private function formatMoney(float $money): string
    {
        $formatted = number_format($money, 2, '.', '');
        $trimmed = rtrim(rtrim($formatted, '0'), '.');
        return $trimmed === '' ? '0' : $trimmed;
    }

    private function normalizePhone($phone): string
    {
        $phone = trim((string)$phone);
        if ($phone === '') {
            return '';
        }
        return preg_replace('/\s+/', '', $phone);
    }

    private function pickOrderColumn(array $candidates): string
    {
        foreach ($candidates as $field) {
            if ($this->hasOrderColumn($field)) {
                return $field;
            }
        }
        return '';
    }

    private function hasOrderColumn(string $field): bool
    {
        $columns = $this->getOrderColumns();
        return isset($columns[$field]);
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    private function getOrderColumns(): array
    {
        if (!empty($this->orderColumnMeta)) {
            return $this->orderColumnMeta;
        }

        try {
            $columns = Db::query('SHOW COLUMNS FROM `crm_client_order`');
        } catch (Throwable $e) {
            $this->orderColumnMeta = [];
            return $this->orderColumnMeta;
        }

        foreach ($columns as $column) {
            if (!empty($column['Field'])) {
                $this->orderColumnMeta[$column['Field']] = $column;
            }
        }
        return $this->orderColumnMeta;
    }
}
