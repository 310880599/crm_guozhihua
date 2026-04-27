<?php

namespace app\admin\service;

use think\Db;
use Throwable;

class SuccessClientOrderService
{
    /** @var array<string,array<string,mixed>> */
    private $orderColumnMeta = [];

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
