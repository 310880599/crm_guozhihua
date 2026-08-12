<?php

namespace app\admin\service;

use think\Db;

/**
 * 供应链分析服务（V1）。
 *
 * 数据来源：
 *   crm_order_item（明细）INNER JOIN crm_client_order（订单头，别名 o）ON oi.order_id = o.id
 *
 * 统计口径（强制约定，禁止修改 application/admin/controller/DataStatistics.php，本服务独立实现，
 * 但时间筛选参数名与优先级、订单状态口径均与 DataStatistics 现有逻辑保持一致）：
 *   1) 订单状态：o.check_status = 2（审核通过）
 *   2) 时间字段：o.order_time
 *   3) 时间参数优先级：month_keys > at_time > timebucket，均为空时默认取当月（month）
 *      - month_keys：逗号分隔的 "YYYY-MM" 列表，可多选
 *      - at_time：自定义区间，支持 "YYYY-MM-DD,YYYY-MM-DD" 或 "YYYY-MM-DD - YYYY-MM-DD"
 *      - timebucket：today/yesterday/week/month/year/last_month
 *
 * 分组规则（强制，禁止按名称分组）：
 *   - 产品维度：按 oi.product_id 分组，product_name 仅作为展示字段
 *   - 供应商维度：按 oi.supplier_id 分组，supplier_name 仅作为展示字段
 *   - product_id / supplier_id 为空字符串的明细会归为一组（"未分类产品" / "未分类供应商"），
 *     不会被丢弃，详见 aggregateRows()。
 *   - 未分类分组仅用于统计展示（参与销量/金额/成本/利润汇总），不允许作为详情下钻对象，
 *     详情下钻的参数校验见 Controller。
 *
 * 展示名称口径（V1 第二轮修正）：
 *   同一 product_id / supplier_id 在当前筛选时间范围内可能对应多笔订单、多个快照名称，
 *   展示名称固定取“当前筛选范围内最近一笔审核通过订单”的快照名称：
 *   fetchRows() 按 o.order_time desc, o.id desc 排序保证行级数据的稳定顺序，
 *   aggregateRows() 按该顺序遍历，一个分组第一次遇到的非空名称即为最近订单的名称
 *   （若最近一笔订单的名称为空，会继续向后取下一笔非空名称；整组均为空才使用兜底占位名）。
 *
 * 利润口径（V1，临时，务必保留后续替换点）：
 *   profit = SUM(oi.sub_profit)
 *   后续如需切换为与 DataStatistics 一致的“订单 profit 按明细金额占比分摊”口径
 *   （参考 OrderProductProfitAllocationService::allocateOrderProfitByItems），
 *   只需改造 resolveLineProfit()（必要时先在 fetchRows() 内按 order_id 分组算出分摊比例），
 *   不影响分组 / 排序 / 分页等其余逻辑。
 *
 * keyword 搜索口径（V1 第三轮修正）：
 *   同一 product_id / supplier_id 在当前筛选时间范围内可能存在多个历史名称快照（如产品改名、
 *   供应商更名），若直接用 "名称 LIKE keyword" 过滤明细再统计，会把该 ID 下不匹配 keyword 的
 *   其他历史快照明细漏掉，导致统计被截断。因此 keyword 非空时采用“两阶段查询”：
 *     第一阶段（findMatchedProductIds / findMatchedSupplierIds）：仅用 "名称 LIKE keyword" +
 *       相同的 check_status/时间筛选，找出匹配到的去重、非空 ID 列表，不做任何聚合；
 *     第二阶段（fetchRows 传入 "ID IN (...)"）：按第一阶段匹配到的 ID，在相同时间范围内取出
 *       这些 ID 的全部历史明细（不再附加名称过滤），再走现有 aggregateRows/summary/sort/paginate。
 *   即 keyword 只决定“哪些 ID 被选中”，不决定“这些 ID 的哪些明细参与统计”。
 *   若第一阶段未匹配到任何 ID，直接返回空结果（list=[]/total=0/summary 全 0），不做兜底全量查询。
 *   “未分类产品/未分类供应商”（ID 为空）不参与名称匹配，规则不变。
 */
class SupplyChainService
{
    /** product_id 为空时的展示名称 */
    const UNCLASSIFIED_PRODUCT_NAME = '未分类产品';

    /** supplier_id 为空时的展示名称 */
    const UNCLASSIFIED_SUPPLIER_NAME = '未分类供应商';

    /** 允许的排序字段白名单，防止排序字段被外部任意传入 */
    const SORT_FIELD_WHITELIST = ['qty', 'total_price', 'purchase_price', 'profit', 'product_count'];

    /** 排序字段缺省/降级时使用的默认字段 */
    const DEFAULT_SORT_FIELD = 'qty';

    /**
     * 产品排行：按 product_id 分组统计销量、销售额、进价、利润。
     *
     * summary 基于筛选条件下的完整聚合结果计算（分页前），而非当前页数据：
     *   product_count：有效（非空）product_id 的不同产品数量，"未分类产品"不计入
     *   qty/total_price/purchase_price/profit：全部分组（含"未分类产品"）之和
     *
     * @param array $params timebucket/at_time/month_keys/keyword/sort_field/sort_order/page/limit
     * @return array{list: array<int, array>, total: int, summary: array}
     */
    public function getProductRank(array $params = []): array
    {
        $extraWhere = [];
        $keyword = trim((string)($params['keyword'] ?? ''));
        if ($keyword !== '') {
            $matchedIds = $this->findMatchedProductIds($params, $keyword);
            if (empty($matchedIds)) {
                return $this->buildEmptyProductRankResult();
            }
            $extraWhere[] = ['oi.product_id', 'in', $matchedIds];
        }

        $rows = $this->fetchRows($params, $extraWhere);
        $list = $this->aggregateRows($rows, 'product_id', 'product_name', self::UNCLASSIFIED_PRODUCT_NAME);

        $totals = $this->buildSummaryTotals($list);
        $summary = [
            'product_count' => $this->countValidGroups($list, 'product_id'),
            'qty' => $totals['qty'],
            'total_price' => $totals['total_price'],
            'purchase_price' => $totals['purchase_price'],
            'profit' => $totals['profit'],
        ];

        $result = $this->sortAndPaginate($list, $params);
        $result['summary'] = $summary;
        return $result;
    }

    /**
     * 供应商排行：按 supplier_id 分组统计销量、销售额、进价、利润，并附带该供应商成交过的
     * 不同 product_id 数量（product_count，按 product_id 去重，空 product_id 不计入）。
     *
     * summary 基于筛选条件下的完整聚合结果计算（分页前），而非当前页数据：
     *   supplier_count：有效（非空）supplier_id 的不同供应商数量，"未分类供应商"不计入
     *   product_count：当前筛选范围内全部行（不分供应商）里有效 product_id 的去重总数
     *   qty/total_price/purchase_price/profit：全部分组（含"未分类供应商"）之和
     *
     * @param array $params timebucket/at_time/month_keys/keyword/sort_field/sort_order/page/limit
     * @return array{list: array<int, array>, total: int, summary: array}
     */
    public function getSupplierRank(array $params = []): array
    {
        $extraWhere = [];
        $keyword = trim((string)($params['keyword'] ?? ''));
        if ($keyword !== '') {
            $matchedIds = $this->findMatchedSupplierIds($params, $keyword);
            if (empty($matchedIds)) {
                return $this->buildEmptySupplierRankResult();
            }
            $extraWhere[] = ['oi.supplier_id', 'in', $matchedIds];
        }

        $rows = $this->fetchRows($params, $extraWhere);
        $list = $this->aggregateRows(
            $rows,
            'supplier_id',
            'supplier_name',
            self::UNCLASSIFIED_SUPPLIER_NAME,
            'product_id',
            'product_count'
        );

        $totals = $this->buildSummaryTotals($list);
        $summary = [
            'supplier_count' => $this->countValidGroups($list, 'supplier_id'),
            'product_count' => $this->countDistinctNonEmpty($rows, 'product_id'),
            'qty' => $totals['qty'],
            'total_price' => $totals['total_price'],
            'purchase_price' => $totals['purchase_price'],
            'profit' => $totals['profit'],
        ];

        $result = $this->sortAndPaginate($list, $params);
        $result['summary'] = $summary;
        return $result;
    }

    /**
     * 指定产品的供应商拆分：给定 product_id，按 supplier_id 分组统计该产品各供应商的占比数据。
     *
     * 注意：未分类分组（product_id 为空）仅用于统计展示，不允许作为详情下钻对象，
     * 空 product_id 的校验由 Controller 拦截，本方法不做二次处理。
     *
     * @param string $productId 产品ID（对应 crm_order_item.product_id，与前端展示的 product_id 保持一致）
     * @param array $params timebucket/at_time/month_keys/sort_field/sort_order/page/limit
     * @return array{list: array<int, array>, total: int, summary: array}
     */
    public function getSupplierBreakdownByProduct(string $productId, array $params = []): array
    {
        $rows = $this->fetchRows($params, [['oi.product_id', '=', trim($productId)]]);
        $list = $this->aggregateRows($rows, 'supplier_id', 'supplier_name', self::UNCLASSIFIED_SUPPLIER_NAME);

        $totals = $this->buildSummaryTotals($list);
        $summary = [
            'supplier_count' => $this->countValidGroups($list, 'supplier_id'),
            'qty' => $totals['qty'],
            'total_price' => $totals['total_price'],
            'purchase_price' => $totals['purchase_price'],
            'profit' => $totals['profit'],
        ];

        $result = $this->sortAndPaginate($list, $params);
        $result['summary'] = $summary;
        return $result;
    }

    /**
     * 指定供应商的产品拆分：给定 supplier_id，按 product_id 分组统计该供应商各产品的占比数据。
     *
     * 注意：未分类分组（supplier_id 为空）仅用于统计展示，不允许作为详情下钻对象，
     * 空 supplier_id 的校验由 Controller 拦截，本方法不做二次处理。
     *
     * @param string $supplierId 供应商ID（对应 crm_order_item.supplier_id）
     * @param array $params timebucket/at_time/month_keys/sort_field/sort_order/page/limit
     * @return array{list: array<int, array>, total: int, summary: array}
     */
    public function getProductBreakdownBySupplier(string $supplierId, array $params = []): array
    {
        $rows = $this->fetchRows($params, [['oi.supplier_id', '=', trim($supplierId)]]);
        $list = $this->aggregateRows($rows, 'product_id', 'product_name', self::UNCLASSIFIED_PRODUCT_NAME);

        $totals = $this->buildSummaryTotals($list);
        $summary = [
            'product_count' => $this->countValidGroups($list, 'product_id'),
            'qty' => $totals['qty'],
            'total_price' => $totals['total_price'],
            'purchase_price' => $totals['purchase_price'],
            'profit' => $totals['profit'],
        ];

        $result = $this->sortAndPaginate($list, $params);
        $result['summary'] = $summary;
        return $result;
    }

    // =========================================================
    // 内部实现
    // =========================================================

    /**
     * 拉取明细行级数据（不在 SQL 层分组，聚合统一在 PHP 层完成，
     * 便于后续把 profit 从"明细自带 sub_profit"切换为"订单级分摊"口径时，
     * 能直接复用同一批行级数据中的 order_id / order_profit）。
     *
     * 固定按 o.order_time desc, o.id desc 排序：保证同一分组内行的遍历顺序稳定，
     * 且第一行即为当前筛选范围内最近一笔审核通过订单，供 aggregateRows() 取展示名称使用。
     *
     * @param array $params timebucket/at_time/month_keys
     * @param array<int, array{0:string,1:string,2:mixed}> $extraWhere 追加的 [field, op, value] 条件
     */
    private function fetchRows(array $params, array $extraWhere = []): array
    {
        $query = Db::table('crm_order_item')->alias('oi')
            ->join('crm_client_order o', 'oi.order_id = o.id', 'INNER');

        $query->where('o.check_status', '=', 2);
        $this->applyTimeFilter(
            $query,
            (string)($params['timebucket'] ?? ''),
            (string)($params['at_time'] ?? ''),
            (string)($params['month_keys'] ?? '')
        );

        foreach ($extraWhere as $condition) {
            $query->where($condition[0], $condition[1], $condition[2]);
        }

        $rows = $query
            ->field(
                'oi.order_id, o.id as o_id, o.order_time as order_time, o.profit as order_profit, ' .
                'oi.product_id, oi.product_name, oi.supplier_id, oi.supplier_name, ' .
                'IFNULL(oi.qty,0) as qty, IFNULL(oi.total_price,0) as total_price, ' .
                'IFNULL(oi.purchase_price,0) as purchase_price, IFNULL(oi.sub_profit,0) as sub_profit'
            )
            ->order('o.order_time desc,o.id desc')
            ->select();

        return (array)$rows;
    }

    /**
     * keyword 两阶段查询 - 第一阶段：按 product_name LIKE keyword 找出匹配的 product_id 列表。
     *
     * 统计基础口径与 fetchRows() 完全一致（crm_order_item INNER JOIN crm_client_order，
     * check_status=2，且调用同一个 applyTimeFilter()，保证“搜索 ID 的时间范围”与
     * “最终统计数据的时间范围”完全一致），仅额外附加 product_name LIKE 条件。
     *
     * 只返回去重、非空的 product_id，不按 product_name 分组，不联查 crm_products /
     * crm_product_category，不做任何聚合（聚合交由 fetchRows() + aggregateRows() 完成）。
     *
     * @param array $params timebucket/at_time/month_keys
     * @param string $keyword 已 trim 过的非空关键字
     * @return array<int, string> 去重后的有效 product_id 列表
     */
    private function findMatchedProductIds(array $params, string $keyword): array
    {
        $query = Db::table('crm_order_item')->alias('oi')
            ->join('crm_client_order o', 'oi.order_id = o.id', 'INNER');

        $query->where('o.check_status', '=', 2);
        $this->applyTimeFilter(
            $query,
            (string)($params['timebucket'] ?? ''),
            (string)($params['at_time'] ?? ''),
            (string)($params['month_keys'] ?? '')
        );
        $query->where('oi.product_name', 'like', "%{$keyword}%");
        $query->where('oi.product_id', '<>', '');

        $ids = (array)$query->field('oi.product_id')->select();

        return $this->extractDistinctNonEmptyIds($ids, 'product_id');
    }

    /**
     * keyword 两阶段查询 - 第一阶段：按 supplier_name LIKE keyword 找出匹配的 supplier_id 列表。
     *
     * 逻辑与 findMatchedProductIds() 对称，详见其注释。
     *
     * @param array $params timebucket/at_time/month_keys
     * @param string $keyword 已 trim 过的非空关键字
     * @return array<int, string> 去重后的有效 supplier_id 列表
     */
    private function findMatchedSupplierIds(array $params, string $keyword): array
    {
        $query = Db::table('crm_order_item')->alias('oi')
            ->join('crm_client_order o', 'oi.order_id = o.id', 'INNER');

        $query->where('o.check_status', '=', 2);
        $this->applyTimeFilter(
            $query,
            (string)($params['timebucket'] ?? ''),
            (string)($params['at_time'] ?? ''),
            (string)($params['month_keys'] ?? '')
        );
        $query->where('oi.supplier_name', 'like', "%{$keyword}%");
        $query->where('oi.supplier_id', '<>', '');

        $ids = (array)$query->field('oi.supplier_id')->select();

        return $this->extractDistinctNonEmptyIds($ids, 'supplier_id');
    }

    /**
     * 从行级结果集中提取指定字段的去重、非空值列表（PHP 层去重，不在 SQL 层 GROUP BY）。
     *
     * @param array $rows select() 返回的行数组
     * @param string $field 需要提取的字段名
     * @return array<int, string>
     */
    private function extractDistinctNonEmptyIds(array $rows, string $field): array
    {
        $set = [];
        foreach ($rows as $row) {
            $val = trim((string)($row[$field] ?? ''));
            if ($val !== '') {
                $set[$val] = true;
            }
        }
        return array_keys($set);
    }

    /**
     * keyword 未匹配到任何 product_id 时的空结果（产品排行），避免 "IN ()" 空集合查询。
     */
    private function buildEmptyProductRankResult(): array
    {
        return [
            'list' => [],
            'total' => 0,
            'summary' => [
                'product_count' => 0,
                'qty' => 0,
                'total_price' => 0,
                'purchase_price' => 0,
                'profit' => 0,
            ],
        ];
    }

    /**
     * keyword 未匹配到任何 supplier_id 时的空结果（供应商排行），避免 "IN ()" 空集合查询。
     */
    private function buildEmptySupplierRankResult(): array
    {
        return [
            'list' => [],
            'total' => 0,
            'summary' => [
                'supplier_count' => 0,
                'product_count' => 0,
                'qty' => 0,
                'total_price' => 0,
                'purchase_price' => 0,
                'profit' => 0,
            ],
        ];
    }

    /**
     * 按指定 ID 字段分组聚合（product_id 或 supplier_id）。
     *
     * 展示名称口径：由于 fetchRows() 已按 o.order_time desc, o.id desc 排序，
     * 遍历中一个分组第一次遇到的非空名称即为"当前筛选范围内最近一笔审核通过订单"的快照名称；
     * 若最近一笔为空，会继续向后（更早的订单）寻找第一个非空名称；整组都为空则使用占位名称。
     *
     * 可选按 $distinctField 统计组内不同值数量（去重、忽略空值），
     * 用于供应商排行的 product_count 等场景，结果写入 $distinctCountKey 对应字段。
     *
     * @param array $rows fetchRows() 返回的行级数据（须已按最近订单在前排序）
     * @param string $groupField 分组字段：product_id / supplier_id
     * @param string $nameField 展示名称字段：product_name / supplier_name
     * @param string $unclassifiedName 分组ID为空时的展示名称
     * @param string|null $distinctField 需要在组内去重计数的字段（如 product_id），不需要则传 null
     * @param string|null $distinctCountKey 去重计数结果写入的字段名（如 product_count）
     * @return array<int, array>
     */
    private function aggregateRows(
        array $rows,
        string $groupField,
        string $nameField,
        string $unclassifiedName,
        ?string $distinctField = null,
        ?string $distinctCountKey = null
    ): array {
        $aggregated = [];
        $distinctSets = [];
        foreach ($rows as $row) {
            $groupKey = trim((string)($row[$groupField] ?? ''));

            if (!isset($aggregated[$groupKey])) {
                $name = trim((string)($row[$nameField] ?? ''));
                $aggregated[$groupKey] = [
                    $groupField => $groupKey,
                    $nameField => $name !== '' ? $name : $unclassifiedName,
                    'qty' => 0.0,
                    'total_price' => 0.0,
                    'purchase_price' => 0.0,
                    'profit' => 0.0,
                ];
                if ($distinctField !== null) {
                    $distinctSets[$groupKey] = [];
                }
            } elseif ($aggregated[$groupKey][$nameField] === $unclassifiedName) {
                // 最近一笔订单的名称为空时，继续向后（更早订单）寻找第一个非空名称
                $name = trim((string)($row[$nameField] ?? ''));
                if ($name !== '') {
                    $aggregated[$groupKey][$nameField] = $name;
                }
            }

            $aggregated[$groupKey]['qty'] += (float)($row['qty'] ?? 0);
            $aggregated[$groupKey]['total_price'] += (float)($row['total_price'] ?? 0);
            $aggregated[$groupKey]['purchase_price'] += (float)($row['purchase_price'] ?? 0);
            $aggregated[$groupKey]['profit'] += $this->resolveLineProfit($row);

            if ($distinctField !== null) {
                $distinctVal = trim((string)($row[$distinctField] ?? ''));
                if ($distinctVal !== '') {
                    $distinctSets[$groupKey][$distinctVal] = true;
                }
            }
        }

        foreach ($aggregated as $groupKey => &$item) {
            $item['total_price'] = round($item['total_price'], 2);
            $item['purchase_price'] = round($item['purchase_price'], 2);
            $item['profit'] = round($item['profit'], 2);
            if ($distinctField !== null && $distinctCountKey !== null) {
                $item[$distinctCountKey] = count($distinctSets[$groupKey] ?? []);
            }
        }
        unset($item);

        return array_values($aggregated);
    }

    /**
     * 统计已聚合分组列表中，分组字段（product_id / supplier_id）为非空的分组数量，
     * 即"未分类"分组不计入。用于 summary 中的 product_count / supplier_count。
     *
     * @param array $list aggregateRows() 返回的完整聚合列表（分页前）
     * @param string $groupField 分组字段名
     */
    private function countValidGroups(array $list, string $groupField): int
    {
        $count = 0;
        foreach ($list as $item) {
            if (trim((string)($item[$groupField] ?? '')) !== '') {
                $count++;
            }
        }
        return $count;
    }

    /**
     * 统计行级数据中指定字段的去重（忽略空值）数量。
     * 用于供应商排行 summary 的 product_count：跨全部供应商去重的产品总数，
     * 与"每个供应商各自的 product_count 之和"含义不同（同一产品可能出现在多个供应商下）。
     *
     * @param array $rows fetchRows() 返回的行级数据
     * @param string $field 需要去重计数的字段名
     */
    private function countDistinctNonEmpty(array $rows, string $field): int
    {
        $set = [];
        foreach ($rows as $row) {
            $val = trim((string)($row[$field] ?? ''));
            if ($val !== '') {
                $set[$val] = true;
            }
        }
        return count($set);
    }

    /**
     * 基于完整聚合列表（分页前）汇总 qty/total_price/purchase_price/profit，
     * 保证 summary 反映"当前筛选条件下的完整统计结果"，而不是分页后的当前页数据。
     * 汇总包含全部分组（含"未分类产品/供应商"），与各排行接口自身的汇总口径一致。
     *
     * @param array $list aggregateRows() 返回的完整聚合列表（分页前）
     * @return array{qty: float, total_price: float, purchase_price: float, profit: float}
     */
    private function buildSummaryTotals(array $list): array
    {
        $summary = [
            'qty' => 0.0,
            'total_price' => 0.0,
            'purchase_price' => 0.0,
            'profit' => 0.0,
        ];
        foreach ($list as $item) {
            $summary['qty'] += (float)($item['qty'] ?? 0);
            $summary['total_price'] += (float)($item['total_price'] ?? 0);
            $summary['purchase_price'] += (float)($item['purchase_price'] ?? 0);
            $summary['profit'] += (float)($item['profit'] ?? 0);
        }
        $summary['total_price'] = round($summary['total_price'], 2);
        $summary['purchase_price'] = round($summary['purchase_price'], 2);
        $summary['profit'] = round($summary['profit'], 2);

        return $summary;
    }

    /**
     * 单行明细的利润取值（唯一的利润口径入口）。
     *
     * V1：直接使用明细自带字段 sub_profit。
     * 预留：后续如需改为"订单 profit 按明细 total_price 占比分摊"口径，
     * 只需在此处替换实现（分摊比例可基于同一行已携带的 order_id / order_profit 计算，
     * 但需要先在 fetchRows() 之后按 order_id 分组求出该订单全部明细的 total_price 合计作为分母），
     * 不需要改动 aggregateRows() 及以外的任何逻辑。
     */
    private function resolveLineProfit(array $row): float
    {
        return (float)($row['sub_profit'] ?? 0);
    }

    /**
     * 排序 + 分页（内存分页：分组后的产品/供应商数量级有限，与 DataStatistics 现有的
     * 同类排行/汇总接口一致，不做数据库层分页）。
     *
     * 默认排序字段：qty desc（销量最多的排前面）。
     *
     * product_count 字段仅存在于供应商排行（每行自带 product_count）；产品排行 /
     * 产品-供应商明细 / 供应商-产品明细 等接口的行数据里没有这个字段。
     * 排序字段虽已加入全局白名单（供供应商排行使用），但若当前列表行不包含该字段
     * （即当前接口场景不支持按 product_count 排序），安全降级为默认 qty desc，
     * 不产生 "Undefined index" 提示，也不按 0 处理导致排序结果失真。
     */
    private function sortAndPaginate(array $list, array $params): array
    {
        $sortField = in_array($params['sort_field'] ?? '', self::SORT_FIELD_WHITELIST, true)
            ? $params['sort_field']
            : self::DEFAULT_SORT_FIELD;

        if ($sortField !== self::DEFAULT_SORT_FIELD && !array_key_exists($sortField, $list[0] ?? [])) {
            $sortField = self::DEFAULT_SORT_FIELD;
        }

        $sortOrder = strtolower((string)($params['sort_order'] ?? 'desc')) === 'asc' ? 'asc' : 'desc';

        usort($list, function ($a, $b) use ($sortField, $sortOrder) {
            $result = ($a[$sortField] ?? 0) <=> ($b[$sortField] ?? 0);
            return $sortOrder === 'asc' ? $result : -$result;
        });

        $total = count($list);

        $page = max(1, (int)($params['page'] ?? 1));
        $limit = (int)($params['limit'] ?? 0);
        if ($limit > 0) {
            $offset = ($page - 1) * $limit;
            $list = array_slice($list, $offset, $limit);
        }

        return [
            'list' => array_values($list),
            'total' => $total,
        ];
    }

    /**
     * 时间筛选：优先级 month_keys > at_time > timebucket > 默认当月。
     * 固定作用于 o.order_time，与 DataStatistics / OrderProductProfitAllocationService 现有口径一致。
     * （因 DataStatistics.php 的同名方法为私有且禁止修改该文件，此处按相同逻辑独立实现。）
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
