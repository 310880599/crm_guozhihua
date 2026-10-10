<?php

namespace app\admin\model;

use think\Model;

/**
 * 客户成交订单主表 crm_client_order
 */
class ClientOrder extends Model
{
    protected $table = 'crm_client_order';

    /**
     * 运营管理-成交订单列表分页
     *
     * @param array $where
     * @param int|string $page
     * @param int|string $limit
     * @param int|null $knownTotal 若已知总数（如检查订单聚合 SQL），传入后跳过分页内部 COUNT；其它页面传 null 保持原行为
     * @return array
     */
    public function paginatePersonSearch($where, $page, $limit, $knownTotal = null)
    {
        $query = $this->alias('o')
            ->where($where)
            ->order('o.order_time', 'desc');

        if ($knownTotal !== null) {
            $knownTotal = (int)$knownTotal;
            if ($knownTotal < 0) {
                $knownTotal = 0;
            }
            return $query
                ->paginate(['list_rows' => $limit, 'page' => $page], $knownTotal)
                ->toArray();
        }

        return $query
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();
    }

    /**
     * 同一筛选条件下一次聚合：COUNT(*) / SUM(money) / SUM(profit)
     * 口径与 paginate 内部 COUNT、sumMoneyByWhere、sumProfitByWhere 一致。
     *
     * @param array $where
     * @return array{count:int,money:float|int|string|null,profit:float|int|string|null}
     */
    public function aggregateStatsByWhere(array $where): array
    {
        $row = $this->alias('o')
            ->where($where)
            ->field('COUNT(*) AS agg_count, SUM(o.money) AS agg_money, SUM(o.profit) AS agg_profit')
            ->find();

        if (empty($row)) {
            return [
                'count'  => 0,
                'money'  => null,
                'profit' => null,
            ];
        }

        $data = is_array($row) ? $row : (method_exists($row, 'toArray') ? $row->toArray() : []);

        return [
            'count'  => (int)($data['agg_count'] ?? 0),
            'money'  => array_key_exists('agg_money', $data) ? $data['agg_money'] : null,
            'profit' => array_key_exists('agg_profit', $data) ? $data['agg_profit'] : null,
        ];
    }

    /**
     * @param array $where
     * @return float|int|string|null
     */
    public function sumMoneyByWhere(array $where)
    {
        return $this->alias('o')->where($where)->sum('o.money');
    }

    /**
     * @param array $where
     * @return float|int|string|null
     */
    public function sumProfitByWhere(array $where)
    {
        return $this->alias('o')->where($where)->sum('o.profit');
    }
}
