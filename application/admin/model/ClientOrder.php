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
     * @return array
     */
    public function paginatePersonSearch($where, $page, $limit)
    {
        return $this->alias('o')
            ->where($where)
            ->order('o.order_time', 'desc')
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();
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
