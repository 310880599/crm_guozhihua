<?php

namespace app\admin\model;

use think\Model;

/**
 * 客户线索 crm_leads
 */
class Lead extends Model
{
    protected $table = 'crm_leads';

    /**
     * 统计（带别名 l，与检查订单 client_where 一致）
     *
     * @param array $where
     * @return int
     */
    public function countWithAlias(array $where): int
    {
        return (int) $this->alias('l')->where($where)->count();
    }
}
