<?php

namespace app\admin\model;

use think\Model;

/**
 * 发货申请主表 crm_shipping_apply
 */
class ShippingApply extends Model
{
    protected $table = 'crm_shipping_apply';
    protected $pk = 'id';

    protected $autoWriteTimestamp = false;
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;
}
