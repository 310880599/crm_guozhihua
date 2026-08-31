<?php

namespace app\admin\model;

use think\Model;

/**
 * 发货申请明细 crm_shipping_apply_item
 */
class ShippingApplyItem extends Model
{
    protected $table = 'crm_shipping_apply_item';
    protected $pk = 'id';

    protected $autoWriteTimestamp = false;
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;
}
