<?php
namespace app\admin\model;

use think\Model;

class ShippingConfig extends Model
{
    protected $table = 'crm_shipping_config';
    protected $pk = 'id';

    protected $autoWriteTimestamp = 'int';
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;
}
