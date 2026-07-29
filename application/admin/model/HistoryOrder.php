<?php
namespace app\admin\model;

use think\Model;

class HistoryOrder extends Model
{
    protected $table = 'crm_client_history_order';
    protected $pk = 'id';
    protected $autoWriteTimestamp = true;
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = 'Y-m-d H:i:s';

    protected $field = [
        'id',
        'client_id',
        'client_phone',
        'order_no',
        'order_time',
        'money',
        'profit',
        'product_id',
        'product_name',
        'pr_user_id',
        'pr_user',
        'voucher_image',
        'remark',
        'create_user_id',
        'create_user',
        'create_time',
        'update_time',
        'is_deleted',
        'deleted_time',
        'deleted_by',
    ];
}
