<?php
namespace app\admin\model;

use think\Model;

class HistoryOrder extends Model
{
    protected $table = 'crm_client_history_order';
    protected $pk = 'id';
    // 该模块的 create_time / update_time 已在 Controller 中手动维护为 datetime 字符串，
    // 关闭 ThinkPHP 自动时间戳写入，避免框架将 datetime 字符串当作 Unix 时间戳处理
    protected $autoWriteTimestamp = false;
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';

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
