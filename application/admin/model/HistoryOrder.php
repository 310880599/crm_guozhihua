<?php
namespace app\admin\model;

use think\Model;

class ReceiveAccount extends Model
{
    // 指定对应的数据表名
    protected $table = 'crm_receive_account';
    protected $pk    = 'id';

    // 用 int 时间戳写入/读取；字段名对应你的表
    protected $autoWriteTimestamp = 'int';
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;  // 不让 TP 在取出时自动格式化
}
