<?php
namespace app\admin\model;

use think\Model;

class LiberumType extends Model
{
    // 指定对应的数据表名
    protected $table = 'crm_liberum_type';
    protected $pk = 'id';

    // 使用 int 时间戳，字段与表结构对应
    protected $autoWriteTimestamp = 'int';
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;
}
