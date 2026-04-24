<?php
namespace app\admin\model;

use think\Model;

class PositionTitle extends Model
{
    // 指定对应的数据表名
    protected $table = 'crm_position_title';
    protected $pk    = 'id';

    // datetime 字段统一由 Service 层显式写入，避免 int 时间戳写入导致 SQL 报错
    protected $autoWriteTimestamp = false;
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = 'Y-m-d H:i:s';
}
