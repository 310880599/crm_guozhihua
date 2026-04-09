<?php
namespace app\admin\model;

use think\Model;

class ClientRank extends Model
{
    // 指定对应的数据表名
    protected $table = 'crm_client_rank';
    protected $pk    = 'id';

    // 用 int 时间戳写入/读取；字段名对应你的表
    protected $autoWriteTimestamp = 'int';
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;  // 不让 TP 在取出时自动格式化

    // 兜底保障：即使调用方遗漏 add_time，模型新增时也补当前时间戳
    protected $insert = ['add_time'];

    protected function setAddTimeAttr($value)
    {
        return empty($value) ? time() : (int)$value;
    }
}
