<?php
namespace app\admin\model;

use think\Model;

class LiberumConfig extends Model
{
    protected $table = 'crm_liberum_config';
    protected $pk = 'id';

    protected $autoWriteTimestamp = 'int';
    protected $createTime = 'create_time';
    protected $updateTime = 'update_time';
    protected $dateFormat = false;
}
