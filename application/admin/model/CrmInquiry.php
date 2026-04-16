<?php

namespace app\admin\model;

use think\Model;

/**
 * 询盘渠道 crm_inquiry
 */
class CrmInquiry extends Model
{
    protected $table = 'crm_inquiry';

    /**
     * 启用状态的渠道名称列表（下拉）
     *
     * @return string[]
     */
    public static function listEnabledNames(): array
    {
        return (new static())
            ->where('status', 0)
            ->order('inquiry_name', 'asc')
            ->column('inquiry_name');
    }
}
