<?php

namespace app\admin\model;

use think\Model;

/**
 * 询盘端口 crm_inquiry_port
 */
class CrmInquiryPort extends Model
{
    protected $table = 'crm_inquiry_port';

    /**
     * 启用状态的端口名称列表（下拉）
     *
     * @return string[]
     */
    public static function listEnabledNames(): array
    {
        return (new static())
            ->where('status', 0)
            ->order('port_name', 'asc')
            ->column('port_name');
    }
}
