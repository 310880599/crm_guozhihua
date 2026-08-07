<?php

namespace app\admin\model;

use think\Model;

/**
 * 客户行颜色标记（crm_client_row_mark）
 * 仅作为简单表模型，不承载业务逻辑（业务逻辑放在 ClientRowMarkService）。
 */
class ClientRowMark extends Model
{
    protected $table = 'crm_client_row_mark';
}
