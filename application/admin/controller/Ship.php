<?php

namespace app\admin\controller;

use app\admin\service\ShippingConfigService;
use think\facade\Request;

/**
 * 发货管理
 * 本次仅实现：发货管理配置（configShip）
 */
class Ship extends Common
{
    /**
     * @return ShippingConfigService
     */
    protected function getShippingConfigService()
    {
        return new ShippingConfigService();
    }

    /**
     * 发货管理配置
     * GET：展示配置页
     * POST：保存配置
     */
    public function configShip()
    {
        $service = $this->getShippingConfigService();

        // 业务层二次权限：超级管理员（不依赖菜单隐藏）
        if (!$service->isShippingConfigAdmin()) {
            if (request()->isPost() || request()->isAjax()) {
                return json(['code' => -200, 'msg' => '您无权限访问发货管理配置', 'data' => []]);
            }
            return $this->error('您无权限访问发货管理配置');
        }

        if (request()->isPost()) {
            $params = Request::param();
            $result = $service->saveConfig(is_array($params) ? $params : []);
            return json($result);
        }

        $pageData = $service->getPageData();
        $this->assign('config', $pageData['config']);
        $this->assign('adminOptions', $pageData['admin_options']);
        $this->assign('productDirector', $pageData['product_director']);
        $this->assign('financeCcUser', $pageData['finance_cc_user']);
        $this->assign('directorDisabledTip', $pageData['director_disabled_tip']);
        $this->assign('financeDisabledTip', $pageData['finance_disabled_tip']);

        return $this->fetch('ship/config_ship');
    }
}
