<?php

namespace app\admin\controller;

use app\admin\service\ShippingApplyService;
use app\admin\service\ShippingConfigService;
use think\facade\Request;

/**
 * 发货管理
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
     * @return ShippingApplyService
     */
    protected function getShippingApplyService()
    {
        return new ShippingApplyService();
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

    /**
     * 新建发货申请
     * GET：展示页面
     * POST：op 分发 AJAX（search_orders / get_order_detail / save_draft）
     */
    public function createShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();

            switch ($op) {
                case 'search_orders':
                    return json($service->searchOrdersForApply(is_array($params) ? $params : []));
                case 'get_order_detail':
                    return json($service->getOrderDetailForApply((int)Request::param('order_id', 0)));
                case 'save_draft':
                    return json($service->saveDraft(is_array($params) ? $params : []));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'data' => []]);
            }
        }

        $draftId = (int)Request::param('id', 0);
        $draftData = null;
        if ($draftId > 0) {
            $draftData = $service->getDraftForEdit($draftId);
        }

        $this->assign('draftDataJson', $draftData ? json_encode($draftData, JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_QUOT | JSON_HEX_AMP) : '');
        $this->assign('draftId', $draftId);

        return $this->fetch('ship/create_ship');
    }
}
