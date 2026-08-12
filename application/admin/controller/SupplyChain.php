<?php

namespace app\admin\controller;

use app\admin\service\SupplyChainService;
use think\facade\Request;

/**
 * 供应链分析模块（V1，仅后端）。
 *
 * 职责边界：
 *   Controller 只负责参数接收、调用 Service、返回 JSON；
 *   不写复杂业务与 SQL（统计口径统一在 app\admin\service\SupplyChainService 中实现）。
 *
 * 权限：
 * 本模块不设置独立 group_id / username 白名单。
 * 统一交由 Common::initialize() 和 CRM 现有 auth_rule/auth_group 权限体系控制。
 * 用户能否看到/访问"供应链管理 → 供应链分析"，由后台现有权限管理配置决定。
 */
class SupplyChain extends Common
{
    public function initialize()
    {
        parent::initialize();
    }

    // =========================================================
    // 页面方法（本阶段只做后端，不产出前端模板；模板由后续阶段补充）
    // =========================================================

    /**
     * 供应链分析首页。
     */
    public function index()
    {
        return $this->fetch();
    }

    /**
     * 产品排行页面。
     */
    public function productRank()
    {
        return $this->fetch();
    }

    /**
     * 供应商排行页面。
     */
    public function supplierRank()
    {
        return $this->fetch();
    }

    /**
     * 产品-供应商详情页面（给定产品，查看其供应商拆分）。
     */
    public function productSupplierDetail()
    {
        return $this->fetch();
    }

    // =========================================================
    // Ajax 数据接口
    // =========================================================

    /**
     * 产品排行数据（按 product_id 分组）。
     */
    public function getProductRankData()
    {
        try {
            $params = $this->collectCommonParams();

            $service = new SupplyChainService();
            $result = $service->getProductRank($params);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result['list'],
                'count' => $result['total'],
                'summary' => $result['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[SupplyChain] getProductRankData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取供应链统计数据失败',
                'data' => [],
                'count' => 0,
                'summary' => [],
            ]);
        }
    }

    /**
     * 供应商排行数据（按 supplier_id 分组）。
     */
    public function getSupplierRankData()
    {
        try {
            $params = $this->collectCommonParams();

            $service = new SupplyChainService();
            $result = $service->getSupplierRank($params);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result['list'],
                'count' => $result['total'],
                'summary' => $result['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[SupplyChain] getSupplierRankData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取供应链统计数据失败',
                'data' => [],
                'count' => 0,
                'summary' => [],
            ]);
        }
    }

    /**
     * 产品-供应商详情数据：给定 product_id，按 supplier_id 拆分展示该产品对应的供应商明细。
     *
     * 注意：product_id 为空代表"未分类产品"分组，仅用于排行页的统计展示，
     * 不允许作为详情下钻对象，此处必须拒绝并返回参数错误。
     */
    public function getProductSupplierDetailData()
    {
        $productId = trim((string)Request::param('product_id', ''));
        if ($productId === '') {
            return json(['code' => -200, 'msg' => 'product_id 不能为空', 'data' => [], 'count' => 0, 'summary' => []]);
        }

        try {
            $params = $this->collectCommonParams();

            $service = new SupplyChainService();
            $result = $service->getSupplierBreakdownByProduct($productId, $params);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result['list'],
                'count' => $result['total'],
                'summary' => $result['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[SupplyChain] getProductSupplierDetailData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取供应链统计数据失败',
                'data' => [],
                'count' => 0,
                'summary' => [],
            ]);
        }
    }

    /**
     * 供应商下的产品明细：给定 supplier_id，按 product_id 拆分展示该供应商对应的产品明细
     * （用于供应商排行页的下钻查看）。
     *
     * 注意：supplier_id 为空代表"未分类供应商"分组，仅用于排行页的统计展示，
     * 不允许作为详情下钻对象，此处必须拒绝并返回参数错误。
     */
    public function getSupplierProducts()
    {
        $supplierId = trim((string)Request::param('supplier_id', ''));
        if ($supplierId === '') {
            return json(['code' => -200, 'msg' => 'supplier_id 不能为空', 'data' => [], 'count' => 0, 'summary' => []]);
        }

        try {
            $params = $this->collectCommonParams();

            $service = new SupplyChainService();
            $result = $service->getProductBreakdownBySupplier($supplierId, $params);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result['list'],
                'count' => $result['total'],
                'summary' => $result['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[SupplyChain] getSupplierProducts failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取供应链统计数据失败',
                'data' => [],
                'count' => 0,
                'summary' => [],
            ]);
        }
    }

    // =========================================================
    // 内部工具方法
    // =========================================================

    /**
     * 收集通用查询参数：时间筛选（与 DataStatistics 现有口径一致）+ 关键字 + 排序 + 分页。
     */
    private function collectCommonParams(): array
    {
        return [
            'timebucket' => (string)Request::param('timebucket', ''),
            'at_time' => (string)Request::param('at_time', ''),
            'month_keys' => trim((string)Request::param('month_keys', '')),
            'keyword' => trim((string)Request::param('keyword', '')),
            'sort_field' => (string)Request::param('sort_field', ''),
            'sort_order' => (string)Request::param('sort_order', ''),
            'page' => (int)Request::param('page', 1),
            'limit' => (int)Request::param('limit', 0),
        ];
    }
}
