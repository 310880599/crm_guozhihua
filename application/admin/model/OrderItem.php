<?php

namespace app\admin\model;

use think\Model;
use think\Db;

/**
 * 订单明细 crm_order_item
 */
class OrderItem extends Model
{
    protected $table = 'crm_order_item';

    /**
     * 按订单 ID 批量查询明细产品名称，用于列表展示
     *
     * @param int[] $orderIds
     * @return array<int, array<int, array{product_name: string}>>
     */
    public static function getProductNamesGroupedByOrderIds(array $orderIds)
    {
        if (empty($orderIds)) {
            return [];
        }

        $items = Db::table('crm_order_item')
            ->alias('oi')
            ->leftJoin('crm_products p', 'oi.product_id = p.id')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id')
            ->whereIn('oi.order_id', $orderIds)
            ->order('oi.order_id asc, oi.line_no asc')
            ->field('oi.order_id, oi.product_name, oi.product_id, p.product_name as product_name_from_table')
            ->select();

        $orderItemsMap = [];
        foreach ($items as $item) {
            $orderId = $item['order_id'];
            $productName = !empty($item['product_name']) ? $item['product_name'] : ($item['product_name_from_table'] ?? '');
            if (!empty($productName)) {
                $orderItemsMap[$orderId][] = [
                    'product_name' => $productName,
                ];
            }
        }

        return $orderItemsMap;
    }
}
