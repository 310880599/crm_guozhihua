<?php

namespace app\admin\service;

use think\Db;
use Throwable;

/**
 * 客户历史订单查询服务
 */
class ClientOrderService
{
    /**
     * 根据客户ID分页获取历史审核通过订单
     *
     * @param int $clientId 客户ID
     * @param int $page 页码
     * @param int $limit 每页条数
     * @param int $excludeOrderId 需排除的订单ID
     * @return array{count:int,data:array}
     */
    public function getHistoryApprovedOrders(int $clientId, int $page = 1, int $limit = 10, int $excludeOrderId = 0): array
    {
        if ($clientId <= 0) {
            return ['count' => 0, 'data' => []];
        }

        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);

        // 第一步：读取客户电话/WhatsApp（contact_type=1/3）用于匹配订单联系方式
        $contactRows = Db::table('crm_contacts')
            ->where('leads_id', $clientId)
            ->where('is_delete', 0)
            ->whereIn('contact_type', [1, 3])
            ->field('contact_value')
            ->select();

        if (empty($contactRows)) {
            return ['count' => 0, 'data' => []];
        }

        $contacts = [];
        foreach ($contactRows as $contactRow) {
            $contact = $this->normalizeContact($contactRow['contact_value'] ?? '');
            if ($contact === '') {
                continue;
            }
            $contacts[$contact] = true;
        }
        $contacts = array_keys($contacts);

        if (empty($contacts)) {
            return ['count' => 0, 'data' => []];
        }

        // 第二步：按联系方式匹配已审核通过订单
        $baseQuery = Db::table('crm_client_order')
            ->whereIn('contact', $contacts)
            ->where('check_status', 2);

        if ($excludeOrderId > 0) {
            $baseQuery->where('id', '<>', $excludeOrderId);
        }

        try {
            $count = (int)(clone $baseQuery)->count('id');
        } catch (Throwable $e) {
            return ['count' => 0, 'data' => []];
        }

        if ($count <= 0) {
            return ['count' => 0, 'data' => []];
        }

        try {
            $rows = (clone $baseQuery)
                ->field('id,order_no,order_time,money,profit,product_name,pr_user')
                ->order('order_time', 'desc')
                ->order('id', 'desc')
                ->page($page, $limit)
                ->select();
        } catch (Throwable $e) {
            return ['count' => 0, 'data' => []];
        }

        foreach ($rows as &$row) {
            $row['order_no'] = trim((string)($row['order_no'] ?? ''));
            if ($row['order_no'] === '') {
                // 兜底：订单编号为空时使用订单ID，保证前端可展示
                $row['order_no'] = (string)($row['id'] ?? '');
            }
            $row['order_time'] = trim((string)($row['order_time'] ?? ''));
            $row['money'] = round((float)($row['money'] ?? 0), 2);
            $row['profit'] = round((float)($row['profit'] ?? 0), 2);
            $row['product_name'] = trim((string)($row['product_name'] ?? ''));
            $row['pr_user'] = trim((string)($row['pr_user'] ?? ''));
        }
        unset($row);

        return ['count' => $count, 'data' => $rows];
    }

    /**
     * 根据客户ID查询历史订单表（crm_client_history_order）
     *
     * 匹配逻辑与 getHistoryApprovedOrders 保持一致：读取客户在 crm_contacts 中
     * contact_type=1（手机）/3（WhatsApp）的联系方式，用于匹配历史订单的
     * client_phone 字段。仅新增，不影响/不修改 crm_client_order 相关逻辑。
     *
     * @param int $clientId 客户ID
     * @return array{count:int,data:array}
     */
    public function getHistoryClientHistoryOrders(int $clientId): array
    {
        if ($clientId <= 0) {
            return ['count' => 0, 'data' => []];
        }

        // 第一步：读取客户电话/WhatsApp（contact_type=1/3）用于匹配历史订单联系方式
        $contactRows = Db::table('crm_contacts')
            ->where('leads_id', $clientId)
            ->where('is_delete', 0)
            ->whereIn('contact_type', [1, 3])
            ->field('contact_value')
            ->select();

        if (empty($contactRows)) {
            return ['count' => 0, 'data' => []];
        }

        $contacts = [];
        foreach ($contactRows as $contactRow) {
            $contact = $this->normalizeContact($contactRow['contact_value'] ?? '');
            if ($contact === '') {
                continue;
            }
            $contacts[$contact] = true;
        }
        $contacts = array_keys($contacts);

        if (empty($contacts)) {
            return ['count' => 0, 'data' => []];
        }

        // 第二步：按联系方式匹配历史订单（crm_client_history_order），排除已删除记录
        try {
            $rows = Db::table('crm_client_history_order')
                ->whereIn('client_phone', $contacts)
                ->where('is_deleted', 0)
                ->field('id,order_no,order_time,money,profit,product_name,pr_user')
                ->order('order_time', 'desc')
                ->order('id', 'desc')
                ->select();
        } catch (Throwable $e) {
            return ['count' => 0, 'data' => []];
        }

        foreach ($rows as &$row) {
            $row['order_no'] = trim((string)($row['order_no'] ?? ''));
            if ($row['order_no'] === '') {
                // 兜底：订单编号为空时使用订单ID，保证前端可展示
                $row['order_no'] = (string)($row['id'] ?? '');
            }
            $row['order_time'] = trim((string)($row['order_time'] ?? ''));
            $row['money'] = round((float)($row['money'] ?? 0), 2);
            $row['profit'] = round((float)($row['profit'] ?? 0), 2);
            $row['product_name'] = trim((string)($row['product_name'] ?? ''));
            $row['pr_user'] = trim((string)($row['pr_user'] ?? ''));
            // 标识数据来源：历史订单
            $row['order_type'] = 'history';
        }
        unset($row);

        return ['count' => count($rows), 'data' => $rows];
    }

    /**
     * 规范化联系方式，避免空格影响匹配
     *
     * @param mixed $contact
     * @return string
     */
    private function normalizeContact($contact): string
    {
        $contact = trim((string)$contact);
        if ($contact === '') {
            return '';
        }
        return preg_replace('/\s+/', '', $contact);
    }
}
