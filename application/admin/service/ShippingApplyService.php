<?php

namespace app\admin\service;

use app\admin\model\ShippingApply;
use app\admin\model\ShippingApplyItem;
use think\Db;
use think\Exception;

class ShippingApplyService
{
    /** @var int[] 占用订单数量的发货申请状态 */
    protected $occupiedStatuses = [10, 30, 50];

    /**
     * 是否超级管理员（可创建全部有效订单的发货申请）
     * admin_id==1 OR group_id==1 OR username==admin
     *
     * @param int $adminId
     * @return bool
     */
    public function isShippingApplySuperAdmin($adminId = 0)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            $adminId = (int)session('aid');
        }
        if ($adminId <= 0) {
            return false;
        }
        if ($adminId === 1) {
            return true;
        }

        $admin = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,group_id,username')
            ->find();
        if (empty($admin)) {
            return false;
        }
        if ((int)($admin['group_id'] ?? 0) === 1) {
            return true;
        }
        if (strtolower(trim((string)($admin['username'] ?? ''))) === 'admin') {
            return true;
        }

        return false;
    }

    /**
     * 获取当前登录管理员信息
     *
     * @return array
     */
    public function getCurrentAdminInfo()
    {
        $adminId = (int)session('aid');
        if ($adminId <= 0) {
            return ['admin_id' => 0, 'username' => ''];
        }
        $row = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,username,group_id')
            ->find();

        return [
            'admin_id' => (int)($row['admin_id'] ?? 0),
            'username' => trim((string)($row['username'] ?? '')),
            'group_id' => (int)($row['group_id'] ?? 0),
        ];
    }

    /**
     * 搜索可创建发货申请的订单
     *
     * @param array $params
     * @return array
     */
    public function searchOrdersForApply($params = [])
    {
        $params = is_array($params) ? $params : [];
        $keyword = trim((string)($params['keyword'] ?? ''));
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, min(100, (int)($params['limit'] ?? 10)));

        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => [], 'count' => 0];
        }

        $query = Db::name('crm_client_order')->alias('o')
            ->where('o.check_status', 2);

        if (!$this->isShippingApplySuperAdmin($adminId)) {
            $query->where(function ($q) use ($adminId) {
                $q->where('o.pr_user_id', $adminId)->whereOr('o.at_user_id', $adminId);
            });
        }

        if ($keyword !== '') {
            $query->where(function ($q) use ($keyword) {
                $q->whereLike('o.order_no', '%' . $keyword . '%')
                    ->whereOr('o.cname', 'like', '%' . $keyword . '%')
                    ->whereOr('o.pr_user', 'like', '%' . $keyword . '%');
            });
        }

        $count = (int)(clone $query)->count('o.id');
        $rows = (clone $query)
            ->field('o.id,o.order_no,o.cname,o.pr_user,o.team_name,o.money,o.order_time')
            ->order('o.order_time', 'desc')
            ->order('o.id', 'desc')
            ->page($page, $limit)
            ->select();

        $data = [];
        foreach ((array)$rows as $row) {
            $data[] = [
                'id' => (int)($row['id'] ?? 0),
                'order_no' => trim((string)($row['order_no'] ?? '')),
                'cname' => trim((string)($row['cname'] ?? '')),
                'pr_user' => trim((string)($row['pr_user'] ?? '')),
                'team_name' => trim((string)($row['team_name'] ?? '')),
                'money' => round((float)($row['money'] ?? 0), 2),
                'order_time' => trim((string)($row['order_time'] ?? '')),
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'data' => $data,
            'count' => $count,
        ];
    }

    /**
     * 获取订单详情（含产品明细与数量统计）
     *
     * @param int $orderId
     * @return array
     */
    public function getOrderDetailForApply($orderId)
    {
        $orderId = (int)$orderId;
        if ($orderId <= 0) {
            return ['code' => -200, 'msg' => '订单不存在。', 'data' => []];
        }

        try {
            $order = $this->assertOrderAccessible($orderId);
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        }

        $orderItems = Db::name('crm_order_item')
            ->where('order_id', $orderId)
            ->order('line_no', 'asc')
            ->order('id', 'asc')
            ->select();

        if (empty($orderItems)) {
            return ['code' => -200, 'msg' => '订单没有产品明细。', 'data' => []];
        }

        $orderItemIds = [];
        foreach ($orderItems as $item) {
            $orderItemIds[] = (int)($item['id'] ?? 0);
        }
        $appliedMap = $this->calcAppliedQtyMap($orderItemIds);
        $warnings = [];

        $items = [];
        foreach ($orderItems as $item) {
            $orderItemId = (int)($item['id'] ?? 0);
            $productName = trim((string)($item['product_name'] ?? ''));
            if ($productName === '') {
                $productName = '未命名产品';
            }

            try {
                $managerInfo = $this->resolveProductManager($item['manager_id'] ?? '', $productName);
            } catch (Exception $e) {
                return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
            }

            $orderQty = (int)($item['qty'] ?? 0);
            $appliedQty = (int)($appliedMap[$orderItemId] ?? 0);
            $remainingQty = $orderQty - $appliedQty;
            if ($remainingQty < 0) {
                $warnings[] = '产品【' . $productName . '】已申请数量超过订单数量，剩余数量按0处理。';
                $remainingQty = 0;
            }

            $items[] = [
                'order_item_id' => $orderItemId,
                'product_name' => $productName,
                'product_manager_name' => $managerInfo['username'],
                'spec_model' => trim((string)($item['spec_model'] ?? '')),
                'unit' => trim((string)($item['unit'] ?? '')),
                'order_qty' => $orderQty,
                'applied_qty' => $appliedQty,
                'remaining_qty' => $remainingQty,
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'data' => [
                'order' => [
                    'id' => (int)$order['id'],
                    'order_no' => trim((string)($order['order_no'] ?? '')),
                    'cname' => trim((string)($order['cname'] ?? '')),
                    'pr_user' => trim((string)($order['pr_user'] ?? '')),
                    'team_name' => trim((string)($order['team_name'] ?? '')),
                    'order_time' => trim((string)($order['order_time'] ?? '')),
                    'money' => round((float)($order['money'] ?? 0), 2),
                    'amount_received' => trim((string)($order['amount_received'] ?? '')),
                    'contact' => trim((string)($order['contact'] ?? '')),
                    'default_receiver_phone' => trim((string)($order['contact'] ?? '')),
                    'default_receiver_address' => $this->buildDefaultReceiverAddress($order),
                ],
                'items' => $items,
                'warnings' => $warnings,
            ],
        ];
    }

    /**
     * 读取草稿用于编辑
     *
     * @param int $applyId
     * @return array|null
     */
    public function getDraftForEdit($applyId)
    {
        $applyId = (int)$applyId;
        if ($applyId <= 0) {
            return null;
        }

        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return null;
        }

        $apply = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
        if (empty($apply) || (int)($apply['status'] ?? -1) !== 0) {
            return null;
        }

        if ((int)($apply['applicant_id'] ?? 0) !== $adminId && !$this->isShippingApplySuperAdmin($adminId)) {
            return null;
        }

        $detailResult = $this->getOrderDetailForApply((int)($apply['order_id'] ?? 0));
        if ((int)($detailResult['code'] ?? -1) !== 0) {
            return null;
        }

        $savedItems = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->field('order_item_id,apply_qty,remark')
            ->select();

        $savedMap = [];
        foreach ((array)$savedItems as $row) {
            $savedMap[(int)($row['order_item_id'] ?? 0)] = [
                'apply_qty' => (int)($row['apply_qty'] ?? 0),
                'remark' => trim((string)($row['remark'] ?? '')),
            ];
        }

        $items = $detailResult['data']['items'] ?? [];
        foreach ($items as &$item) {
            $oid = (int)($item['order_item_id'] ?? 0);
            if (isset($savedMap[$oid])) {
                $item['apply_qty'] = $savedMap[$oid]['apply_qty'];
                $item['remark'] = $savedMap[$oid]['remark'];
            } else {
                $item['apply_qty'] = 0;
                $item['remark'] = '';
            }
        }
        unset($item);

        return [
            'apply_id' => $applyId,
            'apply_no' => trim((string)($apply['apply_no'] ?? '')),
            'order_id' => (int)($apply['order_id'] ?? 0),
            'shipping_date' => trim((string)($apply['shipping_date'] ?? '')),
            'receiver_name' => trim((string)($apply['receiver_name'] ?? '')),
            'receiver_phone' => trim((string)($apply['receiver_phone'] ?? '')),
            'receiver_address' => trim((string)($apply['receiver_address'] ?? '')),
            'shipping_method' => trim((string)($apply['shipping_method'] ?? '')),
            'shipping_remark' => trim((string)($apply['shipping_remark'] ?? '')),
            'order' => $detailResult['data']['order'] ?? [],
            'items' => $items,
        ];
    }

    /**
     * 保存草稿
     *
     * @param array $params
     * @return array
     */
    public function saveDraft($params = [])
    {
        $params = is_array($params) ? $params : [];
        $orderId = (int)($params['order_id'] ?? 0);
        $applyId = (int)($params['apply_id'] ?? 0);
        $itemsRaw = $params['items'] ?? [];

        if ($orderId <= 0) {
            return ['code' => -200, 'msg' => '请先选择有效订单。', 'data' => []];
        }

        if (is_string($itemsRaw)) {
            $decoded = json_decode($itemsRaw, true);
            $itemsRaw = is_array($decoded) ? $decoded : [];
        }
        if (!is_array($itemsRaw)) {
            $itemsRaw = [];
        }

        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }

        $shippingDate = trim((string)($params['shipping_date'] ?? ''));
        if ($shippingDate !== '' && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $shippingDate)) {
            return ['code' => -200, 'msg' => '预计发货日期格式不正确。', 'data' => []];
        }
        if ($shippingDate === '') {
            $shippingDate = null;
        }

        $receiverName = trim((string)($params['receiver_name'] ?? ''));
        $receiverPhone = trim((string)($params['receiver_phone'] ?? ''));
        $receiverAddress = trim((string)($params['receiver_address'] ?? ''));
        $shippingMethod = trim((string)($params['shipping_method'] ?? ''));
        $shippingRemark = trim((string)($params['shipping_remark'] ?? ''));

        $maxRetry = 3;
        for ($attempt = 1; $attempt <= $maxRetry; $attempt++) {
            Db::startTrans();
            try {
                $order = Db::name('crm_client_order')
                    ->where('id', $orderId)
                    ->lock(true)
                    ->find();
                if (empty($order)) {
                    throw new Exception('订单不存在。');
                }

                $this->validateOrderForApply($order, $adminId);

                Db::name('crm_order_item')
                    ->where('order_id', $orderId)
                    ->lock(true)
                    ->select();

                $existingApply = null;
                if ($applyId > 0) {
                    $existingApply = Db::name('crm_shipping_apply')
                        ->where('id', $applyId)
                        ->lock(true)
                        ->find();
                    if (empty($existingApply)) {
                        throw new Exception('草稿不存在。');
                    }
                    if ((int)($existingApply['status'] ?? -1) !== 0) {
                        throw new Exception('只能编辑草稿状态的发货申请。');
                    }
                    if ((int)($existingApply['applicant_id'] ?? 0) !== $adminId && !$this->isShippingApplySuperAdmin($adminId)) {
                        throw new Exception('您无权编辑该草稿。');
                    }
                    if ((int)($existingApply['order_id'] ?? 0) !== $orderId) {
                        throw new Exception('草稿关联订单不匹配。');
                    }
                }

                $orderItems = Db::name('crm_order_item')
                    ->where('order_id', $orderId)
                    ->order('line_no', 'asc')
                    ->order('id', 'asc')
                    ->select();
                if (empty($orderItems)) {
                    throw new Exception('订单没有产品明细。');
                }

                $orderItemMap = [];
                foreach ($orderItems as $row) {
                    $orderItemMap[(int)($row['id'] ?? 0)] = $row;
                }

                $orderItemIds = array_keys($orderItemMap);
                $appliedMap = $this->calcAppliedQtyMap($orderItemIds, $applyId > 0 ? $applyId : 0);

                $itemSnapshots = [];
                $hasPositiveQty = false;
                $seenOrderItemIds = [];

                foreach ($itemsRaw as $inputItem) {
                    if (!is_array($inputItem)) {
                        continue;
                    }

                    $orderItemId = $this->parsePositiveInt(
                        $inputItem['order_item_id'] ?? null,
                        '产品明细参数异常，请刷新页面后重新提交。'
                    );
                    if (isset($seenOrderItemIds[$orderItemId])) {
                        throw new Exception('同一订单产品不能重复提交，请刷新页面后重新操作。');
                    }
                    $seenOrderItemIds[$orderItemId] = true;

                    $applyQty = $this->parseNonNegativeInt(
                        $inputItem['apply_qty'] ?? null,
                        '本次申请发货数量必须是大于等于0的整数。'
                    );
                    $itemRemark = trim((string)($inputItem['remark'] ?? ''));

                    if ($applyQty === 0) {
                        continue;
                    }
                    if (!isset($orderItemMap[$orderItemId])) {
                        throw new Exception('产品明细不属于当前订单。');
                    }

                    $orderItem = $orderItemMap[$orderItemId];
                    $productName = trim((string)($orderItem['product_name'] ?? ''));
                    if ($productName === '') {
                        $productName = '未命名产品';
                    }

                    $orderQty = (int)($orderItem['qty'] ?? 0);
                    $appliedQty = (int)($appliedMap[$orderItemId] ?? 0);
                    $remainingQty = $orderQty - $appliedQty;
                    if ($remainingQty < 0) {
                        $remainingQty = 0;
                    }
                    if ($applyQty > $remainingQty) {
                        throw new Exception('产品【' . $productName . '】剩余可申请数量不足。');
                    }

                    $managerInfo = $this->resolveProductManager($orderItem['manager_id'] ?? '', $productName);
                    $snapshot = $this->buildShippingItemSnapshot($orderItem, $managerInfo, $applyQty, $itemRemark);
                    $itemSnapshots[] = $snapshot;
                    $hasPositiveQty = true;
                }

                if (!$hasPositiveQty) {
                    throw new Exception('至少需要选择一个产品并填写本次发货数量。');
                }

                $now = date('Y-m-d H:i:s');
                $applyNo = '';
                if ($existingApply) {
                    $applyNo = trim((string)($existingApply['apply_no'] ?? ''));
                    $updateData = [
                        'order_no' => trim((string)($order['order_no'] ?? '')),
                        'order_amount' => round((float)($order['money'] ?? 0), 2),
                        'team_name' => trim((string)($order['team_name'] ?? '')),
                        'customer_name' => trim((string)($order['cname'] ?? '')),
                        'customer_phone' => trim((string)($order['contact'] ?? '')),
                        'receiver_name' => $receiverName,
                        'receiver_phone' => $receiverPhone,
                        'receiver_address' => $receiverAddress,
                        'shipping_date' => $shippingDate,
                        'shipping_method' => $shippingMethod,
                        'shipping_remark' => $shippingRemark,
                        'update_time' => $now,
                    ];
                    Db::name('crm_shipping_apply')->where('id', $applyId)->update($updateData);
                    Db::name('crm_shipping_apply_item')->where('shipping_apply_id', $applyId)->delete();
                    $savedApplyId = $applyId;
                } else {
                    $applyNo = $this->generateApplyNo();
                    $insertData = [
                        'apply_no' => $applyNo,
                        'order_id' => $orderId,
                        'order_no' => trim((string)($order['order_no'] ?? '')),
                        'order_amount' => round((float)($order['money'] ?? 0), 2),
                        'applicant_id' => $adminId,
                        'applicant_name' => trim((string)($admin['username'] ?? '')),
                        'team_name' => trim((string)($order['team_name'] ?? '')),
                        'team_leader_id' => null,
                        'team_leader_name' => '',
                        'customer_name' => trim((string)($order['cname'] ?? '')),
                        'customer_phone' => trim((string)($order['contact'] ?? '')),
                        'receiver_name' => $receiverName,
                        'receiver_phone' => $receiverPhone,
                        'receiver_address' => $receiverAddress,
                        'shipping_date' => $shippingDate,
                        'shipping_method' => $shippingMethod,
                        'shipping_remark' => $shippingRemark,
                        'product_director_name' => '',
                        'finance_cc_user_name' => '',
                        'status' => 0,
                        'current_stage' => 0,
                        'submit_time' => null,
                        'approved_time' => null,
                        'create_time' => $now,
                        'update_time' => $now,
                    ];
                    $savedApplyId = (int)Db::name('crm_shipping_apply')->insertGetId($insertData);
                    if ($savedApplyId <= 0) {
                        throw new Exception('草稿保存失败，请稍后重试。');
                    }
                }

                foreach ($itemSnapshots as $snapshot) {
                    $snapshot['shipping_apply_id'] = $savedApplyId;
                    $snapshot['create_time'] = $now;
                    $snapshot['update_time'] = $now;
                    Db::name('crm_shipping_apply_item')->insert($snapshot);
                }

                Db::commit();

                return [
                    'code' => 0,
                    'msg' => '草稿保存成功',
                    'data' => [
                        'apply_id' => $savedApplyId,
                        'apply_no' => $applyNo !== '' ? $applyNo : trim((string)($existingApply['apply_no'] ?? '')),
                    ],
                ];
            } catch (Exception $e) {
                Db::rollback();
                $msg = $e->getMessage();
                if (stripos($msg, 'Duplicate entry') !== false && stripos($msg, 'uniq_apply_no') !== false) {
                    if ($attempt < $maxRetry) {
                        continue;
                    }
                    return ['code' => -200, 'msg' => '发货申请编号生成冲突，请重新保存。', 'data' => []];
                }
                return ['code' => -200, 'msg' => $msg, 'data' => []];
            } catch (\Throwable $e) {
                Db::rollback();
                $msg = $e->getMessage();
                if (stripos($msg, 'Duplicate entry') !== false && stripos($msg, 'uniq_apply_no') !== false) {
                    if ($attempt < $maxRetry) {
                        continue;
                    }
                    return ['code' => -200, 'msg' => '发货申请编号生成冲突，请重新保存。', 'data' => []];
                }
                return ['code' => -200, 'msg' => '草稿保存失败，请稍后重试。', 'data' => []];
            }
        }

        return ['code' => -200, 'msg' => '发货申请编号生成失败。', 'data' => []];
    }

    /**
     * 校验订单可访问并返回订单行
     *
     * @param int $orderId
     * @return array
     * @throws Exception
     */
    public function assertOrderAccessible($orderId)
    {
        $orderId = (int)$orderId;
        $order = Db::name('crm_client_order')->where('id', $orderId)->find();
        if (empty($order)) {
            throw new Exception('订单不存在。');
        }
        $adminId = (int)session('aid');
        $this->validateOrderForApply($order, $adminId);
        return $order;
    }

    /**
     * @param array $order
     * @param int $adminId
     * @throws Exception
     */
    protected function validateOrderForApply($order, $adminId)
    {
        if ((int)($order['check_status'] ?? -1) !== 2) {
            throw new Exception('订单尚未审核通过，不能创建发货申请。');
        }

        if ($this->isShippingApplySuperAdmin($adminId)) {
            return;
        }

        $prUserId = (int)($order['pr_user_id'] ?? 0);
        $atUserId = (int)($order['at_user_id'] ?? 0);
        if ($adminId !== $prUserId && $adminId !== $atUserId) {
            throw new Exception('您无权为该订单创建发货申请。');
        }
    }

    /**
     * 统计已有效申请数量
     *
     * @param int[] $orderItemIds
     * @param int $excludeApplyId 编辑草稿时排除自身
     * @return array<int,int>
     */
    public function calcAppliedQtyMap(array $orderItemIds, $excludeApplyId = 0)
    {
        $orderItemIds = array_values(array_filter(array_map('intval', $orderItemIds), function ($id) {
            return $id > 0;
        }));
        if (empty($orderItemIds)) {
            return [];
        }

        $query = Db::name('crm_shipping_apply_item')->alias('sai')
            ->join('crm_shipping_apply sa', 'sa.id = sai.shipping_apply_id')
            ->whereIn('sai.order_item_id', $orderItemIds)
            ->whereIn('sa.status', $this->occupiedStatuses);

        $excludeApplyId = (int)$excludeApplyId;
        if ($excludeApplyId > 0) {
            $query->where('sa.id', '<>', $excludeApplyId);
        }

        $rows = $query
            ->field('sai.order_item_id, SUM(sai.apply_qty) AS applied_qty')
            ->group('sai.order_item_id')
            ->select();

        $map = [];
        foreach ((array)$rows as $row) {
            $map[(int)($row['order_item_id'] ?? 0)] = (int)($row['applied_qty'] ?? 0);
        }

        return $map;
    }

    /**
     * 生成发货申请编号 FH + YYYYMMDD + 4位流水
     *
     * @return string
     * @throws Exception
     */
    public function generateApplyNo()
    {
        $datePart = date('Ymd');
        $prefix = 'FH' . $datePart;

        $maxApplyNo = Db::name('crm_shipping_apply')
            ->whereLike('apply_no', $prefix . '%')
            ->order('apply_no', 'desc')
            ->lock(true)
            ->value('apply_no');

        $seq = 1;
        if (!empty($maxApplyNo) && preg_match('/^FH\d{8}(\d{4})$/', (string)$maxApplyNo, $matches)) {
            $seq = ((int)$matches[1]) + 1;
        }
        if ($seq > 9999) {
            throw new Exception('发货申请编号生成失败。');
        }

        return $prefix . str_pad((string)$seq, 4, '0', STR_PAD_LEFT);
    }

    /**
     * 构建明细快照（不信任前端快照字段）
     *
     * @param array $orderItem
     * @param array $managerInfo
     * @param int $applyQty
     * @param string $remark
     * @return array
     */
    public function buildShippingItemSnapshot($orderItem, $managerInfo, $applyQty, $remark = '')
    {
        $orderItemId = (int)($orderItem['id'] ?? 0);
        $orderId = (int)($orderItem['order_id'] ?? 0);

        return [
            'order_item_id' => $orderItemId,
            'order_id' => $orderId,
            'line_no' => (int)($orderItem['line_no'] ?? 1),
            'product_id' => trim((string)($orderItem['product_id'] ?? '')),
            'product_name' => trim((string)($orderItem['product_name'] ?? '')),
            'product_manager_id' => (int)($managerInfo['admin_id'] ?? 0),
            'product_manager_name' => trim((string)($managerInfo['username'] ?? '')),
            'spec_model' => trim((string)($orderItem['spec_model'] ?? '')),
            'supplier_id' => trim((string)($orderItem['supplier_id'] ?? '')),
            'supplier_name' => trim((string)($orderItem['supplier_name'] ?? '')),
            'unit' => trim((string)($orderItem['unit'] ?? '')),
            'order_qty' => (int)($orderItem['qty'] ?? 0),
            'apply_qty' => (int)$applyQty,
            'unit_price' => round((float)($orderItem['unit_price'] ?? 0), 2),
            'total_price' => round((float)($orderItem['total_price'] ?? 0), 2),
            'remark' => trim((string)$remark),
        ];
    }

    /**
     * 解析合法正整数（>0）
     *
     * @param mixed $raw
     * @param string $errorMsg
     * @return int
     * @throws Exception
     */
    protected function parsePositiveInt($raw, $errorMsg)
    {
        if (is_int($raw)) {
            if ($raw <= 0) {
                throw new Exception($errorMsg);
            }
            return $raw;
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            if ($raw === '' || !preg_match('/^\d+$/', $raw)) {
                throw new Exception($errorMsg);
            }
            $val = (int)$raw;
            if ($val <= 0) {
                throw new Exception($errorMsg);
            }
            return $val;
        }
        throw new Exception($errorMsg);
    }

    /**
     * 解析非负整数（>=0）
     *
     * @param mixed $raw
     * @param string $errorMsg
     * @return int
     * @throws Exception
     */
    protected function parseNonNegativeInt($raw, $errorMsg)
    {
        if ($raw === null) {
            return 0;
        }
        if (is_int($raw)) {
            if ($raw < 0) {
                throw new Exception($errorMsg);
            }
            return $raw;
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            if ($raw === '' || !preg_match('/^\d+$/', $raw)) {
                throw new Exception($errorMsg);
            }
            return (int)$raw;
        }
        throw new Exception($errorMsg);
    }

    /**
     * 解析产品经理
     *
     * @param mixed $managerIdRaw
     * @param string $productName
     * @return array{admin_id:int,username:string}
     * @throws Exception
     */
    protected function resolveProductManager($managerIdRaw, $productName)
    {
        $productName = trim((string)$productName);
        if ($productName === '') {
            $productName = '未命名产品';
        }

        $raw = trim((string)$managerIdRaw);
        if ($raw === '' || !preg_match('/^\d+$/', $raw)) {
            throw new Exception('产品【' . $productName . '】的产品经理配置异常，请先修正订单产品经理后再创建发货申请。');
        }

        $managerId = (int)$raw;
        if ($managerId <= 0) {
            throw new Exception('产品【' . $productName . '】的产品经理配置异常，请先修正订单产品经理后再创建发货申请。');
        }

        $admin = Db::name('admin')
            ->where('admin_id', $managerId)
            ->field('admin_id,username')
            ->find();
        if (empty($admin) || trim((string)($admin['username'] ?? '')) === '') {
            throw new Exception('产品【' . $productName . '】的产品经理配置异常，请先修正订单产品经理后再创建发货申请。');
        }

        return [
            'admin_id' => (int)$admin['admin_id'],
            'username' => trim((string)$admin['username']),
        ];
    }

    /**
     * 根据订单字段拼接默认收货地址（避免重复拼接省市区）
     *
     * @param array $order
     * @return string
     */
    protected function buildDefaultReceiverAddress(array $order)
    {
        $province = trim((string)($order['province'] ?? ''));
        $city = trim((string)($order['city'] ?? ''));
        $country = trim((string)($order['country'] ?? ''));

        if ($country === '') {
            return $province . $city;
        }

        $prefix = $province . $city;
        if ($prefix !== '' && mb_strpos($country, $prefix) === 0) {
            return $country;
        }
        if ($province !== '' && mb_strpos($country, $province) === 0) {
            return $country;
        }
        if ($prefix !== '') {
            return $prefix . $country;
        }

        return $country;
    }
}
