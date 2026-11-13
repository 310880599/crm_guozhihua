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

    /** DECIMAL(12,2) 绝对值上限 */
    const MONEY_ABS_MAX = '9999999999.99';

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

            $orderPurchaseAmount = $this->normalizeNullableMoney($item['purchase_price'] ?? null);
            $suggestPurchaseUnitPrice = $this->calcSuggestPurchaseUnitPrice($orderPurchaseAmount, $orderQty);

            $items[] = [
                'order_item_id' => $orderItemId,
                'product_name' => $productName,
                'product_manager_id' => (int)($managerInfo['admin_id'] ?? 0),
                'product_manager_name' => $managerInfo['username'],
                'spec_model' => trim((string)($item['spec_model'] ?? '')),
                'unit' => trim((string)($item['unit'] ?? '')),
                'order_qty' => $orderQty,
                'applied_qty' => $appliedQty,
                'remaining_qty' => $remainingQty,
                'unit_price' => $this->normalizeNullableMoney($item['unit_price'] ?? null, false),
                'total_price' => $this->normalizeNullableMoney($item['total_price'] ?? null, false),
                'order_purchase_amount' => $orderPurchaseAmount,
                'suggest_purchase_unit_price' => $suggestPurchaseUnitPrice,
                'supplier_id' => trim((string)($item['supplier_id'] ?? '')),
                'supplier_name' => trim((string)($item['supplier_name'] ?? '')),
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

        $orderSourceItems = $detailResult['data']['items'] ?? [];
        $orderItemMap = [];
        foreach ($orderSourceItems as $src) {
            $orderItemMap[(int)($src['order_item_id'] ?? 0)] = $src;
        }

        $savedItems = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->order('apply_line_no', 'asc')
            ->order('id', 'asc')
            ->select();

        $shippingLines = [];
        $hasInvalidLines = false;
        $warnings = [];

        foreach ((array)$savedItems as $row) {
            $orderItemId = (int)($row['order_item_id'] ?? 0);
            $src = isset($orderItemMap[$orderItemId]) ? $orderItemMap[$orderItemId] : null;
            $isInvalid = empty($src);
            if ($isInvalid) {
                $hasInvalidLines = true;
            }

            $snapProductName = trim((string)($row['product_name'] ?? ''));
            $snapSpecModel = trim((string)($row['spec_model'] ?? ''));
            $snapOrderQty = (int)($row['order_qty'] ?? 0);
            $snapUnitPrice = $this->normalizeNullableMoney($row['unit_price'] ?? null, false);
            $snapTotalPrice = $this->normalizeNullableMoney($row['total_price'] ?? null, false);
            $snapSupplierId = trim((string)($row['supplier_id'] ?? ''));
            $snapSupplierName = trim((string)($row['supplier_name'] ?? ''));

            $orderPurchaseAmount = $this->readNullableDbMoney($row['order_purchase_amount'] ?? null);
            if ($orderPurchaseAmount === null && !$isInvalid && array_key_exists('order_purchase_amount', $src)) {
                $orderPurchaseAmount = $src['order_purchase_amount'];
            }

            $applyProductNameRaw = $row['apply_product_name'] ?? null;
            $applySpecModelRaw = $row['apply_spec_model'] ?? null;
            $applyUnitPrice = $this->readNullableDbMoney($row['apply_unit_price'] ?? null);
            $applyPurchaseUnitPrice = $this->readNullableDbMoney($row['apply_purchase_unit_price'] ?? null);
            $applySalesAmount = $this->readNullableDbMoney($row['apply_sales_amount'] ?? null);
            $applyPurchaseAmount = $this->readNullableDbMoney($row['apply_purchase_amount'] ?? null);

            // 旧草稿 V2 字段为空：展示兼容原订单快照/建议值，但不把 NULL 写成零
            $applyProductName = $this->compatApplyText($applyProductNameRaw, $isInvalid ? $snapProductName : ($src['product_name'] ?? $snapProductName));
            $applySpecModel = $this->compatApplyText($applySpecModelRaw, $isInvalid ? $snapSpecModel : ($src['spec_model'] ?? $snapSpecModel));
            $suggestPurchaseUnitPrice = null;
            if ($applyPurchaseUnitPrice === null) {
                if (!$isInvalid && isset($src['suggest_purchase_unit_price'])) {
                    $suggestPurchaseUnitPrice = $src['suggest_purchase_unit_price'];
                } else {
                    $suggestPurchaseUnitPrice = $this->calcSuggestPurchaseUnitPrice($orderPurchaseAmount, $snapOrderQty > 0 ? $snapOrderQty : (int)($src['order_qty'] ?? 0));
                }
            }

            $displayApplyUnitPrice = $applyUnitPrice;
            $isSuggestUnitPrice = false;
            if ($displayApplyUnitPrice === null && !$isInvalid && isset($src['unit_price'])) {
                $displayApplyUnitPrice = $src['unit_price'];
                $isSuggestUnitPrice = true;
            }

            $displayPurchaseUnitPrice = $applyPurchaseUnitPrice;
            $isSuggestPurchaseUnitPrice = false;
            if ($displayPurchaseUnitPrice === null && $suggestPurchaseUnitPrice !== null) {
                $displayPurchaseUnitPrice = $suggestPurchaseUnitPrice;
                $isSuggestPurchaseUnitPrice = true;
            }

            $line = [
                'item_id' => (int)($row['id'] ?? 0),
                'client_row_key' => '',
                'order_item_id' => $orderItemId,
                'apply_line_no' => (int)($row['apply_line_no'] ?? 0),
                'is_invalid_order_item' => $isInvalid,
                'invalid_tip' => $isInvalid ? '原订单产品行已失效，请重新选择当前有效产品行后再保存。' : '',
                'product_name' => $isInvalid ? $snapProductName : (string)($src['product_name'] ?? $snapProductName),
                'spec_model' => $isInvalid ? $snapSpecModel : (string)($src['spec_model'] ?? $snapSpecModel),
                'unit' => $isInvalid ? trim((string)($row['unit'] ?? '')) : (string)($src['unit'] ?? ''),
                'order_qty' => $isInvalid ? $snapOrderQty : (int)($src['order_qty'] ?? $snapOrderQty),
                'applied_qty' => $isInvalid ? 0 : (int)($src['applied_qty'] ?? 0),
                'remaining_qty' => $isInvalid ? 0 : (int)($src['remaining_qty'] ?? 0),
                'unit_price' => $isInvalid ? $snapUnitPrice : ($src['unit_price'] ?? $snapUnitPrice),
                'total_price' => $isInvalid ? $snapTotalPrice : ($src['total_price'] ?? $snapTotalPrice),
                'order_purchase_amount' => $orderPurchaseAmount,
                'supplier_id' => $isInvalid ? $snapSupplierId : (string)($src['supplier_id'] ?? $snapSupplierId),
                'supplier_name' => $isInvalid ? $snapSupplierName : (string)($src['supplier_name'] ?? $snapSupplierName),
                'product_manager_name' => $isInvalid
                    ? trim((string)($row['product_manager_name'] ?? ''))
                    : (string)($src['product_manager_name'] ?? ''),
                'apply_product_name' => $applyProductName,
                'apply_spec_model' => $applySpecModel,
                'apply_qty' => (int)($row['apply_qty'] ?? 0),
                'apply_unit_price' => $applyUnitPrice,
                'apply_purchase_unit_price' => $applyPurchaseUnitPrice,
                'display_apply_unit_price' => $displayApplyUnitPrice,
                'display_apply_purchase_unit_price' => $displayPurchaseUnitPrice,
                'is_suggest_unit_price' => $isSuggestUnitPrice,
                'is_suggest_purchase_unit_price' => $isSuggestPurchaseUnitPrice,
                'suggest_purchase_unit_price' => $suggestPurchaseUnitPrice,
                'apply_sales_amount' => $applySalesAmount,
                'apply_purchase_amount' => $applyPurchaseAmount,
                'factory_id' => isset($row['factory_id']) && $row['factory_id'] !== null && $row['factory_id'] !== ''
                    ? (int)$row['factory_id']
                    : null,
                'factory_name' => trim((string)($row['factory_name'] ?? '')),
                'remark' => trim((string)($row['remark'] ?? '')),
            ];

            if ($isInvalid) {
                $warnings[] = '发货行「' . ($snapProductName !== '' ? $snapProductName : ('明细#' . $line['item_id'])) . '」原订单产品行已失效，请重新选择。';
            }

            $shippingLines[] = $line;
        }

        $draftOrderAmount = $this->normalizeNullableMoney($apply['order_amount'] ?? null, false);
        $currentOrderAmount = $this->normalizeNullableMoney(($detailResult['data']['order']['money'] ?? null), false);
        $orderAmountChanged = false;
        $orderAmountChangeMessage = '';
        if ($draftOrderAmount !== null && $currentOrderAmount !== null
            && bccomp((string)$draftOrderAmount, (string)$currentOrderAmount, 2) !== 0) {
            $orderAmountChanged = true;
            $orderAmountChangeMessage = '原订单金额已发生变化：草稿记录 '
                . $draftOrderAmount . ' 元，当前订单金额 '
                . $currentOrderAmount . ' 元，请重新核对。';
            $warnings[] = $orderAmountChangeMessage;
        }

        // 兼容旧前端：items 仍返回订单源产品；发货行独立放在 shipping_lines
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
            'items' => $orderSourceItems,
            'order_source_items' => $orderSourceItems,
            'shipping_lines' => $shippingLines,
            'has_invalid_lines' => $hasInvalidLines,
            'draft_order_amount' => $draftOrderAmount,
            'current_order_amount' => $currentOrderAmount,
            'order_amount_changed' => $orderAmountChanged,
            'order_amount_change_message' => $orderAmountChangeMessage,
            'warnings' => $warnings,
            'edit_url' => (string)url('Ship/createShip', ['id' => $applyId]),
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

        $orderAmountConfirmed = (int)($params['order_amount_confirmed'] ?? 0) === 1;
        $confirmedOrderAmountRaw = $params['confirmed_order_amount'] ?? null;

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
                        throw new Exception('草稿关联订单不匹配，禁止静默切换关联订单。');
                    }
                }

                $currentOrderAmount = $this->normalizeNullableMoney($order['money'] ?? null, false);
                if ($existingApply) {
                    $draftOrderAmount = $this->normalizeNullableMoney($existingApply['order_amount'] ?? null, false);
                    if ($draftOrderAmount !== null && $currentOrderAmount !== null
                        && bccomp((string)$draftOrderAmount, (string)$currentOrderAmount, 2) !== 0) {
                        if (!$orderAmountConfirmed) {
                            throw new Exception(
                                '原订单金额已发生变化：草稿记录 ' . $draftOrderAmount
                                . ' 元，当前订单金额 ' . $currentOrderAmount
                                . ' 元，请重新核对并确认后再保存。'
                            );
                        }
                        $confirmedAmount = $this->parseOptionalMoney(
                            $confirmedOrderAmountRaw,
                            '确认的原订单金额格式不正确。'
                        );
                        if ($confirmedAmount === null
                            || bccomp((string)$confirmedAmount, (string)$currentOrderAmount, 2) !== 0) {
                            throw new Exception('原订单金额确认与当前订单金额不一致，请刷新后重新确认。');
                        }
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

                $pendingLines = [];
                $qtySumByOrderItem = [];
                $applyLineNo = 0;

                foreach ($itemsRaw as $inputItem) {
                    if (!is_array($inputItem)) {
                        continue;
                    }

                    $orderItemId = $this->parsePositiveInt(
                        $inputItem['order_item_id'] ?? null,
                        '产品明细参数异常，请刷新页面后重新提交。'
                    );
                    $applyQty = $this->parseNonNegativeInt(
                        $inputItem['apply_qty'] ?? null,
                        '本次申请发货数量必须是大于等于0的整数。'
                    );

                    // 零数量临时行不落库
                    if ($applyQty === 0) {
                        continue;
                    }

                    if (!isset($orderItemMap[$orderItemId])) {
                        throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再保存。');
                    }

                    if (!isset($qtySumByOrderItem[$orderItemId])) {
                        $qtySumByOrderItem[$orderItemId] = 0;
                    }
                    $qtySumByOrderItem[$orderItemId] += $applyQty;
                    if ($qtySumByOrderItem[$orderItemId] > 2147483647) {
                        throw new Exception('本次申请发货数量过大。');
                    }

                    $applyLineNo++;
                    $pendingLines[] = [
                        'order_item_id' => $orderItemId,
                        'apply_qty' => $applyQty,
                        'apply_line_no' => $applyLineNo,
                        'input' => $inputItem,
                    ];
                }

                if (empty($pendingLines)) {
                    throw new Exception('至少需要一条正整数数量的发货行。');
                }

                // 按原订单产品 ID 汇总校验（允许同一 order_item_id 多行）
                foreach ($qtySumByOrderItem as $orderItemId => $sumQty) {
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
                    if ($sumQty > $remainingQty) {
                        throw new Exception('产品【' . $productName . '】剩余可申请数量不足（本次合计 '
                            . $sumQty . '，剩余 ' . $remainingQty . '）。');
                    }
                }

                $itemSnapshots = [];
                foreach ($pendingLines as $pending) {
                    $orderItem = $orderItemMap[$pending['order_item_id']];
                    $productName = trim((string)($orderItem['product_name'] ?? ''));
                    if ($productName === '') {
                        $productName = '未命名产品';
                    }
                    $managerInfo = $this->resolveProductManager($orderItem['manager_id'] ?? '', $productName);
                    $inputItem = $pending['input'];

                    $applyProductName = $this->parseOptionalApplyText(
                        $inputItem['apply_product_name'] ?? null,
                        $productName,
                        200,
                        '本次发货产品名称'
                    );
                    $applySpecModel = $this->parseOptionalApplyText(
                        $inputItem['apply_spec_model'] ?? null,
                        trim((string)($orderItem['spec_model'] ?? '')),
                        200,
                        '本次发货规格型号'
                    );
                    $itemRemark = trim((string)($inputItem['remark'] ?? ''));
                    if (mb_strlen($itemRemark) > 500) {
                        throw new Exception('行备注长度不能超过500个字符。');
                    }

                    $factoryId = $this->parseOptionalFactoryId($inputItem['factory_id'] ?? null);
                    $factoryName = null;
                    if ($factoryId !== null) {
                        $factory = $this->assertFactoryValid($factoryId);
                        $factoryName = $factory['factory_name'];
                    }

                    $applyUnitPrice = $this->parseOptionalMoney(
                        array_key_exists('apply_unit_price', $inputItem) ? $inputItem['apply_unit_price'] : null,
                        '本次销售单价格式不正确。'
                    );
                    $applyPurchaseUnitPrice = $this->parseOptionalMoney(
                        array_key_exists('apply_purchase_unit_price', $inputItem) ? $inputItem['apply_purchase_unit_price'] : null,
                        '本次采购单价格式不正确。'
                    );

                    $applySalesAmount = null;
                    if ($applyUnitPrice !== null) {
                        $applySalesAmount = $this->bcmulMoney((string)$pending['apply_qty'], (string)$applyUnitPrice);
                    }
                    $applyPurchaseAmount = null;
                    if ($applyPurchaseUnitPrice !== null) {
                        $applyPurchaseAmount = $this->bcmulMoney((string)$pending['apply_qty'], (string)$applyPurchaseUnitPrice);
                    }

                    $orderPurchaseAmount = $this->normalizeNullableMoney($orderItem['purchase_price'] ?? null);

                    $snapshot = $this->buildShippingItemSnapshot(
                        $orderItem,
                        $managerInfo,
                        $pending['apply_qty'],
                        $itemRemark,
                        [
                            'apply_line_no' => $pending['apply_line_no'],
                            'apply_product_name' => $applyProductName,
                            'apply_spec_model' => $applySpecModel,
                            'factory_id' => $factoryId,
                            'factory_name' => $factoryName,
                            'order_purchase_amount' => $orderPurchaseAmount,
                            'apply_unit_price' => $applyUnitPrice,
                            'apply_purchase_unit_price' => $applyPurchaseUnitPrice,
                            'apply_sales_amount' => $applySalesAmount,
                            'apply_purchase_amount' => $applyPurchaseAmount,
                        ]
                    );
                    $itemSnapshots[] = $snapshot;
                }

                $now = date('Y-m-d H:i:s');
                $applyNo = '';
                $orderAmountToSave = $currentOrderAmount !== null
                    ? $currentOrderAmount
                    : $this->formatMoneyHalfUp((string)($order['money'] ?? '0'));

                if ($existingApply) {
                    $applyNo = trim((string)($existingApply['apply_no'] ?? ''));
                    $updateData = [
                        'order_no' => trim((string)($order['order_no'] ?? '')),
                        'order_amount' => $orderAmountToSave,
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
                        'order_amount' => $orderAmountToSave,
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

                $finalApplyNo = $applyNo !== '' ? $applyNo : trim((string)($existingApply['apply_no'] ?? ''));

                return [
                    'code' => 0,
                    'msg' => '草稿保存成功',
                    'data' => [
                        'apply_id' => $savedApplyId,
                        'apply_no' => $finalApplyNo,
                        'edit_url' => (string)url('Ship/createShip', ['id' => $savedApplyId]),
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
     * 厂家列表（草稿选择用）
     *
     * @param array $params
     * @return array
     */
    public function listFactories($params = [])
    {
        $params = is_array($params) ? $params : [];
        $admin = $this->getCurrentAdminInfo();
        if ((int)$admin['admin_id'] <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => [], 'count' => 0];
        }

        $keyword = trim((string)($params['keyword'] ?? ''));
        $query = Db::name('crm_product_category')
            ->where('is_deleted', 0);

        // 说明：本 Service 不复用 Controller::getOrgWhere；未自行发明跨组织过滤。
        // 仅校验有效未删除厂家，与订单创建权限（pr_user_id/at_user_id）保持独立。

        if ($keyword !== '') {
            $query->whereLike('category_name', '%' . $keyword . '%');
        }

        $rows = $query
            ->field('id,category_name')
            ->order('id', 'desc')
            ->limit(500)
            ->select();

        $data = [];
        foreach ((array)$rows as $row) {
            $data[] = [
                'factory_id' => (int)($row['id'] ?? 0),
                'factory_name' => trim((string)($row['category_name'] ?? '')),
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'data' => $data,
            'count' => count($data),
        ];
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
     * @param array $applyFields V2 本次发货字段
     * @return array
     */
    public function buildShippingItemSnapshot($orderItem, $managerInfo, $applyQty, $remark = '', $applyFields = [])
    {
        $orderItemId = (int)($orderItem['id'] ?? 0);
        $orderId = (int)($orderItem['order_id'] ?? 0);
        $applyFields = is_array($applyFields) ? $applyFields : [];

        $productName = trim((string)($orderItem['product_name'] ?? ''));
        $specModel = trim((string)($orderItem['spec_model'] ?? ''));
        $unitPrice = $this->normalizeNullableMoney($orderItem['unit_price'] ?? null, false);
        $totalPrice = $this->normalizeNullableMoney($orderItem['total_price'] ?? null, false);

        $applyLineNo = (int)($applyFields['apply_line_no'] ?? 1);
        if ($applyLineNo <= 0) {
            $applyLineNo = 1;
        }

        $applyProductName = array_key_exists('apply_product_name', $applyFields)
            ? trim((string)$applyFields['apply_product_name'])
            : $productName;
        $applySpecModel = array_key_exists('apply_spec_model', $applyFields)
            ? trim((string)$applyFields['apply_spec_model'])
            : $specModel;

        $factoryId = array_key_exists('factory_id', $applyFields) ? $applyFields['factory_id'] : null;
        $factoryName = array_key_exists('factory_name', $applyFields) ? $applyFields['factory_name'] : null;
        if ($factoryId === null || $factoryId === '' || (int)$factoryId <= 0) {
            $factoryId = null;
            $factoryName = null;
        } else {
            $factoryId = (int)$factoryId;
            $factoryName = $factoryName !== null ? trim((string)$factoryName) : null;
        }

        $orderPurchaseAmount = array_key_exists('order_purchase_amount', $applyFields)
            ? $applyFields['order_purchase_amount']
            : $this->normalizeNullableMoney($orderItem['purchase_price'] ?? null);

        return [
            'order_item_id' => $orderItemId,
            'order_id' => $orderId,
            'line_no' => (int)($orderItem['line_no'] ?? 1),
            'apply_line_no' => $applyLineNo,
            'product_id' => trim((string)($orderItem['product_id'] ?? '')),
            'product_name' => $productName,
            'apply_product_name' => $applyProductName,
            'product_manager_id' => (int)($managerInfo['admin_id'] ?? 0),
            'product_manager_name' => trim((string)($managerInfo['username'] ?? '')),
            'spec_model' => $specModel,
            'apply_spec_model' => $applySpecModel,
            'supplier_id' => trim((string)($orderItem['supplier_id'] ?? '')),
            'supplier_name' => trim((string)($orderItem['supplier_name'] ?? '')),
            'factory_id' => $factoryId,
            'factory_name' => $factoryName,
            'unit' => trim((string)($orderItem['unit'] ?? '')),
            'order_qty' => (int)($orderItem['qty'] ?? 0),
            'apply_qty' => (int)$applyQty,
            'unit_price' => $unitPrice !== null ? $unitPrice : '0.00',
            'total_price' => $totalPrice !== null ? $totalPrice : '0.00',
            'order_purchase_amount' => $orderPurchaseAmount,
            'apply_unit_price' => array_key_exists('apply_unit_price', $applyFields) ? $applyFields['apply_unit_price'] : null,
            'apply_purchase_unit_price' => array_key_exists('apply_purchase_unit_price', $applyFields) ? $applyFields['apply_purchase_unit_price'] : null,
            'apply_sales_amount' => array_key_exists('apply_sales_amount', $applyFields) ? $applyFields['apply_sales_amount'] : null,
            'apply_purchase_amount' => array_key_exists('apply_purchase_amount', $applyFields) ? $applyFields['apply_purchase_amount'] : null,
            'remark' => trim((string)$remark),
        ];
    }

    /**
     * 校验厂家存在且未删除；名称由服务端生成
     *
     * @param int $factoryId
     * @return array{factory_id:int,factory_name:string}
     * @throws Exception
     */
    protected function assertFactoryValid($factoryId)
    {
        $factoryId = (int)$factoryId;
        if ($factoryId <= 0) {
            throw new Exception('厂家参数不正确。');
        }

        $row = Db::name('crm_product_category')
            ->where('id', $factoryId)
            ->where('is_deleted', 0)
            ->field('id,category_name')
            ->find();
        if (empty($row)) {
            throw new Exception('所选厂家不存在或已失效。');
        }

        $name = trim((string)($row['category_name'] ?? ''));
        if ($name === '') {
            throw new Exception('所选厂家名称异常。');
        }

        return [
            'factory_id' => (int)$row['id'],
            'factory_name' => $name,
        ];
    }

    /**
     * 解析可选厂家 ID；空表示草稿未选
     *
     * @param mixed $raw
     * @return int|null
     * @throws Exception
     */
    protected function parseOptionalFactoryId($raw)
    {
        if ($raw === null || $raw === '') {
            return null;
        }
        if (is_array($raw) || is_object($raw)) {
            throw new Exception('厂家参数不正确。');
        }
        if (is_bool($raw)) {
            throw new Exception('厂家参数不正确。');
        }
        if (is_int($raw)) {
            if ($raw < 0) {
                throw new Exception('厂家参数不正确。');
            }
            return $raw === 0 ? null : $raw;
        }
        if (is_float($raw)) {
            throw new Exception('厂家参数不正确。');
        }
        $raw = trim((string)$raw);
        if ($raw === '') {
            return null;
        }
        if (!preg_match('/^\d+$/', $raw)) {
            throw new Exception('厂家参数不正确。');
        }
        $val = (int)$raw;
        return $val > 0 ? $val : null;
    }

    /**
     * 解析可选金额：空→NULL；0→0.00；拒绝科学计数法/混合串/负数/超范围
     *
     * @param mixed $raw
     * @param string $errorMsg
     * @return string|null 两位小数字符串或 null
     * @throws Exception
     */
    protected function parseOptionalMoney($raw, $errorMsg)
    {
        if ($raw === null) {
            return null;
        }
        if (is_array($raw) || is_object($raw) || is_bool($raw)) {
            throw new Exception($errorMsg);
        }
        if (is_int($raw)) {
            if ($raw < 0) {
                throw new Exception($errorMsg);
            }
            // 与字符串分支同一上限：用十进制字符串比较，避免 float 与超大整数绕过
            $intAsString = (string)$raw;
            if (bccomp($intAsString, self::MONEY_ABS_MAX, 2) > 0) {
                throw new Exception($errorMsg);
            }
            return $this->formatMoneyHalfUp($intAsString);
        }
        if (is_float($raw)) {
            // 不接受二进制浮点作为权威输入
            throw new Exception($errorMsg);
        }

        $raw = trim((string)$raw);
        if ($raw === '') {
            return null;
        }
        if (!preg_match('/^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/', $raw)) {
            throw new Exception($errorMsg);
        }
        if (bccomp($raw, '0', 2) < 0) {
            throw new Exception($errorMsg);
        }
        if (bccomp($raw, self::MONEY_ABS_MAX, 2) > 0) {
            throw new Exception($errorMsg);
        }

        return $this->formatMoneyHalfUp($raw);
    }

    /**
     * 将库内/可信来源金额规范为两位小数字符串；非法时按 $required 决定抛错或返回 null
     *
     * @param mixed $raw
     * @param bool $allowNull
     * @return string|null
     */
    protected function normalizeNullableMoney($raw, $allowNull = true)
    {
        if ($raw === null || $raw === '') {
            return $allowNull ? null : '0.00';
        }
        if (is_array($raw) || is_object($raw) || is_bool($raw)) {
            return $allowNull ? null : '0.00';
        }
        $raw = trim((string)$raw);
        if ($raw === '') {
            return $allowNull ? null : '0.00';
        }
        if (!preg_match('/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/', $raw)) {
            return $allowNull ? null : '0.00';
        }
        if (bccomp($raw, '0', 8) < 0) {
            return $allowNull ? null : '0.00';
        }
        return $this->formatMoneyHalfUp($raw);
    }

    /**
     * 读取数据库可空金额字段：保留 NULL 语义，不把空写成零
     *
     * @param mixed $raw
     * @return string|null
     */
    protected function readNullableDbMoney($raw)
    {
        if ($raw === null || $raw === '') {
            return null;
        }
        return $this->normalizeNullableMoney($raw, true);
    }

    /**
     * 金额乘法：数量(整数) × 单价(两位小数) → 两位小数
     * 合法输入下结果精确到两位，不依赖 bcmath.scale 默认值；bcmul 第三参为截断标度。
     *
     * @param string $qty
     * @param string $unitPrice
     * @return string
     * @throws Exception
     */
    protected function bcmulMoney($qty, $unitPrice)
    {
        $qty = trim((string)$qty);
        $unitPrice = trim((string)$unitPrice);
        if (!preg_match('/^\d+$/', $qty)) {
            throw new Exception('金额计算数量异常。');
        }
        $product = bcmul($qty, $unitPrice, 2);
        if (bccomp($product, self::MONEY_ABS_MAX, 2) > 0) {
            throw new Exception('金额计算结果超出允许范围。');
        }
        return $this->formatMoneyHalfUp($product);
    }

    /**
     * 统一两位小数（正数/零）；对超过两位的输入按四舍五入
     *
     * @param string $value
     * @return string
     */
    protected function formatMoneyHalfUp($value)
    {
        $value = trim((string)$value);
        if ($value === '' || !preg_match('/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/', $value)) {
            return '0.00';
        }
        if (bccomp($value, '0', 8) < 0) {
            return '0.00';
        }
        // 半入：先乘 100 再加 0.5 后取整，再除回
        $scaled = bcmul($value, '100', 8);
        $scaled = bcadd($scaled, '0.5', 8);
        $intPart = bcmul($scaled, '1', 0);
        return bcdiv($intPart, '100', 2);
    }

    /**
     * 建议采购单价 = 原采购合计 ÷ 原订单数量（两位小数半入）
     *
     * @param string|null $orderPurchaseAmount
     * @param int $orderQty
     * @return string|null
     */
    protected function calcSuggestPurchaseUnitPrice($orderPurchaseAmount, $orderQty)
    {
        $orderQty = (int)$orderQty;
        if ($orderPurchaseAmount === null || $orderPurchaseAmount === '' || $orderQty <= 0) {
            return null;
        }
        $amount = $this->normalizeNullableMoney($orderPurchaseAmount, true);
        if ($amount === null) {
            return null;
        }
        $raw = bcdiv($amount, (string)$orderQty, 8);
        return $this->formatMoneyHalfUp($raw);
    }

    /**
     * 可选文本：空则回退默认；限制长度
     *
     * @param mixed $raw
     * @param string $fallback
     * @param int $maxLen
     * @param string $fieldLabel
     * @return string
     * @throws Exception
     */
    protected function parseOptionalApplyText($raw, $fallback, $maxLen, $fieldLabel)
    {
        if ($raw === null || (is_string($raw) && trim($raw) === '')) {
            $text = trim((string)$fallback);
        } elseif (is_array($raw) || is_object($raw)) {
            throw new Exception($fieldLabel . '格式不正确。');
        } else {
            $text = trim((string)$raw);
        }
        if (mb_strlen($text) > $maxLen) {
            throw new Exception($fieldLabel . '长度不能超过' . $maxLen . '个字符。');
        }
        return $text;
    }

    /**
     * 旧草稿文本兼容：库内空字符串/NULL 时展示回退值
     *
     * @param mixed $raw
     * @param string $fallback
     * @return string
     */
    protected function compatApplyText($raw, $fallback)
    {
        if ($raw === null) {
            return trim((string)$fallback);
        }
        $text = trim((string)$raw);
        if ($text === '') {
            return trim((string)$fallback);
        }
        return $text;
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
