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

    /** 替代产品经理合法角色（含可兼任的产品总监） */
    protected $pmSubstituteGroupIds = [13, 14, 18, 19, 22];

    /** 授权原因最大字符数（对齐 crm_shipping_pm_substitute.reason VARCHAR(500)） */
    const PM_SUBSTITUTE_REASON_MAX = 500;

    /** 审计意见最大字符数（对齐 crm_shipping_approval_log.opinion VARCHAR(1000)） */
    const APPROVAL_LOG_OPINION_MAX = 1000;

    /** auth_version 为 INT UNSIGNED，禁止回绕到旧版本 */
    const PM_SUBSTITUTE_AUTH_VERSION_MAX = 4294967295;

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
        $managerWarnings = [];

        $items = [];
        foreach ($orderItems as $item) {
            $orderItemId = (int)($item['id'] ?? 0);
            $productName = trim((string)($item['product_name'] ?? ''));
            if ($productName === '') {
                $productName = '未命名产品';
            }

            // 草稿展示路径使用宽容解析：单行经理异常不阻断整单产品加载
            $managerInfo = $this->resolveProductManagerForDraft($item['manager_id'] ?? '', $productName);
            if (!empty($managerInfo['manager_invalid'])) {
                $reason = trim((string)($managerInfo['manager_invalid_reason'] ?? ''));
                if ($reason !== '') {
                    $managerWarnings[] = $reason;
                }
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
                'product_manager_id' => $managerInfo['product_manager_id'],
                'product_manager_name' => $managerInfo['product_manager_name'],
                'manager_invalid' => !empty($managerInfo['manager_invalid']),
                'manager_invalid_reason' => (string)($managerInfo['manager_invalid_reason'] ?? ''),
                'manager_status' => (string)($managerInfo['manager_status'] ?? 'valid'),
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

        if (!empty($managerWarnings)) {
            $warnings = array_merge($warnings, $managerWarnings);
            $warnings[] = '当前允许保存草稿，但正式提交审核前需要联系超级管理员指定替代经理。';
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
        // M3-2B：已软删除草稿禁止回填编辑
        if ((int)($apply['is_deleted'] ?? 0) !== 0) {
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
        // 透传订单详情警告；经理异常“请联系超管”类提示改由替代授权摘要统一生成，避免已授权后仍误导
        foreach ((array)($detailResult['data']['warnings'] ?? []) as $detailWarn) {
            $detailWarn = trim((string)$detailWarn);
            if ($detailWarn === '') {
                continue;
            }
            if (mb_strpos($detailWarn, '请联系超级管理员指定替代经理') !== false) {
                continue;
            }
            $warnings[] = $detailWarn;
        }

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
            $snapManagerIdRaw = $row['product_manager_id'] ?? null;
            $snapManagerId = ($snapManagerIdRaw === null || $snapManagerIdRaw === '')
                ? null
                : ((int)$snapManagerIdRaw > 0 ? (int)$snapManagerIdRaw : null);
            $snapManagerName = trim((string)($row['product_manager_name'] ?? ''));

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

            // 经理异常与订单产品行失效使用不同标记；有效行透传实时经理状态
            $managerInvalid = false;
            $managerInvalidReason = '';
            $managerStatus = '';
            $liveManagerId = $snapManagerId;
            $liveManagerName = $snapManagerName;
            if (!$isInvalid) {
                $managerInvalid = !empty($src['manager_invalid']);
                $managerInvalidReason = (string)($src['manager_invalid_reason'] ?? '');
                $managerStatus = (string)($src['manager_status'] ?? 'valid');
                $liveManagerId = array_key_exists('product_manager_id', $src)
                    ? $src['product_manager_id']
                    : $snapManagerId;
                $liveManagerName = (string)($src['product_manager_name'] ?? $snapManagerName);
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
                'product_manager_id' => $liveManagerId,
                'product_manager_name' => $liveManagerName,
                'manager_invalid' => $managerInvalid,
                'manager_invalid_reason' => $managerInvalidReason,
                'manager_status' => $managerStatus,
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

        // M4-1A-4：只读合并替代授权摘要（不写库）
        $substituteMap = $this->loadPmSubstituteMapByApplyId($applyId);
        $substituteWarnKeys = [];
        foreach ($shippingLines as &$shippingLineRef) {
            $oid = (int)($shippingLineRef['order_item_id'] ?? 0);
            $subRow = ($oid > 0 && isset($substituteMap[$oid])) ? $substituteMap[$oid] : null;
            $managerResolve = [
                'manager_invalid' => !empty($shippingLineRef['manager_invalid']),
                'manager_status' => (string)($shippingLineRef['manager_status'] ?? ''),
                'admin_id' => $shippingLineRef['product_manager_id'] ?? null,
                'username' => (string)($shippingLineRef['product_manager_name'] ?? ''),
            ];
            $subState = $this->buildPmSubstituteDisplayState($subRow, $managerResolve);
            $shippingLineRef = array_merge($shippingLineRef, $subState);
            $tip = trim((string)($subState['pm_substitute_tip'] ?? ''));
            if ($tip !== '' && empty($shippingLineRef['is_invalid_order_item'])) {
                $warnKey = $oid . '|' . $tip;
                if (!isset($substituteWarnKeys[$warnKey])) {
                    $substituteWarnKeys[$warnKey] = true;
                    $productLabel = trim((string)($shippingLineRef['product_name'] ?? ''));
                    if ($productLabel === '') {
                        $productLabel = '产品';
                    }
                    $warnings[] = '产品【' . $productLabel . '】' . $tip;
                }
            }
        }
        unset($shippingLineRef);

        // 订单源产品同步只读授权摘要（按 order_item_id）
        foreach ($orderSourceItems as &$srcItemRef) {
            $oid = (int)($srcItemRef['order_item_id'] ?? 0);
            $subRow = ($oid > 0 && isset($substituteMap[$oid])) ? $substituteMap[$oid] : null;
            $managerResolve = [
                'manager_invalid' => !empty($srcItemRef['manager_invalid']),
                'manager_status' => (string)($srcItemRef['manager_status'] ?? ''),
                'admin_id' => $srcItemRef['product_manager_id'] ?? null,
                'username' => (string)($srcItemRef['product_manager_name'] ?? ''),
            ];
            $srcItemRef = array_merge($srcItemRef, $this->buildPmSubstituteDisplayState($subRow, $managerResolve));
        }
        unset($srcItemRef);

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
                    // M3-2B：事务内、明细重建前强制拒绝已软删除草稿
                    if ((int)($existingApply['is_deleted'] ?? 0) !== 0) {
                        throw new Exception('草稿已删除，无法继续编辑保存。');
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
                    // 草稿保存使用宽容解析：异常经理可落库快照，但不表示已获审批资格
                    $managerInfo = $this->resolveProductManagerForDraft($orderItem['manager_id'] ?? '', $productName);
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

                // M4-1A-4：原产品行已从草稿完全移除时，同事务失效对应有效替代授权
                if ($existingApply && $savedApplyId > 0) {
                    $this->invalidatePmSubstitutesForRemovedOrderItems(
                        $savedApplyId,
                        array_keys($qtySumByOrderItem),
                        $adminId,
                        trim((string)($admin['username'] ?? '')),
                        $applyNo !== '' ? $applyNo : trim((string)($existingApply['apply_no'] ?? '')),
                        $now
                    );
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
     * 草稿箱列表（仅 status=0 且未软删除）
     *
     * @param array $params
     * @return array
     */
    public function listDrafts($params = [])
    {
        $params = is_array($params) ? $params : [];

        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'count' => 0, 'data' => []];
        }

        $page = (int)($params['page'] ?? 1);
        if ($page < 1) {
            $page = 1;
        }
        $limit = (int)($params['limit'] ?? 10);
        if ($limit < 1) {
            $limit = 10;
        }
        if ($limit > 100) {
            $limit = 100;
        }

        $keyword = trim((string)($params['keyword'] ?? ''));

        // M3-2B：强制排除已软删除草稿（含超管）
        $query = Db::name('crm_shipping_apply')
            ->where('status', 0)
            ->where('is_deleted', 0);

        // 数据权限：普通业务员仅本人草稿；超管沿用 isShippingApplySuperAdmin
        if (!$this->isShippingApplySuperAdmin($adminId)) {
            $query->where('applicant_id', $adminId);
        }

        if ($keyword !== '') {
            $query->where(function ($q) use ($keyword) {
                $q->whereLike('apply_no', '%' . $keyword . '%')
                    ->whereOr('order_no', 'like', '%' . $keyword . '%')
                    ->whereOr('customer_name', 'like', '%' . $keyword . '%');
            });
        }

        $count = (int)(clone $query)->count('id');
        if ($count <= 0) {
            return [
                'code' => 0,
                'msg' => '获取成功',
                'count' => 0,
                'data' => [],
                'is_super_admin' => $this->isShippingApplySuperAdmin($adminId) ? 1 : 0,
            ];
        }

        $rows = (clone $query)
            ->field('id,apply_no,order_no,customer_name,applicant_name,order_amount,create_time,update_time,status')
            ->order('update_time', 'desc')
            ->order('id', 'desc')
            ->page($page, $limit)
            ->select();

        $rows = (array)$rows;
        $applyIds = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id > 0) {
                $applyIds[] = $id;
            }
        }

        $summaryMap = $this->buildDraftProductSummaries($applyIds);

        $data = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            $data[] = [
                'id' => $id,
                'apply_no' => trim((string)($row['apply_no'] ?? '')),
                'order_no' => trim((string)($row['order_no'] ?? '')),
                'customer_name' => trim((string)($row['customer_name'] ?? '')),
                'applicant_name' => trim((string)($row['applicant_name'] ?? '')),
                'order_amount' => round((float)($row['order_amount'] ?? 0), 2),
                'create_time' => trim((string)($row['create_time'] ?? '')),
                'update_time' => trim((string)($row['update_time'] ?? '')),
                'status' => (int)($row['status'] ?? 0),
                'product_summary' => isset($summaryMap[$id]) ? $summaryMap[$id] : '',
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'count' => $count,
            'data' => $data,
            'is_super_admin' => $this->isShippingApplySuperAdmin($adminId) ? 1 : 0,
        ];
    }

    /**
     * 软删除发货申请草稿（主表软删 + 审计日志，明细保留）
     *
     * @param mixed $applyId
     * @return array
     */
    public function deleteDraft($applyId)
    {
        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }

        try {
            $applyId = $this->parsePositiveInt($applyId, '草稿ID无效');
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        }

        $isSuperAdmin = $this->isShippingApplySuperAdmin($adminId);
        $operatorName = trim((string)($admin['username'] ?? ''));
        if ($operatorName === '') {
            $operatorName = 'admin#' . $adminId;
        }
        $genericDenyMsg = '草稿不存在、已删除或无权限';

        Db::startTrans();
        try {
            $apply = Db::name('crm_shipping_apply')
                ->where('id', $applyId)
                ->lock(true)
                ->find();

            if (empty($apply)) {
                throw new Exception($genericDenyMsg);
            }

            $applicantId = (int)($apply['applicant_id'] ?? 0);
            $canManage = ($applicantId === $adminId) || $isSuperAdmin;

            if ((int)($apply['is_deleted'] ?? 0) !== 0) {
                throw new Exception($canManage ? '草稿已删除' : $genericDenyMsg);
            }

            if ((int)($apply['status'] ?? -1) !== 0) {
                throw new Exception('仅草稿状态的发货申请可以删除');
            }

            if (!$canManage) {
                throw new Exception($genericDenyMsg);
            }

            $now = date('Y-m-d H:i:s');
            $affected = Db::name('crm_shipping_apply')
                ->where('id', $applyId)
                ->where('status', 0)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_by' => $adminId,
                    'deleted_at' => $now,
                    'update_time' => $now,
                ]);

            if ((int)$affected !== 1) {
                throw new Exception($genericDenyMsg);
            }

            $applyNo = trim((string)($apply['apply_no'] ?? ''));
            $applicantName = trim((string)($apply['applicant_name'] ?? ''));
            $opinionPayload = [
                'apply_no' => $applyNo,
                'original_applicant_id' => $applicantId,
                'original_applicant_name' => $applicantName,
                'is_deleted_before' => 0,
                'is_deleted_after' => 1,
            ];
            $opinionJson = json_encode($opinionPayload, JSON_UNESCAPED_UNICODE);
            if ($opinionJson === false) {
                throw new Exception('删除审计信息编码失败');
            }
            if (strlen($opinionJson) > 1000) {
                throw new Exception('删除审计信息过长');
            }

            $logId = Db::name('crm_shipping_approval_log')->insertGetId([
                'shipping_apply_id' => $applyId,
                'task_id' => null,
                'operator_id' => $adminId,
                'operator_name' => mb_substr($operatorName, 0, 100, 'UTF-8'),
                'operator_role' => $isSuperAdmin ? 'system_admin' : 'applicant',
                'action' => 'delete_draft',
                'before_status' => 0,
                'after_status' => 0,
                'opinion' => $opinionJson,
                'create_time' => $now,
            ]);

            if ((int)$logId <= 0) {
                throw new Exception('删除审计日志写入失败');
            }

            Db::commit();

            return ['code' => 0, 'msg' => '草稿删除成功', 'data' => ['id' => $applyId]];
        } catch (Exception $e) {
            Db::rollback();
            $msg = (string)$e->getMessage();
            $safeMsgs = [
                $genericDenyMsg,
                '草稿已删除',
                '仅草稿状态的发货申请可以删除',
                '删除审计信息编码失败',
                '删除审计信息过长',
                '删除审计日志写入失败',
            ];
            if (in_array($msg, $safeMsgs, true)) {
                return ['code' => -200, 'msg' => $msg, 'data' => []];
            }
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[ShippingApplyService::deleteDraft] ' . $msg,
                    'error'
                );
            }
            return ['code' => -200, 'msg' => '草稿删除失败，请稍后重试', 'data' => []];
        } catch (\Throwable $e) {
            Db::rollback();
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[ShippingApplyService::deleteDraft] ' . $e->getMessage(),
                    'error'
                );
            }
            return ['code' => -200, 'msg' => '草稿删除失败，请稍后重试', 'data' => []];
        }
    }

    /**
     * 批量组装本页草稿的发货产品摘要（避免 N+1）
     *
     * @param int[] $applyIds
     * @return array<int,string>
     */
    protected function buildDraftProductSummaries(array $applyIds)
    {
        $applyIds = array_values(array_unique(array_filter(array_map('intval', $applyIds), function ($id) {
            return $id > 0;
        })));
        if (empty($applyIds)) {
            return [];
        }

        $items = Db::name('crm_shipping_apply_item')
            ->whereIn('shipping_apply_id', $applyIds)
            ->field('id,shipping_apply_id,apply_line_no,order_item_id,apply_product_name,product_name,apply_qty')
            ->order('apply_line_no', 'asc')
            ->order('id', 'asc')
            ->select();

        /** @var array<int,array<int,string>> $partsByApply */
        $partsByApply = [];
        /** @var array<int,int> $totalByApply */
        $totalByApply = [];

        foreach ((array)$items as $item) {
            $applyId = (int)($item['shipping_apply_id'] ?? 0);
            if ($applyId <= 0) {
                continue;
            }

            $name = trim((string)($item['apply_product_name'] ?? ''));
            if ($name === '') {
                $name = trim((string)($item['product_name'] ?? ''));
            }
            if ($name === '') {
                $name = '未命名产品';
            }

            $qty = (int)($item['apply_qty'] ?? 0);
            if (!isset($partsByApply[$applyId])) {
                $partsByApply[$applyId] = [];
                $totalByApply[$applyId] = 0;
            }
            // 按明细行追加，禁止用 order_item_id 覆盖同产品多行
            $partsByApply[$applyId][] = $name . '×' . $qty;
            $totalByApply[$applyId]++;
        }

        $maxShow = 4;
        $maxLen = 80;
        $result = [];
        foreach ($applyIds as $applyId) {
            $parts = isset($partsByApply[$applyId]) ? $partsByApply[$applyId] : [];
            $total = (int)($totalByApply[$applyId] ?? 0);
            if ($total <= 0 || empty($parts)) {
                $result[$applyId] = '';
                continue;
            }

            $shown = [];
            $shownCount = 0;
            foreach ($parts as $part) {
                if ($shownCount >= $maxShow) {
                    break;
                }
                $candidate = empty($shown) ? $part : (implode('，', $shown) . '，' . $part);
                if ($shownCount > 0 && mb_strlen($candidate, 'UTF-8') > $maxLen) {
                    break;
                }
                $shown[] = $part;
                $shownCount++;
            }

            $summary = implode('，', $shown);
            $remain = $total - $shownCount;
            if ($remain > 0) {
                $summary .= '，还有' . $remain . '行';
            }
            $result[$applyId] = $summary;
        }

        return $result;
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

        // 经理快照：非法 ID 保存 NULL，禁止把 0 当作有效 admin_id
        $productManagerId = null;
        if (array_key_exists('product_manager_id', $managerInfo)) {
            $productManagerId = $managerInfo['product_manager_id'];
        } elseif (array_key_exists('admin_id', $managerInfo)) {
            $productManagerId = $managerInfo['admin_id'];
        }
        if ($productManagerId === null || $productManagerId === '') {
            $productManagerId = null;
        } else {
            $productManagerId = (int)$productManagerId;
            if ($productManagerId <= 0) {
                $productManagerId = null;
            }
        }
        if (array_key_exists('product_manager_name', $managerInfo)) {
            $productManagerName = trim((string)$managerInfo['product_manager_name']);
        } else {
            $productManagerName = trim((string)($managerInfo['username'] ?? ''));
        }

        return [
            'order_item_id' => $orderItemId,
            'order_id' => $orderId,
            'line_no' => (int)($orderItem['line_no'] ?? 1),
            'apply_line_no' => $applyLineNo,
            'product_id' => trim((string)($orderItem['product_id'] ?? '')),
            'product_name' => $productName,
            'apply_product_name' => $applyProductName,
            'product_manager_id' => $productManagerId,
            'product_manager_name' => $productManagerName,
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
     * 严格解析产品经理（用于需要有效审批人的业务场景）
     *
     * 账号不存在、用户名为空、ID 非法时抛出异常。
     * 草稿展示/保存请使用 resolveProductManagerForDraft()，勿将本方法改为始终放行异常经理。
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
     * 草稿专用：宽容解析产品经理
     *
     * 仅表示「草稿可以保存/展示」，绝不表示「已获得审批资格」。
     * 不信任前端传入的经理 ID/姓名/异常标志；状态由服务端按原订单 manager_id + admin 表判断。
     * 数据库连接失败、SQL 错误等系统异常仍向上抛出，不得伪装为经理不存在。
     *
     * @param mixed $managerIdRaw 原订单产品行 manager_id
     * @param string $productName
     * @return array{
     *   admin_id:?int,
     *   username:string,
     *   product_manager_id:?int,
     *   product_manager_name:string,
     *   manager_invalid:bool,
     *   manager_invalid_reason:string,
     *   manager_status:string
     * }
     */
    protected function resolveProductManagerForDraft($managerIdRaw, $productName)
    {
        $productName = trim((string)$productName);
        if ($productName === '') {
            $productName = '未命名产品';
        }

        $raw = $managerIdRaw === null ? '' : trim((string)$managerIdRaw);
        if ($raw === '' || !preg_match('/^\d+$/', $raw) || (int)$raw <= 0) {
            return $this->buildDraftManagerResolveResult(
                null,
                '',
                true,
                '产品【' . $productName . '】的产品经理ID非法（空/0/非数字），请联系超级管理员指定替代经理。',
                'invalid_id'
            );
        }

        $managerId = (int)$raw;
        $admin = Db::name('admin')
            ->where('admin_id', $managerId)
            ->field('admin_id,username,is_open')
            ->find();

        if (empty($admin)) {
            return $this->buildDraftManagerResolveResult(
                $managerId,
                '',
                true,
                '产品【' . $productName . '】的原产品经理账号不存在，请联系超级管理员指定替代经理。',
                'missing'
            );
        }

        $username = trim((string)($admin['username'] ?? ''));
        if ($username === '') {
            return $this->buildDraftManagerResolveResult(
                $managerId,
                '',
                true,
                '产品【' . $productName . '】的原产品经理用户名为空，请联系超级管理员指定替代经理。',
                'missing'
            );
        }

        if ((int)($admin['is_open'] ?? 0) !== 1) {
            return $this->buildDraftManagerResolveResult(
                (int)$admin['admin_id'],
                $username,
                true,
                '产品【' . $productName . '】的原产品经理「' . $username . '」已停用，请联系超级管理员指定替代经理。',
                'disabled'
            );
        }

        return $this->buildDraftManagerResolveResult(
            (int)$admin['admin_id'],
            $username,
            false,
            '',
            'valid'
        );
    }

    /**
     * 统一草稿经理解析返回结构
     *
     * @param int|null $adminId
     * @param string $username
     * @param bool $invalid
     * @param string $reason
     * @param string $status valid|missing|disabled|invalid_id
     * @return array
     */
    protected function buildDraftManagerResolveResult($adminId, $username, $invalid, $reason, $status)
    {
        if ($adminId !== null) {
            $adminId = (int)$adminId;
            if ($adminId <= 0) {
                $adminId = null;
            }
        }

        return [
            'admin_id' => $adminId,
            'username' => (string)$username,
            'product_manager_id' => $adminId,
            'product_manager_name' => (string)$username,
            'manager_invalid' => (bool)$invalid,
            'manager_invalid_reason' => (string)$reason,
            'manager_status' => (string)$status,
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

    // =========================
    // M4-1A-4 异常产品经理受控替代
    // =========================

    /**
     * 超管：列出草稿中可管理的替代授权目标（按 order_item_id 去重）
     *
     * @param mixed $applyId
     * @return array
     */
    public function listDraftPmSubstituteTargets($applyId)
    {
        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }
        if (!$this->isShippingApplySuperAdmin($adminId)) {
            return ['code' => -200, 'msg' => '仅超级管理员可管理替代产品经理授权', 'data' => []];
        }

        try {
            $applyId = $this->parsePositiveInt($applyId, '草稿ID无效');
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        }

        $apply = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
        if (empty($apply) || (int)($apply['is_deleted'] ?? 0) !== 0 || (int)($apply['status'] ?? -1) !== 0) {
            return ['code' => -200, 'msg' => '仅有效草稿可查看替代授权', 'data' => []];
        }

        $orderId = (int)($apply['order_id'] ?? 0);
        $applyItems = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->field('order_item_id,product_name,apply_product_name,apply_qty,apply_line_no')
            ->order('apply_line_no', 'asc')
            ->order('id', 'asc')
            ->select();

        $orderItemIds = [];
        $lineMeta = [];
        foreach ((array)$applyItems as $row) {
            $oid = (int)($row['order_item_id'] ?? 0);
            if ($oid <= 0) {
                continue;
            }
            if (!isset($lineMeta[$oid])) {
                $orderItemIds[] = $oid;
                $name = trim((string)($row['apply_product_name'] ?? ''));
                if ($name === '') {
                    $name = trim((string)($row['product_name'] ?? ''));
                }
                $lineMeta[$oid] = [
                    'product_name' => $name !== '' ? $name : '未命名产品',
                    'line_count' => 0,
                    'total_qty' => 0,
                ];
            }
            $lineMeta[$oid]['line_count']++;
            $lineMeta[$oid]['total_qty'] += (int)($row['apply_qty'] ?? 0);
        }

        if (empty($orderItemIds)) {
            return [
                'code' => 0,
                'msg' => '获取成功',
                'data' => [
                    'apply_id' => $applyId,
                    'apply_no' => trim((string)($apply['apply_no'] ?? '')),
                    'items' => [],
                ],
            ];
        }

        $orderItems = Db::name('crm_order_item')
            ->where('order_id', $orderId)
            ->whereIn('id', $orderItemIds)
            ->field('id,manager_id,product_name')
            ->select();
        $orderItemMap = [];
        foreach ((array)$orderItems as $oi) {
            $orderItemMap[(int)$oi['id']] = $oi;
        }

        $subMap = $this->loadPmSubstituteMapByApplyId($applyId);
        $items = [];
        foreach ($orderItemIds as $oid) {
            $meta = $lineMeta[$oid];
            $oi = isset($orderItemMap[$oid]) ? $orderItemMap[$oid] : null;
            $productName = $meta['product_name'];
            if ($oi && trim((string)($oi['product_name'] ?? '')) !== '') {
                $productName = trim((string)$oi['product_name']);
            }
            $managerInfo = $this->resolveProductManagerForDraft(
                $oi ? ($oi['manager_id'] ?? '') : '',
                $productName
            );
            $subRow = isset($subMap[$oid]) ? $subMap[$oid] : null;
            $display = $this->buildPmSubstituteDisplayState($subRow, $managerInfo);
            $canAuthorize = !empty($managerInfo['manager_invalid'])
                && in_array((string)($managerInfo['manager_status'] ?? ''), ['missing', 'disabled', 'invalid_id'], true)
                && (string)($display['pm_substitute_display_status'] ?? '') !== 'frozen';

            $items[] = [
                'order_item_id' => $oid,
                'product_name' => $productName,
                'line_count' => (int)$meta['line_count'],
                'total_qty' => (int)$meta['total_qty'],
                'original_manager_id' => $managerInfo['admin_id'],
                'original_manager_name' => (string)($managerInfo['username'] ?? ''),
                'manager_invalid' => !empty($managerInfo['manager_invalid']),
                'manager_status' => (string)($managerInfo['manager_status'] ?? ''),
                'manager_invalid_reason' => (string)($managerInfo['manager_invalid_reason'] ?? ''),
                'can_authorize' => $canAuthorize ? 1 : 0,
                'substitute_record_id' => (int)($display['pm_substitute_record_id'] ?? 0),
                'substitute_status' => (int)($display['pm_substitute_record_status'] ?? -1),
                'substitute_manager_id' => $display['substitute_manager_id'],
                'substitute_manager_name' => (string)($display['substitute_manager_name'] ?? ''),
                'authorized_by' => (int)($display['pm_substitute_authorized_by'] ?? 0),
                'authorized_by_name' => (string)($display['pm_substitute_authorized_by_name'] ?? ''),
                'reason' => (string)($display['pm_substitute_reason'] ?? ''),
                'version_token' => (string)($display['pm_substitute_version_token'] ?? 'none'),
                'display_status' => (string)($display['pm_substitute_display_status'] ?? 'none'),
                'display_tip' => (string)($display['pm_substitute_tip'] ?? ''),
                'effective' => !empty($display['pm_substitute_effective']) ? 1 : 0,
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'data' => [
                'apply_id' => $applyId,
                'apply_no' => trim((string)($apply['apply_no'] ?? '')),
                'items' => $items,
            ],
        ];
    }

    /**
     * 超管：查询合格替代产品经理候选人
     *
     * @param array $params
     * @return array
     */
    public function listPmSubstituteCandidates($params = [])
    {
        $params = is_array($params) ? $params : [];
        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => [], 'count' => 0];
        }
        if (!$this->isShippingApplySuperAdmin($adminId)) {
            return ['code' => -200, 'msg' => '仅超级管理员可查询替代产品经理候选人', 'data' => [], 'count' => 0];
        }

        $keyword = trim((string)($params['keyword'] ?? ''));
        $query = Db::name('admin')
            ->where('is_open', 1)
            ->whereIn('group_id', $this->pmSubstituteGroupIds)
            ->where('username', '<>', '');

        if ($keyword !== '') {
            if (preg_match('/^\d+$/', $keyword)) {
                $kid = (int)$keyword;
                $query->where(function ($q) use ($keyword, $kid) {
                    $q->whereLike('username', '%' . $keyword . '%')
                        ->whereOr('admin_id', $kid);
                });
            } else {
                $query->whereLike('username', '%' . $keyword . '%');
            }
        }

        $rows = $query
            ->field('admin_id,username,group_id')
            ->order('username', 'asc')
            ->order('admin_id', 'asc')
            ->limit(300)
            ->select();

        $data = [];
        foreach ((array)$rows as $row) {
            $cid = (int)($row['admin_id'] ?? 0);
            if ($cid <= 0) {
                continue;
            }
            // 排除一切具备超管身份的账号（不仅排除当前操作者）
            if ($this->isShippingApplySuperAdmin($cid)) {
                continue;
            }
            $name = trim((string)($row['username'] ?? ''));
            if ($name === '') {
                continue;
            }
            $data[] = [
                'admin_id' => $cid,
                'username' => $name,
                'group_id' => (int)($row['group_id'] ?? 0),
                'display_name' => $name . '（ID:' . $cid . '）',
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
     * 超管：首次指定或修改替代产品经理
     *
     * @param array $params
     * @return array
     */
    public function authorizePmSubstitute($params = [])
    {
        $params = is_array($params) ? $params : [];
        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }
        if (!$this->isShippingApplySuperAdmin($adminId)) {
            return ['code' => -200, 'msg' => '仅超级管理员可指定替代产品经理', 'data' => []];
        }
        $operatorName = trim((string)($admin['username'] ?? ''));
        if ($operatorName === '') {
            $operatorName = 'admin#' . $adminId;
        }

        try {
            $applyId = $this->parsePositiveInt($params['apply_id'] ?? null, '草稿ID无效');
            $orderItemId = $this->parsePositiveInt($params['order_item_id'] ?? null, '原订单产品ID无效');
            $substituteManagerId = $this->parsePositiveInt(
                $params['substitute_manager_id'] ?? null,
                '替代产品经理ID无效'
            );
            $reason = $this->parseRequiredPmSubstituteReason($params['reason'] ?? null);
            $expectedToken = trim((string)($params['expected_version_token'] ?? $params['version_token'] ?? ''));
            if ($expectedToken === '') {
                $expectedToken = 'none';
            }
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        }

        Db::startTrans();
        try {
            $ctx = $this->lockPmSubstituteDraftContext($applyId, $orderItemId);
            $apply = $ctx['apply'];
            $orderItem = $ctx['order_item'];
            $applyNo = trim((string)($apply['apply_no'] ?? ''));
            $now = date('Y-m-d H:i:s');

            $productName = trim((string)($orderItem['product_name'] ?? ''));
            if ($productName === '') {
                $productName = '未命名产品';
            }
            $managerInfo = $this->resolveProductManagerForDraft($orderItem['manager_id'] ?? '', $productName);
            if (empty($managerInfo['manager_invalid'])) {
                // 原经理已恢复：禁止新增/修改授权。
                // 若仍有 status=1 旧授权，须在本事务内失效并提交（失效+审计同事务）；
                // 不得先写入再抛异常，否则外层 catch 的 rollback 会吞掉已成功的失效与审计。
                $existing = $this->lockPmSubstituteRow($applyId, $orderItemId);
                if (!empty($existing) && (int)($existing['status'] ?? 0) === 1) {
                    $this->markPmSubstituteInvalid(
                        $existing,
                        $adminId,
                        $operatorName,
                        $applyNo,
                        '原产品经理已恢复正常，替代授权自动失效',
                        'original_restored',
                        $now
                    );
                    $saved = Db::name('crm_shipping_pm_substitute')->where('id', (int)$existing['id'])->find();
                    Db::commit();
                    return [
                        'code' => -200,
                        'msg' => '原产品经理当前有效，不允许指定替代经理',
                        'data' => $this->formatPmSubstituteWriteResult($saved, $managerInfo, true),
                    ];
                }
                // 无有效旧授权：直接拒绝，不写无意义审计
                Db::rollback();
                return [
                    'code' => -200,
                    'msg' => '原产品经理当前有效，不允许指定替代经理',
                    'data' => [],
                ];
            }
            $managerStatus = (string)($managerInfo['manager_status'] ?? '');
            if (!in_array($managerStatus, ['missing', 'disabled', 'invalid_id'], true)) {
                throw new Exception('原产品经理当前状态不允许替代授权');
            }

            $candidate = $this->assertValidPmSubstituteCandidate($substituteManagerId);
            $invalidReason = $this->mapManagerStatusToInvalidReason($managerStatus);

            $existing = $this->lockPmSubstituteRow($applyId, $orderItemId);
            $currentToken = $this->buildPmSubstituteVersionToken($existing);
            if ($currentToken !== $expectedToken) {
                throw new Exception('授权信息已被他人修改，请刷新后重试');
            }

            if (!empty($existing) && (int)($existing['status'] ?? 0) === 2) {
                throw new Exception('该授权已提交冻结，不能修改');
            }

            // 幂等：有效授权且目标经理/原因未变
            if (!empty($existing)
                && (int)($existing['status'] ?? 0) === 1
                && (int)($existing['substitute_manager_id'] ?? 0) === $substituteManagerId
                && (string)($existing['reason'] ?? '') === $reason
            ) {
                Db::commit();
                return [
                    'code' => 0,
                    'msg' => '授权未变化',
                    'data' => $this->formatPmSubstituteWriteResult($existing, $managerInfo, false),
                ];
            }

            $isFirstAuthorize = empty($existing) || (int)($existing['status'] ?? 0) === 0;
            $action = $isFirstAuthorize ? 'pm_substitute_authorize' : 'pm_substitute_change';
            $beforeStatus = empty($existing) ? null : (int)$existing['status'];
            $beforeManagerId = empty($existing) ? null : (int)($existing['substitute_manager_id'] ?? 0);
            $beforeManagerName = empty($existing) ? '' : (string)($existing['substitute_manager_name'] ?? '');
            $beforeReason = empty($existing) ? '' : (string)($existing['reason'] ?? '');
            $beforeAuthorizedBy = empty($existing) ? null : (int)($existing['authorized_by'] ?? 0);
            $beforeAuthorizedByName = empty($existing) ? '' : (string)($existing['authorized_by_name'] ?? '');

            $saveData = [
                'shipping_apply_id' => $applyId,
                'order_item_id' => $orderItemId,
                'original_manager_id' => $managerInfo['admin_id'],
                'original_manager_name' => (string)($managerInfo['username'] ?? ''),
                'invalid_reason' => $invalidReason,
                'substitute_manager_id' => $substituteManagerId,
                'substitute_manager_name' => $candidate['username'],
                'authorized_by' => $adminId,
                'authorized_by_name' => mb_substr($operatorName, 0, 255, 'UTF-8'),
                'reason' => $reason,
                'status' => 1,
                'update_time' => $now,
            ];

            if (empty($existing)) {
                $saveData['create_time'] = $now;
                $saveData['auth_version'] = 1;
                try {
                    $newId = (int)Db::name('crm_shipping_pm_substitute')->insertGetId($saveData);
                } catch (\Throwable $e) {
                    if (stripos($e->getMessage(), 'Duplicate entry') !== false) {
                        throw new Exception('授权信息已被他人修改，请刷新后重试');
                    }
                    throw $e;
                }
                if ($newId <= 0) {
                    throw new Exception('授权写入失败');
                }
                $saved = Db::name('crm_shipping_pm_substitute')->where('id', $newId)->find();
            } else {
                $this->updatePmSubstituteWithAuthVersionBump(
                    (int)$existing['id'],
                    (int)($existing['auth_version'] ?? 0),
                    $saveData
                );
                $saved = Db::name('crm_shipping_pm_substitute')->where('id', (int)$existing['id'])->find();
            }

            if (empty($saved)) {
                throw new Exception('授权写入后读取失败');
            }

            $this->writePmSubstituteAuditLogs([
                'action' => $action,
                'shipping_apply_id' => $applyId,
                'apply_no' => $applyNo,
                'order_item_id' => $orderItemId,
                'product_name' => $productName,
                'original_manager_id' => $managerInfo['admin_id'],
                'original_manager_name' => (string)($managerInfo['username'] ?? ''),
                'manager_status' => $managerStatus,
                'invalid_reason' => $invalidReason,
                'before_substitute_manager_id' => $beforeManagerId,
                'before_substitute_manager_name' => $beforeManagerName,
                'after_substitute_manager_id' => $substituteManagerId,
                'after_substitute_manager_name' => $candidate['username'],
                'before_status' => $beforeStatus,
                'after_status' => 1,
                'before_reason' => $beforeReason,
                'after_reason' => $reason,
                'before_authorized_by' => $beforeAuthorizedBy,
                'before_authorized_by_name' => $beforeAuthorizedByName,
                'operator_id' => $adminId,
                'operator_name' => $operatorName,
                'now' => $now,
            ]);

            Db::commit();
            return [
                'code' => 0,
                'msg' => $isFirstAuthorize ? '替代产品经理授权成功' : '替代产品经理已更新',
                'data' => $this->formatPmSubstituteWriteResult($saved, $managerInfo, true),
            ];
        } catch (Exception $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        } catch (\Throwable $e) {
            Db::rollback();
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[ShippingApplyService::authorizePmSubstitute] ' . $e->getMessage(),
                    'error'
                );
            }
            return ['code' => -200, 'msg' => '授权失败，请稍后重试', 'data' => []];
        }
    }

    /**
     * 超管：撤销替代产品经理授权
     *
     * @param array $params
     * @return array
     */
    public function revokePmSubstitute($params = [])
    {
        $params = is_array($params) ? $params : [];
        $admin = $this->getCurrentAdminInfo();
        $adminId = (int)$admin['admin_id'];
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }
        if (!$this->isShippingApplySuperAdmin($adminId)) {
            return ['code' => -200, 'msg' => '仅超级管理员可撤销替代产品经理授权', 'data' => []];
        }
        $operatorName = trim((string)($admin['username'] ?? ''));
        if ($operatorName === '') {
            $operatorName = 'admin#' . $adminId;
        }

        try {
            $applyId = $this->parsePositiveInt($params['apply_id'] ?? null, '草稿ID无效');
            $orderItemId = $this->parsePositiveInt($params['order_item_id'] ?? null, '原订单产品ID无效');
            $reason = $this->parseRequiredPmSubstituteReason($params['reason'] ?? null);
            $expectedToken = trim((string)($params['expected_version_token'] ?? $params['version_token'] ?? ''));
            if ($expectedToken === '') {
                throw new Exception('缺少授权版本信息，请刷新后重试');
            }
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        }

        Db::startTrans();
        try {
            $ctx = $this->lockPmSubstituteDraftContext($applyId, $orderItemId);
            $apply = $ctx['apply'];
            $orderItem = $ctx['order_item'];
            $applyNo = trim((string)($apply['apply_no'] ?? ''));
            $now = date('Y-m-d H:i:s');

            $productName = trim((string)($orderItem['product_name'] ?? ''));
            if ($productName === '') {
                $productName = '未命名产品';
            }
            $managerInfo = $this->resolveProductManagerForDraft($orderItem['manager_id'] ?? '', $productName);

            $existing = $this->lockPmSubstituteRow($applyId, $orderItemId);
            if (empty($existing)) {
                throw new Exception('未找到可撤销的授权记录');
            }
            $currentToken = $this->buildPmSubstituteVersionToken($existing);
            if ($currentToken !== $expectedToken) {
                throw new Exception('授权信息已被他人修改，请刷新后重试');
            }
            if ((int)($existing['status'] ?? 0) === 2) {
                throw new Exception('该授权已提交冻结，不能撤销');
            }
            if ((int)($existing['status'] ?? 0) === 0) {
                // 幂等：已撤销且原因相同则直接成功；否则提示刷新
                Db::commit();
                return [
                    'code' => 0,
                    'msg' => '授权已处于撤销状态',
                    'data' => $this->formatPmSubstituteWriteResult($existing, $managerInfo, false),
                ];
            }

            // 原经理已恢复：走失效日志，避免再写 revoke 造成语义混淆
            if (empty($managerInfo['manager_invalid'])) {
                $this->markPmSubstituteInvalid(
                    $existing,
                    $adminId,
                    $operatorName,
                    $applyNo,
                    $reason,
                    'original_restored',
                    $now
                );
                $saved = Db::name('crm_shipping_pm_substitute')->where('id', (int)$existing['id'])->find();
                Db::commit();
                return [
                    'code' => 0,
                    'msg' => '原产品经理已恢复，替代授权已失效',
                    'data' => $this->formatPmSubstituteWriteResult($saved, $managerInfo, true),
                ];
            }

            // 保留原授权原因；撤销原因写入审计；auth_version 原子自增防 ABA
            $this->updatePmSubstituteWithAuthVersionBump(
                (int)$existing['id'],
                (int)($existing['auth_version'] ?? 0),
                [
                    'status' => 0,
                    'update_time' => $now,
                ],
                ['status' => 1]
            );

            $saved = Db::name('crm_shipping_pm_substitute')->where('id', (int)$existing['id'])->find();
            $this->writePmSubstituteAuditLogs([
                'action' => 'pm_substitute_revoke',
                'shipping_apply_id' => $applyId,
                'apply_no' => $applyNo,
                'order_item_id' => $orderItemId,
                'product_name' => $productName,
                'original_manager_id' => $existing['original_manager_id'] ?? $managerInfo['admin_id'],
                'original_manager_name' => (string)($existing['original_manager_name'] ?? $managerInfo['username'] ?? ''),
                'manager_status' => (string)($managerInfo['manager_status'] ?? ''),
                'invalid_reason' => (int)($existing['invalid_reason'] ?? 0),
                'before_substitute_manager_id' => (int)($existing['substitute_manager_id'] ?? 0),
                'before_substitute_manager_name' => (string)($existing['substitute_manager_name'] ?? ''),
                'after_substitute_manager_id' => (int)($existing['substitute_manager_id'] ?? 0),
                'after_substitute_manager_name' => (string)($existing['substitute_manager_name'] ?? ''),
                'before_status' => 1,
                'after_status' => 0,
                'before_reason' => (string)($existing['reason'] ?? ''),
                'after_reason' => $reason,
                'before_authorized_by' => (int)($existing['authorized_by'] ?? 0),
                'before_authorized_by_name' => (string)($existing['authorized_by_name'] ?? ''),
                'operator_id' => $adminId,
                'operator_name' => $operatorName,
                'now' => $now,
            ]);

            Db::commit();
            return [
                'code' => 0,
                'msg' => '替代授权已撤销',
                'data' => $this->formatPmSubstituteWriteResult($saved, $managerInfo, true),
            ];
        } catch (Exception $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => $e->getMessage(), 'data' => []];
        } catch (\Throwable $e) {
            Db::rollback();
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[ShippingApplyService::revokePmSubstitute] ' . $e->getMessage(),
                    'error'
                );
            }
            return ['code' => -200, 'msg' => '撤销失败，请稍后重试', 'data' => []];
        }
    }

    /**
     * 锁序：订单 → 订单产品 → 发货申请 → 校验发货行存在
     *
     * @param int $applyId
     * @param int $orderItemId
     * @return array{apply:array,order:array,order_item:array}
     * @throws Exception
     */
    protected function lockPmSubstituteDraftContext($applyId, $orderItemId)
    {
        $applyId = (int)$applyId;
        $orderItemId = (int)$orderItemId;

        $applyPreview = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
        if (empty($applyPreview)) {
            throw new Exception('发货申请不存在');
        }
        $orderId = (int)($applyPreview['order_id'] ?? 0);
        if ($orderId <= 0) {
            throw new Exception('发货申请关联订单无效');
        }

        $order = Db::name('crm_client_order')
            ->where('id', $orderId)
            ->lock(true)
            ->find();
        if (empty($order)) {
            throw new Exception('关联原订单不存在');
        }

        $orderItem = Db::name('crm_order_item')
            ->where('id', $orderItemId)
            ->where('order_id', $orderId)
            ->lock(true)
            ->find();
        if (empty($orderItem)) {
            throw new Exception('原订单产品行不存在或不属于该订单');
        }

        $apply = Db::name('crm_shipping_apply')
            ->where('id', $applyId)
            ->lock(true)
            ->find();
        if (empty($apply)) {
            throw new Exception('发货申请不存在');
        }
        if ((int)($apply['is_deleted'] ?? 0) !== 0) {
            throw new Exception('草稿已删除，不能授权');
        }
        if ((int)($apply['status'] ?? -1) !== 0) {
            throw new Exception('仅草稿状态可管理替代授权');
        }
        if ((int)($apply['order_id'] ?? 0) !== $orderId) {
            throw new Exception('发货申请关联订单异常');
        }

        $lineExists = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->where('order_item_id', $orderItemId)
            ->lock(true)
            ->find();
        if (empty($lineExists)) {
            throw new Exception('当前草稿不存在该原产品的有效发货行');
        }

        return [
            'apply' => $apply,
            'order' => $order,
            'order_item' => $orderItem,
        ];
    }

    /**
     * @param int $applyId
     * @param int $orderItemId
     * @return array|null
     */
    protected function lockPmSubstituteRow($applyId, $orderItemId)
    {
        $row = Db::name('crm_shipping_pm_substitute')
            ->where('shipping_apply_id', (int)$applyId)
            ->where('order_item_id', (int)$orderItemId)
            ->lock(true)
            ->find();
        return empty($row) ? null : $row;
    }

    /**
     * 草稿保存后：对已不在有效发货行中的 order_item_id 失效替代授权
     *
     * @param int $applyId
     * @param int[] $keptOrderItemIds
     * @param int $operatorId
     * @param string $operatorName
     * @param string $applyNo
     * @param string $now
     * @throws Exception
     */
    protected function invalidatePmSubstitutesForRemovedOrderItems(
        $applyId,
        array $keptOrderItemIds,
        $operatorId,
        $operatorName,
        $applyNo,
        $now
    ) {
        $applyId = (int)$applyId;
        if ($applyId <= 0) {
            return;
        }
        $keptMap = [];
        foreach ($keptOrderItemIds as $oid) {
            $oid = (int)$oid;
            if ($oid > 0) {
                $keptMap[$oid] = true;
            }
        }

        $rows = Db::name('crm_shipping_pm_substitute')
            ->where('shipping_apply_id', $applyId)
            ->where('status', 1)
            ->lock(true)
            ->select();

        foreach ((array)$rows as $row) {
            $oid = (int)($row['order_item_id'] ?? 0);
            if ($oid > 0 && isset($keptMap[$oid])) {
                continue;
            }
            $this->markPmSubstituteInvalid(
                $row,
                (int)$operatorId,
                (string)$operatorName,
                (string)$applyNo,
                '草稿已移除该原订单产品的全部发货行，替代授权自动失效',
                'order_item_removed',
                $now
            );
        }
    }

    /**
     * 将有效授权置为失效并写审计（调用方须已在事务内且已锁行）
     *
     * @param array $row
     * @param int $operatorId
     * @param string $operatorName
     * @param string $applyNo
     * @param string $reason
     * @param string $invalidateType
     * @param string $now
     * @throws Exception
     */
    protected function markPmSubstituteInvalid(
        array $row,
        $operatorId,
        $operatorName,
        $applyNo,
        $reason,
        $invalidateType,
        $now
    ) {
        $id = (int)($row['id'] ?? 0);
        if ($id <= 0 || (int)($row['status'] ?? 0) !== 1) {
            return;
        }
        $this->updatePmSubstituteWithAuthVersionBump(
            $id,
            (int)($row['auth_version'] ?? 0),
            [
                'status' => 0,
                'update_time' => $now,
            ],
            ['status' => 1]
        );

        $reason = trim((string)$reason);
        if ($reason === '') {
            $reason = '替代授权自动失效';
        }
        if (mb_strlen($reason, 'UTF-8') > self::PM_SUBSTITUTE_REASON_MAX) {
            $reason = mb_substr($reason, 0, self::PM_SUBSTITUTE_REASON_MAX, 'UTF-8');
        }

        $this->writePmSubstituteAuditLogs([
            'action' => 'pm_substitute_invalidate',
            'shipping_apply_id' => (int)($row['shipping_apply_id'] ?? 0),
            'apply_no' => (string)$applyNo,
            'order_item_id' => (int)($row['order_item_id'] ?? 0),
            'product_name' => '',
            'original_manager_id' => $row['original_manager_id'] ?? null,
            'original_manager_name' => (string)($row['original_manager_name'] ?? ''),
            'manager_status' => '',
            'invalid_reason' => (int)($row['invalid_reason'] ?? 0),
            'before_substitute_manager_id' => (int)($row['substitute_manager_id'] ?? 0),
            'before_substitute_manager_name' => (string)($row['substitute_manager_name'] ?? ''),
            'after_substitute_manager_id' => (int)($row['substitute_manager_id'] ?? 0),
            'after_substitute_manager_name' => (string)($row['substitute_manager_name'] ?? ''),
            'before_status' => 1,
            'after_status' => 0,
            'before_reason' => (string)($row['reason'] ?? ''),
            'after_reason' => $reason,
            'before_authorized_by' => (int)($row['authorized_by'] ?? 0),
            'before_authorized_by_name' => (string)($row['authorized_by_name'] ?? ''),
            'invalidate_type' => (string)$invalidateType,
            'operator_id' => (int)$operatorId,
            'operator_name' => (string)$operatorName,
            'now' => $now,
        ]);
    }

    /**
     * @param mixed $raw
     * @return string
     * @throws Exception
     */
    protected function parseRequiredPmSubstituteReason($raw)
    {
        if (is_array($raw) || is_object($raw)) {
            throw new Exception('授权原因格式不正确');
        }
        $reason = trim((string)$raw);
        if ($reason === '') {
            throw new Exception('请填写授权原因');
        }
        if (mb_strlen($reason, 'UTF-8') > self::PM_SUBSTITUTE_REASON_MAX) {
            throw new Exception('授权原因不能超过' . self::PM_SUBSTITUTE_REASON_MAX . '个字符');
        }
        return $reason;
    }

    /**
     * @param int $adminId
     * @return array{admin_id:int,username:string,group_id:int}
     * @throws Exception
     */
    protected function assertValidPmSubstituteCandidate($adminId)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            throw new Exception('替代产品经理ID无效');
        }
        $row = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,username,group_id,is_open')
            ->find();
        if (empty($row)) {
            throw new Exception('替代产品经理账号不存在');
        }
        $username = trim((string)($row['username'] ?? ''));
        if ($username === '') {
            throw new Exception('替代产品经理姓名无效');
        }
        if ((int)($row['is_open'] ?? 0) !== 1) {
            throw new Exception('不能指定已停用的账号作为替代产品经理');
        }
        $groupId = (int)($row['group_id'] ?? 0);
        if (!in_array($groupId, $this->pmSubstituteGroupIds, true)) {
            throw new Exception('替代产品经理角色不在允许范围');
        }
        if ($this->isShippingApplySuperAdmin($adminId)) {
            throw new Exception('不能指定超级管理员账号作为替代产品经理');
        }
        return [
            'admin_id' => $adminId,
            'username' => $username,
            'group_id' => $groupId,
        ];
    }

    /**
     * @param string $managerStatus
     * @return int 1账号不存在或原ID无效 2账号停用
     */
    protected function mapManagerStatusToInvalidReason($managerStatus)
    {
        return ((string)$managerStatus === 'disabled') ? 2 : 1;
    }

    /**
     * 并发版本令牌：使用记录 ID + 不可回退的 auth_version，消除业务字段 A→B→A 的 ABA
     *
     * @param array|null $row
     * @return string
     */
    protected function buildPmSubstituteVersionToken($row)
    {
        if (empty($row) || !is_array($row)) {
            return 'none';
        }
        $id = (int)($row['id'] ?? 0);
        if ($id <= 0) {
            return 'none';
        }
        $ver = (int)($row['auth_version'] ?? 0);
        if ($ver <= 0) {
            $ver = 1;
        }
        return $id . '|' . $ver;
    }

    /**
     * 条件更新并原子递增 auth_version（auth_version=auth_version+1）
     * 影响行数必须为 1，否则抛错由外层事务回滚
     *
     * @param int $id
     * @param int $expectedVersion
     * @param array $data 业务字段（勿直接写入 PHP 计算后的 auth_version）
     * @param array $extraWhere 额外 WHERE，如 ['status'=>1]
     * @return void
     * @throws Exception
     */
    protected function updatePmSubstituteWithAuthVersionBump($id, $expectedVersion, array $data, array $extraWhere = [])
    {
        $id = (int)$id;
        $expectedVersion = (int)$expectedVersion;
        if ($id <= 0 || $expectedVersion <= 0) {
            throw new Exception('授权版本数据异常，请刷新后重试');
        }
        if ($expectedVersion >= self::PM_SUBSTITUTE_AUTH_VERSION_MAX) {
            throw new Exception('授权版本号已达上限，请联系管理员');
        }
        unset($data['auth_version']);
        $data['auth_version'] = Db::raw('auth_version+1');

        $query = Db::name('crm_shipping_pm_substitute')
            ->where('id', $id)
            ->where('auth_version', $expectedVersion);
        foreach ($extraWhere as $field => $value) {
            $query->where($field, $value);
        }
        $affected = $query->update($data);
        if ((int)$affected !== 1) {
            throw new Exception('授权信息已被他人修改，请刷新后重试');
        }
    }

    /**
     * @param int $applyId
     * @return array<int,array>
     */
    protected function loadPmSubstituteMapByApplyId($applyId)
    {
        $applyId = (int)$applyId;
        if ($applyId <= 0) {
            return [];
        }
        $rows = Db::name('crm_shipping_pm_substitute')
            ->where('shipping_apply_id', $applyId)
            ->select();
        $map = [];
        foreach ((array)$rows as $row) {
            $oid = (int)($row['order_item_id'] ?? 0);
            if ($oid > 0) {
                $map[$oid] = $row;
            }
        }
        return $map;
    }

    /**
     * 只读展示态（不写库）
     *
     * @param array|null $subRow
     * @param array $managerResolve
     * @return array
     */
    protected function buildPmSubstituteDisplayState($subRow, array $managerResolve)
    {
        $empty = [
            'pm_substitute_record_id' => 0,
            'pm_substitute_record_status' => -1,
            'pm_substitute_version_token' => 'none',
            'pm_substitute_effective' => false,
            'pm_substitute_display_status' => 'none',
            'pm_substitute_tip' => '',
            'substitute_manager_id' => null,
            'substitute_manager_name' => '',
            'pm_substitute_authorized_by' => 0,
            'pm_substitute_authorized_by_name' => '',
            'pm_substitute_reason' => '',
        ];

        $managerInvalid = !empty($managerResolve['manager_invalid']);
        if (empty($subRow) || !is_array($subRow)) {
            if ($managerInvalid) {
                $empty['pm_substitute_display_status'] = 'need_authorize';
                $empty['pm_substitute_tip'] = '原产品经理异常，尚未指定替代经理，请联系超级管理员处理';
            }
            return $empty;
        }

        $token = $this->buildPmSubstituteVersionToken($subRow);
        $status = (int)($subRow['status'] ?? 0);
        $subId = (int)($subRow['substitute_manager_id'] ?? 0);
        $subName = trim((string)($subRow['substitute_manager_name'] ?? ''));
        $base = [
            'pm_substitute_record_id' => (int)($subRow['id'] ?? 0),
            'pm_substitute_record_status' => $status,
            'pm_substitute_version_token' => $token,
            'pm_substitute_effective' => false,
            'pm_substitute_display_status' => 'none',
            'pm_substitute_tip' => '',
            'substitute_manager_id' => $subId > 0 ? $subId : null,
            'substitute_manager_name' => $subName,
            'pm_substitute_authorized_by' => (int)($subRow['authorized_by'] ?? 0),
            'pm_substitute_authorized_by_name' => trim((string)($subRow['authorized_by_name'] ?? '')),
            'pm_substitute_reason' => (string)($subRow['reason'] ?? ''),
        ];

        if (!$managerInvalid) {
            $base['pm_substitute_display_status'] = 'original_restored';
            $base['pm_substitute_tip'] = '原产品经理已恢复，替代授权不再生效';
            return $base;
        }

        if ($status === 2) {
            $base['pm_substitute_display_status'] = 'frozen';
            $base['pm_substitute_tip'] = '替代授权已提交冻结';
            return $base;
        }

        if ($status === 0) {
            $base['pm_substitute_display_status'] = 'revoked';
            $base['pm_substitute_tip'] = '替代授权已撤销，请重新联系超级管理员';
            return $base;
        }

        // status=1：再校验替代人当前是否仍合格
        try {
            $this->assertValidPmSubstituteCandidate($subId);
            $base['pm_substitute_effective'] = true;
            $base['pm_substitute_display_status'] = 'authorized';
            $base['pm_substitute_tip'] = '已指定替代产品经理：' . ($subName !== '' ? $subName : ('ID:' . $subId));
        } catch (Exception $e) {
            $base['pm_substitute_display_status'] = 'substitute_unavailable';
            $base['pm_substitute_tip'] = '替代产品经理当前不可用（' . $e->getMessage() . '）';
        }

        return $base;
    }

    /**
     * @param array|null $saved
     * @param array $managerInfo
     * @param bool $changed
     * @return array
     */
    protected function formatPmSubstituteWriteResult($saved, array $managerInfo, $changed)
    {
        $display = $this->buildPmSubstituteDisplayState($saved, $managerInfo);
        return [
            'changed' => $changed ? 1 : 0,
            'record_id' => (int)($display['pm_substitute_record_id'] ?? 0),
            'version_token' => (string)($display['pm_substitute_version_token'] ?? 'none'),
            'status' => (int)($display['pm_substitute_record_status'] ?? -1),
            'display_status' => (string)($display['pm_substitute_display_status'] ?? 'none'),
            'display_tip' => (string)($display['pm_substitute_tip'] ?? ''),
            'effective' => !empty($display['pm_substitute_effective']) ? 1 : 0,
            'substitute_manager_id' => $display['substitute_manager_id'],
            'substitute_manager_name' => (string)($display['substitute_manager_name'] ?? ''),
            'reason' => (string)($display['pm_substitute_reason'] ?? ''),
        ];
    }

    /**
     * 写入授权审计；必要时拆成多条 opinion 以保证完整旧/新原因可恢复
     *
     * @param array $payload
     * @throws Exception
     */
    protected function writePmSubstituteAuditLogs(array $payload)
    {
        $action = trim((string)($payload['action'] ?? ''));
        if ($action === '' || mb_strlen($action, 'UTF-8') > 50) {
            throw new Exception('审计动作类型无效');
        }
        $applyId = (int)($payload['shipping_apply_id'] ?? 0);
        if ($applyId <= 0) {
            throw new Exception('审计关联申请无效');
        }
        $operatorId = (int)($payload['operator_id'] ?? 0);
        $operatorName = trim((string)($payload['operator_name'] ?? ''));
        if ($operatorName === '') {
            $operatorName = 'admin#' . $operatorId;
        }
        $now = (string)($payload['now'] ?? date('Y-m-d H:i:s'));
        $eventId = sprintf(
            'pmsub-%d-%d-%s',
            $applyId,
            (int)($payload['order_item_id'] ?? 0),
            str_replace('.', '', uniqid('', true))
        );

        $beforeReason = (string)($payload['before_reason'] ?? '');
        $afterReason = (string)($payload['after_reason'] ?? '');

        $meta = [
            'event_id' => $eventId,
            'part' => 1,
            'apply_id' => $applyId,
            'apply_no' => (string)($payload['apply_no'] ?? ''),
            'order_item_id' => (int)($payload['order_item_id'] ?? 0),
            'product_name' => (string)($payload['product_name'] ?? ''),
            'original_manager_id' => $payload['original_manager_id'] ?? null,
            'original_manager_name' => (string)($payload['original_manager_name'] ?? ''),
            'manager_status' => (string)($payload['manager_status'] ?? ''),
            'invalid_reason' => (int)($payload['invalid_reason'] ?? 0),
            'before_substitute_manager_id' => $payload['before_substitute_manager_id'] ?? null,
            'before_substitute_manager_name' => (string)($payload['before_substitute_manager_name'] ?? ''),
            'after_substitute_manager_id' => $payload['after_substitute_manager_id'] ?? null,
            'after_substitute_manager_name' => (string)($payload['after_substitute_manager_name'] ?? ''),
            'before_auth_status' => $payload['before_status'] ?? null,
            'after_auth_status' => $payload['after_status'] ?? null,
            'before_authorized_by' => $payload['before_authorized_by'] ?? null,
            'before_authorized_by_name' => (string)($payload['before_authorized_by_name'] ?? ''),
            'operator_id' => $operatorId,
            'operator_name' => $operatorName,
            'invalidate_type' => (string)($payload['invalidate_type'] ?? ''),
            'before_reason_len' => mb_strlen($beforeReason, 'UTF-8'),
            'after_reason_len' => mb_strlen($afterReason, 'UTF-8'),
            'time' => $now,
        ];

        $parts = [];
        $tryFull = $meta;
        $tryFull['before_reason'] = $beforeReason;
        $tryFull['after_reason'] = $afterReason;
        $tryFull['parts'] = 1;
        $fullJson = $this->encodeApprovalLogOpinion($tryFull);
        if ($this->approvalLogOpinionFits($fullJson)) {
            $parts[] = $fullJson;
        } else {
            $meta['parts'] = 1; // 先占位，稍后回填
            $meta['reason_storage'] = 'split';
            $headJson = $this->encodeApprovalLogOpinion($meta);
            if (!$this->approvalLogOpinionFits($headJson)) {
                // 极端：压缩产品名再试
                $meta['product_name'] = mb_substr((string)$meta['product_name'], 0, 40, 'UTF-8');
                $headJson = $this->encodeApprovalLogOpinion($meta);
            }
            if (!$this->approvalLogOpinionFits($headJson)) {
                throw new Exception('授权审计信息过长，无法安全写入');
            }

            $detailParts = [];
            if ($beforeReason !== '') {
                $detailParts = array_merge(
                    $detailParts,
                    $this->splitOpinionTextParts($eventId, 'before_reason', $beforeReason)
                );
            }
            if ($afterReason !== '') {
                $detailParts = array_merge(
                    $detailParts,
                    $this->splitOpinionTextParts($eventId, 'after_reason', $afterReason)
                );
            }
            if (empty($detailParts)) {
                throw new Exception('授权审计原因拆分失败');
            }
            $totalParts = 1 + count($detailParts);
            $meta['parts'] = $totalParts;
            $headJson = $this->encodeApprovalLogOpinion($meta);
            if (!$this->approvalLogOpinionFits($headJson)) {
                throw new Exception('授权审计信息过长，无法安全写入');
            }
            $parts[] = $headJson;
            foreach ($detailParts as $idx => $detail) {
                $detail['part'] = $idx + 2;
                $detail['parts'] = $totalParts;
                $detailJson = $this->encodeApprovalLogOpinion($detail);
                if (!$this->approvalLogOpinionFits($detailJson)) {
                    throw new Exception('授权审计分片写入失败');
                }
                $parts[] = $detailJson;
            }
        }

        $applyStatus = 0;
        $operatorRole = $this->isShippingApplySuperAdmin($operatorId) ? 'system_admin' : 'applicant';
        // 草稿保存触发的自动失效：标记为 system，便于与超管手工授权区分
        if ($action === 'pm_substitute_invalidate'
            && in_array((string)($payload['invalidate_type'] ?? ''), ['order_item_removed', 'original_restored'], true)
            && $operatorRole !== 'system_admin'
        ) {
            $operatorRole = 'system';
        }
        foreach ($parts as $opinionJson) {
            $logId = Db::name('crm_shipping_approval_log')->insertGetId([
                'shipping_apply_id' => $applyId,
                'task_id' => null,
                'operator_id' => $operatorId,
                'operator_name' => mb_substr($operatorName, 0, 100, 'UTF-8'),
                'operator_role' => $operatorRole,
                'action' => $action,
                'before_status' => $applyStatus,
                'after_status' => $applyStatus,
                'opinion' => $opinionJson,
                'create_time' => $now,
            ]);
            if ((int)$logId <= 0) {
                throw new Exception('授权审计日志写入失败');
            }
        }
    }

    /**
     * @param array $payload
     * @return string
     * @throws Exception
     */
    protected function encodeApprovalLogOpinion(array $payload)
    {
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE);
        if ($json === false) {
            throw new Exception('授权审计信息编码失败');
        }
        return $json;
    }

    /**
     * MySQL utf8mb4 VARCHAR 按字符计；同时保守限制字节，避免连接层异常
     *
     * @param string $json
     * @return bool
     */
    protected function approvalLogOpinionFits($json)
    {
        $json = (string)$json;
        if (mb_strlen($json, 'UTF-8') > self::APPROVAL_LOG_OPINION_MAX) {
            return false;
        }
        // 保守：utf8mb4 单字符最多 4 字节
        if (strlen($json) > (self::APPROVAL_LOG_OPINION_MAX * 4)) {
            return false;
        }
        return true;
    }

    /**
     * @param string $eventId
     * @param string $field
     * @param string $text
     * @return array
     * @throws Exception
     */
    protected function splitOpinionTextParts($eventId, $field, $text)
    {
        $text = (string)$text;
        $chunks = [];
        // 预留 JSON 外壳空间，按约 700 字符切分正文
        $chunkSize = 700;
        $len = mb_strlen($text, 'UTF-8');
        if ($len <= 0) {
            return [];
        }
        for ($offset = 0; $offset < $len; $offset += $chunkSize) {
            $chunks[] = mb_substr($text, $offset, $chunkSize, 'UTF-8');
        }
        $result = [];
        $total = count($chunks);
        foreach ($chunks as $i => $chunk) {
            $result[] = [
                'event_id' => $eventId,
                'field' => $field,
                'chunk_index' => $i + 1,
                'chunk_total' => $total,
                'content' => $chunk,
            ];
        }
        return $result;
    }
}
