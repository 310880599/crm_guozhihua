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
     * 正式提交成功后的合法业务状态（与占量解耦；不含草稿 0）
     * 依据 V1.6 BR-12：10 待产品经理 / 20·40 驳回 / 30 待总监 / 50 审批通过 / 60 取消
     */
    const HISTORICAL_SUBMIT_SUCCESS_STATUSES = [10, 20, 30, 40, 50, 60];

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
     * M4-1B 正式提交载荷哈希规范版本（仅写入规范化 JSON，不落独立 DB 字段）
     * 未来若规范化规则变更，应递增此版本，避免旧幂等语义误判。
     */
    const SUBMIT_PAYLOAD_HASH_SCHEMA_VERSION = 1;

    /** 业务时区：正式提交日期「今天」判断依据（不信任浏览器上报日期） */
    const SHIPPING_BUSINESS_TIMEZONE = 'Asia/Shanghai';

    /** 对齐 crm_shipping_apply 字段容量 */
    const SUBMIT_RECEIVER_NAME_MAX = 100;
    const SUBMIT_RECEIVER_PHONE_MAX = 100;
    const SUBMIT_RECEIVER_ADDRESS_MAX = 500;
    const SUBMIT_SHIPPING_METHOD_MAX = 100;
    const SUBMIT_SHIPPING_REMARK_MAX = 1000;
    const SUBMIT_APPLY_PRODUCT_NAME_MAX = 200;
    const SUBMIT_APPLY_SPEC_MODEL_MAX = 200;
    const SUBMIT_ITEM_REMARK_MAX = 500;

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
     * 正式提交严格正整数解析（独立于草稿 parsePositiveInt，避免放宽草稿行为）
     *
     * 允许：正 int；纯数字字符串且不超过 PHP_INT_MAX（先 bccomp 再 (int)，防截断/饱和）。
     * 拒绝：float/bool/array/object/null/空串/0/负值/小数点/科学计数法/夹杂非数字/超大数字串。
     *
     * @param mixed $raw
     * @param string $errorMsg
     * @return int
     * @throws Exception
     */
    public function parseStrictPositiveInt($raw, $errorMsg)
    {
        if (is_int($raw)) {
            if ($raw <= 0) {
                throw new Exception($errorMsg);
            }
            return $raw;
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            // 禁止前导零与「0」：仅 [1-9] 开头的十进制整数字符串
            if ($raw === '' || !preg_match('/^[1-9]\d*$/', $raw)) {
                throw new Exception($errorMsg);
            }
            if (bccomp($raw, (string)PHP_INT_MAX, 0) > 0) {
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
     * 严格解析授权 invalid_reason：仅允许整数 1/2 或纯数字字符串 '1'/'2'。
     * 禁止 NULL、空串、0、带后缀数字、浮点、数组、对象等。
     *
     * @param mixed $raw
     * @return int 1|2
     * @throws Exception
     */
    protected function parseStrictAuthInvalidReason($raw)
    {
        if (is_int($raw)) {
            if ($raw === 1 || $raw === 2) {
                return $raw;
            }
            throw new Exception('替代授权异常原因无效，拒绝继续。');
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            if ($raw === '1' || $raw === '2') {
                return (int)$raw;
            }
            throw new Exception('替代授权异常原因无效，拒绝继续。');
        }
        throw new Exception('替代授权异常原因无效，拒绝继续。');
    }

    /**
     * 校验替代授权归属当前申请/产品，并解析可信 auth_id / auth_version。
     * apply_id / order_item_id 输出始终取自已校验的调用上下文，不采用未验证授权字段回填。
     *
     * @param array $authRow
     * @param int $applyId
     * @param int $orderItemId
     * @return array{auth_id:int,apply_id:int,order_item_id:int,auth_version:int}
     * @throws Exception
     */
    protected function assertPmSubstituteAuthOwnershipForSubmit(array $authRow, $applyId, $orderItemId)
    {
        $applyId = (int)$applyId;
        $orderItemId = (int)$orderItemId;
        if ($applyId <= 0) {
            throw new Exception('草稿ID无效。');
        }
        if ($orderItemId <= 0) {
            throw new Exception('原订单产品行无效。');
        }

        if (!array_key_exists('shipping_apply_id', $authRow)) {
            throw new Exception('替代授权缺少申请ID，拒绝继续。');
        }
        try {
            $authApplyId = $this->parseStrictPositiveInt(
                $authRow['shipping_apply_id'],
                '替代授权申请ID无效。'
            );
        } catch (Exception $e) {
            throw new Exception('替代授权申请ID无效，拒绝继续。');
        }
        if ($authApplyId !== $applyId) {
            throw new Exception('替代授权不属于当前发货申请，拒绝继续。');
        }

        if (!array_key_exists('order_item_id', $authRow)) {
            throw new Exception('替代授权缺少原订单产品ID，拒绝继续。');
        }
        try {
            $authOrderItemId = $this->parseStrictPositiveInt(
                $authRow['order_item_id'],
                '替代授权原产品ID无效。'
            );
        } catch (Exception $e) {
            throw new Exception('替代授权原产品ID无效，拒绝继续。');
        }
        if ($authOrderItemId !== $orderItemId) {
            throw new Exception('替代授权与原订单产品行不匹配，拒绝继续。');
        }

        if (!array_key_exists('id', $authRow)) {
            throw new Exception('替代授权记录ID缺失，拒绝继续。');
        }
        try {
            $authId = $this->parseStrictPositiveInt($authRow['id'], '替代授权记录ID无效。');
        } catch (Exception $e) {
            throw new Exception('替代授权记录ID无效，拒绝继续。');
        }

        if (!array_key_exists('auth_version', $authRow)) {
            throw new Exception('替代授权版本号缺失，拒绝继续。');
        }
        try {
            $authVersion = $this->parseStrictPositiveInt(
                $authRow['auth_version'],
                '替代授权版本号无效。'
            );
        } catch (Exception $e) {
            throw new Exception('替代授权版本号无效，拒绝继续。');
        }
        if ($authVersion > self::PM_SUBSTITUTE_AUTH_VERSION_MAX) {
            throw new Exception('替代授权版本号超出允许范围，拒绝继续。');
        }

        return [
            'auth_id' => $authId,
            'apply_id' => $applyId,
            'order_item_id' => $orderItemId,
            'auth_version' => $authVersion,
        ];
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

    // =====================================================================
    // M4-1B-1：正式提交公共校验 / 规范化载荷哈希 / 锁序 / 数量当前读
    // 说明：本轮仅提供可复用内部能力；不开放 HTTP 提交接口，不写库、不改状态。
    // 未来集成点：M4-1B-2/3 的 submitApply 事务内依次调用：
    //   lockOrder → lockOrderItems → validate → normalize → hash
    //   → calcAppliedQtyMapForSubmit →（写主表/明细/任务/幂等 token）
    // 幂等语义（同 token+同申请人+同 hash 返回原成功；异载荷/异申请人拒绝）属 M4-1B-3。
    // =====================================================================

    /**
     * 正式提交确认标志严格解析（仅允许未传/null/0/'0'/1/'1'）
     *
     * 拒绝：'1abc'、'2'、布尔、浮点、空串、空白、数组/对象等。
     * 规范化输出供哈希：调用方将 true/false 写成 1/0。
     * 仅用于正式提交路径；saveDraft 不得改用本方法。
     *
     * @param mixed $raw
     * @param string $fieldLabel
     * @return bool
     * @throws Exception
     */
    public function parseSubmitConfirmationFlag($raw, $fieldLabel)
    {
        $fieldLabel = trim((string)$fieldLabel);
        if ($fieldLabel === '') {
            $fieldLabel = '确认标志';
        }
        $err = $fieldLabel . '无效，仅允许 0 或 1。';

        if ($raw === null) {
            return false;
        }
        if (is_bool($raw) || is_float($raw) || is_array($raw) || is_object($raw)) {
            throw new Exception($err);
        }
        if (is_int($raw)) {
            if ($raw === 0) {
                return false;
            }
            if ($raw === 1) {
                return true;
            }
            throw new Exception($err);
        }
        if (is_string($raw)) {
            // 不 trim：纯空白与带空白填充均非法，避免静默放宽
            if ($raw === '0') {
                return false;
            }
            if ($raw === '1') {
                return true;
            }
            throw new Exception($err);
        }

        throw new Exception($err);
    }

    /**
     * 正式提交事务内严格校验入口（失败即关闭；不得静默跳过核心校验）
     *
     * 【调用前置条件｜须由未来 M4-1B-3 submitApply 事务调用链保证】
     * 本方法无法仅凭 PHP 数组证明数据来自当前事务行锁：
     * 1. 调用方已开启事务；
     * 2. $context['order'] 来自 lockOrderForShippingSubmit() 锁内读取；
     * 3. $context['order_item_map'] 来自 lockOrderItemsForShippingSubmit() 锁内读取；
     * 4. $context['applied_qty_map'] 来自 calcAppliedQtyMapForSubmit($ids, $excludeApplyId)
     *    当前读结果，且对每个有效原产品 ID 均含键（占用为 0 时键仍须存在）；
     * 5. $context['applicant_id'] 为服务端会话确认，禁止信任前端。
     *
     * 【本方法可静态保证】上下文键齐全、订单 ID 一致、产品行归属、占用键齐全且非负、
     * 强制厂家存在性校验、审核通过与订单访问权限（复用 validateOrderForApply）。
     * 【不可静态保证】真实行锁 / 隔离级别 / 并发正确性——属数据库联调待验证。
     *
     * 忽略调用方传入的 assert_factory=false，始终强制厂家校验。
     *
     * @param array $params 与 saveDraft 同源前端字段
     * @param array $context 必须含 applicant_id / order / order_item_map / applied_qty_map
     * @return array{header:array,lines:array,apply_id:int,order_id:int,applicant_id:int}
     * @throws Exception
     */
    public function validateSubmitPayloadForCommit(array $params, array $context)
    {
        if (!array_key_exists('applicant_id', $context)
            || $context['applicant_id'] === null
            || $context['applicant_id'] === ''
        ) {
            throw new Exception('正式提交缺少服务端确认的申请人身份，拒绝继续。');
        }
        // 严格正整数：禁止 (int)'394abc'→394；正式提交身份须来自服务端 Session，不得信任前端任意指定
        try {
            $applicantId = $this->parseStrictPositiveInt(
                $context['applicant_id'],
                '正式提交申请人身份无效，拒绝继续。'
            );
        } catch (Exception $e) {
            throw new Exception('正式提交申请人身份无效，拒绝继续。');
        }

        if (!array_key_exists('order', $context) || !is_array($context['order']) || empty($context['order'])) {
            throw new Exception('正式提交缺少事务内锁定的原订单快照，拒绝继续。');
        }
        $order = $context['order'];
        $lockedOrderId = (int)($order['id'] ?? 0);
        if ($lockedOrderId <= 0) {
            throw new Exception('正式提交原订单快照无效，拒绝继续。');
        }

        $paramOrderId = $this->parsePositiveInt($params['order_id'] ?? null, '请先选择有效订单。');
        if ($paramOrderId !== $lockedOrderId) {
            throw new Exception('提交订单与事务内锁定订单不一致，拒绝继续。');
        }

        // 审核状态 + 访问权限：快照级校验；真锁由调用链保证
        $this->validateOrderForApply($order, $applicantId);

        if (!array_key_exists('order_item_map', $context) || !is_array($context['order_item_map'])) {
            throw new Exception('正式提交缺少事务内锁定的原订单产品行，拒绝继续。');
        }
        $orderItemMap = $context['order_item_map'];

        if (!array_key_exists('applied_qty_map', $context) || !is_array($context['applied_qty_map'])) {
            throw new Exception('正式提交缺少数量占用当前读结果，拒绝继续。');
        }
        $appliedQtyMap = $context['applied_qty_map'];

        $itemsRaw = $params['items'] ?? [];
        if (is_string($itemsRaw)) {
            $decoded = json_decode($itemsRaw, true);
            $itemsRaw = is_array($decoded) ? $decoded : [];
        }
        if (!is_array($itemsRaw)) {
            $itemsRaw = [];
        }

        $effectiveOrderItemIds = [];
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
            // 零数量临时行不进入正式提交有效行，不要求占用键
            if ($applyQty === 0) {
                continue;
            }
            $effectiveOrderItemIds[$orderItemId] = $orderItemId;
        }

        if (empty($effectiveOrderItemIds)) {
            throw new Exception('至少需要一条正整数数量的发货行。');
        }

        foreach ($effectiveOrderItemIds as $orderItemId) {
            if (!isset($orderItemMap[$orderItemId]) || !is_array($orderItemMap[$orderItemId])) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }
            $orderItem = $orderItemMap[$orderItemId];
            // P1-2：产品行 order_id 必须严格等于锁定订单；0/缺失/指向其他订单一律拒绝
            $itemOrderId = (int)($orderItem['order_id'] ?? 0);
            if ($itemOrderId !== $lockedOrderId) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }
            if ((int)($orderItem['id'] ?? 0) !== $orderItemId) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }

            // 区分「占用为 0」与「未查询」：缺失键必须拒绝，禁止 ?? 0
            if (!array_key_exists($orderItemId, $appliedQtyMap)) {
                throw new Exception('正式提交缺少产品行占用数量当前读结果，拒绝继续。');
            }
            $appliedRaw = $appliedQtyMap[$orderItemId];
            if ($appliedRaw === null || is_bool($appliedRaw) || is_float($appliedRaw)
                || is_array($appliedRaw) || is_object($appliedRaw)
            ) {
                throw new Exception('正式提交占用数量当前读结果非法，拒绝继续。');
            }
            if (is_int($appliedRaw)) {
                if ($appliedRaw < 0) {
                    throw new Exception('正式提交占用数量当前读结果非法，拒绝继续。');
                }
            } elseif (is_string($appliedRaw)) {
                if ($appliedRaw === '' || !preg_match('/^\d+$/', $appliedRaw)) {
                    throw new Exception('正式提交占用数量当前读结果非法，拒绝继续。');
                }
            } else {
                throw new Exception('正式提交占用数量当前读结果非法，拒绝继续。');
            }
        }

        // 强制厂家校验；禁止 assert_factory=false 绕过
        $strictContext = [
            'applicant_id' => $applicantId,
            'order_item_map' => $orderItemMap,
            'applied_qty_map' => $appliedQtyMap,
            'assert_factory' => true,
        ];

        $validated = $this->validateSubmitPayload($params, $strictContext);

        // 极小范围：若用户已确认金额，须与锁内订单金额一致（完整金额变化策略属 M4-1B-3）
        if ((int)($validated['header']['order_amount_confirmed'] ?? 0) === 1) {
            $confirmed = $validated['header']['confirmed_order_amount'] ?? null;
            $currentOrderAmount = $this->normalizeNullableMoney($order['money'] ?? null, false);
            if ($confirmed === null || $currentOrderAmount === null
                || bccomp((string)$confirmed, (string)$currentOrderAmount, 2) !== 0
            ) {
                throw new Exception('原订单金额确认与当前锁定订单金额不一致，请刷新后重新确认。');
            }
        }

        return $validated;
    }

    /**
     * 正式提交专用必填校验（严格；不得用于 saveDraft 宽松路径）
     *
     * 覆盖：新建直接提交（apply_id=0）与已有草稿提交（apply_id>0）同一套规则。
     * 零数量临时行与 saveDraft 一致：跳过，不落有效提交行。
     * 厂家存在性、原订单归属、剩余数量等需 DB 的校验：当 $context 提供完整 map 时执行。
     * 正式提交事务请优先走 validateSubmitPayloadForCommit()，禁止依赖本方法在缺上下文时跳过核心校验。
     *
     * @param array $params 与 saveDraft 同源前端字段
     * @param array $context [
     *   applicant_id:int 服务端确认的申请人（必填，不信任前端）,
     *   order_item_map?: array<int,array> 已锁原订单产品行,
     *   applied_qty_map?: array<int,int> 当前读占用（正式提交用）,
     *   assert_factory?: bool 默认 true（有 order_item_map 时校验厂家存在；仅纯函数测试可关）
     * ]
     * @return array{header:array,lines:array,apply_id:int,order_id:int,applicant_id:int}
     * @throws Exception
     */
    public function validateSubmitPayload(array $params, array $context = [])
    {
        try {
            $applicantId = $this->parseStrictPositiveInt(
                $context['applicant_id'] ?? null,
                '申请人身份无效。'
            );
        } catch (Exception $e) {
            throw new Exception('申请人身份无效。');
        }

        $orderId = $this->parsePositiveInt($params['order_id'] ?? null, '请先选择有效订单。');
        $applyId = 0;
        if (isset($params['apply_id']) && $params['apply_id'] !== '' && $params['apply_id'] !== null) {
            if (is_array($params['apply_id']) || is_object($params['apply_id'])) {
                throw new Exception('草稿ID无效。');
            }
            $applyIdRaw = trim((string)$params['apply_id']);
            if ($applyIdRaw !== '' && $applyIdRaw !== '0') {
                $applyId = $this->parsePositiveInt($params['apply_id'], '草稿ID无效。');
            }
        }

        $historyConfirmed = $this->parseSubmitConfirmationFlag(
            array_key_exists('shipping_date_history_confirmed', $params)
                ? $params['shipping_date_history_confirmed']
                : null,
            '历史发货日期确认标志'
        );
        // 幂等哈希纯解析：跳过「今天」动态闸门，避免历史成功重试因日期滚动误失败；确认标志仍按原文参与哈希
        $skipHistoryDateGate = !empty($context['for_idempotent_hash']);
        $shippingDate = $this->parseRequiredSubmitDate(
            $params['shipping_date'] ?? null,
            $historyConfirmed || $skipHistoryDateGate
        );

        $shippingMethod = $this->parseRequiredSubmitText(
            $params['shipping_method'] ?? null,
            self::SUBMIT_SHIPPING_METHOD_MAX,
            '发货方式'
        );
        $receiverName = $this->parseRequiredSubmitText(
            $params['receiver_name'] ?? null,
            self::SUBMIT_RECEIVER_NAME_MAX,
            '收货人'
        );
        $receiverPhone = $this->parseRequiredSubmitReceiverPhone($params['receiver_phone'] ?? null);
        $receiverAddress = $this->parseRequiredSubmitText(
            $params['receiver_address'] ?? null,
            self::SUBMIT_RECEIVER_ADDRESS_MAX,
            '收货地址'
        );
        $shippingRemark = $this->parseOptionalSubmitBoundedText(
            $params['shipping_remark'] ?? null,
            self::SUBMIT_SHIPPING_REMARK_MAX,
            '发货备注'
        );

        $orderAmountConfirmed = $this->parseSubmitConfirmationFlag(
            array_key_exists('order_amount_confirmed', $params)
                ? $params['order_amount_confirmed']
                : null,
            '原订单金额变化确认标志'
        );
        $confirmedOrderAmount = null;
        if ($orderAmountConfirmed) {
            $confirmedOrderAmount = $this->parseRequiredSubmitMoney(
                array_key_exists('confirmed_order_amount', $params) ? $params['confirmed_order_amount'] : null,
                '确认的原订单金额格式不正确。'
            );
        }

        $itemsRaw = $params['items'] ?? [];
        if (is_string($itemsRaw)) {
            $decoded = json_decode($itemsRaw, true);
            $itemsRaw = is_array($decoded) ? $decoded : [];
        }
        if (!is_array($itemsRaw)) {
            $itemsRaw = [];
        }

        $orderItemMap = isset($context['order_item_map']) && is_array($context['order_item_map'])
            ? $context['order_item_map']
            : null;
        $appliedQtyMap = isset($context['applied_qty_map']) && is_array($context['applied_qty_map'])
            ? $context['applied_qty_map']
            : null;
        $assertFactory = array_key_exists('assert_factory', $context)
            ? (bool)$context['assert_factory']
            : ($orderItemMap !== null);

        $lines = [];
        $qtySumByOrderItem = [];

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
            // 与 saveDraft 一致：零数量临时行不进入正式提交有效行
            if ($applyQty === 0) {
                continue;
            }
            if ($applyQty > 2147483647) {
                throw new Exception('本次申请发货数量过大。');
            }

            if ($orderItemMap !== null && !isset($orderItemMap[$orderItemId])) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }

            if (!isset($qtySumByOrderItem[$orderItemId])) {
                $qtySumByOrderItem[$orderItemId] = 0;
            }
            $qtySumByOrderItem[$orderItemId] += $applyQty;
            if ($qtySumByOrderItem[$orderItemId] > 2147483647) {
                throw new Exception('本次申请发货数量过大。');
            }

            $fallbackProductName = '未命名产品';
            $fallbackSpec = '';
            if ($orderItemMap !== null && isset($orderItemMap[$orderItemId])) {
                $oi = $orderItemMap[$orderItemId];
                $pn = trim((string)($oi['product_name'] ?? ''));
                if ($pn !== '') {
                    $fallbackProductName = $pn;
                }
                $fallbackSpec = trim((string)($oi['spec_model'] ?? ''));
            }

            $applyProductName = $this->parseRequiredSubmitText(
                array_key_exists('apply_product_name', $inputItem) && trim((string)$inputItem['apply_product_name']) !== ''
                    ? $inputItem['apply_product_name']
                    : $fallbackProductName,
                self::SUBMIT_APPLY_PRODUCT_NAME_MAX,
                '本次发货产品名称'
            );
            // 规格型号允许为空（正式提交不因此拒绝）
            $applySpecModel = $this->parseOptionalSubmitBoundedText(
                array_key_exists('apply_spec_model', $inputItem) ? $inputItem['apply_spec_model'] : $fallbackSpec,
                self::SUBMIT_APPLY_SPEC_MODEL_MAX,
                '本次发货规格型号'
            );

            $factoryId = $this->parseOptionalFactoryId($inputItem['factory_id'] ?? null);
            if ($factoryId === null) {
                throw new Exception('正式提交必须为每条发货行选择有效厂家。');
            }
            $factoryName = null;
            if ($assertFactory) {
                $factory = $this->assertFactoryValid($factoryId);
                $factoryName = $factory['factory_name'];
            }

            // 正式价必填：NULL/空/空白非法；0.00 合法；不自动采用建议价
            $applyUnitPrice = $this->parseRequiredSubmitMoney(
                array_key_exists('apply_unit_price', $inputItem) ? $inputItem['apply_unit_price'] : null,
                '本次销售单价必填且格式不正确（允许 0.00，不允许空值）。'
            );
            $applyPurchaseUnitPrice = $this->parseRequiredSubmitMoney(
                array_key_exists('apply_purchase_unit_price', $inputItem) ? $inputItem['apply_purchase_unit_price'] : null,
                '本次采购单价必填且格式不正确（允许 0.00，不允许空值）。'
            );

            $itemRemark = $this->parseOptionalSubmitBoundedText(
                $inputItem['remark'] ?? null,
                self::SUBMIT_ITEM_REMARK_MAX,
                '行备注'
            );

            $lines[] = [
                'order_item_id' => $orderItemId,
                'apply_product_name' => $applyProductName,
                'apply_spec_model' => $applySpecModel,
                'apply_qty' => $applyQty,
                'factory_id' => $factoryId,
                'factory_name' => $factoryName,
                'apply_unit_price' => $applyUnitPrice,
                'apply_purchase_unit_price' => $applyPurchaseUnitPrice,
                'remark' => $itemRemark,
            ];
        }

        if (empty($lines)) {
            throw new Exception('至少需要一条正整数数量的发货行。');
        }

        if ($orderItemMap !== null && $appliedQtyMap !== null) {
            foreach ($qtySumByOrderItem as $orderItemId => $sumQty) {
                $orderItem = $orderItemMap[$orderItemId];
                $productName = trim((string)($orderItem['product_name'] ?? ''));
                if ($productName === '') {
                    $productName = '未命名产品';
                }
                $orderQty = (int)($orderItem['qty'] ?? 0);
                $appliedQty = (int)($appliedQtyMap[$orderItemId] ?? 0);
                $remainingQty = $orderQty - $appliedQty;
                if ($remainingQty < 0) {
                    $remainingQty = 0;
                }
                if ($sumQty > $remainingQty) {
                    throw new Exception(
                        '产品【' . $productName . '】剩余可申请数量不足（本次合计 '
                        . $sumQty . '，剩余 ' . $remainingQty . '）。'
                    );
                }
            }
        }

        return [
            'applicant_id' => $applicantId,
            'order_id' => $orderId,
            'apply_id' => $applyId,
            'header' => [
                'receiver_name' => $receiverName,
                'receiver_phone' => $receiverPhone,
                'receiver_address' => $receiverAddress,
                'shipping_date' => $shippingDate,
                'shipping_method' => $shippingMethod,
                'shipping_remark' => $shippingRemark,
                'order_amount_confirmed' => $orderAmountConfirmed ? 1 : 0,
                'confirmed_order_amount' => $confirmedOrderAmount,
                'shipping_date_history_confirmed' => $historyConfirmed ? 1 : 0,
            ],
            'lines' => $lines,
        ];
    }

    /**
     * 正式提交业务载荷规范化（字段集合/顺序固定；用于 SHA-256）
     *
     * 不含：当前时间、自增 ID、申请编号、审批配置、经理启用状态、
     * auth_version、审批任务 ID、submit_token、建议价。
     * 发货行按业务键稳定排序；完全相同的行不去重。
     * UI 行顺序本身无独立业务含义（apply_line_no 由服务端按输入序生成，不纳入哈希）。
     *
     * @param array $validated validateSubmitPayload 的返回值，或等价结构
     * @return array
     * @throws Exception
     */
    public function normalizeSubmitPayload(array $validated)
    {
        $applicantId = (int)($validated['applicant_id'] ?? 0);
        $orderId = (int)($validated['order_id'] ?? 0);
        $applyId = (int)($validated['apply_id'] ?? 0);
        if ($applicantId <= 0 || $orderId <= 0) {
            throw new Exception('规范化载荷缺少有效的申请人或订单标识。');
        }
        if ($applyId < 0) {
            throw new Exception('规范化载荷申请标识无效。');
        }

        $header = isset($validated['header']) && is_array($validated['header'])
            ? $validated['header']
            : [];
        $lines = isset($validated['lines']) && is_array($validated['lines'])
            ? $validated['lines']
            : [];
        if (empty($lines)) {
            throw new Exception('规范化载荷缺少有效发货行。');
        }

        $normalizedLines = [];
        foreach ($lines as $line) {
            if (!is_array($line)) {
                throw new Exception('规范化载荷发货行格式不正确。');
            }
            $orderItemId = (int)($line['order_item_id'] ?? 0);
            $factoryId = (int)($line['factory_id'] ?? 0);
            $applyQty = (int)($line['apply_qty'] ?? 0);
            if ($orderItemId <= 0 || $factoryId <= 0 || $applyQty <= 0) {
                throw new Exception('规范化载荷发货行关键字段无效。');
            }

            $sale = $this->parseRequiredSubmitMoney(
                array_key_exists('apply_unit_price', $line) ? $line['apply_unit_price'] : null,
                '规范化销售单价格式不正确。'
            );
            $purchase = $this->parseRequiredSubmitMoney(
                array_key_exists('apply_purchase_unit_price', $line) ? $line['apply_purchase_unit_price'] : null,
                '规范化采购单价格式不正确。'
            );

            $normalizedLines[] = [
                'order_item_id' => $orderItemId,
                'apply_product_name' => $this->canonicalizeUtf8Text((string)($line['apply_product_name'] ?? '')),
                'apply_spec_model' => $this->canonicalizeUtf8Text((string)($line['apply_spec_model'] ?? '')),
                'apply_qty' => $applyQty,
                'factory_id' => $factoryId,
                'apply_unit_price' => $sale,
                'apply_purchase_unit_price' => $purchase,
                'remark' => $this->canonicalizeUtf8Text((string)($line['remark'] ?? '')),
            ];
        }

        $this->sortSubmitPayloadLines($normalizedLines);

        $confirmedAmount = null;
        if (array_key_exists('confirmed_order_amount', $header) && $header['confirmed_order_amount'] !== null
            && $header['confirmed_order_amount'] !== '') {
            $confirmedAmount = $this->parseRequiredSubmitMoney(
                $header['confirmed_order_amount'],
                '规范化确认订单金额格式不正确。'
            );
        }

        // 固定键顺序；新建 apply_id 使用 0 作为空标记
        return [
            'hash_schema_version' => self::SUBMIT_PAYLOAD_HASH_SCHEMA_VERSION,
            'applicant_id' => $applicantId,
            'order_id' => $orderId,
            'apply_id' => $applyId > 0 ? $applyId : 0,
            'receiver_name' => $this->canonicalizeUtf8Text((string)($header['receiver_name'] ?? '')),
            'receiver_phone' => $this->canonicalizeUtf8Text((string)($header['receiver_phone'] ?? '')),
            'receiver_address' => $this->canonicalizeUtf8Text((string)($header['receiver_address'] ?? '')),
            'shipping_date' => (string)($header['shipping_date'] ?? ''),
            'shipping_method' => $this->canonicalizeUtf8Text((string)($header['shipping_method'] ?? '')),
            'shipping_remark' => $this->canonicalizeUtf8Text((string)($header['shipping_remark'] ?? '')),
            'order_amount_confirmed' => ((int)($header['order_amount_confirmed'] ?? 0) === 1) ? 1 : 0,
            'confirmed_order_amount' => $confirmedAmount,
            'shipping_date_history_confirmed' => ((int)($header['shipping_date_history_confirmed'] ?? 0) === 1) ? 1 : 0,
            'items' => $normalizedLines,
        ];
    }

    /**
     * 将规范化载荷编码为稳定 UTF-8 JSON，并计算小写 SHA-256（64 hex）
     *
     * 未来 M4-1B-3 幂等：写入 crm_shipping_apply.submit_payload_hash，
     * 与 submit_token + applicant_id 联合判定重试；本轮不写库。
     *
     * @param array $normalized normalizeSubmitPayload 的返回值
     * @return array{hash:string,canonical_json:string,normalized:array}
     * @throws Exception
     */
    public function computeSubmitPayloadHash(array $normalized)
    {
        $canonicalJson = $this->encodeSubmitPayloadCanonicalJson($normalized);
        $hash = hash('sha256', $canonicalJson);
        if (!is_string($hash) || strlen($hash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $hash)) {
            throw new Exception('正式提交载荷哈希计算失败。');
        }

        return [
            'hash' => $hash,
            'canonical_json' => $canonicalJson,
            'normalized' => $normalized,
        ];
    }

    /**
     * 稳定 JSON 编码（UTF-8；失败必须报错）
     *
     * @param array $normalized
     * @return string
     * @throws Exception
     */
    public function encodeSubmitPayloadCanonicalJson(array $normalized)
    {
        $flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;
        if (defined('JSON_INVALID_UTF8_ERROR')) {
            // PHP 7.2+：非法 UTF-8 直接失败，避免静默替换导致哈希漂移
            $flags |= JSON_INVALID_UTF8_ERROR;
        }

        $json = json_encode($normalized, $flags);
        if ($json === false) {
            $err = function_exists('json_last_error_msg') ? json_last_error_msg() : 'unknown';
            throw new Exception('正式提交载荷 JSON 编码失败：' . $err);
        }
        if (!mb_check_encoding($json, 'UTF-8')) {
            throw new Exception('正式提交载荷 JSON 不是合法 UTF-8。');
        }

        return $json;
    }

    /**
     * 正式提交事务内：锁定原成交订单（须已在事务中）
     *
     * 锁序第 1 步；与 saveDraft / 超管授权一致：先订单。
     *
     * @param int $orderId
     * @return array
     * @throws Exception
     */
    public function lockOrderForShippingSubmit($orderId)
    {
        $orderId = (int)$orderId;
        if ($orderId <= 0) {
            throw new Exception('订单参数无效。');
        }

        $order = Db::name('crm_client_order')
            ->where('id', $orderId)
            ->lock(true)
            ->find();
        if (empty($order)) {
            throw new Exception('订单不存在。');
        }

        return $order;
    }

    /**
     * 正式提交事务内：按固定 ID 升序锁定原订单产品行（须已在事务中且已锁订单）
     *
     * 锁序第 2 步。
     * - $orderItemIds 为空：锁定该订单全部产品行（与 saveDraft 一致）
     * - 非空：去重后按 id ASC 逐行锁定，避免多行并发死锁
     *
     * @param int $orderId
     * @param int[] $orderItemIds
     * @return array<int,array> order_item_id => row
     * @throws Exception
     */
    public function lockOrderItemsForShippingSubmit($orderId, array $orderItemIds = [])
    {
        $orderId = (int)$orderId;
        if ($orderId <= 0) {
            throw new Exception('订单参数无效。');
        }

        $ids = [];
        foreach ($orderItemIds as $id) {
            $id = (int)$id;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }
        $ids = array_values($ids);
        sort($ids, SORT_NUMERIC);

        if (empty($ids)) {
            $rows = Db::name('crm_order_item')
                ->where('order_id', $orderId)
                ->order('id', 'asc')
                ->lock(true)
                ->select();
        } else {
            $rows = [];
            foreach ($ids as $itemId) {
                $row = Db::name('crm_order_item')
                    ->where('id', $itemId)
                    ->where('order_id', $orderId)
                    ->lock(true)
                    ->find();
                if (empty($row)) {
                    throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
                }
                $rows[] = $row;
            }
        }

        $map = [];
        foreach ((array)$rows as $row) {
            $map[(int)($row['id'] ?? 0)] = $row;
        }

        return $map;
    }

    /**
     * 正式提交数量占用「当前读」（须在事务内、已获取必要订单锁之后调用）
     *
     * 不可复用 calcAppliedQtyMap()：普通 SELECT 在 MySQL5.7 REPEATABLE READ 下可能读到旧快照。
     * 本方法：
     * 1) 按明细主键升序 SELECT ... FOR UPDATE（锁定读=当前读）
     * 2) 过滤占量状态 10/30/50、软删除、排除自身申请
     * 3) 在 PHP 侧按 order_item_id 聚合（避免对聚合 SQL 机械加 FOR UPDATE）
     *
     * 占量规则不变：0/20/40/60 不占量；草稿不占量。
     *
     * @param int[] $orderItemIds
     * @param int $excludeApplyId 当前草稿/申请 ID；新建传 0
     * @return array<int,int>
     * @throws Exception
     */
    public function calcAppliedQtyMapForSubmit(array $orderItemIds, $excludeApplyId = 0)
    {
        $orderItemIds = array_values(array_unique(array_filter(array_map('intval', $orderItemIds), function ($id) {
            return $id > 0;
        })));
        sort($orderItemIds, SORT_NUMERIC);

        $map = [];
        foreach ($orderItemIds as $oid) {
            $map[$oid] = 0;
        }
        if (empty($orderItemIds)) {
            return $map;
        }

        $excludeApplyId = (int)$excludeApplyId;

        $query = Db::name('crm_shipping_apply_item')->alias('sai')
            ->join('crm_shipping_apply sa', 'sa.id = sai.shipping_apply_id')
            ->whereIn('sai.order_item_id', $orderItemIds)
            ->whereIn('sa.status', $this->occupiedStatuses)
            ->where('sa.is_deleted', 0);

        if ($excludeApplyId > 0) {
            $query->where('sa.id', '<>', $excludeApplyId);
        }

        // 按明细 id 升序锁定，降低与其它事务交叉锁顺序风险；FOR UPDATE → 当前读
        $rows = $query
            ->field('sai.id AS item_row_id, sai.order_item_id, sai.apply_qty, sa.id AS apply_id, sa.status, sa.is_deleted')
            ->order('sai.id', 'asc')
            ->lock(true)
            ->select();

        foreach ((array)$rows as $row) {
            $oid = (int)($row['order_item_id'] ?? 0);
            if ($oid <= 0 || !array_key_exists($oid, $map)) {
                continue;
            }
            // 双保险：状态/软删已在 SQL 过滤
            if ((int)($row['is_deleted'] ?? 0) !== 0) {
                continue;
            }
            if (!in_array((int)($row['status'] ?? -1), $this->occupiedStatuses, true)) {
                continue;
            }
            if ($excludeApplyId > 0 && (int)($row['apply_id'] ?? 0) === $excludeApplyId) {
                continue;
            }
            $qty = (int)($row['apply_qty'] ?? 0);
            if ($qty > 0) {
                $map[$oid] += $qty;
            }
        }

        return $map;
    }

    /**
     * 纯逻辑：按占用 map 校验本次合计是否超剩余（可供无 DB 单测）
     *
     * @param array<int,int> $orderQtyMap order_item_id => 订单数量
     * @param array<int,int> $appliedQtyMap order_item_id => 已占用
     * @param array<int,int> $requestQtySum order_item_id => 本次合计
     * @return true
     * @throws Exception
     */
    public function assertSubmitQtyNotExceedRemaining(array $orderQtyMap, array $appliedQtyMap, array $requestQtySum)
    {
        foreach ($requestQtySum as $orderItemId => $sumQty) {
            $orderItemId = (int)$orderItemId;
            $sumQty = (int)$sumQty;
            $orderQty = (int)($orderQtyMap[$orderItemId] ?? 0);
            $appliedQty = (int)($appliedQtyMap[$orderItemId] ?? 0);
            $remaining = $orderQty - $appliedQty;
            if ($remaining < 0) {
                $remaining = 0;
            }
            if ($sumQty > $remaining) {
                throw new Exception(
                    '产品行#' . $orderItemId . '剩余可申请数量不足（本次合计 '
                    . $sumQty . '，剩余 ' . $remaining . '）。'
                );
            }
        }
        return true;
    }

    /**
     * 正式提交必填金额：拒绝 NULL/空/空白；允许 0.00；两位小数 BCMath
     *
     * @param mixed $raw
     * @param string $errorMsg
     * @return string
     * @throws Exception
     */
    public function parseRequiredSubmitMoney($raw, $errorMsg)
    {
        if ($raw === null) {
            throw new Exception($errorMsg);
        }
        if (is_string($raw) && trim($raw) === '') {
            throw new Exception($errorMsg);
        }
        // 复用草稿可选金额解析内核，但禁止把空转成 null
        $parsed = $this->parseOptionalMoney($raw, $errorMsg);
        if ($parsed === null) {
            throw new Exception($errorMsg);
        }
        return $parsed;
    }

    /**
     * @param mixed $raw
     * @param bool $historyConfirmed
     * @return string Y-m-d
     * @throws Exception
     */
    protected function parseRequiredSubmitDate($raw, $historyConfirmed)
    {
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw)) {
            throw new Exception('预计发货日期不能为空。');
        }
        $raw = trim((string)$raw);
        if ($raw === '') {
            throw new Exception('预计发货日期不能为空。');
        }
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $raw)) {
            throw new Exception('预计发货日期格式不正确。');
        }

        $tz = new \DateTimeZone(self::SHIPPING_BUSINESS_TIMEZONE);
        // ! 前缀重置时分秒，避免残留时间影响；溢出日期 format 对不上即可拒绝
        $dt = \DateTime::createFromFormat('!Y-m-d', $raw, $tz);
        $errors = \DateTime::getLastErrors();
        $hasDtErrors = is_array($errors)
            && (((int)($errors['warning_count'] ?? 0) > 0) || ((int)($errors['error_count'] ?? 0) > 0));
        if ($dt === false || $dt->format('Y-m-d') !== $raw || $hasDtErrors) {
            throw new Exception('预计发货日期不是有效日历日期。');
        }

        $today = new \DateTime('now', $tz);
        $today->setTime(0, 0, 0);
        $dt->setTime(0, 0, 0);
        if ($dt < $today && !$historyConfirmed) {
            throw new Exception('预计发货日期早于今天，请确认历史发货日期后再提交。');
        }

        return $raw;
    }

    /**
     * @param mixed $raw
     * @param int $maxLen
     * @param string $fieldLabel
     * @return string
     * @throws Exception
     */
    protected function parseRequiredSubmitText($raw, $maxLen, $fieldLabel)
    {
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw)) {
            throw new Exception($fieldLabel . '不能为空。');
        }
        $text = trim((string)$raw);
        if ($text === '') {
            throw new Exception($fieldLabel . '不能为空。');
        }
        if (mb_strlen($text, 'UTF-8') > (int)$maxLen) {
            throw new Exception($fieldLabel . '长度不能超过' . (int)$maxLen . '个字符。');
        }
        return $text;
    }

    /**
     * 可选文本：空→空字符串；仍受长度限制
     *
     * @param mixed $raw
     * @param int $maxLen
     * @param string $fieldLabel
     * @return string
     * @throws Exception
     */
    protected function parseOptionalSubmitBoundedText($raw, $maxLen, $fieldLabel)
    {
        if ($raw === null) {
            return '';
        }
        if (is_array($raw) || is_object($raw) || is_bool($raw)) {
            throw new Exception($fieldLabel . '格式不正确。');
        }
        $text = trim((string)$raw);
        if (mb_strlen($text, 'UTF-8') > (int)$maxLen) {
            throw new Exception($fieldLabel . '长度不能超过' . (int)$maxLen . '个字符。');
        }
        return $text;
    }

    /**
     * 收货电话：允许手机号与固定电话；拒绝空值与明显非法内容
     *
     * @param mixed $raw
     * @return string
     * @throws Exception
     */
    protected function parseRequiredSubmitReceiverPhone($raw)
    {
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw)) {
            throw new Exception('收货电话不能为空。');
        }
        $phone = trim((string)$raw);
        if ($phone === '') {
            throw new Exception('收货电话不能为空。');
        }
        if (mb_strlen($phone, 'UTF-8') > self::SUBMIT_RECEIVER_PHONE_MAX) {
            throw new Exception('收货电话长度不能超过' . self::SUBMIT_RECEIVER_PHONE_MAX . '个字符。');
        }
        // 允许数字及常见固话分隔符；中文括号兼容
        if (!preg_match('/^[0-9+\-\s()（）#*]+$/u', $phone)) {
            throw new Exception('收货电话格式不正确。');
        }
        $digits = preg_replace('/\D+/', '', $phone);
        $digitLen = strlen((string)$digits);
        if ($digitLen < 7 || $digitLen > 15) {
            throw new Exception('收货电话格式不正确。');
        }
        // 仅当 11 位且以 1 开头时按手机号校验；0 开头的 11 位按固话数字接受（如 01012345678）
        if ($digitLen === 11 && isset($digits[0]) && $digits[0] === '1'
            && !preg_match('/^1[3-9]\d{9}$/', $digits)
        ) {
            throw new Exception('收货电话格式不正确。');
        }

        return $phone;
    }

    /**
     * @param string $text
     * @return string
     * @throws Exception
     */
    protected function canonicalizeUtf8Text($text)
    {
        $text = (string)$text;
        if ($text !== '' && !mb_check_encoding($text, 'UTF-8')) {
            throw new Exception('正式提交文本不是合法 UTF-8。');
        }
        return $text;
    }

    /**
     * 稳定排序：不改变重复行出现次数；不因请求顺序改变哈希
     *
     * @param array $lines
     * @return void
     */
    protected function sortSubmitPayloadLines(array &$lines)
    {
        usort($lines, function ($a, $b) {
            $keys = [
                'order_item_id',
                'factory_id',
                'apply_spec_model',
                'apply_qty',
                'apply_unit_price',
                'apply_purchase_unit_price',
                'apply_product_name',
                'remark',
            ];
            foreach ($keys as $key) {
                $va = $a[$key] ?? '';
                $vb = $b[$key] ?? '';
                if (in_array($key, ['order_item_id', 'factory_id', 'apply_qty'], true)) {
                    $ia = (int)$va;
                    $ib = (int)$vb;
                    if ($ia !== $ib) {
                        return $ia < $ib ? -1 : 1;
                    }
                    continue;
                }
                $sa = (string)$va;
                $sb = (string)$vb;
                if ($sa !== $sb) {
                    return strcmp($sa, $sb);
                }
            }
            return 0;
        });
    }

    // =========================
    // M4-1B-2A 事务与审批人解析（只读核验 / 快照准备；禁止写提交态）
    // =========================

    /** 审批阈值对齐 crm_shipping_apply.approval_threshold DECIMAL(10,2) */
    const APPROVAL_THRESHOLD_ABS_MAX = '99999999.99';

    /**
     * M4-1B-2B 正式提交事务调用约定（只读文档契约；本方法不开启事务、不写库）
     *
     * @return array
     */
    public function getSubmitTransactionCallOrderContract()
    {
        return [
            'outside_tx' => [
                'assertApplicantActiveForSubmit',
                'parseStrictSubmitToken',
                'parseSubmitBusinessIntentForHash',
                'normalizeSubmitPayload',
                'computeSubmitPayloadHash',
                'optional_readonly_token_preview',
                'optional_preview_draft_order_id',
            ],
            'inside_tx' => [
                '1_findApplyBySubmitTokenForUpdate_and_resolveIdempotent',
                '2_lockOrderForShippingSubmit',
                '3_lockOrderItemsForShippingSubmit',
                '4_lock_draft_apply_if_any',
                '5_lock_apply_items_if_any',
                '6_lock_pm_substitute_rows',
                '7_calcAppliedQtyMapForSubmit',
                '8_validateSubmitPayloadForCommit',
                '9_assertDraftOrderAmountConfirmedForSubmit',
                '10_resolveSubmitManagers',
                '11_buildApprovalConfigSnapshot',
                '12_confirm_intent_hash_unchanged',
                '13_write_apply_header_and_lines',
                '14_freeze_used_pm_substitutes',
                '15_invalidate_restored_and_removed_auths',
                '16_create_pm_approval_tasks',
                '17_precreate_inactive_director_task_if_route2',
                '18_write_submit_and_finance_anomaly_audit',
                '19_commit',
            ],
            'deferred_to_later_milestones' => [
                'activate_director_task',
                'approve_or_reject',
                'http_controller_submit_entry',
            ],
            'forbidden' => [
                'nested_Db_startTrans_inside_resolvers',
                'Db_commit_inside_resolvers',
                'trust_frontend_approver_ids',
                'multi_transaction_submit_assembly',
                'ajax_save_draft_then_ajax_submit_as_two_tx',
                'super_admin_submit_on_behalf',
            ],
        ];
    }

    /**
     * 查询管理员行（正式提交用）。数据库异常向上抛出，不得伪装为账号不存在。
     *
     * @param int $adminId
     * @return array|null 确定不存在时返回 null
     * @throws \Throwable
     */
    protected function lookupAdminRowForSubmit($adminId)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            return null;
        }
        $row = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,username,is_open,group_id,parent_id')
            ->find();
        if (empty($row)) {
            return null;
        }
        return $row;
    }

    /**
     * 解析审批阈值金额（非负；两位小数；拒科学计数法/浮点/负数）
     *
     * @param mixed $raw
     * @return string
     * @throws Exception
     */
    public function parseApprovalThresholdMoney($raw)
    {
        $err = '审批金额阈值配置非法，拒绝继续正式提交准备。';
        if ($raw === null || is_bool($raw) || is_array($raw) || is_object($raw) || is_float($raw)) {
            throw new Exception($err);
        }
        if (is_int($raw)) {
            if ($raw < 0) {
                throw new Exception($err);
            }
            $raw = (string)$raw;
        } else {
            $raw = trim((string)$raw);
        }
        if ($raw === '' || !preg_match('/^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/', $raw)) {
            throw new Exception($err);
        }
        if (bccomp($raw, '0', 2) < 0) {
            throw new Exception($err);
        }
        if (bccomp($raw, self::APPROVAL_THRESHOLD_ABS_MAX, 2) > 0) {
            throw new Exception($err);
        }
        return $this->formatMoneyHalfUp($raw);
    }

    /**
     * 单行原产品经理正式审批资格分类（仅依据 crm_order_item.manager_id）
     *
     * @param mixed $managerIdRaw
     * @param string $productName
     * @return array
     * @throws \Throwable DB 异常原样上抛
     */
    public function classifyOrderItemManagerForSubmit($managerIdRaw, $productName = '')
    {
        // 复用草稿分类语义（含 is_open）；正式路径对其失败结果整单拒绝/走替代，而不是宽容保存
        $info = $this->resolveProductManagerForDraft($managerIdRaw, $productName);
        return [
            'manager_status' => (string)($info['manager_status'] ?? 'invalid_id'),
            'manager_invalid' => !empty($info['manager_invalid']),
            'original_manager_id' => $info['admin_id'] !== null ? (int)$info['admin_id'] : null,
            'original_manager_name' => (string)($info['username'] ?? ''),
            'manager_invalid_reason' => (string)($info['manager_invalid_reason'] ?? ''),
            'product_name' => trim((string)$productName) !== '' ? trim((string)$productName) : '未命名产品',
        ];
    }

    /**
     * 解析单行正式审批人：正常用原经理；异常须合法替代授权；新建异常直接拒绝。
     *
     * @param array $context [
     *   apply_id:int,
     *   order_item_id:int,
     *   order_item:array,
     *   auth_row:?array 锁内授权行（无则 null；不得用事务外缓存冒充）,
     * ]
     * @return array
     * @throws Exception
     */
    public function resolveApproverManagerForSubmit(array $context)
    {
        $applyId = (int)($context['apply_id'] ?? 0);
        if ($applyId < 0) {
            throw new Exception('草稿ID无效。');
        }
        $orderItemId = $this->parseStrictPositiveInt(
            $context['order_item_id'] ?? null,
            '原订单产品行无效。'
        );
        if (!isset($context['order_item']) || !is_array($context['order_item']) || empty($context['order_item'])) {
            throw new Exception('正式提交缺少锁定的原订单产品行，拒绝继续。');
        }
        $orderItem = $context['order_item'];
        if ((int)($orderItem['id'] ?? 0) !== $orderItemId) {
            throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
        }

        $productName = trim((string)($orderItem['product_name'] ?? ''));
        if ($productName === '') {
            $productName = '未命名产品';
        }
        $classified = $this->classifyOrderItemManagerForSubmit($orderItem['manager_id'] ?? null, $productName);
        $authRow = array_key_exists('auth_row', $context) ? $context['auth_row'] : null;
        if ($authRow !== null && !is_array($authRow)) {
            throw new Exception('替代授权锁内数据非法，拒绝继续。');
        }

        // 原经理恢复/正常：必须用原经理；旧 status=1 授权仅列入待失效，本轮不 UPDATE
        if (empty($classified['manager_invalid']) && (string)$classified['manager_status'] === 'valid') {
            $invalidate = null;
            if (is_array($authRow) && (int)($authRow['status'] ?? 0) === 1) {
                // P1：生成待失效清单前必须完整校验归属/ID/版本；apply/order_item 取自可信上下文
                $trusted = $this->assertPmSubstituteAuthOwnershipForSubmit($authRow, $applyId, $orderItemId);
                $invalidate = [
                    'auth_id' => $trusted['auth_id'],
                    'apply_id' => $trusted['apply_id'],
                    'order_item_id' => $trusted['order_item_id'],
                    'expected_auth_version' => $trusted['auth_version'],
                    'expected_status' => 1,
                    'target_status' => 0,
                    'invalidate_type' => 'original_restored',
                    'reason' => '原产品经理已恢复正常，正式提交时失效旧替代授权',
                ];
            }
            return [
                'order_item_id' => $orderItemId,
                'resolved_manager_id' => (int)$classified['original_manager_id'],
                'resolved_manager_name' => (string)$classified['original_manager_name'],
                'manager_source' => 'original',
                'manager_status' => 'valid',
                'original_manager_id' => (int)$classified['original_manager_id'],
                'original_manager_name' => (string)$classified['original_manager_name'],
                'auth_id' => null,
                'auth_version' => null,
                'freeze_authorization' => null,
                'invalidate_authorization' => $invalidate,
            ];
        }

        // 异常原经理
        if ($applyId <= 0) {
            throw new Exception('请先保存草稿，并联系超级管理员指定替代产品经理。');
        }

        $usable = $this->assertPmSubstituteUsableForSubmit([
            'apply_id' => $applyId,
            'order_item_id' => $orderItemId,
            'order_item' => $orderItem,
            'classified' => $classified,
            'auth_row' => $authRow,
        ]);
        $freeze = $this->assertPmSubstituteReadyToFreeze($usable);

        return [
            'order_item_id' => $orderItemId,
            'resolved_manager_id' => (int)$usable['substitute_manager_id'],
            'resolved_manager_name' => (string)$usable['substitute_manager_name'],
            'manager_source' => 'substitute',
            'manager_status' => (string)$classified['manager_status'],
            'original_manager_id' => $classified['original_manager_id'],
            'original_manager_name' => (string)$classified['original_manager_name'],
            'auth_id' => (int)$usable['auth_id'],
            'auth_version' => (int)$usable['auth_version'],
            'freeze_authorization' => $freeze,
            'invalidate_authorization' => null,
        ];
    }

    /**
     * 异常经理替代授权严格核验（只读；依赖锁内 auth_row）
     *
     * @param array $context
     * @return array
     * @throws Exception
     */
    public function assertPmSubstituteUsableForSubmit(array $context)
    {
        $applyId = $this->parseStrictPositiveInt($context['apply_id'] ?? null, '草稿ID无效。');
        $orderItemId = $this->parseStrictPositiveInt($context['order_item_id'] ?? null, '原订单产品行无效。');
        $classified = isset($context['classified']) && is_array($context['classified'])
            ? $context['classified']
            : null;
        if ($classified === null) {
            throw new Exception('正式提交缺少原经理状态分类结果，拒绝继续。');
        }
        $managerStatus = (string)($classified['manager_status'] ?? '');
        if (!in_array($managerStatus, ['missing', 'disabled', 'invalid_id'], true)) {
            throw new Exception('原产品经理当前状态不需要替代授权。');
        }

        $authRow = $context['auth_row'] ?? null;
        if ($authRow === null || !is_array($authRow) || empty($authRow)) {
            throw new Exception('请先保存草稿，并联系超级管理员指定替代产品经理。');
        }

        // 归属 / 授权 ID / 版本：字段缺失即异常，不得用当前上下文自动补齐
        $trusted = $this->assertPmSubstituteAuthOwnershipForSubmit($authRow, $applyId, $orderItemId);
        $authId = $trusted['auth_id'];
        $authVersion = $trusted['auth_version'];

        $status = (int)($authRow['status'] ?? -1);
        if ($status === 0) {
            throw new Exception('替代授权已撤销或失效，请联系超级管理员重新指定。');
        }
        if ($status === 2) {
            throw new Exception('替代授权已冻结，不能作为首次正式提交的新授权。');
        }
        if ($status !== 1) {
            throw new Exception('替代授权状态无效，拒绝继续。');
        }

        // P1：invalid_reason 必须与当前原经理异常状态一致（复用 mapManagerStatusToInvalidReason）
        if (!array_key_exists('invalid_reason', $authRow)) {
            throw new Exception('替代授权缺少异常原因，拒绝继续。');
        }
        $authInvalidReason = $this->parseStrictAuthInvalidReason($authRow['invalid_reason']);
        $expectedInvalidReason = (int)$this->mapManagerStatusToInvalidReason($managerStatus);
        if ($authInvalidReason !== $expectedInvalidReason) {
            throw new Exception('替代授权异常原因与当前原经理状态不一致，拒绝继续。');
        }

        // original_manager 匹配
        // 边界：表未保存原始非法 manager 字符串；invalid_id 间互变现有结构无法完整检测
        $authOriginalRaw = $authRow['original_manager_id'] ?? null;
        if ($managerStatus === 'invalid_id') {
            // 授权侧 original_manager_id 应为 NULL；拒绝 0 / 正整数冒充
            if ($authOriginalRaw !== null && $authOriginalRaw !== '') {
                throw new Exception('替代授权原经理信息不匹配（invalid_id 授权原经理应为空），拒绝继续。');
            }
            if ($authOriginalRaw === 0 || $authOriginalRaw === '0') {
                throw new Exception('替代授权原经理信息不匹配（invalid_id 授权原经理应为空），拒绝继续。');
            }
        } else {
            $expectedOriginalId = (int)($classified['original_manager_id'] ?? 0);
            if ($expectedOriginalId <= 0) {
                throw new Exception('原产品经理ID异常，拒绝继续。');
            }
            if ($authOriginalRaw === null || $authOriginalRaw === '') {
                throw new Exception('替代授权原经理信息不匹配，拒绝继续。');
            }
            if ((int)$authOriginalRaw !== $expectedOriginalId) {
                throw new Exception('替代授权原经理与当前订单原经理不一致，拒绝继续。');
            }
        }

        $substituteId = (int)($authRow['substitute_manager_id'] ?? 0);
        // 复用既有候选人资格判断，不得另写一套
        $candidate = $this->assertValidPmSubstituteCandidate($substituteId);

        return [
            'auth_id' => $authId,
            'apply_id' => $applyId,
            'order_item_id' => $orderItemId,
            'auth_version' => $authVersion,
            'status' => 1,
            'manager_status' => $managerStatus,
            'original_manager_id' => $classified['original_manager_id'],
            'substitute_manager_id' => (int)$candidate['admin_id'],
            'substitute_manager_name' => (string)$candidate['username'],
            'auth_row_locked' => true,
        ];
    }

    /**
     * 授权冻结前只读核验：仅返回冻结所需可信参数，不执行 UPDATE / 不写审计。
     * 调用成功 ≠ 授权已冻结。
     *
     * @param array $usable assertPmSubstituteUsableForSubmit 的返回
     * @return array
     * @throws Exception
     */
    public function assertPmSubstituteReadyToFreeze(array $usable)
    {
        $authId = (int)($usable['auth_id'] ?? 0);
        $applyId = (int)($usable['apply_id'] ?? 0);
        $orderItemId = (int)($usable['order_item_id'] ?? 0);
        $authVersion = (int)($usable['auth_version'] ?? 0);
        $substituteId = (int)($usable['substitute_manager_id'] ?? 0);
        if ($authId <= 0 || $applyId <= 0 || $orderItemId <= 0 || $authVersion <= 0 || $substituteId <= 0) {
            throw new Exception('替代授权冻结准备数据不完整，拒绝继续。');
        }
        if ((int)($usable['status'] ?? 0) !== 1) {
            throw new Exception('仅有效替代授权可准备冻结。');
        }
        if (empty($usable['auth_row_locked'])) {
            throw new Exception('替代授权行未在正式提交事务中锁定，拒绝冻结准备。');
        }
        $managerStatus = (string)($usable['manager_status'] ?? '');
        if (!in_array($managerStatus, ['missing', 'disabled', 'invalid_id'], true)) {
            throw new Exception('原产品经理已恢复，不能冻结旧替代授权。');
        }
        // 再次确认替代人仍合格（防 TOCTOU 窗口内变化；仍不写库）
        $candidate = $this->assertValidPmSubstituteCandidate($substituteId);

        return [
            'auth_id' => $authId,
            'apply_id' => $applyId,
            'order_item_id' => $orderItemId,
            'expected_auth_version' => $authVersion,
            'expected_status' => 1,
            'target_status' => 2,
            'substitute_manager_id' => (int)$candidate['admin_id'],
            'substitute_manager_name' => (string)$candidate['username'],
            'version_bump' => true,
            'same_transaction_with_submit' => true,
            'executed' => false,
            'note' => 'ready_to_freeze_only_not_frozen',
        ];
    }

    /**
     * 按 admin_id 去重构建未来产品经理审批任务基础数据
     *
     * @param array $lineManagers resolveApproverManagerForSubmit 结果列表
     * @return array
     */
    public function buildDedupedPmApprovers(array $lineManagers)
    {
        $approverMap = [];
        foreach ($lineManagers as $line) {
            if (!is_array($line)) {
                continue;
            }
            $approverId = (int)($line['resolved_manager_id'] ?? 0);
            if ($approverId <= 0) {
                throw new Exception('存在无法解析的产品经理审批人，拒绝继续。');
            }
            $orderItemId = (int)($line['order_item_id'] ?? 0);
            if ($orderItemId <= 0) {
                throw new Exception('发货行原产品ID无效，拒绝继续。');
            }
            if (!isset($approverMap[$approverId])) {
                $approverMap[$approverId] = [
                    'approver_id' => $approverId,
                    'approver_name' => (string)($line['resolved_manager_name'] ?? ''),
                    'order_item_ids' => [],
                    'approval_type' => 1,
                    'initial_status' => 1,
                ];
            }
            if (!in_array($orderItemId, $approverMap[$approverId]['order_item_ids'], true)) {
                $approverMap[$approverId]['order_item_ids'][] = $orderItemId;
            }
        }
        $approvers = array_values($approverMap);
        usort($approvers, function ($a, $b) {
            return ((int)$a['approver_id']) - ((int)$b['approver_id']);
        });
        return $approvers;
    }

    /**
     * 正式提交：解析全部有效发货行审批经理（只返回数据，不写审批任务 / 不冻结授权）
     *
     * @param array $context [
     *   apply_id:int,
     *   lines:array 已校验有效发货行（含 order_item_id）,
     *   order_item_map:array 锁内产品行,
     *   pm_substitute_map:array order_item_id => auth_row|null,
     * ]
     * @return array
     * @throws Exception
     */
    public function resolveSubmitManagers(array $context)
    {
        $applyId = (int)($context['apply_id'] ?? 0);
        if ($applyId < 0) {
            throw new Exception('草稿ID无效。');
        }
        if (!isset($context['lines']) || !is_array($context['lines']) || empty($context['lines'])) {
            throw new Exception('至少需要一条正整数数量的发货行。');
        }
        if (!isset($context['order_item_map']) || !is_array($context['order_item_map'])) {
            throw new Exception('正式提交缺少事务内锁定的原订单产品行，拒绝继续。');
        }
        $orderItemMap = $context['order_item_map'];
        $authMap = isset($context['pm_substitute_map']) && is_array($context['pm_substitute_map'])
            ? $context['pm_substitute_map']
            : [];

        // 同原产品拆多行：每个 order_item_id 只解析一次
        $uniqueOrderItemIds = [];
        foreach ($context['lines'] as $line) {
            if (!is_array($line)) {
                continue;
            }
            $oid = $this->parseStrictPositiveInt(
                $line['order_item_id'] ?? null,
                '产品明细参数异常，请刷新页面后重新提交。'
            );
            $uniqueOrderItemIds[$oid] = $oid;
        }
        if (empty($uniqueOrderItemIds)) {
            throw new Exception('至少需要一条正整数数量的发货行。');
        }
        $ids = array_values($uniqueOrderItemIds);
        sort($ids, SORT_NUMERIC);

        $lineManagers = [];
        $freezeList = [];
        $invalidateList = [];

        foreach ($ids as $orderItemId) {
            if (!isset($orderItemMap[$orderItemId]) || !is_array($orderItemMap[$orderItemId])) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }
            $authRow = array_key_exists($orderItemId, $authMap) ? $authMap[$orderItemId] : null;
            $resolved = $this->resolveApproverManagerForSubmit([
                'apply_id' => $applyId,
                'order_item_id' => $orderItemId,
                'order_item' => $orderItemMap[$orderItemId],
                'auth_row' => $authRow,
            ]);
            $lineManagers[] = $resolved;
            if (!empty($resolved['freeze_authorization']) && is_array($resolved['freeze_authorization'])) {
                $freezeList[] = $resolved['freeze_authorization'];
            }
            if (!empty($resolved['invalidate_authorization']) && is_array($resolved['invalidate_authorization'])) {
                $invalidateList[] = $resolved['invalidate_authorization'];
            }
        }

        $approvers = $this->buildDedupedPmApprovers($lineManagers);

        return [
            'apply_id' => $applyId,
            'line_managers' => $lineManagers,
            'approvers' => $approvers,
            'freeze_authorizations' => $freezeList,
            'invalidate_authorizations' => $invalidateList,
            'writes_executed' => false,
        ];
    }

    /**
     * 动态审批配置与人员快照（只读准备；不写主表 / 不插任务 / 不写财务异常日志）
     *
     * @param array $context [
     *   order:array 锁内订单（须含 money）,
     *   applicant_id:int|string 服务端确认申请人,
     *   config_map?:array 可注入（测试）；缺省走 ShippingConfigService::getConfigMapForSubmit,
     *   raw_amount_basis?:string,
     *   admin_resolver?: callable 测试注入,
     * ]
     * @return array
     * @throws Exception
     */
    public function buildApprovalConfigSnapshot(array $context)
    {
        if (!isset($context['order']) || !is_array($context['order']) || empty($context['order'])) {
            throw new Exception('正式提交缺少事务内锁定的原订单快照，拒绝继续。');
        }
        $order = $context['order'];
        $applicantId = $this->parseStrictPositiveInt(
            $context['applicant_id'] ?? null,
            '正式提交申请人身份无效，拒绝继续。'
        );

        $moneyRaw = $order['money'] ?? null;
        if (is_float($moneyRaw) || is_bool($moneyRaw) || is_array($moneyRaw) || is_object($moneyRaw)) {
            throw new Exception('原订单金额无效，拒绝继续正式提交准备。');
        }
        $orderAmount = $this->parseOptionalMoney($moneyRaw, '原订单金额无效，拒绝继续正式提交准备。');
        if ($orderAmount === null) {
            throw new Exception('原订单金额无效，拒绝继续正式提交准备。');
        }

        if (isset($context['config_map']) && is_array($context['config_map'])) {
            $configMap = $context['config_map'];
            $rawAmountBasis = array_key_exists('raw_amount_basis', $context)
                ? trim((string)$context['raw_amount_basis'])
                : trim((string)($configMap['amount_basis'] ?? 'order_total'));
        } else {
            $configService = new ShippingConfigService();
            $loaded = $configService->getConfigMapForSubmit();
            $configMap = $loaded['map'];
            $rawAmountBasis = trim((string)$loaded['raw_amount_basis']);
        }

        if ($rawAmountBasis !== '' && $rawAmountBasis !== 'order_total') {
            throw new Exception('审批金额判断依据配置非法，拒绝继续正式提交准备。');
        }

        $threshold = $this->parseApprovalThresholdMoney($configMap['approval_threshold'] ?? null);
        // 原订单总金额 vs 阈值：< → route1；>= → route2（等于也走总监）
        $cmp = bccomp($orderAmount, $threshold, 2);
        $approvalRoute = ($cmp < 0) ? 1 : 2;

        $resolveAdmin = null;
        if (isset($context['admin_resolver']) && is_callable($context['admin_resolver'])) {
            $resolveAdmin = $context['admin_resolver'];
        } else {
            $self = $this;
            $resolveAdmin = function ($adminId) use ($self) {
                return $self->lookupAdminRowForSubmit($adminId);
            };
        }

        // —— 产品总监 ——
        $directorIdRaw = trim((string)($configMap['product_director_id'] ?? ''));
        $directorId = 0;
        $directorName = '';
        $directorValid = false;
        $directorReason = 'missing';
        if ($directorIdRaw !== '') {
            if (!preg_match('/^[1-9]\d*$/', $directorIdRaw) || bccomp($directorIdRaw, (string)PHP_INT_MAX, 0) > 0) {
                if ($approvalRoute === 2) {
                    throw new Exception('产品总监配置非法，无法准备高金额审批路线。');
                }
                $directorReason = 'invalid_id';
            } else {
                $directorId = (int)$directorIdRaw;
                try {
                    $directorRow = $resolveAdmin($directorId);
                } catch (\Throwable $e) {
                    // 不得伪装成账号不存在
                    throw $e;
                }
                if ($directorRow === null || empty($directorRow)) {
                    $directorReason = 'missing';
                    $directorValid = false;
                } else {
                    $directorName = trim((string)($directorRow['username'] ?? ''));
                    if ($directorName === '') {
                        $directorReason = 'missing';
                        $directorValid = false;
                    } elseif ((int)($directorRow['is_open'] ?? 0) !== 1) {
                        $directorReason = 'disabled';
                        $directorValid = false;
                    } else {
                        $directorValid = true;
                        $directorReason = '';
                    }
                }
            }
        }

        $directorTaskPlan = null;
        $snapshotDirectorId = null;
        $snapshotDirectorName = '';
        if ($approvalRoute === 2) {
            if (!$directorValid) {
                throw new Exception(
                    $directorReason === 'disabled'
                        ? '产品总监账号已停用，无法准备高金额审批路线。'
                        : '产品总监未配置或账号无效，无法准备高金额审批路线。'
                );
            }
            $snapshotDirectorId = $directorId;
            $snapshotDirectorName = $directorName;
            $directorTaskPlan = [
                'approval_type' => 2,
                'approver_id' => $directorId,
                'approver_name' => $directorName,
                'initial_status' => 0,
                'activate_time' => null,
                'note' => 'precreate_inactive_until_all_pm_passed',
            ];
        } else {
            // 低金额：不建总监审批任务；有效总监可作抄送快照，无效不阻断
            if ($directorValid) {
                $snapshotDirectorId = $directorId;
                $snapshotDirectorName = $directorName;
            }
        }

        // —— 销售主管（申请人 parent_id；仅抄送快照）——
        $teamLeaderId = null;
        $teamLeaderName = '';
        try {
            $applicantRow = $resolveAdmin($applicantId);
        } catch (\Throwable $e) {
            throw $e;
        }
        if (!empty($applicantRow)) {
            $parentId = (int)($applicantRow['parent_id'] ?? 0);
            if ($parentId > 0) {
                try {
                    $leaderRow = $resolveAdmin($parentId);
                } catch (\Throwable $e) {
                    throw $e;
                }
                if (!empty($leaderRow)) {
                    $ln = trim((string)($leaderRow['username'] ?? ''));
                    if ($ln !== '' && (int)($leaderRow['is_open'] ?? 0) === 1) {
                        $teamLeaderId = (int)$leaderRow['admin_id'];
                        $teamLeaderName = $ln;
                    }
                }
            }
        }

        // —— 财务抄送 ——
        $financeIdRaw = trim((string)($configMap['finance_cc_user_id'] ?? ''));
        $financeCcValid = false;
        $financeCcUserId = null;
        $financeCcUserName = '';
        $financeAnomaly = null;
        $configuredFinanceId = null;

        if ($financeIdRaw === '') {
            $financeAnomaly = [
                'reason' => 'missing',
                'configured_admin_id' => null,
                'requires_follow_up' => true,
                'audit_action' => 'submit_finance_cc_anomaly',
            ];
        } elseif (!preg_match('/^[1-9]\d*$/', $financeIdRaw) || bccomp($financeIdRaw, (string)PHP_INT_MAX, 0) > 0) {
            $financeAnomaly = [
                'reason' => 'invalid_id',
                'configured_admin_id' => null,
                'configured_raw' => $financeIdRaw,
                'requires_follow_up' => true,
                'audit_action' => 'submit_finance_cc_anomaly',
            ];
        } else {
            $configuredFinanceId = (int)$financeIdRaw;
            try {
                $financeRow = $resolveAdmin($configuredFinanceId);
            } catch (\Throwable $e) {
                throw $e;
            }
            if ($financeRow === null || empty($financeRow)) {
                $financeAnomaly = [
                    'reason' => 'missing',
                    'configured_admin_id' => $configuredFinanceId,
                    'requires_follow_up' => true,
                    'audit_action' => 'submit_finance_cc_anomaly',
                ];
            } else {
                $fn = trim((string)($financeRow['username'] ?? ''));
                if ($fn === '') {
                    $financeAnomaly = [
                        'reason' => 'missing',
                        'configured_admin_id' => $configuredFinanceId,
                        'requires_follow_up' => true,
                        'audit_action' => 'submit_finance_cc_anomaly',
                    ];
                } elseif ((int)($financeRow['is_open'] ?? 0) !== 1) {
                    $financeAnomaly = [
                        'reason' => 'disabled',
                        'configured_admin_id' => $configuredFinanceId,
                        'requires_follow_up' => true,
                        'audit_action' => 'submit_finance_cc_anomaly',
                    ];
                } else {
                    $financeCcValid = true;
                    $financeCcUserId = $configuredFinanceId;
                    $financeCcUserName = $fn;
                }
            }
        }

        return [
            'order_amount' => $orderAmount,
            'amount_basis' => 'order_total',
            'approval_threshold' => $threshold,
            'approval_route' => $approvalRoute,
            'product_director_id' => $snapshotDirectorId,
            'product_director_name' => $snapshotDirectorName,
            'product_director_valid' => $directorValid,
            'product_director_task_plan' => $directorTaskPlan,
            'team_leader_id' => $teamLeaderId,
            'team_leader_name' => $teamLeaderName,
            'finance_cc_user_id' => $financeCcUserId,
            'finance_cc_user_name' => $financeCcUserName,
            'finance_cc_valid' => $financeCcValid,
            'finance_cc_anomaly' => $financeAnomaly,
            'writes_executed' => false,
            'consistency_note' => 'config/admin 普通 SELECT；MySQL5.7 RR 下与行锁数据非同一锁集合，残留并发 TOCTOU 风险由 M4-1B-2B/联调覆盖',
        ];
    }

    // =========================
    // M4-1B-2B-2A 正式提交单事务写库
    // =========================

    /** submit_token 对齐 crm_shipping_apply.submit_token VARCHAR(64) */
    const SUBMIT_TOKEN_MAX_LEN = 64;

    /** 瞬时死锁/锁等待有限重试次数 */
    const SUBMIT_TX_TRANSIENT_MAX_RETRY = 3;

    /** 申请编号唯一冲突有限重试次数 */
    const SUBMIT_APPLY_NO_MAX_RETRY = 3;

    /**
     * Service 内部正式提交入口（单事务写库；不暴露 HTTP）
     *
     * @param array $params
     * @return array
     */
    public function submitApply(array $params = [])
    {
        $params = is_array($params) ? $params : [];

        try {
            $applicant = $this->assertApplicantActiveForSubmit();
            $applicantId = (int)$applicant['admin_id'];
            $applicantName = (string)$applicant['username'];
            $submitToken = $this->parseStrictSubmitToken(
                array_key_exists('submit_token', $params) ? $params['submit_token'] : null
            );

            // 忽略前端伪造的申请人；哈希身份仅来自 Session
            $paramsForIntent = $params;
            unset($paramsForIntent['applicant_id']);

            $intent = $this->parseSubmitBusinessIntentForHash($paramsForIntent, $applicantId);
            $normalized = $this->normalizeSubmitPayload($intent);
            $hashPack = $this->computeSubmitPayloadHash($normalized);
            $payloadHash = (string)$hashPack['hash'];

            $applyIdHint = (int)($intent['apply_id'] ?? 0);
            $orderId = (int)($intent['order_id'] ?? 0);
            if ($orderId <= 0) {
                return $this->buildSubmitFailResult('请先选择有效订单。');
            }

            // 草稿：事务外只读定位 order_id（不锁）；最终以锁内为准
            if ($applyIdHint > 0) {
                $preview = Db::name('crm_shipping_apply')
                    ->where('id', $applyIdHint)
                    ->field('id,order_id,applicant_id,status,is_deleted,submit_token')
                    ->find();
                if (!empty($preview)) {
                    $previewOrderId = (int)($preview['order_id'] ?? 0);
                    if ($previewOrderId > 0 && $previewOrderId !== $orderId) {
                        return $this->buildSubmitFailResult('草稿关联订单不匹配，禁止静默切换关联订单。');
                    }
                }
            }
        } catch (Exception $e) {
            return $this->buildSubmitFailResult($e->getMessage());
        } catch (\Throwable $e) {
            $this->logSubmitError('submitApply.precheck', $e);
            return $this->buildSubmitFailResult('正式提交失败，请稍后重试。');
        }

        $maxRetry = self::SUBMIT_TX_TRANSIENT_MAX_RETRY;
        $applyNoRetryBudget = self::SUBMIT_APPLY_NO_MAX_RETRY;

        for ($attempt = 1; $attempt <= $maxRetry; $attempt++) {
            $txStarted = false;
            $commitAttempted = false;
            try {
                $this->beginSubmitTransaction();
                $txStarted = true;

                $idempotent = $this->resolveIdempotentSubmitResult(
                    $this->findApplyBySubmitTokenForUpdate($submitToken),
                    $applicantId,
                    $payloadHash
                );
                if ($idempotent !== null) {
                    // 只读幂等快路径：COMMIT 异常同样不得重入写库
                    $commitAttempted = true;
                    $this->commitSubmitTransaction();
                    $txStarted = false;
                    return $idempotent;
                }

                $result = $this->executeSubmitApplyInTransaction([
                    'params' => $paramsForIntent,
                    'applicant_id' => $applicantId,
                    'applicant_name' => $applicantName,
                    'submit_token' => $submitToken,
                    'payload_hash' => $payloadHash,
                    'intent' => $intent,
                    'order_id' => $orderId,
                    'apply_id_hint' => $applyIdHint,
                ]);

                $commitAttempted = true;
                $this->commitSubmitTransaction();
                $txStarted = false;
                return $result;
            } catch (\Throwable $e) {
                // 统一入口：Exception / RuntimeException / PDOException / Error 等均先判 COMMIT 阶段
                $rollbackFailed = false;
                if ($txStarted) {
                    try {
                        $this->rollbackSubmitTransaction();
                    } catch (\Throwable $rollbackEx) {
                        $rollbackFailed = true;
                        $this->logSubmitError('submitApply.rollback', $rollbackEx);
                    }
                    $txStarted = false;
                }

                if ($commitAttempted) {
                    return $this->finalizeSubmitAfterCommitUncertainty(
                        $e,
                        $submitToken,
                        $applicantId,
                        $payloadHash
                    );
                }

                // rollback 失败且事务状态不明：失败关闭，禁止继续写事务重试
                if ($rollbackFailed) {
                    $this->logSubmitError('submitApply.rollback_failed_pre_commit', $e);
                    return $this->buildSubmitUncertainResult($submitToken);
                }

                $dupKind = $this->classifySubmitDuplicateKeyError($e);
                if ($dupKind === 'submit_token') {
                    try {
                        $replay = $this->recoverIdempotentAfterTokenConflict(
                            $submitToken,
                            $applicantId,
                            $payloadHash
                        );
                        if ($replay !== null) {
                            return $replay;
                        }
                    } catch (Exception $idemEx) {
                        return $this->buildSubmitFailResult($idemEx->getMessage());
                    } catch (\Throwable $idemEx) {
                        $this->logSubmitError('recoverIdempotentAfterTokenConflict', $idemEx);
                    }
                    return $this->buildSubmitFailResult('提交标识冲突，请确认后重试。');
                }
                if ($dupKind === 'apply_no' && $applyNoRetryBudget > 1 && $applyIdHint <= 0) {
                    $applyNoRetryBudget--;
                    continue;
                }
                if ($dupKind === 'approval_task') {
                    return $this->buildSubmitFailResult('审批任务初始化冲突，请稍后重试。');
                }

                if ($this->isTransientSubmitDbError($e) && $attempt < $maxRetry) {
                    continue;
                }

                // 保留原分支语义：业务 Exception 可回传文案；其他 Throwable 不暴露内部细节
                if ($e instanceof Exception) {
                    return $this->buildSubmitFailResult($e->getMessage());
                }
                $this->logSubmitError('submitApply', $e);
                return $this->buildSubmitFailResult('正式提交失败，请稍后重试。');
            }
        }

        return $this->buildSubmitFailResult('正式提交失败，请稍后重试。');
    }

    /**
     * Session 申请人有效性（正式提交专用）
     *
     * @return array{admin_id:int,username:string,group_id:int,parent_id:int}
     * @throws Exception
     * @throws \Throwable
     */
    public function assertApplicantActiveForSubmit()
    {
        $aidRaw = session('aid');
        if ($aidRaw === null || $aidRaw === '' || is_array($aidRaw) || is_object($aidRaw) || is_bool($aidRaw)) {
            throw new Exception('请先登录');
        }
        try {
            $adminId = $this->parseStrictPositiveInt($aidRaw, '请先登录');
        } catch (Exception $e) {
            throw new Exception('请先登录');
        }

        try {
            $row = $this->lookupAdminRowForSubmit($adminId);
        } catch (\Throwable $e) {
            throw $e;
        }
        if ($row === null || empty($row)) {
            throw new Exception('申请人账号不存在或已失效');
        }
        $username = trim((string)($row['username'] ?? ''));
        if ($username === '') {
            throw new Exception('申请人姓名无效');
        }
        if ((int)($row['is_open'] ?? 0) !== 1) {
            throw new Exception('申请人账号已停用，不能正式提交');
        }

        return [
            'admin_id' => (int)$row['admin_id'],
            'username' => $username,
            'group_id' => (int)($row['group_id'] ?? 0),
            'parent_id' => (int)($row['parent_id'] ?? 0),
        ];
    }

    /**
     * 严格解析 submit_token（VARCHAR(64)；禁止服务端改写/自动生成替代）
     *
     * 允许字符：UUID（含连字符）或安全十六进制/字母数字/_/- 串，长度 8～64。
     *
     * @param mixed $raw
     * @return string
     * @throws Exception
     */
    public function parseStrictSubmitToken($raw)
    {
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            throw new Exception('提交标识无效');
        }
        $token = trim((string)$raw);
        if ($token === '') {
            throw new Exception('提交标识不能为空');
        }
        if (strlen($token) > self::SUBMIT_TOKEN_MAX_LEN) {
            throw new Exception('提交标识长度不能超过' . self::SUBMIT_TOKEN_MAX_LEN . '个字符');
        }
        if (strlen($token) < 8) {
            throw new Exception('提交标识无效');
        }
        if (!preg_match('/^[A-Za-z0-9_-]+$/', $token)) {
            throw new Exception('提交标识格式不正确');
        }
        return $token;
    }

    /**
     * 不依赖当前库动态业务状态的业务意图纯解析（用于稳定哈希 / 幂等重试）
     *
     * - 不查厂家/订单/占用/经理
     * - 跳过「今天」日期闸门（确认标志仍按原文进入哈希）
     * - 不信任前端 applicant_id
     *
     * @param array $params
     * @param int $applicantId Session 申请人
     * @return array
     * @throws Exception
     */
    public function parseSubmitBusinessIntentForHash(array $params, $applicantId)
    {
        return $this->validateSubmitPayload($params, [
            'applicant_id' => $applicantId,
            'assert_factory' => false,
            'for_idempotent_hash' => true,
        ]);
    }

    /**
     * 事务内按 token 当前读加锁
     *
     * @param string $submitToken
     * @return array|null
     */
    public function findApplyBySubmitTokenForUpdate($submitToken)
    {
        $submitToken = trim((string)$submitToken);
        if ($submitToken === '') {
            return null;
        }
        $row = Db::name('crm_shipping_apply')
            ->where('submit_token', $submitToken)
            ->lock(true)
            ->find();
        return empty($row) ? null : $row;
    }

    /**
     * 识别历史正式提交成功（与当前占量状态解耦）
     *
     * @param array $apply
     * @return bool true=可幂等返回；false=非成功草稿等；异常不完整由调用方拒绝
     */
    public function isHistoricalSubmitSuccessRecord(array $apply)
    {
        if ((int)($apply['is_deleted'] ?? 0) !== 0) {
            return false;
        }
        $status = $this->parseDefinedShippingApplyStatus($apply['status'] ?? null);
        if ($status === null || !in_array($status, self::HISTORICAL_SUBMIT_SUCCESS_STATUSES, true)) {
            return false;
        }
        $submitTime = trim((string)($apply['submit_time'] ?? ''));
        if ($submitTime === '') {
            return false;
        }
        $hash = strtolower(trim((string)($apply['submit_payload_hash'] ?? '')));
        if ($hash === '' || strlen($hash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $hash)) {
            return false;
        }
        $token = trim((string)($apply['submit_token'] ?? ''));
        if ($token === '') {
            return false;
        }
        $round = (int)($apply['approval_round'] ?? 0);
        if ($round < 1) {
            return false;
        }
        // 合法已定义正式态 + 完整提交证据 = 历史正式提交成功（含驳回/通过/取消等后续态）
        return true;
    }

    /**
     * 严格解析发货申请 status（仅 int 或纯十进制数字字符串；拒绝杂讯/布尔/浮点等）
     *
     * @param mixed $raw
     * @return int|null
     */
    protected function parseDefinedShippingApplyStatus($raw)
    {
        if (is_int($raw)) {
            if ($raw < 0) {
                return null;
            }
            return $raw;
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            if ($raw === '' || !preg_match('/^(0|[1-9]\d*)$/', $raw)) {
                return null;
            }
            if (strlen($raw) > 10) {
                return null;
            }
            return (int)$raw;
        }
        return null;
    }

    /**
     * @param array|null $applyRow
     * @param int $applicantId
     * @param string $payloadHash
     * @return array|null 命中历史成功时返回响应；未命中返回 null；非法抛异常
     * @throws Exception
     */
    public function resolveIdempotentSubmitResult($applyRow, $applicantId, $payloadHash)
    {
        if ($applyRow === null || !is_array($applyRow) || empty($applyRow)) {
            return null;
        }

        $applicantId = (int)$applicantId;
        $payloadHash = strtolower(trim((string)$payloadHash));
        $rowApplicant = (int)($applyRow['applicant_id'] ?? 0);
        if ($rowApplicant !== $applicantId) {
            throw new Exception('提交标识不可用');
        }

        $rowHash = strtolower(trim((string)($applyRow['submit_payload_hash'] ?? '')));
        if ($rowHash === '' || $payloadHash === '' || !hash_equals($rowHash, $payloadHash)) {
            throw new Exception('相同提交标识与业务载荷不一致，拒绝继续。');
        }

        if ((int)($applyRow['is_deleted'] ?? 0) !== 0) {
            $this->logSubmitError('idempotent.soft_deleted', new Exception('token row soft-deleted apply_id=' . (int)($applyRow['id'] ?? 0)));
            throw new Exception('提交记录状态异常，请联系管理员');
        }

        $rowStatus = $this->parseDefinedShippingApplyStatus($applyRow['status'] ?? null);
        if ($rowStatus === 0) {
            // 同 token 落在未提交草稿：视为不完整/异常，不得冒充成功
            $this->logSubmitError('idempotent.draft_token', new Exception('token on draft apply_id=' . (int)($applyRow['id'] ?? 0)));
            throw new Exception('提交记录状态异常，请联系管理员');
        }

        if (!$this->isHistoricalSubmitSuccessRecord($applyRow)) {
            $this->logSubmitError('idempotent.incomplete', new Exception('incomplete submit evidence apply_id=' . (int)($applyRow['id'] ?? 0)));
            throw new Exception('提交记录不完整，请联系管理员');
        }

        return $this->buildSubmitSuccessResult($applyRow, true);
    }

    /**
     * 事务内正式提交写库主体（调用方已 startTrans，且已完成幂等未命中检查）
     *
     * @param array $ctx
     * @return array
     * @throws Exception
     */
    protected function executeSubmitApplyInTransaction(array $ctx)
    {
        $params = $ctx['params'];
        $applicantId = (int)$ctx['applicant_id'];
        $applicantName = (string)$ctx['applicant_name'];
        $submitToken = (string)$ctx['submit_token'];
        $payloadHash = (string)$ctx['payload_hash'];
        $intent = $ctx['intent'];
        $orderId = (int)$ctx['order_id'];
        $applyIdHint = (int)$ctx['apply_id_hint'];

        $order = $this->lockOrderForShippingSubmit($orderId);
        $this->validateOrderForApply($order, $applicantId);

        // 锁定本请求涉及的原产品行（固定升序）
        $requestOrderItemIds = [];
        foreach ((array)($intent['lines'] ?? []) as $line) {
            if (!is_array($line)) {
                continue;
            }
            $oid = (int)($line['order_item_id'] ?? 0);
            if ($oid > 0) {
                $requestOrderItemIds[$oid] = $oid;
            }
        }
        $orderItemMap = $this->lockOrderItemsForShippingSubmit(
            $orderId,
            array_values($requestOrderItemIds)
        );

        $existingApply = null;
        $beforeStatus = 0;
        if ($applyIdHint > 0) {
            $existingApply = Db::name('crm_shipping_apply')
                ->where('id', $applyIdHint)
                ->lock(true)
                ->find();
            if (empty($existingApply)) {
                throw new Exception('草稿不存在。');
            }
            if ((int)($existingApply['is_deleted'] ?? 0) !== 0) {
                throw new Exception('草稿已删除，无法正式提交。');
            }
            if ((int)($existingApply['status'] ?? -1) !== 0) {
                throw new Exception('该发货申请已提交，不能重复提交。');
            }
            // 正式提交：即使超管也不得代他人提交
            if ((int)($existingApply['applicant_id'] ?? 0) !== $applicantId) {
                throw new Exception('您无权提交该发货申请。');
            }
            if ((int)($existingApply['order_id'] ?? 0) !== $orderId) {
                throw new Exception('草稿关联订单不匹配，禁止静默切换关联订单。');
            }
            $existingToken = trim((string)($existingApply['submit_token'] ?? ''));
            if ($existingToken !== '' && !hash_equals($existingToken, $submitToken)) {
                // 同草稿已有其他 token 痕迹但仍为草稿：允许本次 token（未成功提交）
                // 若未来出现非空 token + 草稿，视为异常
                $this->logSubmitError(
                    'submit.draft_token_mismatch',
                    new Exception('draft_id=' . $applyIdHint)
                );
            }

            Db::name('crm_shipping_apply_item')
                ->where('shipping_apply_id', $applyIdHint)
                ->order('id', 'asc')
                ->lock(true)
                ->select();

            $beforeStatus = 0;
        }

        // 锁本申请全部替代授权行（含将被移除产品的旧授权）
        $pmSubstituteMap = [];
        if ($applyIdHint > 0) {
            $authRows = Db::name('crm_shipping_pm_substitute')
                ->where('shipping_apply_id', $applyIdHint)
                ->order('id', 'asc')
                ->lock(true)
                ->select();
            foreach ((array)$authRows as $authRow) {
                $oid = (int)($authRow['order_item_id'] ?? 0);
                if ($oid > 0) {
                    $pmSubstituteMap[$oid] = $authRow;
                }
            }
        }

        $appliedQtyMap = $this->calcAppliedQtyMapForSubmit(
            array_keys($orderItemMap),
            $applyIdHint > 0 ? $applyIdHint : 0
        );

        $validated = $this->validateSubmitPayloadForCommit($params, [
            'applicant_id' => $applicantId,
            'order' => $order,
            'order_item_map' => $orderItemMap,
            'applied_qty_map' => $appliedQtyMap,
        ]);

        if ($existingApply) {
            $this->assertDraftOrderAmountConfirmedForSubmit(
                $existingApply,
                $order,
                $validated
            );
        }

        $managers = $this->resolveSubmitManagers([
            'apply_id' => $applyIdHint > 0 ? $applyIdHint : 0,
            'lines' => $validated['lines'],
            'order_item_map' => $orderItemMap,
            'pm_substitute_map' => $pmSubstituteMap,
        ]);

        $configSnap = $this->buildApprovalConfigSnapshot([
            'order' => $order,
            'applicant_id' => $applicantId,
        ]);

        // 哈希必须与事务外纯解析意图一致（禁止因 DB 回填产品名等漂移）
        $intentAgain = $this->parseSubmitBusinessIntentForHash($params, $applicantId);
        $hashAgain = $this->computeSubmitPayloadHash($this->normalizeSubmitPayload($intentAgain));
        if (!hash_equals($payloadHash, (string)$hashAgain['hash'])) {
            throw new Exception('提交业务载荷哈希不一致，拒绝继续。');
        }

        $now = date('Y-m-d H:i:s');
        $orderAmountToSave = $this->normalizeNullableMoney($order['money'] ?? null, false);
        if ($orderAmountToSave === null) {
            $orderAmountToSave = $this->formatMoneyHalfUp((string)($order['money'] ?? '0'));
        }

        $header = $validated['header'];
        $lineManagersByOrderItem = [];
        foreach ((array)$managers['line_managers'] as $lm) {
            if (!is_array($lm)) {
                continue;
            }
            $loid = (int)($lm['order_item_id'] ?? 0);
            if ($loid > 0) {
                $lineManagersByOrderItem[$loid] = $lm;
            }
        }

        $itemSnapshots = [];
        $applyLineNo = 0;
        $keptOrderItemIds = [];
        foreach ($validated['lines'] as $line) {
            $orderItemId = (int)$line['order_item_id'];
            if (!isset($orderItemMap[$orderItemId])) {
                throw new Exception('存在已失效或不属于当前订单的产品行，请重新选择有效原订单产品后再提交。');
            }
            if (!isset($lineManagersByOrderItem[$orderItemId])) {
                throw new Exception('存在无法解析的产品经理审批人，拒绝继续。');
            }
            $lm = $lineManagersByOrderItem[$orderItemId];
            $orderItem = $orderItemMap[$orderItemId];
            $applyLineNo++;
            $keptOrderItemIds[$orderItemId] = $orderItemId;

            $applyQty = (int)$line['apply_qty'];
            $sale = (string)$line['apply_unit_price'];
            $purchase = (string)$line['apply_purchase_unit_price'];
            $salesAmount = $this->bcmulMoney((string)$applyQty, $sale);
            $purchaseAmount = $this->bcmulMoney((string)$applyQty, $purchase);
            $orderPurchaseAmount = $this->normalizeNullableMoney($orderItem['purchase_price'] ?? null);

            $managerInfo = [
                'product_manager_id' => (int)$lm['resolved_manager_id'],
                'product_manager_name' => (string)$lm['resolved_manager_name'],
            ];

            $itemSnapshots[] = $this->buildShippingItemSnapshot(
                $orderItem,
                $managerInfo,
                $applyQty,
                (string)($line['remark'] ?? ''),
                [
                    'apply_line_no' => $applyLineNo,
                    'apply_product_name' => (string)$line['apply_product_name'],
                    'apply_spec_model' => (string)$line['apply_spec_model'],
                    'factory_id' => (int)$line['factory_id'],
                    'factory_name' => (string)($line['factory_name'] ?? ''),
                    'order_purchase_amount' => $orderPurchaseAmount,
                    'apply_unit_price' => $sale,
                    'apply_purchase_unit_price' => $purchase,
                    'apply_sales_amount' => $salesAmount,
                    'apply_purchase_amount' => $purchaseAmount,
                ]
            );
        }
        if (empty($itemSnapshots)) {
            throw new Exception('至少需要一条正整数数量的发货行。');
        }

        $applyNo = '';
        $savedApplyId = 0;
        $mainData = [
            'order_no' => trim((string)($order['order_no'] ?? '')),
            'order_amount' => $orderAmountToSave,
            'approval_threshold' => (string)$configSnap['approval_threshold'],
            'approval_route' => (int)$configSnap['approval_route'],
            'approval_round' => 1,
            'team_name' => trim((string)($order['team_name'] ?? '')),
            'team_leader_id' => $configSnap['team_leader_id'],
            'team_leader_name' => (string)($configSnap['team_leader_name'] ?? ''),
            'customer_name' => trim((string)($order['cname'] ?? '')),
            'customer_phone' => trim((string)($order['contact'] ?? '')),
            'receiver_name' => (string)$header['receiver_name'],
            'receiver_phone' => (string)$header['receiver_phone'],
            'receiver_address' => (string)$header['receiver_address'],
            'shipping_date' => (string)$header['shipping_date'],
            'shipping_method' => (string)$header['shipping_method'],
            'shipping_remark' => (string)$header['shipping_remark'],
            'product_director_id' => $configSnap['product_director_id'],
            'product_director_name' => (string)($configSnap['product_director_name'] ?? ''),
            'finance_cc_user_id' => $configSnap['finance_cc_user_id'],
            'finance_cc_user_name' => (string)($configSnap['finance_cc_user_name'] ?? ''),
            'submit_token' => $submitToken,
            'submit_payload_hash' => $payloadHash,
            'status' => 10,
            'current_stage' => 1,
            'submit_time' => $now,
            'update_time' => $now,
        ];

        if ($existingApply) {
            $applyNo = trim((string)($existingApply['apply_no'] ?? ''));
            $savedApplyId = (int)$existingApply['id'];
            // 禁止覆盖原申请人 / 原订单 / 软删字段 / approved_time
            $affected = Db::name('crm_shipping_apply')
                ->where('id', $savedApplyId)
                ->where('status', 0)
                ->where('is_deleted', 0)
                ->where('applicant_id', $applicantId)
                ->where('order_id', $orderId)
                ->update($mainData);
            if ((int)$affected !== 1) {
                throw new Exception('发货申请提交失败，请刷新后重试。');
            }
            Db::name('crm_shipping_apply_item')->where('shipping_apply_id', $savedApplyId)->delete();
        } else {
            $applyNo = $this->generateApplyNo();
            $insertData = array_merge($mainData, [
                'apply_no' => $applyNo,
                'order_id' => $orderId,
                'applicant_id' => $applicantId,
                'applicant_name' => $applicantName,
                'create_time' => $now,
                'approved_time' => null,
            ]);
            $savedApplyId = (int)Db::name('crm_shipping_apply')->insertGetId($insertData);
            if ($savedApplyId <= 0) {
                throw new Exception('发货申请提交失败，请稍后重试。');
            }
        }

        foreach ($itemSnapshots as $snapshot) {
            $snapshot['shipping_apply_id'] = $savedApplyId;
            $snapshot['create_time'] = $now;
            $snapshot['update_time'] = $now;
            $itemId = (int)Db::name('crm_shipping_apply_item')->insertGetId($snapshot);
            if ($itemId <= 0) {
                throw new Exception('发货明细写入失败，请稍后重试。');
            }
        }

        // 冻结实际使用的替代授权
        foreach ((array)$managers['freeze_authorizations'] as $freezePlan) {
            if (!is_array($freezePlan)) {
                continue;
            }
            $this->executeFreezePmSubstituteForSubmit(
                $freezePlan,
                $pmSubstituteMap,
                $applicantId,
                $applicantName,
                $applyNo,
                $now
            );
        }

        // 原经理恢复：失效旧授权
        foreach ((array)$managers['invalidate_authorizations'] as $invPlan) {
            if (!is_array($invPlan)) {
                continue;
            }
            $authId = (int)($invPlan['auth_id'] ?? 0);
            $oid = (int)($invPlan['order_item_id'] ?? 0);
            $row = null;
            if ($oid > 0 && isset($pmSubstituteMap[$oid]) && (int)($pmSubstituteMap[$oid]['id'] ?? 0) === $authId) {
                $row = $pmSubstituteMap[$oid];
            }
            if ($row === null) {
                throw new Exception('待失效替代授权未锁定，拒绝继续。');
            }
            if ((int)($row['auth_version'] ?? 0) !== (int)($invPlan['expected_auth_version'] ?? -1)) {
                throw new Exception('授权信息已被他人修改，请刷新后重试');
            }
            $this->markPmSubstituteInvalid(
                $row,
                $applicantId,
                $applicantName,
                $applyNo,
                (string)($invPlan['reason'] ?? '原产品经理已恢复正常，正式提交时失效旧替代授权'),
                (string)($invPlan['invalidate_type'] ?? 'original_restored'),
                $now
            );
            // 防止后续移除逻辑重复失效
            $pmSubstituteMap[$oid]['status'] = 0;
        }

        // 草稿移除产品：失效对应有效授权
        if ($existingApply && $savedApplyId > 0) {
            $this->invalidatePmSubstitutesForRemovedOrderItems(
                $savedApplyId,
                array_values($keptOrderItemIds),
                $applicantId,
                $applicantName,
                $applyNo,
                $now
            );
        }

        // 产品经理审批任务（按经理去重）
        foreach ((array)$managers['approvers'] as $approver) {
            if (!is_array($approver)) {
                continue;
            }
            $taskId = (int)Db::name('crm_shipping_approval_task')->insertGetId([
                'shipping_apply_id' => $savedApplyId,
                'approval_type' => 1,
                'approver_id' => (int)$approver['approver_id'],
                'approver_name' => (string)($approver['approver_name'] ?? ''),
                'approval_round' => 1,
                'status' => 1,
                'opinion' => '',
                'activate_time' => $now,
                'audit_time' => null,
                'create_time' => $now,
                'update_time' => $now,
            ]);
            if ($taskId <= 0) {
                throw new Exception('审批任务初始化失败，请稍后重试。');
            }
        }

        // 高金额：预创建总监未激活任务
        $directorPlan = $configSnap['product_director_task_plan'] ?? null;
        if (is_array($directorPlan) && (int)($configSnap['approval_route'] ?? 0) === 2) {
            $taskId = (int)Db::name('crm_shipping_approval_task')->insertGetId([
                'shipping_apply_id' => $savedApplyId,
                'approval_type' => 2,
                'approver_id' => (int)$directorPlan['approver_id'],
                'approver_name' => (string)($directorPlan['approver_name'] ?? ''),
                'approval_round' => 1,
                'status' => 0,
                'opinion' => '',
                'activate_time' => null,
                'audit_time' => null,
                'create_time' => $now,
                'update_time' => $now,
            ]);
            if ($taskId <= 0) {
                throw new Exception('产品总监审批任务初始化失败，请稍后重试。');
            }
        }

        $this->writeSubmitAuditLog([
            'shipping_apply_id' => $savedApplyId,
            'apply_no' => $applyNo,
            'operator_id' => $applicantId,
            'operator_name' => $applicantName,
            'before_status' => $beforeStatus,
            'after_status' => 10,
            'approval_round' => 1,
            'now' => $now,
        ]);

        if (!empty($configSnap['finance_cc_anomaly']) && is_array($configSnap['finance_cc_anomaly'])) {
            $this->writeSubmitFinanceCcAnomalyAuditLog([
                'shipping_apply_id' => $savedApplyId,
                'apply_no' => $applyNo,
                'operator_id' => $applicantId,
                'operator_name' => $applicantName,
                'before_status' => $beforeStatus,
                'after_status' => 10,
                'approval_round' => 1,
                'anomaly' => $configSnap['finance_cc_anomaly'],
                'now' => $now,
            ]);
        }

        // 提交后一致性再读
        $finalRow = Db::name('crm_shipping_apply')->where('id', $savedApplyId)->find();
        if (empty($finalRow)
            || (int)($finalRow['status'] ?? -1) !== 10
            || (int)($finalRow['approval_round'] ?? 0) !== 1
            || trim((string)($finalRow['submit_token'] ?? '')) !== $submitToken
            || strtolower(trim((string)($finalRow['submit_payload_hash'] ?? ''))) !== strtolower($payloadHash)
        ) {
            throw new Exception('发货申请提交状态校验失败，请稍后重试。');
        }

        return $this->buildSubmitSuccessResult($finalRow, false);
    }

    /**
     * 草稿正式提交：原订单金额变化强制确认
     *
     * @param array $existingApply
     * @param array $order
     * @param array $validated
     * @throws Exception
     */
    public function assertDraftOrderAmountConfirmedForSubmit(array $existingApply, array $order, array $validated)
    {
        $currentOrderAmount = $this->normalizeNullableMoney($order['money'] ?? null, false);
        $draftOrderAmount = $this->normalizeNullableMoney($existingApply['order_amount'] ?? null, false);
        if ($draftOrderAmount === null || $currentOrderAmount === null) {
            return;
        }
        if (bccomp((string)$draftOrderAmount, (string)$currentOrderAmount, 2) === 0) {
            return;
        }

        $header = isset($validated['header']) && is_array($validated['header']) ? $validated['header'] : [];
        if ((int)($header['order_amount_confirmed'] ?? 0) !== 1) {
            throw new Exception(
                '原订单金额已发生变化：草稿记录 ' . $draftOrderAmount
                . ' 元，当前订单金额 ' . $currentOrderAmount
                . ' 元，请重新核对并确认后再提交。'
            );
        }
        $confirmed = $header['confirmed_order_amount'] ?? null;
        if ($confirmed === null || bccomp((string)$confirmed, (string)$currentOrderAmount, 2) !== 0) {
            throw new Exception('原订单金额确认与当前锁定订单金额不一致，请刷新后重新确认。');
        }
    }

    /**
     * 冻结替代授权（status 1→2）；影响行数必须为 1
     *
     * @param array $freezePlan
     * @param array $pmSubstituteMap
     * @param int $operatorId
     * @param string $operatorName
     * @param string $applyNo
     * @param string $now
     * @throws Exception
     */
    protected function executeFreezePmSubstituteForSubmit(
        array $freezePlan,
        array $pmSubstituteMap,
        $operatorId,
        $operatorName,
        $applyNo,
        $now
    ) {
        $authId = (int)($freezePlan['auth_id'] ?? 0);
        $applyId = (int)($freezePlan['apply_id'] ?? 0);
        $orderItemId = (int)($freezePlan['order_item_id'] ?? 0);
        $expectedVersion = (int)($freezePlan['expected_auth_version'] ?? 0);
        if ($authId <= 0 || $applyId <= 0 || $orderItemId <= 0 || $expectedVersion <= 0) {
            throw new Exception('替代授权冻结参数不完整，拒绝继续。');
        }
        if ($expectedVersion >= self::PM_SUBSTITUTE_AUTH_VERSION_MAX) {
            throw new Exception('授权版本号已达上限，请联系管理员');
        }
        if (!isset($pmSubstituteMap[$orderItemId]) || !is_array($pmSubstituteMap[$orderItemId])) {
            throw new Exception('替代授权行未在正式提交事务中锁定，拒绝冻结。');
        }
        $row = $pmSubstituteMap[$orderItemId];
        if ((int)($row['id'] ?? 0) !== $authId) {
            throw new Exception('替代授权记录不匹配，拒绝冻结。');
        }
        if ((int)($row['status'] ?? 0) !== 1) {
            throw new Exception('仅有效替代授权可冻结。');
        }
        if ((int)($row['auth_version'] ?? 0) !== $expectedVersion) {
            throw new Exception('授权信息已被他人修改，请刷新后重试');
        }

        $this->updatePmSubstituteWithAuthVersionBump(
            $authId,
            $expectedVersion,
            [
                'status' => 2,
                'update_time' => $now,
            ],
            [
                'status' => 1,
                'shipping_apply_id' => $applyId,
                'order_item_id' => $orderItemId,
            ]
        );

        $this->writePmSubstituteAuditLogs([
            'action' => 'pm_substitute_freeze',
            'shipping_apply_id' => $applyId,
            'apply_no' => (string)$applyNo,
            'order_item_id' => $orderItemId,
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
            'after_status' => 2,
            'before_reason' => (string)($row['reason'] ?? ''),
            'after_reason' => '正式提交冻结替代授权',
            'before_authorized_by' => (int)($row['authorized_by'] ?? 0),
            'before_authorized_by_name' => (string)($row['authorized_by_name'] ?? ''),
            'invalidate_type' => 'submit_freeze',
            'operator_id' => (int)$operatorId,
            'operator_name' => (string)$operatorName,
            'now' => $now,
        ]);
    }

    /**
     * @param array $payload
     * @throws Exception
     */
    protected function writeSubmitAuditLog(array $payload)
    {
        $applyId = (int)($payload['shipping_apply_id'] ?? 0);
        if ($applyId <= 0) {
            throw new Exception('正式提交审计关联申请无效');
        }
        $opinion = [
            'apply_no' => (string)($payload['apply_no'] ?? ''),
            'approval_round' => (int)($payload['approval_round'] ?? 1),
            'action' => 'submit',
            'note' => '发货申请正式提交',
        ];
        $opinionJson = json_encode($opinion, JSON_UNESCAPED_UNICODE);
        if ($opinionJson === false) {
            throw new Exception('正式提交审计信息编码失败');
        }
        if (strlen($opinionJson) > self::APPROVAL_LOG_OPINION_MAX) {
            throw new Exception('正式提交审计信息过长');
        }
        $logId = (int)Db::name('crm_shipping_approval_log')->insertGetId([
            'shipping_apply_id' => $applyId,
            'task_id' => null,
            'operator_id' => (int)($payload['operator_id'] ?? 0),
            'operator_name' => mb_substr((string)($payload['operator_name'] ?? ''), 0, 100, 'UTF-8'),
            'operator_role' => 'applicant',
            'action' => 'submit',
            'before_status' => (int)($payload['before_status'] ?? 0),
            'after_status' => (int)($payload['after_status'] ?? 10),
            'opinion' => $opinionJson,
            'create_time' => (string)($payload['now'] ?? date('Y-m-d H:i:s')),
        ]);
        if ($logId <= 0) {
            throw new Exception('正式提交审计日志写入失败');
        }
    }

    /**
     * @param array $payload
     * @throws Exception
     */
    protected function writeSubmitFinanceCcAnomalyAuditLog(array $payload)
    {
        $applyId = (int)($payload['shipping_apply_id'] ?? 0);
        if ($applyId <= 0) {
            throw new Exception('财务异常审计关联申请无效');
        }
        $anomaly = isset($payload['anomaly']) && is_array($payload['anomaly']) ? $payload['anomaly'] : [];
        $opinion = [
            'apply_no' => (string)($payload['apply_no'] ?? ''),
            'approval_round' => (int)($payload['approval_round'] ?? 1),
            'action' => 'submit_finance_cc_anomaly',
            'reason' => (string)($anomaly['reason'] ?? 'unknown'),
            'configured_admin_id' => $anomaly['configured_admin_id'] ?? null,
            'requires_follow_up' => !empty($anomaly['requires_follow_up']),
            'note' => '财务抄送配置异常，正式提交已允许但需后续补处理',
        ];
        if (isset($anomaly['configured_raw'])) {
            $opinion['configured_raw'] = (string)$anomaly['configured_raw'];
        }
        $opinionJson = json_encode($opinion, JSON_UNESCAPED_UNICODE);
        if ($opinionJson === false) {
            throw new Exception('财务异常审计信息编码失败');
        }
        if (strlen($opinionJson) > self::APPROVAL_LOG_OPINION_MAX) {
            throw new Exception('财务异常审计信息过长');
        }
        $logId = (int)Db::name('crm_shipping_approval_log')->insertGetId([
            'shipping_apply_id' => $applyId,
            'task_id' => null,
            'operator_id' => (int)($payload['operator_id'] ?? 0),
            'operator_name' => mb_substr((string)($payload['operator_name'] ?? ''), 0, 100, 'UTF-8'),
            'operator_role' => 'system',
            'action' => 'submit_finance_cc_anomaly',
            'before_status' => (int)($payload['before_status'] ?? 0),
            'after_status' => (int)($payload['after_status'] ?? 10),
            'opinion' => $opinionJson,
            'create_time' => (string)($payload['now'] ?? date('Y-m-d H:i:s')),
        ]);
        if ($logId <= 0) {
            throw new Exception('财务异常审计日志写入失败');
        }
    }

    /**
     * @param array $applyRow
     * @param bool $idempotentReplay
     * @return array
     */
    protected function buildSubmitSuccessResult(array $applyRow, $idempotentReplay)
    {
        $status = (int)($applyRow['status'] ?? 10);
        return [
            'code' => 0,
            'msg' => $idempotentReplay ? '该发货申请此前已提交成功' : '发货申请已提交审核',
            'data' => [
                'apply_id' => (int)($applyRow['id'] ?? 0),
                'apply_no' => trim((string)($applyRow['apply_no'] ?? '')),
                'status' => $status,
                'approval_round' => (int)($applyRow['approval_round'] ?? 1),
                'current_stage' => (int)($applyRow['current_stage'] ?? 0),
                'submission_succeeded' => true,
                'idempotent_replay' => $idempotentReplay ? true : false,
            ],
        ];
    }

    /**
     * @param string $msg
     * @return array
     */
    protected function buildSubmitFailResult($msg)
    {
        $msg = trim((string)$msg);
        if ($msg === '') {
            $msg = '正式提交失败，请稍后重试。';
        }
        // 不向调用方泄露 SQL / 堆栈
        if (preg_match('/SQLSTATE|Duplicate entry|Integrity constraint|PDOException/i', $msg)) {
            $msg = '正式提交失败，请稍后重试。';
        }
        return ['code' => -200, 'msg' => $msg, 'data' => []];
    }

    /**
     * COMMIT 结果未知且无法确认成功时的安全返回
     *
     * @param string $submitToken
     * @return array
     */
    protected function buildSubmitUncertainResult($submitToken)
    {
        return [
            'code' => -200,
            'msg' => '提交结果待确认，请使用原请求标识查询或重试',
            'data' => [
                'submit_token' => (string)$submitToken,
                'result_uncertain' => true,
            ],
        ];
    }

    /**
     * 已尝试 COMMIT 后的统一恢复（禁止自动重试写库）
     *
     * @param \Throwable $e
     * @param string $submitToken
     * @param int $applicantId
     * @param string $payloadHash
     * @return array
     */
    protected function finalizeSubmitAfterCommitUncertainty($e, $submitToken, $applicantId, $payloadHash)
    {
        $this->logSubmitError('submitApply.commit_uncertain', $e);
        $recovered = $this->recoverSubmitResultAfterCommitUncertainty(
            $submitToken,
            $applicantId,
            $payloadHash
        );
        if ($recovered !== null) {
            return $recovered;
        }
        return $this->buildSubmitUncertainResult($submitToken);
    }

    /**
     * 事务边界钩子（生产直连 Db；专项测试可隔离/注入，无 HTTP 暴露）
     *
     * @return void
     */
    protected function beginSubmitTransaction()
    {
        Db::startTrans();
    }

    /**
     * @return void
     */
    protected function commitSubmitTransaction()
    {
        Db::commit();
    }

    /**
     * @return void
     */
    protected function rollbackSubmitTransaction()
    {
        Db::rollback();
    }

    /**
     * 按 submit_token 当前读（无锁）；供冲突恢复 / COMMIT 不确定核验复用
     *
     * @param string $submitToken
     * @return array|null
     */
    protected function findApplyBySubmitTokenCurrentRead($submitToken)
    {
        $submitToken = trim((string)$submitToken);
        if ($submitToken === '') {
            return null;
        }
        $row = Db::name('crm_shipping_apply')
            ->where('submit_token', $submitToken)
            ->find();
        return empty($row) ? null : $row;
    }

    /**
     * token 唯一键冲突后：新事务当前读核验
     *
     * @param string $submitToken
     * @param int $applicantId
     * @param string $payloadHash
     * @return array|null
     */
    protected function recoverIdempotentAfterTokenConflict($submitToken, $applicantId, $payloadHash)
    {
        // 新查询 = 当前读；避免沿用已回滚事务的旧快照
        $row = $this->findApplyBySubmitTokenCurrentRead($submitToken);
        if ($row === null) {
            return null;
        }
        // 身份/hash 不匹配等业务拒绝向上抛，由调用方转为安全失败响应
        return $this->resolveIdempotentSubmitResult($row, $applicantId, $payloadHash);
    }

    /**
     * COMMIT 结果未知时的安全核验
     *
     * @param string $submitToken
     * @param int $applicantId
     * @param string $payloadHash
     * @return array|null null=查无记录（仍由调用方返回 uncertain，不宣称已回滚）
     */
    protected function recoverSubmitResultAfterCommitUncertainty($submitToken, $applicantId, $payloadHash)
    {
        try {
            $row = $this->findApplyBySubmitTokenCurrentRead($submitToken);
            if ($row === null) {
                return null;
            }
            if ((int)($row['applicant_id'] ?? 0) !== (int)$applicantId) {
                // 不向其他申请人泄露历史详情
                return $this->buildSubmitUncertainResult($submitToken);
            }
            $rowHash = strtolower(trim((string)($row['submit_payload_hash'] ?? '')));
            if ($rowHash === '' || !hash_equals($rowHash, strtolower(trim((string)$payloadHash)))) {
                return $this->buildSubmitUncertainResult($submitToken);
            }
            if ($this->isHistoricalSubmitSuccessRecord($row)) {
                return $this->buildSubmitSuccessResult($row, true);
            }
            return $this->buildSubmitUncertainResult($submitToken);
        } catch (\Throwable $e) {
            $this->logSubmitError('recoverSubmitResultAfterCommitUncertainty', $e);
            return $this->buildSubmitUncertainResult($submitToken);
        }
    }

    /**
     * @param \Throwable $e
     * @return string ''|submit_token|apply_no|approval_task
     */
    protected function classifySubmitDuplicateKeyError($e)
    {
        $msg = (string)$e->getMessage();
        if (stripos($msg, 'Duplicate entry') === false && stripos($msg, '1062') === false) {
            return '';
        }
        if (stripos($msg, 'uniq_submit_token') !== false || stripos($msg, 'submit_token') !== false) {
            return 'submit_token';
        }
        if (stripos($msg, 'uniq_apply_no') !== false || stripos($msg, 'apply_no') !== false) {
            return 'apply_no';
        }
        if (stripos($msg, 'uniq_apply_round_type_approver') !== false) {
            return 'approval_task';
        }
        return '';
    }

    /**
     * @param \Throwable $e
     * @return bool
     */
    protected function isTransientSubmitDbError($e)
    {
        $msg = (string)$e->getMessage();
        if (stripos($msg, 'Deadlock') !== false || stripos($msg, '1213') !== false) {
            return true;
        }
        if (stripos($msg, 'Lock wait timeout') !== false || stripos($msg, '1205') !== false) {
            return true;
        }
        if (stripos($msg, '40001') !== false) {
            return true;
        }
        return false;
    }

    /**
     * @param string $scene
     * @param \Throwable $e
     * @return void
     */
    protected function logSubmitError($scene, $e)
    {
        if (class_exists('\\think\\facade\\Log')) {
            \think\facade\Log::write(
                '[ShippingApplyService::' . $scene . '] ' . $e->getMessage(),
                'error'
            );
        }
    }

    // -------------------------------------------------------------------------
    // M4-2A：我的发货申请列表 + 统一只读详情（只读；不写库）
    // -------------------------------------------------------------------------

    /** 列表关键词最大字符数 */
    const MY_APPLY_KEYWORD_MAX = 100;

    /** 列表分页上限 */
    const MY_APPLY_LIST_LIMIT_MAX = 100;

    /** 统一详情拒绝文案（无权限 / 不存在 / 已删 / 非正式申请统一） */
    const APPLY_VIEW_DENY_MSG = '无权查看或记录不存在';

    /**
     * 我的发货申请列表（Layui 分页）
     *
     * P0：任意登录身份（含超管）仅查询 applicant_id = session('aid')。
     * 禁止信任前端 applicant_id / user_id / is_admin 等扩大范围。
     *
     * @param array $params
     * @return array{code:int,msg:string,count:int,data:array}
     */
    public function listMyApplies($params = [])
    {
        $params = is_array($params) ? $params : [];
        // 显式丢弃前端伪造身份字段，避免后续误用
        unset(
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['can_view'],
            $params['viewer_role'],
            $params['can_approve']
        );

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
        if ($limit > self::MY_APPLY_LIST_LIMIT_MAX) {
            $limit = self::MY_APPLY_LIST_LIMIT_MAX;
        }

        $keyword = trim((string)($params['keyword'] ?? ''));
        if (mb_strlen($keyword, 'UTF-8') > self::MY_APPLY_KEYWORD_MAX) {
            return ['code' => -200, 'msg' => '关键字过长', 'count' => 0, 'data' => []];
        }

        $statusFilter = null;
        if (array_key_exists('status', $params) && $params['status'] !== '' && $params['status'] !== null) {
            $statusRaw = $params['status'];
            if (is_bool($statusRaw) || is_array($statusRaw) || is_object($statusRaw) || is_float($statusRaw)) {
                return ['code' => -200, 'msg' => '审批状态无效', 'count' => 0, 'data' => []];
            }
            if (is_string($statusRaw)) {
                $statusRaw = trim($statusRaw);
                if ($statusRaw === '' || !preg_match('/^(0|[1-9]\d*)$/', $statusRaw)) {
                    return ['code' => -200, 'msg' => '审批状态无效', 'count' => 0, 'data' => []];
                }
                $statusFilter = (int)$statusRaw;
            } elseif (is_int($statusRaw)) {
                $statusFilter = $statusRaw;
            } else {
                return ['code' => -200, 'msg' => '审批状态无效', 'count' => 0, 'data' => []];
            }
            if (!in_array($statusFilter, self::HISTORICAL_SUBMIT_SUCCESS_STATUSES, true)) {
                return ['code' => -200, 'msg' => '审批状态无效', 'count' => 0, 'data' => []];
            }
        }

        $startTime = trim((string)($params['start_time'] ?? ''));
        $endTime = trim((string)($params['end_time'] ?? ''));
        $startBound = null;
        $endBound = null;
        if ($startTime !== '') {
            $startBound = $this->parseMyApplyDateBound($startTime, true);
            if ($startBound === null) {
                return ['code' => -200, 'msg' => '开始日期格式无效', 'count' => 0, 'data' => []];
            }
        }
        if ($endTime !== '') {
            $endBound = $this->parseMyApplyDateBound($endTime, false);
            if ($endBound === null) {
                return ['code' => -200, 'msg' => '结束日期格式无效', 'count' => 0, 'data' => []];
            }
        }
        if ($startBound !== null && $endBound !== null && strcmp($startBound, $endBound) > 0) {
            return ['code' => -200, 'msg' => '开始日期不能晚于结束日期', 'count' => 0, 'data' => []];
        }

        // P0：本人过滤对超管同样强制；仅正式提交成功状态白名单；排除软删
        $query = Db::name('crm_shipping_apply')
            ->where('applicant_id', $adminId)
            ->where('is_deleted', 0)
            ->whereIn('status', self::HISTORICAL_SUBMIT_SUCCESS_STATUSES);

        if ($statusFilter !== null) {
            $query->where('status', $statusFilter);
        }
        if ($keyword !== '') {
            $query->where(function ($q) use ($keyword) {
                $q->whereLike('apply_no', '%' . $keyword . '%')
                    ->whereOr('order_no', 'like', '%' . $keyword . '%')
                    ->whereOr('customer_name', 'like', '%' . $keyword . '%');
            });
        }
        if ($startBound !== null) {
            $query->where('submit_time', '>=', $startBound);
        }
        if ($endBound !== null) {
            $query->where('submit_time', '<=', $endBound);
        }

        try {
            $count = (int)(clone $query)->count('id');
            if ($count <= 0) {
                return [
                    'code' => 0,
                    'msg' => '获取成功',
                    'count' => 0,
                    'data' => [],
                ];
            }

            $rows = (clone $query)
                ->field('id,apply_no,order_no,customer_name,order_amount,shipping_date,submit_time,status,approval_round,approval_route,approval_threshold,applicant_name')
                ->order('submit_time', 'desc')
                ->order('id', 'desc')
                ->page($page, $limit)
                ->select();
            $rows = (array)$rows;
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[ShippingApplyService::listMyApplies] ' . $e->getMessage(), 'error');
            }
            return ['code' => -200, 'msg' => '列表加载失败，请稍后重试', 'count' => 0, 'data' => []];
        }

        $applyIds = [];
        $roundMap = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id > 0) {
                $applyIds[] = $id;
                $roundMap[$id] = (int)($row['approval_round'] ?? 0);
            }
        }

        $summaryMap = $this->buildDraftProductSummaries($applyIds);
        $approverMap = $this->loadCurrentApproverMap($applyIds, $roundMap, $rows);

        $data = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            $status = (int)($row['status'] ?? 0);
            $approverInfo = isset($approverMap[$id]) && is_array($approverMap[$id])
                ? $approverMap[$id]
                : [
                    'current_approver_text' => '',
                    'next_stage_text' => '',
                ];
            $data[] = [
                'id' => $id,
                'apply_no' => trim((string)($row['apply_no'] ?? '')),
                'order_no' => trim((string)($row['order_no'] ?? '')),
                'customer_name' => trim((string)($row['customer_name'] ?? '')),
                'order_amount' => round((float)($row['order_amount'] ?? 0), 2),
                'shipping_date' => trim((string)($row['shipping_date'] ?? '')),
                'submit_time' => trim((string)($row['submit_time'] ?? '')),
                'status' => $status,
                'status_text' => $this->mapApplyStatusText($status),
                'approval_round' => (int)($row['approval_round'] ?? 0),
                'approval_route' => (int)($row['approval_route'] ?? 0),
                'approval_route_text' => $this->mapApprovalRouteText((int)($row['approval_route'] ?? 0)),
                'product_summary' => isset($summaryMap[$id]) ? $summaryMap[$id] : '',
                'current_approver_text' => (string)($approverInfo['current_approver_text'] ?? ''),
                'next_stage_text' => (string)($approverInfo['next_stage_text'] ?? ''),
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'count' => $count,
            'data' => $data,
        ];
    }

    /**
     * 待我审核列表（M4-2B-2A 只读；Layui 分页）
     *
     * 权限依据：crm_shipping_approval_task.approver_id = session('aid')
     * 且任务 status=1、approval_round=申请当前轮次、主状态与审批类型匹配：
     *   status=10 → approval_type=1；status=30 → approval_type=2。
     * 禁止信任前端 approver_id / group_id / is_admin；超管不扩大范围。
     *
     * 去重键：shipping_apply_id（同一审批人对同一申请本轮同阶段多条任务只展示一行）。
     * 异常重复：保留 MIN(task.id) 作为 pending_task_id，matched_task_cnt>1 时写 error 日志，不掩盖计数。
     *
     * @param array $params
     * @return array{code:int,msg:string,count:int,data:array}
     */
    public function listPendingApplies($params = [])
    {
        $params = is_array($params) ? $params : [];
        // 显式丢弃前端伪造身份 / 审批字段，避免后续误用
        unset(
            $params['approver_id'],
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['can_view'],
            $params['viewer_role'],
            $params['can_approve'],
            $params['approval_type'],
            $params['task_status']
        );

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
        if ($limit > self::MY_APPLY_LIST_LIMIT_MAX) {
            $limit = self::MY_APPLY_LIST_LIMIT_MAX;
        }

        $keyword = trim((string)($params['keyword'] ?? ''));
        if (mb_strlen($keyword, 'UTF-8') > self::MY_APPLY_KEYWORD_MAX) {
            return ['code' => -200, 'msg' => '关键字过长', 'count' => 0, 'data' => []];
        }

        $startTime = trim((string)($params['start_time'] ?? ''));
        $endTime = trim((string)($params['end_time'] ?? ''));
        $startBound = null;
        $endBound = null;
        if ($startTime !== '') {
            $startBound = $this->parseMyApplyDateBound($startTime, true);
            if ($startBound === null) {
                return ['code' => -200, 'msg' => '开始日期格式无效', 'count' => 0, 'data' => []];
            }
        }
        if ($endTime !== '') {
            $endBound = $this->parseMyApplyDateBound($endTime, false);
            if ($endBound === null) {
                return ['code' => -200, 'msg' => '结束日期格式无效', 'count' => 0, 'data' => []];
            }
        }
        if ($startBound !== null && $endBound !== null && strcmp($startBound, $endBound) > 0) {
            return ['code' => -200, 'msg' => '开始日期不能晚于结束日期', 'count' => 0, 'data' => []];
        }

        $query = Db::name('crm_shipping_approval_task')->alias('t')
            ->join('crm_shipping_apply a', 'a.id = t.shipping_apply_id')
            ->where('t.approver_id', $adminId)
            ->where('t.status', 1)
            ->where('a.is_deleted', 0)
            ->whereRaw('t.approval_round = a.approval_round')
            ->where(function ($q) {
                $q->where(function ($q2) {
                    $q2->where('a.status', 10)->where('t.approval_type', 1);
                })->whereOr(function ($q2) {
                    $q2->where('a.status', 30)->where('t.approval_type', 2);
                });
            });

        if ($keyword !== '') {
            $query->where(function ($q) use ($keyword) {
                $q->whereLike('a.apply_no', '%' . $keyword . '%')
                    ->whereOr('a.order_no', 'like', '%' . $keyword . '%')
                    ->whereOr('a.customer_name', 'like', '%' . $keyword . '%')
                    ->whereOr('a.applicant_name', 'like', '%' . $keyword . '%');
            });
        }
        if ($startBound !== null) {
            $query->where('a.submit_time', '>=', $startBound);
        }
        if ($endBound !== null) {
            $query->where('a.submit_time', '<=', $endBound);
        }

        try {
            // 按申请去重计数，避免同申请多任务导致分页总数偏大
            $count = (int)(clone $query)->count('DISTINCT a.id');
            if ($count <= 0) {
                return [
                    'code' => 0,
                    'msg' => '获取成功',
                    'count' => 0,
                    'data' => [],
                ];
            }

            $rows = (clone $query)
                ->field(
                    'a.id,a.apply_no,a.order_no,a.customer_name,a.applicant_id,a.applicant_name,'
                    . 'a.order_amount,a.shipping_date,a.submit_time,a.status,a.approval_round,a.approval_route,'
                    . 'MIN(t.id) AS pending_task_id,COUNT(t.id) AS matched_task_cnt,'
                    . 'MAX(t.approval_type) AS matched_approval_type'
                )
                ->group('a.id')
                ->order('a.submit_time', 'desc')
                ->order('a.id', 'desc')
                ->page($page, $limit)
                ->select();
            $rows = (array)$rows;
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[ShippingApplyService::listPendingApplies] ' . $e->getMessage(), 'error');
            }
            return ['code' => -200, 'msg' => '列表加载失败，请稍后重试', 'count' => 0, 'data' => []];
        }

        $applyIds = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id > 0) {
                $applyIds[] = $id;
            }
            $matchedCnt = (int)($row['matched_task_cnt'] ?? 0);
            if ($matchedCnt > 1 && class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[ShippingApplyService::listPendingApplies] duplicate pending tasks'
                    . ' apply_id=' . $id
                    . ' approver_id=' . $adminId
                    . ' matched_task_cnt=' . $matchedCnt
                    . ' kept_task_id=' . (int)($row['pending_task_id'] ?? 0),
                    'error'
                );
            }
        }

        $summaryMap = $this->buildDraftProductSummaries($applyIds);

        $data = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            $status = (int)($row['status'] ?? 0);
            $data[] = [
                'id' => $id,
                'apply_no' => trim((string)($row['apply_no'] ?? '')),
                'order_no' => trim((string)($row['order_no'] ?? '')),
                'customer_name' => trim((string)($row['customer_name'] ?? '')),
                'applicant_id' => (int)($row['applicant_id'] ?? 0),
                'applicant_name' => trim((string)($row['applicant_name'] ?? '')),
                'order_amount' => round((float)($row['order_amount'] ?? 0), 2),
                'shipping_date' => trim((string)($row['shipping_date'] ?? '')),
                'submit_time' => trim((string)($row['submit_time'] ?? '')),
                'status' => $status,
                'status_text' => $this->mapApplyStatusText($status),
                'current_stage_text' => $this->mapPendingApprovalStageText($status),
                'approval_round' => (int)($row['approval_round'] ?? 0),
                'approval_route' => (int)($row['approval_route'] ?? 0),
                'approval_route_text' => $this->mapApprovalRouteText((int)($row['approval_route'] ?? 0)),
                'product_summary' => isset($summaryMap[$id]) ? $summaryMap[$id] : '',
                'pending_task_id' => (int)($row['pending_task_id'] ?? 0),
                'matched_approval_type' => (int)($row['matched_approval_type'] ?? 0),
                // 本轮只读入口；写操作属 M4-2B-2B，后端强制不可审
                'can_approve' => false,
            ];
        }

        return [
            'code' => 0,
            'msg' => '获取成功',
            'count' => $count,
            'data' => $data,
        ];
    }

    /**
     * 待我审核列表：当前审批阶段文案（仅 10/30 有效）
     *
     * @param int $status
     * @return string
     */
    public function mapPendingApprovalStageText($status)
    {
        $status = (int)$status;
        if ($status === 10) {
            return '产品经理审核';
        }
        if ($status === 30) {
            return '产品总监审核';
        }
        return '-';
    }

    /**
     * 统一发货申请只读详情（鉴权后返回；供页面与后续「待我审核」复用）
     *
     * @param mixed $applyId
     * @return array{code:int,msg:string,data:array}
     */
    public function getApplyDetailForView($applyId)
    {
        $admin = $this->getCurrentAdminInfo();
        $viewerId = (int)$admin['admin_id'];
        if ($viewerId <= 0) {
            return ['code' => -200, 'msg' => '请先登录', 'data' => []];
        }

        try {
            $applyId = $this->parsePositiveInt($applyId, self::APPLY_VIEW_DENY_MSG);
        } catch (Exception $e) {
            return ['code' => -200, 'msg' => self::APPLY_VIEW_DENY_MSG, 'data' => []];
        }

        try {
            $apply = Db::name('crm_shipping_apply')
                ->where('id', $applyId)
                ->find();
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[ShippingApplyService::getApplyDetailForView] ' . $e->getMessage(), 'error');
            }
            return ['code' => -200, 'msg' => '详情加载失败，请稍后重试', 'data' => []];
        }

        if (empty($apply) || !is_array($apply)) {
            return ['code' => -200, 'msg' => self::APPLY_VIEW_DENY_MSG, 'data' => []];
        }
        if ((int)($apply['is_deleted'] ?? 0) !== 0) {
            return ['code' => -200, 'msg' => self::APPLY_VIEW_DENY_MSG, 'data' => []];
        }
        $applyStatus = (int)($apply['status'] ?? -1);
        if (!in_array($applyStatus, self::HISTORICAL_SUBMIT_SUCCESS_STATUSES, true)) {
            return ['code' => -200, 'msg' => self::APPLY_VIEW_DENY_MSG, 'data' => []];
        }

        $access = $this->resolveApplyViewAccess($apply, $viewerId);
        if (empty($access['ok'])) {
            // 无权：不返回任何业务快照
            return ['code' => -200, 'msg' => self::APPLY_VIEW_DENY_MSG, 'data' => []];
        }

        $viewerRole = (string)($access['role'] ?? '');
        $canViewAllPurchase = !empty($access['can_view_all_purchase']);
        $viewerManagerId = (int)($access['viewer_manager_id'] ?? 0);
        $round = (int)($apply['approval_round'] ?? 0);

        try {
            $items = Db::name('crm_shipping_apply_item')
                ->where('shipping_apply_id', $applyId)
                ->field('id,shipping_apply_id,order_item_id,apply_line_no,product_name,apply_product_name,spec_model,apply_spec_model,apply_qty,apply_unit_price,apply_sales_amount,apply_purchase_unit_price,apply_purchase_amount,factory_id,factory_name,product_manager_id,product_manager_name,remark,unit')
                ->order('apply_line_no', 'asc')
                ->order('id', 'asc')
                ->select();
            $items = (array)$items;

            $tasks = [];
            if ($round > 0) {
                $tasks = Db::name('crm_shipping_approval_task')
                    ->where('shipping_apply_id', $applyId)
                    ->where('approval_round', $round)
                    ->field('id,shipping_apply_id,approval_round,approval_type,approver_id,approver_name,status,opinion,activate_time,audit_time,create_time')
                    ->order('approval_type', 'asc')
                    ->order('id', 'asc')
                    ->select();
                $tasks = (array)$tasks;
            }

            $logs = Db::name('crm_shipping_approval_log')
                ->where('shipping_apply_id', $applyId)
                ->field('id,shipping_apply_id,task_id,operator_id,operator_name,operator_role,action,before_status,after_status,opinion,create_time')
                ->order('id', 'asc')
                ->select();
            $logs = (array)$logs;
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[ShippingApplyService::getApplyDetailForView.load] ' . $e->getMessage(), 'error');
            }
            return ['code' => -200, 'msg' => '详情加载失败，请稍后重试', 'data' => []];
        }

        $itemRows = [];
        foreach ($items as $item) {
            if (!is_array($item)) {
                continue;
            }
            $lineManagerId = (int)($item['product_manager_id'] ?? 0);
            $canSeePurchase = $canViewAllPurchase
                || ($viewerRole === 'product_manager' && $viewerManagerId > 0 && $lineManagerId === $viewerManagerId);

            $purchaseUnit = null;
            $purchaseAmount = null;
            $purchaseMasked = false;
            if ($canSeePurchase) {
                $purchaseUnit = $item['apply_purchase_unit_price'] !== null && $item['apply_purchase_unit_price'] !== ''
                    ? round((float)$item['apply_purchase_unit_price'], 2)
                    : null;
                $purchaseAmount = $item['apply_purchase_amount'] !== null && $item['apply_purchase_amount'] !== ''
                    ? round((float)$item['apply_purchase_amount'], 2)
                    : null;
            } else {
                $purchaseMasked = true;
            }

            $productName = trim((string)($item['apply_product_name'] ?? ''));
            if ($productName === '') {
                $productName = trim((string)($item['product_name'] ?? ''));
            }
            $specModel = trim((string)($item['apply_spec_model'] ?? ''));
            if ($specModel === '') {
                $specModel = trim((string)($item['spec_model'] ?? ''));
            }

            $itemRows[] = [
                'id' => (int)($item['id'] ?? 0),
                'apply_line_no' => (int)($item['apply_line_no'] ?? 0),
                'source_product_name' => trim((string)($item['product_name'] ?? '')),
                'apply_product_name' => $productName,
                'apply_spec_model' => $specModel,
                'apply_qty' => (int)($item['apply_qty'] ?? 0),
                'unit' => trim((string)($item['unit'] ?? '')),
                'apply_unit_price' => $item['apply_unit_price'] !== null && $item['apply_unit_price'] !== ''
                    ? round((float)$item['apply_unit_price'], 2)
                    : null,
                'apply_sales_amount' => $item['apply_sales_amount'] !== null && $item['apply_sales_amount'] !== ''
                    ? round((float)$item['apply_sales_amount'], 2)
                    : null,
                'apply_purchase_unit_price' => $purchaseUnit,
                'apply_purchase_amount' => $purchaseAmount,
                'purchase_price_masked' => $purchaseMasked ? 1 : 0,
                'factory_id' => isset($item['factory_id']) && $item['factory_id'] !== null && $item['factory_id'] !== ''
                    ? (int)$item['factory_id']
                    : null,
                'factory_name' => trim((string)($item['factory_name'] ?? '')),
                'product_manager_id' => $lineManagerId > 0 ? $lineManagerId : null,
                'product_manager_name' => trim((string)($item['product_manager_name'] ?? '')),
                'remark' => trim((string)($item['remark'] ?? '')),
            ];
        }

        $progress = $this->buildApprovalProgress($apply, $tasks);
        $logRows = $this->formatApprovalLogsForView($logs, $viewerRole);

        $header = [
            'id' => (int)$apply['id'],
            'apply_no' => trim((string)($apply['apply_no'] ?? '')),
            'order_id' => (int)($apply['order_id'] ?? 0),
            'order_no' => trim((string)($apply['order_no'] ?? '')),
            'customer_name' => trim((string)($apply['customer_name'] ?? '')),
            'applicant_id' => (int)($apply['applicant_id'] ?? 0),
            'applicant_name' => trim((string)($apply['applicant_name'] ?? '')),
            'submit_time' => trim((string)($apply['submit_time'] ?? '')),
            'order_amount' => round((float)($apply['order_amount'] ?? 0), 2),
            'approval_threshold' => round((float)($apply['approval_threshold'] ?? 0), 2),
            'approval_route' => (int)($apply['approval_route'] ?? 0),
            'approval_route_text' => $this->mapApprovalRouteText((int)($apply['approval_route'] ?? 0)),
            'approval_round' => $round,
            'status' => $applyStatus,
            'status_text' => $this->mapApplyStatusText($applyStatus),
            'shipping_date' => trim((string)($apply['shipping_date'] ?? '')),
            'shipping_method' => trim((string)($apply['shipping_method'] ?? '')),
            'receiver_name' => trim((string)($apply['receiver_name'] ?? '')),
            'receiver_phone' => trim((string)($apply['receiver_phone'] ?? '')),
            'receiver_address' => trim((string)($apply['receiver_address'] ?? '')),
            'shipping_remark' => trim((string)($apply['shipping_remark'] ?? '')),
            'product_director_name' => trim((string)($apply['product_director_name'] ?? '')),
        ];

        return [
            'code' => 0,
            'msg' => '获取成功',
            'data' => [
                'apply' => $header,
                'items' => $itemRows,
                'approval_progress' => $progress,
                'approval_logs' => $logRows,
                // 预留给 M4-2B；本轮只读，后端强制 false，不信任前端
                'can_approve' => false,
                'viewer_role' => $viewerRole,
            ],
        ];
    }

    /**
     * 申请主状态文案
     *
     * @param int $status
     * @return string
     */
    public function mapApplyStatusText($status)
    {
        $status = (int)$status;
        $map = [
            0 => '草稿',
            10 => '待产品经理审核',
            20 => '产品经理驳回',
            30 => '待产品总监审核',
            40 => '产品总监驳回',
            50 => '审批通过',
            60 => '已取消',
        ];
        if (isset($map[$status])) {
            return $map[$status];
        }
        return '状态待确认(' . $status . ')';
    }

    /**
     * 审批路线文案
     *
     * @param int $route
     * @return string
     */
    public function mapApprovalRouteText($route)
    {
        $route = (int)$route;
        if ($route === 1) {
            return '产品经理审批';
        }
        if ($route === 2) {
            return '产品经理+产品总监审批';
        }
        return '路线待确认(' . $route . ')';
    }

    /**
     * 批量聚合当前页申请的当前审批人 / 下一阶段（避免 N+1；不 JOIN 分页）
     *
     * @param int[] $applyIds
     * @param array<int,int> $roundMap apply_id => approval_round
     * @param array $applyRows 当前页主表行（含 status/approval_route）
     * @return array<int,array{current_approver_text:string,next_stage_text:string}>
     */
    public function loadCurrentApproverMap(array $applyIds, array $roundMap, array $applyRows = [])
    {
        $applyIds = array_values(array_unique(array_filter(array_map('intval', $applyIds), function ($id) {
            return $id > 0;
        })));
        $result = [];
        foreach ($applyIds as $id) {
            $result[$id] = [
                'current_approver_text' => '',
                'next_stage_text' => '',
            ];
        }
        if (empty($applyIds)) {
            return $result;
        }

        $statusRouteMap = [];
        foreach ($applyRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $id = (int)($row['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $statusRouteMap[$id] = [
                'status' => (int)($row['status'] ?? 0),
                'approval_route' => (int)($row['approval_route'] ?? 0),
            ];
        }

        try {
            $tasks = Db::name('crm_shipping_approval_task')
                ->whereIn('shipping_apply_id', $applyIds)
                ->field('id,shipping_apply_id,approval_round,approval_type,approver_id,approver_name,status')
                ->order('id', 'asc')
                ->select();
            $tasks = (array)$tasks;
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[ShippingApplyService::loadCurrentApproverMap] ' . $e->getMessage(), 'error');
            }
            return $result;
        }

        /** @var array<int,array<int,string>> $currentNames */
        $currentNames = [];
        /** @var array<int,array<int,string>> $nextNames */
        $nextNames = [];

        foreach ($tasks as $task) {
            if (!is_array($task)) {
                continue;
            }
            $applyId = (int)($task['shipping_apply_id'] ?? 0);
            if ($applyId <= 0 || !isset($roundMap[$applyId])) {
                continue;
            }
            $taskRound = (int)($task['approval_round'] ?? 0);
            if ($taskRound !== (int)$roundMap[$applyId]) {
                continue;
            }

            $name = trim((string)($task['approver_name'] ?? ''));
            if ($name === '') {
                $aid = (int)($task['approver_id'] ?? 0);
                $name = $aid > 0 ? ('用户#' . $aid) : '未知审批人';
            }
            $taskStatus = (int)($task['status'] ?? -1);
            $approvalType = (int)($task['approval_type'] ?? 0);
            $approverId = (int)($task['approver_id'] ?? 0);

            $applyMeta = isset($statusRouteMap[$applyId]) ? $statusRouteMap[$applyId] : null;
            $applyStatus = $applyMeta ? (int)$applyMeta['status'] : -1;
            // 主状态与任务类型必须匹配：10→经理(1)，30→总监(2)；驳回/通过/取消/草稿/未知 → 无当前审批人
            $allowedApprovalType = 0;
            if ($applyStatus === 10) {
                $allowedApprovalType = 1;
            } elseif ($applyStatus === 30) {
                $allowedApprovalType = 2;
            }

            // 当前审批人：本轮 + 已激活待审 + 类型与主状态一致
            if ($taskStatus === 1 && $allowedApprovalType > 0 && $approvalType === $allowedApprovalType) {
                if (!isset($currentNames[$applyId])) {
                    $currentNames[$applyId] = [];
                }
                $dedupeKey = $approverId > 0 ? ('id:' . $approverId) : ('name:' . $name);
                if (!isset($currentNames[$applyId][$dedupeKey])) {
                    $currentNames[$applyId][$dedupeKey] = $name;
                }
            }

            // 下一阶段：仅 status=10 + 高金额路线 + 本轮总监预创建未激活（驳回/取消/完成不展示）
            if ($applyMeta
                && $applyStatus === 10
                && (int)$applyMeta['approval_route'] === 2
                && $approvalType === 2
                && $taskStatus === 0
            ) {
                if (!isset($nextNames[$applyId])) {
                    $nextNames[$applyId] = [];
                }
                $dedupeKey = $approverId > 0 ? ('id:' . $approverId) : ('name:' . $name);
                if (!isset($nextNames[$applyId][$dedupeKey])) {
                    $nextNames[$applyId][$dedupeKey] = $name . '（未激活）';
                }
            }
        }

        foreach ($applyIds as $id) {
            $curr = isset($currentNames[$id]) ? array_values($currentNames[$id]) : [];
            $next = isset($nextNames[$id]) ? array_values($nextNames[$id]) : [];
            $result[$id] = [
                'current_approver_text' => implode('、', $curr),
                'next_stage_text' => empty($next) ? '' : ('下一阶段：' . implode('、', $next)),
            ];
        }

        return $result;
    }

    /**
     * 组装当前轮审批进度（只读展示）
     *
     * @param array $apply
     * @param array $tasks 已过滤为本轮的任务
     * @return array
     */
    public function buildApprovalProgress(array $apply, array $tasks)
    {
        $route = (int)($apply['approval_route'] ?? 0);
        $status = (int)($apply['status'] ?? 0);
        $round = (int)($apply['approval_round'] ?? 0);

        $pmTasks = [];
        $directorTasks = [];
        foreach ($tasks as $task) {
            if (!is_array($task)) {
                continue;
            }
            $type = (int)($task['approval_type'] ?? 0);
            $node = [
                'task_id' => (int)($task['id'] ?? 0),
                'approver_id' => (int)($task['approver_id'] ?? 0),
                'approver_name' => trim((string)($task['approver_name'] ?? '')),
                'status' => (int)($task['status'] ?? -1),
                'status_text' => $this->mapApprovalTaskStatusText((int)($task['status'] ?? -1)),
                'activate_time' => trim((string)($task['activate_time'] ?? '')),
                'audit_time' => trim((string)($task['audit_time'] ?? '')),
            ];
            if ($type === 1) {
                $pmTasks[] = $node;
            } elseif ($type === 2) {
                $directorTasks[] = $node;
            }
        }

        $stages = [];
        $stages[] = [
            'key' => 'submit',
            'title' => '业务员正式提交',
            'done' => true,
            'nodes' => [
                [
                    'approver_name' => trim((string)($apply['applicant_name'] ?? '')),
                    'status_text' => '已提交',
                    'time' => trim((string)($apply['submit_time'] ?? '')),
                ],
            ],
        ];
        $stages[] = [
            'key' => 'product_manager',
            'title' => '产品经理审核',
            'done' => in_array($status, [30, 50], true) || ($status === 20),
            'active' => $status === 10,
            'rejected' => $status === 20,
            'nodes' => $pmTasks,
        ];
        if ($route === 2) {
            $stages[] = [
                'key' => 'product_director',
                'title' => '产品总监审核',
                'done' => $status === 50 || $status === 40,
                'active' => $status === 30,
                'rejected' => $status === 40,
                'nodes' => $directorTasks,
            ];
        }
        $stages[] = [
            'key' => 'finished',
            'title' => '审批完成',
            'done' => $status === 50,
            'active' => false,
            'nodes' => [],
        ];

        return [
            'approval_round' => $round,
            'approval_route' => $route,
            'apply_status' => $status,
            'apply_status_text' => $this->mapApplyStatusText($status),
            'stages' => $stages,
        ];
    }

    /**
     * 审批任务状态文案（仅确认 0/1；其余不猜测）
     *
     * @param int $taskStatus
     * @return string
     */
    public function mapApprovalTaskStatusText($taskStatus)
    {
        $taskStatus = (int)$taskStatus;
        if ($taskStatus === 0) {
            return '未激活';
        }
        if ($taskStatus === 1) {
            return '待审核';
        }
        return '状态待确认(' . $taskStatus . ')';
    }

    /**
     * 整理审批历史为安全可读记录（过滤内部替代授权审计；不输出原始 opinion JSON）
     *
     * @param array $logs
     * @param string $viewerRole
     * @return array
     */
    public function formatApprovalLogsForView(array $logs, $viewerRole = '')
    {
        $viewerRole = (string)$viewerRole;
        $rows = [];
        foreach ($logs as $log) {
            if (!is_array($log)) {
                continue;
            }
            $action = trim((string)($log['action'] ?? ''));
            if ($action === '') {
                continue;
            }

            // 内部管理审计：本轮不对普通查看者展示原文
            if (strpos($action, 'pm_substitute_') === 0
                || $action === 'submit_finance_cc_anomaly'
                || $action === 'delete_draft'
            ) {
                continue;
            }

            $summary = $this->buildPublicApprovalLogSummary($action, $log);
            if ($summary === null) {
                // 未知动作：仅显示动作码，不展开 opinion
                $summary = [
                    'action_text' => '操作记录(' . $action . ')',
                    'opinion_text' => '',
                ];
            }

            $rows[] = [
                'id' => (int)($log['id'] ?? 0),
                'action' => $action,
                'action_text' => (string)$summary['action_text'],
                'operator_name' => trim((string)($log['operator_name'] ?? '')),
                'operator_role' => trim((string)($log['operator_role'] ?? '')),
                'before_status' => isset($log['before_status']) && $log['before_status'] !== null && $log['before_status'] !== ''
                    ? (int)$log['before_status']
                    : null,
                'after_status' => isset($log['after_status']) && $log['after_status'] !== null && $log['after_status'] !== ''
                    ? (int)$log['after_status']
                    : null,
                'before_status_text' => isset($log['before_status']) && $log['before_status'] !== null && $log['before_status'] !== ''
                    ? $this->mapApplyStatusText((int)$log['before_status'])
                    : '',
                'after_status_text' => isset($log['after_status']) && $log['after_status'] !== null && $log['after_status'] !== ''
                    ? $this->mapApplyStatusText((int)$log['after_status'])
                    : '',
                'opinion_text' => (string)$summary['opinion_text'],
                'create_time' => trim((string)($log['create_time'] ?? '')),
            ];
        }

        return $rows;
    }

    /**
     * 解析列表日期边界（仅允许 Y-m-d）
     *
     * @param string $date
     * @param bool $isStart true→00:00:00；false→23:59:59
     * @return string|null
     */
    protected function parseMyApplyDateBound($date, $isStart)
    {
        $date = trim((string)$date);
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) {
            return null;
        }
        $dt = \DateTime::createFromFormat('Y-m-d', $date);
        if (!$dt || $dt->format('Y-m-d') !== $date) {
            return null;
        }
        return $isStart ? ($date . ' 00:00:00') : ($date . ' 23:59:59');
    }

    /**
     * 统一详情查看鉴权（申请人 / 本轮激活审批人；超管本轮不额外放行）
     *
     * @param array $apply
     * @param int $viewerId
     * @return array{ok:bool,role:string,can_view_all_purchase:bool,viewer_manager_id:int}
     */
    protected function resolveApplyViewAccess(array $apply, $viewerId)
    {
        $deny = [
            'ok' => false,
            'role' => '',
            'can_view_all_purchase' => false,
            'viewer_manager_id' => 0,
        ];
        $viewerId = (int)$viewerId;
        if ($viewerId <= 0) {
            return $deny;
        }

        if ((int)($apply['applicant_id'] ?? 0) === $viewerId) {
            return [
                'ok' => true,
                'role' => 'applicant',
                'can_view_all_purchase' => true,
                'viewer_manager_id' => 0,
            ];
        }

        $applyId = (int)($apply['id'] ?? 0);
        $round = (int)($apply['approval_round'] ?? 0);
        $status = (int)($apply['status'] ?? -1);
        if ($applyId <= 0 || $round <= 0) {
            return $deny;
        }

        // 产品经理：本轮激活任务 + 申请处于待经理审核
        if ($status === 10) {
            try {
                $pmTask = Db::name('crm_shipping_approval_task')
                    ->where('shipping_apply_id', $applyId)
                    ->where('approval_round', $round)
                    ->where('approver_id', $viewerId)
                    ->where('approval_type', 1)
                    ->where('status', 1)
                    ->field('id')
                    ->find();
            } catch (\Throwable $e) {
                return $deny;
            }
            if (!empty($pmTask)) {
                return [
                    'ok' => true,
                    'role' => 'product_manager',
                    'can_view_all_purchase' => false,
                    'viewer_manager_id' => $viewerId,
                ];
            }
        }

        // 产品总监：本轮激活任务 + 申请处于待总监审核；预创建未激活不得越权
        if ($status === 30) {
            try {
                $dirTask = Db::name('crm_shipping_approval_task')
                    ->where('shipping_apply_id', $applyId)
                    ->where('approval_round', $round)
                    ->where('approver_id', $viewerId)
                    ->where('approval_type', 2)
                    ->where('status', 1)
                    ->field('id')
                    ->find();
            } catch (\Throwable $e) {
                return $deny;
            }
            if (!empty($dirTask)) {
                return [
                    'ok' => true,
                    'role' => 'product_director',
                    // 总监审整单；采购价对其开放（仍禁止未激活预创建任务）
                    'can_view_all_purchase' => true,
                    'viewer_manager_id' => 0,
                ];
            }
        }

        // 超管本轮不额外开放跨人查看
        return $deny;
    }

    /**
     * @param string $action
     * @param array $log
     * @return array{action_text:string,opinion_text:string}|null
     */
    protected function buildPublicApprovalLogSummary($action, array $log)
    {
        $opinionRaw = trim((string)($log['opinion'] ?? ''));
        if ($action === 'submit') {
            $note = '发货申请已正式提交';
            if ($opinionRaw !== '' && $opinionRaw[0] === '{') {
                $decoded = json_decode($opinionRaw, true);
                if (is_array($decoded) && isset($decoded['note']) && is_string($decoded['note'])) {
                    $noteText = trim($decoded['note']);
                    if ($noteText !== '') {
                        $note = $noteText;
                    }
                }
            } elseif ($opinionRaw !== '' && $opinionRaw[0] !== '{') {
                // 非 JSON 意见：截断后作为纯文本
                $note = mb_substr($opinionRaw, 0, 200, 'UTF-8');
            }
            return [
                'action_text' => '正式提交',
                'opinion_text' => $note,
            ];
        }

        // 预留：未来审批通过/驳回（M4-2B）；本轮仅安全展示，不猜字段契约
        if ($action === 'approve' || $action === 'pm_approve' || $action === 'director_approve') {
            $text = $this->extractPlainOpinionText($opinionRaw);
            return [
                'action_text' => '审核通过',
                'opinion_text' => $text,
            ];
        }
        if ($action === 'reject' || $action === 'pm_reject' || $action === 'director_reject') {
            $text = $this->extractPlainOpinionText($opinionRaw);
            return [
                'action_text' => '审核驳回',
                'opinion_text' => $text,
            ];
        }

        return null;
    }

    /**
     * 从 opinion 提取对用户安全的纯文本（JSON 仅取常见可读键；禁止整段 JSON 原样输出）
     *
     * @param string $opinionRaw
     * @return string
     */
    protected function extractPlainOpinionText($opinionRaw)
    {
        $opinionRaw = trim((string)$opinionRaw);
        if ($opinionRaw === '') {
            return '';
        }
        if ($opinionRaw[0] === '{') {
            $decoded = json_decode($opinionRaw, true);
            if (!is_array($decoded)) {
                return '';
            }
            foreach (['opinion', 'reason', 'note', 'comment'] as $key) {
                if (isset($decoded[$key]) && is_string($decoded[$key])) {
                    $t = trim($decoded[$key]);
                    if ($t !== '') {
                        return mb_substr($t, 0, 500, 'UTF-8');
                    }
                }
            }
            return '';
        }
        return mb_substr($opinionRaw, 0, 500, 'UTF-8');
    }
}
