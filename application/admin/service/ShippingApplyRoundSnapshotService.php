<?php

namespace app\admin\service;

use think\Db;
use think\Exception;

/**
 * 发货申请审批轮次不可变业务快照（M4-2C-2B-2A）
 *
 * 职责：
 * - 构建/规范化 Round submit 快照 JSON
 * - 计算快照 SHA-256（与 submit_payload_hash 用途不同）
 * - 在外部已开启的同一 MySQL 事务内 INSERT
 * - 按申请+轮次只读读取与完整性核验
 * - 为正式提交幂等 / COMMIT 不确定恢复提供快照证据判断
 *
 * 不提供普通业务 UPDATE/DELETE；不自行 startTrans/commit/rollback。
 * 不可变性为应用层保护，不宣称数据库物理不可篡改。
 */
class ShippingApplyRoundSnapshotService
{
    const SNAPSHOT_STAGE_SUBMIT = 'submit';
    const SNAPSHOT_SCHEMA_VERSION = 1;

    /**
     * 本地已知基线参考时刻（Asia/Shanghai）。
     *
     * 不再作为所有环境的自动门禁默认值。
     * 仅当：无显式注入 / 无环境变量 / 无配置键 / 本库尚无任何 submit 快照落库证据，
     * 且既有本地测试开关 `shipping_submit.enabled === true` 时，
     * 才作为**受限本地兼容基线**使用（非请求级机制版本标识，不得用于自动补存）。
     *
     * 生产姿态（enabled !== true）在无显式边界且无法从本库派生时 → fail-closed。
     * 跨环境必须以 env/config 显式配置，或待本库产生最早 submit 快照后自动派生。
     */
    const MECHANISM_EFFECTIVE_AT = '2026-11-24 15:00:00';

    const EVIDENCE_VALID_SNAPSHOT = 'valid_snapshot';
    const EVIDENCE_LEGACY_COMPATIBLE = 'legacy_compatible';
    const EVIDENCE_MISSING_REQUIRED = 'missing_required';
    const EVIDENCE_INVALID = 'invalid';
    const EVIDENCE_ABSENT = 'absent';

    const BOUNDARY_SOURCE_OVERRIDE = 'override';
    const BOUNDARY_SOURCE_ENV = 'env';
    const BOUNDARY_SOURCE_CONFIG = 'config';
    const BOUNDARY_SOURCE_DERIVED = 'derived_snapshot_table';
    const BOUNDARY_SOURCE_LOCAL_BASELINE = 'local_baseline_constant';
    const BOUNDARY_SOURCE_NONE = 'none';

    /**
     * 机制切换边界显式覆盖。
     * false=未覆盖；null=强制无边界（fail-closed）；string=合法 datetime。
     *
     * @var string|null|false
     */
    protected $mechanismEffectiveAtOverride = false;

    /** @var string 最近一次 resolve 的边界来源（审计/测试用） */
    protected $lastBoundarySource = self::BOUNDARY_SOURCE_NONE;

    /** @var string[] header_json 固定键序 */
    const HEADER_KEYS = [
        'shipping_apply_id',
        'apply_no',
        'order_id',
        'order_no',
        'applicant_id',
        'applicant_name',
        'team_name',
        'team_leader_id',
        'team_leader_name',
        'customer_name',
        'customer_phone',
        'order_amount',
        'approval_threshold',
        'approval_route',
        'product_director_id',
        'product_director_name',
        'finance_cc_user_id',
        'finance_cc_user_name',
        'receiver_name',
        'receiver_phone',
        'receiver_address',
        'shipping_date',
        'shipping_method',
        'shipping_remark',
        'approval_round',
        'submit_time',
        'status_at_submit',
        'current_stage_at_submit',
        'submit_payload_hash',
    ];

    /** @var string[] items_json 单行固定键序 */
    const ITEM_KEYS = [
        'shipping_apply_item_id',
        'order_item_id',
        'order_id',
        'line_no',
        'apply_line_no',
        'product_id',
        'product_name',
        'spec_model',
        'unit',
        'order_qty',
        'apply_product_name',
        'apply_spec_model',
        'apply_qty',
        'unit_price',
        'total_price',
        'order_purchase_amount',
        'apply_unit_price',
        'apply_purchase_unit_price',
        'apply_sales_amount',
        'apply_purchase_amount',
        'factory_id',
        'factory_name',
        'supplier_id',
        'supplier_name',
        'product_manager_id',
        'product_manager_name',
        'remark',
    ];

    /** JSON 编码标志（PHP 7.4；禁止使用不存在的 JSON_INVALID_UTF8_ERROR） */
    const JSON_FLAGS = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR;

    /**
     * 从主表行构建 header（固定键序；不含 submit_token）
     *
     * 正式提交路径专用：要求参数与主表当前状态均为 status=10 / current_stage=1。
     *
     * @param array $applyRow
     * @param int $statusAtSubmit 正式提交当时状态（首轮应为 10）
     * @param int $currentStageAtSubmit 正式提交当时阶段（首轮应为 1）
     * @return array
     * @throws Exception
     */
    public function buildHeaderFromApplyRow(array $applyRow, $statusAtSubmit, $currentStageAtSubmit)
    {
        return $this->buildHeaderInternal($applyRow, $statusAtSubmit, $currentStageAtSubmit, true);
    }

    /**
     * 【历史补存专用】从已通过证据核验的主表行构建 header。
     *
     * 不检查主表当前 status/current_stage（允许 20/0 等驳回态）。
     * 仍强制快照内 status_at_submit=10 且 current_stage_at_submit=1。
     * 禁止用于正式提交 / createSubmitRoundSnapshotInCurrentTx。
     *
     * @param array $applyRow 当前主表行（业务载荷须已由补存证据核验）
     * @param int $statusAtSubmit 证据证明的提交当时状态（必须为 10）
     * @param int $currentStageAtSubmit 证据证明的提交当时阶段（必须为 1）
     * @return array
     * @throws Exception
     */
    public function buildHeaderFromHistoricalSubmitEvidence(array $applyRow, $statusAtSubmit, $currentStageAtSubmit)
    {
        return $this->buildHeaderInternal($applyRow, $statusAtSubmit, $currentStageAtSubmit, false);
    }

    /**
     * 【历史补存专用】在外部事务内写入预构建的 submit 快照（不自行 COMMIT/ROLLBACK）。
     *
     * 调用方须已完成证据核验；本方法不再调用 buildHeaderFromApplyRow（避免放宽正式路径门禁）。
     * 禁止 REPLACE / UPDATE；唯一冲突仅在内容完全一致时视为可接受幂等。
     *
     * @param int $applyId
     * @param int $round
     * @param int $operatorId
     * @param array $header
     * @param array $items
     * @return array
     * @throws Exception
     */
    public function insertPrebuiltSubmitRoundSnapshotInCurrentTx(
        $applyId,
        $round,
        $operatorId,
        array $header,
        array $items
    ) {
        $applyId = (int)$applyId;
        $round = (int)$round;
        $operatorId = (int)$operatorId;
        if ($applyId <= 0) {
            throw new Exception('历史补存快照申请 ID 无效');
        }
        if ($round < 1) {
            throw new Exception('历史补存快照审批轮次无效');
        }
        if ($operatorId <= 0) {
            throw new Exception('历史补存快照操作人无效');
        }
        if (empty($items)) {
            throw new Exception('历史补存快照明细不能为空');
        }

        // 身份与提交态硬门禁（即使调用方已验，仍二次断言）
        if ((int)($header['shipping_apply_id'] ?? 0) !== $applyId) {
            throw new Exception('历史补存 header 申请 ID 不一致');
        }
        if ((int)($header['approval_round'] ?? 0) !== $round) {
            throw new Exception('历史补存 header 审批轮次不一致');
        }
        if ((int)($header['status_at_submit'] ?? -1) !== 10
            || (int)($header['current_stage_at_submit'] ?? -1) !== 1
        ) {
            throw new Exception('历史补存快照提交态必须为 10/1');
        }

        $hashPack = $this->computeSubmitSnapshotHash(
            $header,
            $items,
            $applyId,
            $round,
            self::SNAPSHOT_STAGE_SUBMIT,
            self::SNAPSHOT_SCHEMA_VERSION
        );
        $headerJson = $this->encodeCanonicalJson($header);
        $itemsJson = $this->encodeCanonicalJson($items);
        $now = date('Y-m-d H:i:s');

        $insert = [
            'shipping_apply_id' => $applyId,
            'approval_round' => $round,
            'snapshot_stage' => self::SNAPSHOT_STAGE_SUBMIT,
            'snapshot_schema_version' => self::SNAPSHOT_SCHEMA_VERSION,
            'header_json' => $headerJson,
            'items_json' => $itemsJson,
            'payload_hash' => $hashPack['hash'],
            'operator_id' => $operatorId,
            'create_time' => $now,
        ];

        try {
            $snapshotId = (int)Db::name('crm_shipping_apply_round_snapshot')->insertGetId($insert);
        } catch (\Throwable $e) {
            if ($this->isSnapshotUniqueConflict($e)) {
                $existing = $this->findSubmitRoundSnapshot($applyId, $round);
                if ($existing === null) {
                    throw new Exception('快照唯一索引冲突且无法读取已有记录');
                }
                $this->assertStoredSnapshotMatchesExpected($existing, $hashPack['hash'], $headerJson, $itemsJson);
                return $existing;
            }
            throw new Exception('历史补存快照写入失败：' . $e->getMessage(), 0, $e);
        }

        if ($snapshotId <= 0) {
            throw new Exception('历史补存快照写入失败');
        }

        $stored = $this->findSubmitRoundSnapshotById($snapshotId);
        if ($stored === null) {
            throw new Exception('历史补存快照写入后回读失败');
        }
        $this->assertStoredSnapshotIntegrity($stored);
        if (!hash_equals(strtolower((string)$stored['payload_hash']), $hashPack['hash'])) {
            throw new Exception('历史补存快照写入后 hash 校验失败');
        }
        if ((string)$stored['header_json'] !== $headerJson || (string)$stored['items_json'] !== $itemsJson) {
            throw new Exception('历史补存快照写入后 JSON 内容校验失败');
        }

        return $stored;
    }

    /**
     * @param array $applyRow
     * @param int $statusAtSubmit
     * @param int $currentStageAtSubmit
     * @param bool $requireCurrentSubmitState true=正式提交路径；false=历史补存路径
     * @return array
     * @throws Exception
     */
    protected function buildHeaderInternal(
        array $applyRow,
        $statusAtSubmit,
        $currentStageAtSubmit,
        $requireCurrentSubmitState
    ) {
        $applyId = (int)($applyRow['id'] ?? 0);
        if ($applyId <= 0) {
            throw new Exception('快照主表申请 ID 无效');
        }

        $applyNo = trim((string)($applyRow['apply_no'] ?? ''));
        if ($applyNo === '') {
            throw new Exception('快照缺少发货申请编号');
        }

        $orderId = (int)($applyRow['order_id'] ?? 0);
        if ($orderId <= 0) {
            throw new Exception('快照缺少原订单 ID');
        }

        $round = (int)($applyRow['approval_round'] ?? 0);
        if ($round < 1) {
            throw new Exception('快照审批轮次无效');
        }

        $submitTime = trim((string)($applyRow['submit_time'] ?? ''));
        if ($submitTime === '') {
            throw new Exception('快照缺少正式提交时间');
        }

        $payloadHash = strtolower(trim((string)($applyRow['submit_payload_hash'] ?? '')));
        if ($payloadHash === '' || strlen($payloadHash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $payloadHash)) {
            throw new Exception('快照缺少有效 submit_payload_hash');
        }

        $statusAtSubmit = (int)$statusAtSubmit;
        $currentStageAtSubmit = (int)$currentStageAtSubmit;
        if ($statusAtSubmit !== 10 || $currentStageAtSubmit !== 1) {
            // 本阶段仅支持首次正式提交快照；禁止用后续驳回态冒充提交态
            throw new Exception('提交快照状态/阶段与正式提交事务不一致');
        }
        if ($requireCurrentSubmitState) {
            if ((int)($applyRow['status'] ?? -1) !== 10 || (int)($applyRow['current_stage'] ?? -1) !== 1) {
                throw new Exception('主表当前状态与正式提交快照要求不一致');
            }
        }

        $header = [
            'shipping_apply_id' => $applyId,
            'apply_no' => $applyNo,
            'order_id' => $orderId,
            'order_no' => (string)($applyRow['order_no'] ?? ''),
            'applicant_id' => (int)($applyRow['applicant_id'] ?? 0),
            'applicant_name' => (string)($applyRow['applicant_name'] ?? ''),
            'team_name' => (string)($applyRow['team_name'] ?? ''),
            'team_leader_id' => $this->normalizeNullablePositiveInt($applyRow['team_leader_id'] ?? null),
            'team_leader_name' => (string)($applyRow['team_leader_name'] ?? ''),
            'customer_name' => (string)($applyRow['customer_name'] ?? ''),
            'customer_phone' => $this->normalizeNullableString($applyRow['customer_phone'] ?? null),
            'order_amount' => $this->normalizeRequiredMoney($applyRow['order_amount'] ?? null, 'order_amount'),
            'approval_threshold' => $this->normalizeRequiredMoney($applyRow['approval_threshold'] ?? null, 'approval_threshold'),
            'approval_route' => (int)($applyRow['approval_route'] ?? 0),
            'product_director_id' => $this->normalizeNullablePositiveInt($applyRow['product_director_id'] ?? null),
            'product_director_name' => (string)($applyRow['product_director_name'] ?? ''),
            'finance_cc_user_id' => $this->normalizeNullablePositiveInt($applyRow['finance_cc_user_id'] ?? null),
            'finance_cc_user_name' => (string)($applyRow['finance_cc_user_name'] ?? ''),
            'receiver_name' => (string)($applyRow['receiver_name'] ?? ''),
            'receiver_phone' => (string)($applyRow['receiver_phone'] ?? ''),
            'receiver_address' => (string)($applyRow['receiver_address'] ?? ''),
            'shipping_date' => $this->normalizeNullableString($applyRow['shipping_date'] ?? null),
            'shipping_method' => (string)($applyRow['shipping_method'] ?? ''),
            'shipping_remark' => (string)($applyRow['shipping_remark'] ?? ''),
            'approval_round' => $round,
            'submit_time' => $submitTime,
            'status_at_submit' => $statusAtSubmit,
            'current_stage_at_submit' => $currentStageAtSubmit,
            'submit_payload_hash' => $payloadHash,
        ];

        return $this->orderAssocByKeys($header, self::HEADER_KEYS);
    }

    /**
     * 从明细行构建 items（稳定排序 + 固定键序）
     *
     * @param array $itemRows
     * @return array
     * @throws Exception
     */
    public function buildItemsFromItemRows(array $itemRows)
    {
        if (empty($itemRows)) {
            throw new Exception('快照明细不能为空');
        }

        $sorted = $this->sortItemRowsStable($itemRows);
        $items = [];
        foreach ($sorted as $row) {
            if (!is_array($row)) {
                throw new Exception('快照明细行格式无效');
            }
            $itemId = (int)($row['id'] ?? 0);
            if ($itemId <= 0) {
                throw new Exception('快照明细缺少 shipping_apply_item_id');
            }
            $orderItemId = (int)($row['order_item_id'] ?? 0);
            if ($orderItemId <= 0) {
                throw new Exception('快照明细缺少 order_item_id');
            }

            $item = [
                'shipping_apply_item_id' => $itemId,
                'order_item_id' => $orderItemId,
                'order_id' => (int)($row['order_id'] ?? 0),
                'line_no' => (int)($row['line_no'] ?? 0),
                'apply_line_no' => (int)($row['apply_line_no'] ?? 0),
                'product_id' => $this->normalizeNullableString($row['product_id'] ?? null),
                'product_name' => (string)($row['product_name'] ?? ''),
                'spec_model' => $this->normalizeNullableString($row['spec_model'] ?? null),
                'unit' => $this->normalizeNullableString($row['unit'] ?? null),
                'order_qty' => (int)($row['order_qty'] ?? 0),
                'apply_product_name' => $this->normalizeNullableString($row['apply_product_name'] ?? null),
                'apply_spec_model' => $this->normalizeNullableString($row['apply_spec_model'] ?? null),
                'apply_qty' => $this->normalizeRequiredPositiveInt($row['apply_qty'] ?? null, 'apply_qty'),
                'unit_price' => $this->normalizeRequiredMoney($row['unit_price'] ?? null, 'unit_price'),
                'total_price' => $this->normalizeRequiredMoney($row['total_price'] ?? null, 'total_price'),
                'order_purchase_amount' => $this->normalizeNullableMoney($row['order_purchase_amount'] ?? null),
                'apply_unit_price' => $this->normalizeNullableMoney($row['apply_unit_price'] ?? null),
                'apply_purchase_unit_price' => $this->normalizeNullableMoney($row['apply_purchase_unit_price'] ?? null),
                'apply_sales_amount' => $this->normalizeNullableMoney($row['apply_sales_amount'] ?? null),
                'apply_purchase_amount' => $this->normalizeNullableMoney($row['apply_purchase_amount'] ?? null),
                'factory_id' => $this->normalizeNullablePositiveInt($row['factory_id'] ?? null),
                'factory_name' => $this->normalizeNullableString($row['factory_name'] ?? null),
                'supplier_id' => $this->normalizeNullableString($row['supplier_id'] ?? null),
                'supplier_name' => $this->normalizeNullableString($row['supplier_name'] ?? null),
                'product_manager_id' => $this->normalizeNullablePositiveInt($row['product_manager_id'] ?? null),
                'product_manager_name' => (string)($row['product_manager_name'] ?? ''),
                'remark' => (string)($row['remark'] ?? ''),
            ];
            $items[] = $this->orderAssocByKeys($item, self::ITEM_KEYS);
        }

        return $items;
    }

    /**
     * 产品明细稳定排序：apply_line_no ASC → order_item_id ASC → id ASC
     *
     * @param array $itemRows
     * @return array
     */
    public function sortItemRowsStable(array $itemRows)
    {
        $rows = array_values($itemRows);
        usort($rows, function ($a, $b) {
            $a = is_array($a) ? $a : [];
            $b = is_array($b) ? $b : [];
            $la = (int)($a['apply_line_no'] ?? 0);
            $lb = (int)($b['apply_line_no'] ?? 0);
            if ($la !== $lb) {
                return $la < $lb ? -1 : 1;
            }
            $oa = (int)($a['order_item_id'] ?? 0);
            $ob = (int)($b['order_item_id'] ?? 0);
            if ($oa !== $ob) {
                return $oa < $ob ? -1 : 1;
            }
            $ia = (int)($a['id'] ?? 0);
            $ib = (int)($b['id'] ?? 0);
            if ($ia === $ib) {
                return 0;
            }
            return $ia < $ib ? -1 : 1;
        });
        return $rows;
    }

    /**
     * 组装规范化 hash 载荷（固定顶层键序）
     *
     * @param int $schemaVersion
     * @param string $stage
     * @param int $applyId
     * @param int $round
     * @param array $header
     * @param array $items
     * @return array
     */
    public function buildCanonicalPayload($schemaVersion, $stage, $applyId, $round, array $header, array $items)
    {
        return [
            'snapshot_schema_version' => (int)$schemaVersion,
            'snapshot_stage' => (string)$stage,
            'shipping_apply_id' => (int)$applyId,
            'approval_round' => (int)$round,
            'header' => $header,
            'items' => $items,
        ];
    }

    /**
     * 规范化 JSON 编码；失败抛异常（含非法 UTF-8）
     *
     * @param mixed $data
     * @return string
     * @throws Exception
     */
    public function encodeCanonicalJson($data)
    {
        try {
            $json = json_encode($data, self::JSON_FLAGS);
        } catch (\JsonException $e) {
            throw new Exception('快照 JSON 编码失败：' . $e->getMessage(), 0, $e);
        }
        if (!is_string($json) || $json === '') {
            throw new Exception('快照 JSON 编码结果为空');
        }
        return $json;
    }

    /**
     * 计算快照 payload_hash（64 位小写 hex）
     *
     * @param array $canonicalPayload
     * @return array{hash:string,canonical_json:string}
     * @throws Exception
     */
    public function computePayloadHashFromCanonical(array $canonicalPayload)
    {
        $canonicalJson = $this->encodeCanonicalJson($canonicalPayload);
        $hash = hash('sha256', $canonicalJson);
        if (!is_string($hash) || strlen($hash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $hash)) {
            throw new Exception('快照 hash 生成失败');
        }
        return [
            'hash' => $hash,
            'canonical_json' => $canonicalJson,
        ];
    }

    /**
     * 由 header/items 计算 hash
     *
     * @param array $header
     * @param array $items
     * @param int $applyId
     * @param int $round
     * @param string $stage
     * @param int $schemaVersion
     * @return array{hash:string,canonical_json:string,payload:array}
     * @throws Exception
     */
    public function computeSubmitSnapshotHash(
        array $header,
        array $items,
        $applyId,
        $round,
        $stage = self::SNAPSHOT_STAGE_SUBMIT,
        $schemaVersion = self::SNAPSHOT_SCHEMA_VERSION
    ) {
        $payload = $this->buildCanonicalPayload($schemaVersion, $stage, $applyId, $round, $header, $items);
        $pack = $this->computePayloadHashFromCanonical($payload);
        return [
            'hash' => $pack['hash'],
            'canonical_json' => $pack['canonical_json'],
            'payload' => $payload,
        ];
    }

    /**
     * 在当前外部事务内创建 Round submit 快照（不自行 COMMIT/ROLLBACK）
     *
     * @param int $applyId
     * @param int $operatorId
     * @param array|null $applyRow 事务内已写入的主表行；null 则按 id 再读
     * @return array 已写入并核验的快照行（含解码后的 header/items 可选）
     * @throws Exception
     */
    public function createSubmitRoundSnapshotInCurrentTx($applyId, $operatorId, array $applyRow = null)
    {
        $applyId = (int)$applyId;
        $operatorId = (int)$operatorId;
        if ($applyId <= 0) {
            throw new Exception('快照写入申请 ID 无效');
        }
        if ($operatorId <= 0) {
            throw new Exception('快照操作人无效');
        }

        if ($applyRow === null || empty($applyRow)) {
            $applyRow = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
        }
        if (empty($applyRow) || (int)($applyRow['id'] ?? 0) !== $applyId) {
            throw new Exception('快照写入时主表记录不存在');
        }

        $round = (int)($applyRow['approval_round'] ?? 0);
        if ($round < 1) {
            throw new Exception('快照写入时审批轮次无效');
        }

        $itemRows = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->order('apply_line_no', 'asc')
            ->order('order_item_id', 'asc')
            ->order('id', 'asc')
            ->select();
        if ($itemRows instanceof \think\Collection) {
            $itemRows = $itemRows->toArray();
        }
        $itemRows = is_array($itemRows) ? $itemRows : [];

        $header = $this->buildHeaderFromApplyRow($applyRow, 10, 1);
        $items = $this->buildItemsFromItemRows($itemRows);
        $hashPack = $this->computeSubmitSnapshotHash(
            $header,
            $items,
            $applyId,
            $round,
            self::SNAPSHOT_STAGE_SUBMIT,
            self::SNAPSHOT_SCHEMA_VERSION
        );

        $headerJson = $this->encodeCanonicalJson($header);
        $itemsJson = $this->encodeCanonicalJson($items);
        $now = date('Y-m-d H:i:s');

        $insert = [
            'shipping_apply_id' => $applyId,
            'approval_round' => $round,
            'snapshot_stage' => self::SNAPSHOT_STAGE_SUBMIT,
            'snapshot_schema_version' => self::SNAPSHOT_SCHEMA_VERSION,
            'header_json' => $headerJson,
            'items_json' => $itemsJson,
            'payload_hash' => $hashPack['hash'],
            'operator_id' => $operatorId,
            'create_time' => $now,
        ];

        try {
            $snapshotId = (int)Db::name('crm_shipping_apply_round_snapshot')->insertGetId($insert);
        } catch (\Throwable $e) {
            if ($this->isSnapshotUniqueConflict($e)) {
                $existing = $this->findSubmitRoundSnapshot($applyId, $round);
                if ($existing === null) {
                    throw new Exception('快照唯一索引冲突且无法读取已有记录');
                }
                $this->assertStoredSnapshotMatchesExpected($existing, $hashPack['hash'], $headerJson, $itemsJson);
                return $existing;
            }
            throw new Exception('快照写入失败：' . $e->getMessage(), 0, $e);
        }

        if ($snapshotId <= 0) {
            throw new Exception('快照写入失败');
        }

        $stored = $this->findSubmitRoundSnapshotById($snapshotId);
        if ($stored === null) {
            throw new Exception('快照写入后回读失败');
        }
        $this->assertStoredSnapshotIntegrity($stored);
        if (!hash_equals(strtolower((string)$stored['payload_hash']), $hashPack['hash'])) {
            throw new Exception('快照写入后 hash 校验失败');
        }
        if ((string)$stored['header_json'] !== $headerJson || (string)$stored['items_json'] !== $itemsJson) {
            throw new Exception('快照写入后 JSON 内容校验失败');
        }

        return $stored;
    }

    /**
     * 按申请+轮次读取 submit 快照（只读）
     *
     * @param int $applyId
     * @param int $round
     * @return array|null
     */
    public function findSubmitRoundSnapshot($applyId, $round)
    {
        $applyId = (int)$applyId;
        $round = (int)$round;
        if ($applyId <= 0 || $round < 1) {
            return null;
        }
        $row = Db::name('crm_shipping_apply_round_snapshot')
            ->where('shipping_apply_id', $applyId)
            ->where('approval_round', $round)
            ->where('snapshot_stage', self::SNAPSHOT_STAGE_SUBMIT)
            ->find();
        return empty($row) ? null : $row;
    }

    /**
     * @param int $snapshotId
     * @return array|null
     */
    public function findSubmitRoundSnapshotById($snapshotId)
    {
        $snapshotId = (int)$snapshotId;
        if ($snapshotId <= 0) {
            return null;
        }
        $row = Db::name('crm_shipping_apply_round_snapshot')->where('id', $snapshotId)->find();
        return empty($row) ? null : $row;
    }

    /**
     * 基于已保存 JSON 本身重算并核验 payload_hash（不依赖当前主表/明细）
     *
     * @param array $snapshotRow
     * @return array{ok:bool,hash:string|null,header:array|null,items:array|null,reason:string}
     */
    public function verifyStoredSnapshotIntegrity(array $snapshotRow)
    {
        try {
            $this->assertStoredSnapshotIntegrity($snapshotRow);
            $header = $this->decodeJsonObject((string)($snapshotRow['header_json'] ?? ''), 'header_json');
            $items = $this->decodeJsonArray((string)($snapshotRow['items_json'] ?? ''), 'items_json');
            return [
                'ok' => true,
                'hash' => strtolower(trim((string)($snapshotRow['payload_hash'] ?? ''))),
                'header' => $header,
                'items' => $items,
                'reason' => '',
            ];
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'hash' => null,
                'header' => null,
                'items' => null,
                'reason' => $e->getMessage(),
            ];
        }
    }

    /**
     * @param array $snapshotRow
     * @throws Exception
     */
    public function assertStoredSnapshotIntegrity(array $snapshotRow)
    {
        $stage = (string)($snapshotRow['snapshot_stage'] ?? '');
        if ($stage !== self::SNAPSHOT_STAGE_SUBMIT) {
            throw new Exception('快照阶段不是 submit');
        }
        $schemaVersion = (int)($snapshotRow['snapshot_schema_version'] ?? 0);
        if ($schemaVersion !== self::SNAPSHOT_SCHEMA_VERSION) {
            throw new Exception('快照结构版本不受支持');
        }
        $applyId = (int)($snapshotRow['shipping_apply_id'] ?? 0);
        $round = (int)($snapshotRow['approval_round'] ?? 0);
        if ($applyId <= 0 || $round < 1) {
            throw new Exception('快照关联申请或轮次无效');
        }
        $storedHash = strtolower(trim((string)($snapshotRow['payload_hash'] ?? '')));
        if ($storedHash === '' || strlen($storedHash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $storedHash)) {
            throw new Exception('快照 payload_hash 无效');
        }

        $header = $this->decodeJsonObject((string)($snapshotRow['header_json'] ?? ''), 'header_json');
        $items = $this->decodeJsonArray((string)($snapshotRow['items_json'] ?? ''), 'items_json');
        if (empty($items)) {
            throw new Exception('快照明细 JSON 为空');
        }

        // schema v1 必要结构：先校验键集合与身份，禁止缺键后重排仍能“hash 自洽”
        $this->assertSchemaV1HeaderStructure($header, $applyId, $round);
        $itemsOrdered = [];
        foreach ($items as $idx => $item) {
            if (!is_array($item)) {
                throw new Exception('快照明细元素无效');
            }
            $this->assertSchemaV1ItemStructure($item, $idx);
            $itemsOrdered[] = $this->orderAssocByKeys($item, self::ITEM_KEYS);
        }

        // 回读核验只依赖已保存 JSON，不回查当前业务表；不修补、不补零、不重写
        $headerOrdered = $this->orderAssocByKeys($header, self::HEADER_KEYS);
        // 稳定排序后再哈希，防止历史写入时顺序异常
        usort($itemsOrdered, function ($a, $b) {
            $la = (int)($a['apply_line_no'] ?? 0);
            $lb = (int)($b['apply_line_no'] ?? 0);
            if ($la !== $lb) {
                return $la < $lb ? -1 : 1;
            }
            $oa = (int)($a['order_item_id'] ?? 0);
            $ob = (int)($b['order_item_id'] ?? 0);
            if ($oa !== $ob) {
                return $oa < $ob ? -1 : 1;
            }
            $ia = (int)($a['shipping_apply_item_id'] ?? 0);
            $ib = (int)($b['shipping_apply_item_id'] ?? 0);
            if ($ia === $ib) {
                return 0;
            }
            return $ia < $ib ? -1 : 1;
        });

        $pack = $this->computeSubmitSnapshotHash(
            $headerOrdered,
            $itemsOrdered,
            $applyId,
            $round,
            $stage,
            $schemaVersion
        );
        if (!hash_equals($storedHash, $pack['hash'])) {
            throw new Exception('快照 payload_hash 与已保存 JSON 不一致');
        }
    }

    /**
     * 正式提交幂等 / COMMIT 恢复共用的快照证据判断
     *
     * @param array $applyRow 主表行（须已通过历史成功身份/token/hash 初筛）
     * @param string|null $requestPayloadHash 当前请求 submit_payload_hash；null 表示仅核快照自身
     * @param bool $allowLegacyCompatible 是否允许旧机制无快照兼容
     * @return array{decision:string,snapshot:?array,reason:string}
     */
    public function evaluateSubmitSnapshotEvidence(
        array $applyRow,
        $requestPayloadHash = null,
        $allowLegacyCompatible = true
    ) {
        $applyId = (int)($applyRow['id'] ?? 0);
        $round = (int)($applyRow['approval_round'] ?? 0);
        if ($applyId <= 0 || $round < 1) {
            return [
                'decision' => self::EVIDENCE_INVALID,
                'snapshot' => null,
                'reason' => '申请或轮次无效',
            ];
        }

        $snapshot = $this->findSubmitRoundSnapshot($applyId, $round);
        if ($snapshot !== null) {
            $verify = $this->verifyStoredSnapshotIntegrity($snapshot);
            if (!$verify['ok']) {
                return [
                    'decision' => self::EVIDENCE_INVALID,
                    'snapshot' => $snapshot,
                    'reason' => $verify['reason'] !== '' ? $verify['reason'] : '快照完整性校验失败',
                ];
            }
            $header = $verify['header'];
            if (!is_array($header)) {
                return [
                    'decision' => self::EVIDENCE_INVALID,
                    'snapshot' => $snapshot,
                    'reason' => '快照 header 无效',
                ];
            }
            if ((int)($header['shipping_apply_id'] ?? 0) !== $applyId
                || (int)($header['approval_round'] ?? 0) !== $round
            ) {
                return [
                    'decision' => self::EVIDENCE_INVALID,
                    'snapshot' => $snapshot,
                    'reason' => '快照 header 身份与申请不一致',
                ];
            }
            if ($requestPayloadHash !== null && $requestPayloadHash !== '') {
                $reqHash = strtolower(trim((string)$requestPayloadHash));
                $snapSubmitHash = strtolower(trim((string)($header['submit_payload_hash'] ?? '')));
                if ($snapSubmitHash === '' || !hash_equals($snapSubmitHash, $reqHash)) {
                    return [
                        'decision' => self::EVIDENCE_INVALID,
                        'snapshot' => $snapshot,
                        'reason' => '快照与当前请求 submit_payload_hash 不一致',
                    ];
                }
                $rowHash = strtolower(trim((string)($applyRow['submit_payload_hash'] ?? '')));
                if ($rowHash === '' || !hash_equals($rowHash, $reqHash)) {
                    return [
                        'decision' => self::EVIDENCE_INVALID,
                        'snapshot' => $snapshot,
                        'reason' => '主表 submit_payload_hash 与请求不一致',
                    ];
                }
            }
            return [
                'decision' => self::EVIDENCE_VALID_SNAPSHOT,
                'snapshot' => $snapshot,
                'reason' => '',
            ];
        }

        // 无快照
        if (!$allowLegacyCompatible) {
            return [
                'decision' => self::EVIDENCE_MISSING_REQUIRED,
                'snapshot' => null,
                'reason' => '缺少 submit 快照',
            ];
        }

        if ($this->isLegacyPreSnapshotSubmit($applyRow)) {
            return [
                'decision' => self::EVIDENCE_LEGACY_COMPATIBLE,
                'snapshot' => null,
                'reason' => '机制生效前历史正式提交（无快照兼容）',
            ];
        }

        return [
            'decision' => self::EVIDENCE_MISSING_REQUIRED,
            'snapshot' => null,
            'reason' => '新机制下正式提交缺少有效 submit 快照',
        ];
    }

    /**
     * 是否可识别为快照机制生效前的历史正式提交（受限兼容）
     *
     * 正证据（须同时满足）：
     * 1. 有效 submit_time / approval_round / submit_payload_hash；
     * 2. 存在可解析的机制切换边界，且 submit_time 严格早于该边界。
     *
     * 不得仅凭「无快照」判定为旧记录。
     * 不得使用当前系统时间猜测归属。
     * 无法解析边界时 fail-closed（返回 false）。
     *
     * @param array $applyRow
     * @return bool
     */
    public function isLegacyPreSnapshotSubmit(array $applyRow)
    {
        $submitTime = trim((string)($applyRow['submit_time'] ?? ''));
        if ($submitTime === '') {
            return false;
        }
        // 规范化比较：要求可解析的 datetime 字面量
        if (!preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $submitTime)) {
            return false;
        }
        $round = (int)($applyRow['approval_round'] ?? 0);
        if ($round < 1) {
            return false;
        }
        $hash = strtolower(trim((string)($applyRow['submit_payload_hash'] ?? '')));
        if ($hash === '' || strlen($hash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $hash)) {
            return false;
        }

        $boundary = $this->resolveMechanismEffectiveAt();
        if ($boundary === null || $boundary === '') {
            return false;
        }
        if (strcmp($submitTime, $boundary) >= 0) {
            return false;
        }
        return true;
    }

    /**
     * 显式设置机制切换边界（测试或运维注入）。
     * 传 null 表示强制无边界（fail-closed，不走旧兼容）。
     *
     * @param string|null $datetime
     * @throws Exception
     */
    public function setMechanismEffectiveAt($datetime)
    {
        if ($datetime === null) {
            $this->mechanismEffectiveAtOverride = null;
            return;
        }
        $dt = trim((string)$datetime);
        if ($dt === '' || !preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $dt)) {
            throw new Exception('机制切换边界时间格式无效');
        }
        $this->mechanismEffectiveAtOverride = $dt;
    }

    /**
     * 清除显式覆盖，恢复 env/config/派生/本地基线解析链
     */
    public function clearMechanismEffectiveAtOverride()
    {
        $this->mechanismEffectiveAtOverride = false;
    }

    /**
     * @return string
     */
    public function getLastBoundarySource()
    {
        return (string)$this->lastBoundarySource;
    }

    /**
     * 解析本环境机制切换边界（不使用当前系统时间）。
     *
     * 优先级：
     * 1. 显式 override
     * 2. 环境变量 shipping_snapshot.mechanism_effective_at
     * 3. 配置键 shipping_submit.snapshot_mechanism_effective_at（若已存在；本轮不新增该键）
     * 4. 本库最早 submit 快照 create_time（派生，按环境数据自校准）
     * 5. 仅当 shipping_submit.enabled === true 时使用本地基线常量（受限）
     * 6. 否则 null → fail-closed
     *
     * @return string|null Y-m-d H:i:s 或 null
     */
    public function resolveMechanismEffectiveAt()
    {
        if ($this->mechanismEffectiveAtOverride !== false) {
            $this->lastBoundarySource = self::BOUNDARY_SOURCE_OVERRIDE;
            return $this->mechanismEffectiveAtOverride;
        }

        $fromEnv = $this->readDatetimeEnv('shipping_snapshot.mechanism_effective_at');
        if ($fromEnv !== null) {
            $this->lastBoundarySource = self::BOUNDARY_SOURCE_ENV;
            return $fromEnv;
        }

        $fromConfig = $this->readDatetimeConfig('shipping_submit.snapshot_mechanism_effective_at');
        if ($fromConfig !== null) {
            $this->lastBoundarySource = self::BOUNDARY_SOURCE_CONFIG;
            return $fromConfig;
        }

        $derived = $this->deriveMechanismEffectiveAtFromSnapshotTable();
        if ($derived !== null) {
            $this->lastBoundarySource = self::BOUNDARY_SOURCE_DERIVED;
            return $derived;
        }

        // 受限本地基线：复用既有 shipping_submit.enabled，不修改配置文件
        if (function_exists('config') && config('shipping_submit.enabled') === true) {
            $this->lastBoundarySource = self::BOUNDARY_SOURCE_LOCAL_BASELINE;
            return self::MECHANISM_EFFECTIVE_AT;
        }

        $this->lastBoundarySource = self::BOUNDARY_SOURCE_NONE;
        return null;
    }

    /**
     * @param \Throwable $e
     * @return bool
     */
    public function isSnapshotUniqueConflict($e)
    {
        $msg = (string)$e->getMessage();
        if (stripos($msg, 'Duplicate entry') === false && stripos($msg, '1062') === false) {
            return false;
        }
        return stripos($msg, 'uk_apply_round_stage') !== false;
    }

    /**
     * 唯一冲突时：仅当内容证据一致才视为可接受；禁止 REPLACE/UPDATE 覆盖
     *
     * @param array $existing
     * @param string $expectedHash
     * @param string $expectedHeaderJson
     * @param string $expectedItemsJson
     * @throws Exception
     */
    protected function assertStoredSnapshotMatchesExpected(
        array $existing,
        $expectedHash,
        $expectedHeaderJson,
        $expectedItemsJson
    ) {
        $this->assertStoredSnapshotIntegrity($existing);
        $storedHash = strtolower(trim((string)($existing['payload_hash'] ?? '')));
        if (!hash_equals($storedHash, strtolower(trim((string)$expectedHash)))) {
            throw new Exception('快照唯一索引冲突且内容不一致，拒绝覆盖');
        }
        if ((string)($existing['header_json'] ?? '') !== (string)$expectedHeaderJson
            || (string)($existing['items_json'] ?? '') !== (string)$expectedItemsJson
        ) {
            throw new Exception('快照唯一索引冲突且 JSON 内容不一致，拒绝覆盖');
        }
    }

    /**
     * @param string $json
     * @param string $label
     * @return array
     * @throws Exception
     */
    protected function decodeJsonObject($json, $label)
    {
        $json = (string)$json;
        if ($json === '') {
            throw new Exception($label . ' 为空');
        }
        try {
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new Exception($label . ' JSON 非法：' . $e->getMessage(), 0, $e);
        }
        if (!is_array($data) || $this->isListArray($data)) {
            throw new Exception($label . ' 必须为 JSON 对象');
        }
        return $data;
    }

    /**
     * @param string $json
     * @param string $label
     * @return array
     * @throws Exception
     */
    protected function decodeJsonArray($json, $label)
    {
        $json = (string)$json;
        if ($json === '') {
            throw new Exception($label . ' 为空');
        }
        try {
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new Exception($label . ' JSON 非法：' . $e->getMessage(), 0, $e);
        }
        if (!is_array($data) || !$this->isListArray($data)) {
            throw new Exception($label . ' 必须为 JSON 数组');
        }
        return $data;
    }

    /**
     * @param array $arr
     * @return bool
     */
    protected function isListArray(array $arr)
    {
        if ($arr === []) {
            return true;
        }
        return array_keys($arr) === range(0, count($arr) - 1);
    }

    /**
     * @param array $data
     * @param string[] $keys
     * @return array
     */
    protected function orderAssocByKeys(array $data, array $keys)
    {
        $ordered = [];
        foreach ($keys as $key) {
            if (array_key_exists($key, $data)) {
                $ordered[$key] = $data[$key];
            }
        }
        return $ordered;
    }

    /**
     * NULL 与空字符串区分：DB NULL → PHP null；否则原字符串（含 ''）
     *
     * @param mixed $raw
     * @return string|null
     */
    protected function normalizeNullableString($raw)
    {
        if ($raw === null) {
            return null;
        }
        return (string)$raw;
    }

    /**
     * @param mixed $raw
     * @return int|null
     */
    protected function normalizeNullablePositiveInt($raw)
    {
        if ($raw === null || $raw === '') {
            return null;
        }
        if (is_bool($raw) || is_array($raw) || is_object($raw)) {
            return null;
        }
        if (is_string($raw) && !preg_match('/^(0|[1-9]\d*)$/', trim($raw))) {
            return null;
        }
        $n = (int)$raw;
        return $n > 0 ? $n : null;
    }

    /**
     * 必要金额：null/空串拒绝；合法零值保存为 "0.00"；禁止默默补零冒充真实金额。
     *
     * @param mixed $raw
     * @param string $field
     * @return string
     * @throws Exception
     */
    public function normalizeRequiredMoney($raw, $field)
    {
        if ($raw === null || $raw === '') {
            throw new Exception('快照必要金额字段缺失：' . $field);
        }
        $money = $this->normalizeNullableMoney($raw);
        if ($money === null) {
            throw new Exception('快照金额字段无效：' . $field);
        }
        return $money;
    }

    /**
     * 金额规范化为两位小数字符串；NULL/空 → null；禁止 float 直编
     *
     * @param mixed $raw
     * @return string|null
     * @throws Exception
     */
    public function normalizeNullableMoney($raw)
    {
        if ($raw === null || $raw === '') {
            return null;
        }
        if (is_bool($raw) || is_array($raw) || is_object($raw)) {
            throw new Exception('快照金额格式无效');
        }
        // 禁止 float 直接进入（浮点二进制误差）
        if (is_float($raw)) {
            throw new Exception('快照金额禁止使用浮点数');
        }
        if (is_int($raw)) {
            $raw = (string)$raw;
        }
        $raw = trim((string)$raw);
        if ($raw === '') {
            return null;
        }
        if (!preg_match('/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/', $raw)) {
            throw new Exception('快照金额格式无效');
        }
        if (function_exists('bccomp') && bccomp($raw, '0', 8) < 0) {
            throw new Exception('快照金额不能为负');
        }
        return $this->formatMoneyHalfUp($raw);
    }

    /**
     * @param mixed $raw
     * @param string $field
     * @return int
     * @throws Exception
     */
    protected function normalizeRequiredPositiveInt($raw, $field)
    {
        if ($raw === null || $raw === '') {
            throw new Exception('快照必要整数字段缺失：' . $field);
        }
        if (is_bool($raw) || is_array($raw) || is_object($raw) || is_float($raw)) {
            throw new Exception('快照必要整数字段无效：' . $field);
        }
        $s = trim((string)$raw);
        if ($s === '' || !preg_match('/^[1-9]\d*$/', $s)) {
            throw new Exception('快照必要整数字段无效：' . $field);
        }
        return (int)$s;
    }

    /**
     * schema v1 header 必要结构与身份一致性（不回查业务表、不修补）
     *
     * @param array $header
     * @param int $applyId
     * @param int $round
     * @throws Exception
     */
    protected function assertSchemaV1HeaderStructure(array $header, $applyId, $round)
    {
        $this->assertExactKeySet($header, self::HEADER_KEYS, 'header');

        if ((int)($header['shipping_apply_id'] ?? 0) !== (int)$applyId) {
            throw new Exception('快照 header.shipping_apply_id 与表关联不一致');
        }
        if ((int)($header['approval_round'] ?? 0) !== (int)$round) {
            throw new Exception('快照 header.approval_round 与表关联不一致');
        }
        if ((int)($header['status_at_submit'] ?? -1) !== 10) {
            throw new Exception('快照 status_at_submit 必须为 10');
        }
        if ((int)($header['current_stage_at_submit'] ?? -1) !== 1) {
            throw new Exception('快照 current_stage_at_submit 必须为 1');
        }

        $payloadHash = strtolower(trim((string)($header['submit_payload_hash'] ?? '')));
        if ($payloadHash === '' || strlen($payloadHash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $payloadHash)) {
            throw new Exception('快照 header.submit_payload_hash 无效');
        }

        $this->assertStoredRequiredMoneyString($header['order_amount'] ?? null, 'order_amount');
        $this->assertStoredRequiredMoneyString($header['approval_threshold'] ?? null, 'approval_threshold');

        // 关键关键类型（schema v1）
        if (!is_int($header['shipping_apply_id']) && !preg_match('/^[1-9]\d*$/', (string)$header['shipping_apply_id'])) {
            throw new Exception('快照 header.shipping_apply_id 类型无效');
        }
        if (!is_string($header['apply_no']) || trim($header['apply_no']) === '') {
            throw new Exception('快照 header.apply_no 无效');
        }
        if (!is_int($header['order_id']) && !preg_match('/^[1-9]\d*$/', (string)$header['order_id'])) {
            throw new Exception('快照 header.order_id 类型无效');
        }
        if (!is_int($header['approval_route']) && !preg_match('/^(0|[1-9]\d*)$/', (string)$header['approval_route'])) {
            throw new Exception('快照 header.approval_route 类型无效');
        }
        $submitTime = trim((string)($header['submit_time'] ?? ''));
        if ($submitTime === '' || !preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $submitTime)) {
            throw new Exception('快照 header.submit_time 无效');
        }
    }

    /**
     * @param array $item
     * @param int $idx
     * @throws Exception
     */
    protected function assertSchemaV1ItemStructure(array $item, $idx)
    {
        $label = 'items[' . (int)$idx . ']';
        $this->assertExactKeySet($item, self::ITEM_KEYS, $label);

        $itemId = $item['shipping_apply_item_id'] ?? null;
        if (!$this->isPositiveIntValue($itemId)) {
            throw new Exception($label . '.shipping_apply_item_id 必须为正整数');
        }
        $orderItemId = $item['order_item_id'] ?? null;
        if (!$this->isPositiveIntValue($orderItemId)) {
            throw new Exception($label . '.order_item_id 必须为正整数');
        }
        $applyQty = $item['apply_qty'] ?? null;
        if (!$this->isPositiveIntValue($applyQty)) {
            throw new Exception($label . '.apply_qty 必须为正整数');
        }

        $this->assertStoredRequiredMoneyString($item['unit_price'] ?? null, $label . '.unit_price');
        $this->assertStoredRequiredMoneyString($item['total_price'] ?? null, $label . '.total_price');

        $nullableMoneyFields = [
            'order_purchase_amount',
            'apply_unit_price',
            'apply_purchase_unit_price',
            'apply_sales_amount',
            'apply_purchase_amount',
        ];
        foreach ($nullableMoneyFields as $f) {
            $v = $item[$f] ?? null;
            if ($v === null) {
                continue;
            }
            $this->assertStoredRequiredMoneyString($v, $label . '.' . $f);
        }
    }

    /**
     * 必要键必须全部存在；额外键拒绝（与写入时固定键序一致，避免缺键后 hash 自洽）
     *
     * @param array $data
     * @param string[] $expectedKeys
     * @param string $label
     * @throws Exception
     */
    protected function assertExactKeySet(array $data, array $expectedKeys, $label)
    {
        foreach ($expectedKeys as $key) {
            if (!array_key_exists($key, $data)) {
                throw new Exception($label . ' 缺少必要字段：' . $key);
            }
        }
        $extra = array_diff(array_keys($data), $expectedKeys);
        if (!empty($extra)) {
            throw new Exception($label . ' 含未知字段：' . implode(',', $extra));
        }
    }

    /**
     * @param mixed $raw
     * @return bool
     */
    protected function isPositiveIntValue($raw)
    {
        if (is_int($raw)) {
            return $raw > 0;
        }
        if (is_string($raw)) {
            $raw = trim($raw);
            return $raw !== '' && preg_match('/^[1-9]\d*$/', $raw);
        }
        return false;
    }

    /**
     * 已保存 JSON 中的必要金额必须已是规范化两位小数字符串（不补零、不重写）
     *
     * @param mixed $raw
     * @param string $field
     * @throws Exception
     */
    protected function assertStoredRequiredMoneyString($raw, $field)
    {
        if ($raw === null || $raw === '') {
            throw new Exception('快照必要金额缺失：' . $field);
        }
        if (is_float($raw) || is_int($raw) || is_bool($raw) || is_array($raw) || is_object($raw)) {
            throw new Exception('快照必要金额类型无效：' . $field);
        }
        $s = trim((string)$raw);
        if (!preg_match('/^(?:0|[1-9]\d*)\.\d{2}$/', $s)) {
            throw new Exception('快照必要金额格式无效：' . $field);
        }
        if (function_exists('bccomp') && bccomp($s, '0', 2) < 0) {
            throw new Exception('快照必要金额不能为负：' . $field);
        }
    }

    /**
     * @param string $envKey
     * @return string|null
     */
    protected function readDatetimeEnv($envKey)
    {
        $raw = null;
        if (function_exists('env')) {
            $raw = env($envKey, null);
        }
        return $this->normalizeBoundaryDatetime($raw);
    }

    /**
     * @param string $configKey
     * @return string|null
     */
    protected function readDatetimeConfig($configKey)
    {
        if (!function_exists('config')) {
            return null;
        }
        try {
            // 键不存在时不视为已配置；避免把空数组等脏值当边界
            if (strpos($configKey, '.') !== false) {
                $has = config('?' . $configKey);
                if ($has !== true) {
                    return null;
                }
            }
            return $this->normalizeBoundaryDatetime(config($configKey));
        } catch (\Throwable $e) {
            return null;
        }
    }

    /**
     * 从本库最早 submit 快照 create_time 派生边界；无快照则 null（不猜测）
     *
     * @return string|null
     */
    protected function deriveMechanismEffectiveAtFromSnapshotTable()
    {
        try {
            $min = Db::name('crm_shipping_apply_round_snapshot')
                ->where('snapshot_stage', self::SNAPSHOT_STAGE_SUBMIT)
                ->min('create_time');
            if ($min === null || $min === false || $min === '') {
                return null;
            }
            return $this->normalizeBoundaryDatetime($min);
        } catch (\Throwable $e) {
            return null;
        }
    }

    /**
     * @param mixed $raw
     * @return string|null
     */
    protected function normalizeBoundaryDatetime($raw)
    {
        if ($raw === null || $raw === '') {
            return null;
        }
        if (is_bool($raw) || is_array($raw) || is_object($raw)) {
            return null;
        }
        $dt = trim((string)$raw);
        if ($dt === '' || !preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $dt)) {
            return null;
        }
        return $dt;
    }

    /**
     * @param string $value
     * @return string
     * @throws Exception
     */
    protected function formatMoneyHalfUp($value)
    {
        $value = trim((string)$value);
        if ($value === '' || !preg_match('/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/', $value)) {
            throw new Exception('快照金额格式无效');
        }
        if (!function_exists('bcmul')) {
            // 极端兜底：仍避免 float；仅处理已是两位的字面量
            if (preg_match('/^(?:0|[1-9]\d*)\.\d{2}$/', $value)) {
                return $value;
            }
            throw new Exception('BCMath 不可用，无法规范化金额');
        }
        if (bccomp($value, '0', 8) < 0) {
            throw new Exception('快照金额不能为负');
        }
        $scaled = bcmul($value, '100', 8);
        $scaled = bcadd($scaled, '0.5', 8);
        $intPart = bcmul($scaled, '1', 0);
        return bcdiv($intPart, '100', 2);
    }
}
