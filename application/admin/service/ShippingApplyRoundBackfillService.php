<?php

namespace app\admin\service;

use think\Db;
use think\Exception;

/**
 * 发货申请历史 Round submit 快照受控补存（M4-2C-2B-2B-2）
 *
 * 独立于正式提交 / 草稿保存 / 审批决定 / 驳回编辑。
 * 本构建默认仅允许只读 dry-run；真实写入能力硬关闭，等待后续独立入口与环境授权评审。
 *
 * 禁止：
 * - 被 saveDraft / submitApply / decideApply / failedShip / getRejectedApplyForEdit 调用
 * - HTTP / 菜单 / 定时任务自动触发
 * - 批量补存
 * - 修改主表状态/轮次/明细/任务/占量
 * - REPLACE / UPDATE / DELETE 既有快照
 */
class ShippingApplyRoundBackfillService
{
    /** 补存审计 action（详情页未知动作会显示为「操作记录(snapshot_backfill)」；不会误标为通过/重提） */
    const AUDIT_ACTION = 'snapshot_backfill';

    /** 运维补存角色标识（写入审计；不代表业务审批人） */
    const OPERATOR_ROLE_OPS_BACKFILL = 'ops_backfill';

    /** 本轮允许补存的当前申请状态：经理驳回 / 总监驳回 */
    const ALLOWED_CURRENT_STATUSES = [20, 40];

    /** 本构建硬关闭真实写入；即使显式传参也不得写库 */
    const WRITE_CAPABILITY_ENABLED = false;

    const MODE_DRY_RUN = 'dry_run';
    const MODE_EXECUTE = 'execute';

    const TRUST_A = 'A';
    const TRUST_B = 'B';
    const TRUST_C = 'C';

    const DECISION_ELIGIBLE = 'eligible';
    const DECISION_REJECTED = 'rejected';
    const DECISION_IDEMPOTENT_EXISTS = 'idempotent_exists';
    const DECISION_WRITE_BLOCKED = 'write_blocked';
    const DECISION_COMMIT_UNCERTAIN = 'commit_uncertain';

    /** @var ShippingApplyRoundSnapshotService|null */
    protected $snapshotService;

    /** @var ShippingApplyService|null */
    protected $submitService;

    /** @var array|null 测试可注入的证据包（跳过真实 DB 读取） */
    protected $injectedEvidenceBundle;

    /** @var callable|null 测试可注入的事务执行器 */
    protected $txRunnerOverride;

    /**
     * @param ShippingApplyRoundSnapshotService|null $snapshotService
     * @param ShippingApplyService|null $submitService
     */
    public function __construct(
        ShippingApplyRoundSnapshotService $snapshotService = null,
        ShippingApplyService $submitService = null
    ) {
        $this->snapshotService = $snapshotService;
        $this->submitService = $submitService;
    }

    /**
     * @param ShippingApplyRoundSnapshotService $svc
     * @return $this
     */
    public function setSnapshotService(ShippingApplyRoundSnapshotService $svc)
    {
        $this->snapshotService = $svc;
        return $this;
    }

    /**
     * @param ShippingApplyService $svc
     * @return $this
     */
    public function setSubmitService(ShippingApplyService $svc)
    {
        $this->submitService = $svc;
        return $this;
    }

    /**
     * 测试注入完整证据包（只读评估 / Mock；不触达真实 DB）
     *
     * @param array|null $bundle
     * @return $this
     */
    public function setInjectedEvidenceBundle(array $bundle = null)
    {
        $this->injectedEvidenceBundle = $bundle;
        return $this;
    }

    /**
     * @param callable|null $runner function(callable $fn): mixed
     * @return $this
     */
    public function setTxRunnerOverride($runner)
    {
        $this->txRunnerOverride = is_callable($runner) ? $runner : null;
        return $this;
    }

    /**
     * 默认公开入口：只读候选检查（永远不写库）
     *
     * @param int $applyId
     * @param array $options
     * @return array
     */
    public function inspectCandidate($applyId, array $options = [])
    {
        $options['mode'] = self::MODE_DRY_RUN;
        $options['allow_write'] = false;
        return $this->runControlledBackfill((int)$applyId, $options);
    }

    /**
     * dry-run：核验证据并构建快照 JSON/hash，不写库
     *
     * @param int $applyId
     * @param array $options
     * @return array
     */
    public function dryRunBuild($applyId, array $options = [])
    {
        $options['mode'] = self::MODE_DRY_RUN;
        $options['allow_write'] = false;
        return $this->runControlledBackfill((int)$applyId, $options);
    }

    /**
     * 受控执行入口。
     *
     * 本构建：WRITE_CAPABILITY_ENABLED=false → 任何 execute 请求均失败关闭，不写库。
     * 未来启用须另批：独立环境身份 + 单申请授权 + 显式 allow_write + 用户批准。
     *
     * @param int $applyId
     * @param array $options
     * @return array
     */
    public function executeControlledBackfill($applyId, array $options = [])
    {
        if (!array_key_exists('mode', $options) || $options['mode'] === '' || $options['mode'] === null) {
            $options['mode'] = self::MODE_DRY_RUN;
        }
        return $this->runControlledBackfill((int)$applyId, $options);
    }

    /**
     * 纯证据包评估（Mock / 只读；不写库）
     *
     * @param array $bundle
     * @return array
     */
    public function evaluateEvidenceBundle(array $bundle)
    {
        return $this->evaluateCandidateCore($bundle);
    }

    /**
     * 有限合法历史 hash 候选（来自正式提交代码路径，禁止任意爆破）
     *
     * @param array $apply
     * @param array $itemRows
     * @return array[]
     */
    public function enumerateLegalHashCandidates(array $apply, array $itemRows)
    {
        $applicantId = (int)($apply['applicant_id'] ?? 0);
        $orderId = (int)($apply['order_id'] ?? 0);
        $actualApplyId = (int)($apply['id'] ?? 0);
        $orderAmount = isset($apply['order_amount']) ? (string)$apply['order_amount'] : null;

        $lines = [];
        foreach ($itemRows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $qty = (int)($row['apply_qty'] ?? 0);
            if ($qty <= 0) {
                continue;
            }
            $lines[] = [
                'order_item_id' => (int)($row['order_item_id'] ?? 0),
                'apply_product_name' => (string)($row['apply_product_name'] ?? ''),
                'apply_spec_model' => (string)($row['apply_spec_model'] ?? ''),
                'apply_qty' => $qty,
                'factory_id' => (int)($row['factory_id'] ?? 0),
                'apply_unit_price' => (string)($row['apply_unit_price'] ?? ''),
                'apply_purchase_unit_price' => (string)($row['apply_purchase_unit_price'] ?? ''),
                'remark' => (string)($row['remark'] ?? ''),
            ];
        }

        $headerBase = [
            'receiver_name' => (string)($apply['receiver_name'] ?? ''),
            'receiver_phone' => (string)($apply['receiver_phone'] ?? ''),
            'receiver_address' => (string)($apply['receiver_address'] ?? ''),
            'shipping_date' => (string)($apply['shipping_date'] ?? ''),
            'shipping_method' => (string)($apply['shipping_method'] ?? ''),
            'shipping_remark' => (string)($apply['shipping_remark'] ?? ''),
        ];

        // 直接正式提交：hash 内 apply_id=0；草稿提交：apply_id=真实 ID
        // 确认标志：库中无持久化字段，仅允许代码路径上的有限组合（最多 8）
        $applyIdOptions = [0];
        if ($actualApplyId > 0) {
            $applyIdOptions[] = $actualApplyId;
        }

        $flagCombos = [
            ['order_amount_confirmed' => 0, 'confirmed_order_amount' => null, 'shipping_date_history_confirmed' => 0],
            ['order_amount_confirmed' => 1, 'confirmed_order_amount' => $orderAmount, 'shipping_date_history_confirmed' => 0],
            ['order_amount_confirmed' => 0, 'confirmed_order_amount' => null, 'shipping_date_history_confirmed' => 1],
            ['order_amount_confirmed' => 1, 'confirmed_order_amount' => $orderAmount, 'shipping_date_history_confirmed' => 1],
        ];

        $candidates = [];
        foreach ($applyIdOptions as $hashApplyId) {
            foreach ($flagCombos as $flags) {
                // 金额确认但无可用金额 → 跳过非法组合
                if ((int)$flags['order_amount_confirmed'] === 1
                    && ($flags['confirmed_order_amount'] === null || $flags['confirmed_order_amount'] === '')
                ) {
                    continue;
                }
                $candidates[] = [
                    'label' => sprintf(
                        'apply_id=%d;amount_confirmed=%d;history_confirmed=%d',
                        (int)$hashApplyId,
                        (int)$flags['order_amount_confirmed'],
                        (int)$flags['shipping_date_history_confirmed']
                    ),
                    'hash_apply_id' => (int)$hashApplyId,
                    'validated' => [
                        'applicant_id' => $applicantId,
                        'order_id' => $orderId,
                        'apply_id' => (int)$hashApplyId,
                        'header' => array_merge($headerBase, $flags),
                        'lines' => $lines,
                    ],
                ];
            }
        }

        return $candidates;
    }

    /**
     * 在有限合法候选中重算并命中原 submit_payload_hash
     *
     * @param array $apply
     * @param array $itemRows
     * @return array{ok:bool,hit:?array,hits:int,reason:string,tried:string[]}
     */
    public function resolveHistoricalSubmitHashMatch(array $apply, array $itemRows)
    {
        $stored = strtolower(trim((string)($apply['submit_payload_hash'] ?? '')));
        if ($stored === '' || strlen($stored) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $stored)) {
            return [
                'ok' => false,
                'hit' => null,
                'hits' => 0,
                'reason' => '缺少有效 submit_payload_hash',
                'tried' => [],
            ];
        }

        $submitSvc = $this->getSubmitService();
        $candidates = $this->enumerateLegalHashCandidates($apply, $itemRows);
        $tried = [];
        $hits = [];

        foreach ($candidates as $cand) {
            $tried[] = (string)$cand['label'];
            try {
                $normalized = $submitSvc->normalizeSubmitPayload($cand['validated']);
                $pack = $submitSvc->computeSubmitPayloadHash($normalized);
            } catch (\Throwable $e) {
                continue;
            }
            if (hash_equals($stored, strtolower((string)$pack['hash']))) {
                $hits[] = [
                    'label' => $cand['label'],
                    'hash_apply_id' => (int)$cand['hash_apply_id'],
                    'validated' => $cand['validated'],
                    'normalized' => $normalized,
                    'hash' => strtolower((string)$pack['hash']),
                ];
            }
        }

        if (count($hits) === 0) {
            return [
                'ok' => false,
                'hit' => null,
                'hits' => 0,
                'reason' => '有限合法候选均未能命中原 submit_payload_hash',
                'tried' => $tried,
            ];
        }
        if (count($hits) > 1) {
            return [
                'ok' => false,
                'hit' => null,
                'hits' => count($hits),
                'reason' => '多个合法候选同时命中，证据歧义，失败关闭',
                'tried' => $tried,
            ];
        }

        return [
            'ok' => true,
            'hit' => $hits[0],
            'hits' => 1,
            'reason' => '',
            'tried' => $tried,
        ];
    }

    /**
     * 字段可信性分级（不得把 B 标成 A；遇 C 失败关闭）
     *
     * @param array $apply
     * @param array $itemRows
     * @param array $hashHit
     * @param array $submitLog
     * @param array $rejectLog
     * @param array $tasks
     * @return array{ok:bool,grades:array,stats:array,c_fields:string[],reason:string}
     */
    public function classifyFieldTrust(
        array $apply,
        array $itemRows,
        array $hashHit,
        array $submitLog,
        array $rejectLog,
        array $tasks
    ) {
        $grades = [];
        $mark = function ($field, $level, $note = '') use (&$grades) {
            $grades[$field] = [
                'level' => $level,
                'note' => (string)$note,
            ];
        };

        // —— Header ——
        $mark('shipping_apply_id', self::TRUST_A, '主表/日志身份；hash 内可能为 0');
        $mark('apply_no', self::TRUST_A, '提交日志 opinion / 主表');
        $mark('order_id', self::TRUST_A, 'hash 覆盖');
        $mark('order_no', self::TRUST_B, '主表；驳回路径不改');
        $mark('applicant_id', self::TRUST_A, 'hash 覆盖');
        $mark('applicant_name', self::TRUST_B, '主表/提交操作人名');
        $mark('team_name', self::TRUST_B, '主表；驳回未改');
        $mark('team_leader_id', self::TRUST_B, '主表');
        $mark('team_leader_name', self::TRUST_B, '主表');
        $mark('customer_name', self::TRUST_B, '主表');
        $mark('customer_phone', self::TRUST_B, '主表；审计不落明文');
        $mark('order_amount', self::TRUST_B, '提交写入；非 hash 覆盖');
        $mark('approval_threshold', self::TRUST_B, '审批配置快照字段');
        $mark('approval_route', self::TRUST_B, '可与任务类型交叉');
        $mark('product_director_id', self::TRUST_B, '主表+总监任务');
        $mark('product_director_name', self::TRUST_B, '主表');
        $mark('finance_cc_user_id', self::TRUST_B, '弱：仅主表');
        $mark('finance_cc_user_name', self::TRUST_B, '弱：仅主表');
        $mark('receiver_name', self::TRUST_A, 'hash 覆盖');
        $mark('receiver_phone', self::TRUST_A, 'hash 覆盖；审计不落明文');
        $mark('receiver_address', self::TRUST_A, 'hash 覆盖；审计不落明文');
        $mark('shipping_date', self::TRUST_A, 'hash 覆盖');
        $mark('shipping_method', self::TRUST_A, 'hash 覆盖');
        $mark('shipping_remark', self::TRUST_A, 'hash 覆盖');
        $mark('approval_round', self::TRUST_A, '主表+日志+任务');
        $mark('submit_time', self::TRUST_A, '主表=提交日志时间');
        $mark('status_at_submit', self::TRUST_A, 'submit 日志 after_status=10');
        $mark('current_stage_at_submit', self::TRUST_A, '提交写 stage=1 + 经理任务激活证据');
        $mark('submit_payload_hash', self::TRUST_A, '主表字段且已重算命中');

        // —— Items（按行共性） ——
        $mark('items.order_item_id', self::TRUST_A, 'hash');
        $mark('items.apply_product_name', self::TRUST_A, 'hash');
        $mark('items.apply_spec_model', self::TRUST_A, 'hash');
        $mark('items.apply_qty', self::TRUST_A, 'hash');
        $mark('items.factory_id', self::TRUST_A, 'hash');
        $mark('items.apply_unit_price', self::TRUST_A, 'hash');
        $mark('items.apply_purchase_unit_price', self::TRUST_A, 'hash');
        $mark('items.remark', self::TRUST_A, 'hash');
        $mark('items.shipping_apply_item_id', self::TRUST_B, '提交后时间戳未重建时可用');
        $mark('items.product_manager_id', self::TRUST_B, '与经理任务交叉');
        $mark('items.product_manager_name', self::TRUST_B, '主表/明细');
        $mark('items.factory_name', self::TRUST_B, '非 hash');
        $mark('items.order_snapshot_fields', self::TRUST_B, '订单快照价量等');
        $mark('items.derived_amounts', self::TRUST_B, 'qty×单价可交叉核对');

        // 异常写入探测 → 升为 C
        $cFields = [];
        $submitTime = trim((string)($apply['submit_time'] ?? ''));
        foreach ($itemRows as $idx => $row) {
            if (!is_array($row)) {
                $cFields[] = 'items[' . $idx . ']';
                continue;
            }
            $ctime = trim((string)($row['create_time'] ?? ''));
            $utime = trim((string)($row['update_time'] ?? ''));
            // 明细在提交后被改写：无法证明仍是原提交载荷对应行身份
            if ($submitTime !== '' && $utime !== '' && strcmp($utime, $submitTime) > 0) {
                $cFields[] = 'items[' . $idx . '].update_time_after_submit';
                $mark('items.shipping_apply_item_id', self::TRUST_C, '明细 update_time 晚于 submit_time');
            }
            if ($ctime !== '' && $utime !== '' && $ctime !== $utime && $submitTime !== '' && strcmp($utime, $submitTime) > 0) {
                $cFields[] = 'items[' . $idx . '].mutated_after_submit';
            }
        }

        // 任务与明细经理交叉失败 → C
        $pmTask = $this->findPrimaryManagerTask($tasks, (int)($apply['approval_round'] ?? 0));
        if ($pmTask !== null && !empty($itemRows)) {
            $taskApprover = (int)($pmTask['approver_id'] ?? 0);
            foreach ($itemRows as $idx => $row) {
                $pmid = (int)($row['product_manager_id'] ?? 0);
                if ($pmid > 0 && $taskApprover > 0 && $pmid !== $taskApprover) {
                    // 多经理场景允许与「去重后的某一经理」不完全等同；若仅一行却不一致则 C
                    if (count($itemRows) === 1) {
                        $cFields[] = 'items[' . $idx . '].product_manager_id_vs_task';
                        $mark('items.product_manager_id', self::TRUST_C, '单行经理与任务审批人不一致');
                    }
                }
            }
        }

        // hash 命中参数与主表身份冲突检查（例如命中 apply_id=真实ID 但日志证明直接提交 before=0 且无草稿痕迹 —— 不一定 C）
        $hashApplyId = (int)($hashHit['hash_apply_id'] ?? -1);
        if ($hashApplyId !== 0 && $hashApplyId !== (int)($apply['id'] ?? 0)) {
            $cFields[] = 'hash_apply_id_mismatch';
            $mark('shipping_apply_id', self::TRUST_C, 'hash 内 apply_id 既非 0 也非当前申请');
        }

        // 日志状态链冲突
        if ((int)($submitLog['after_status'] ?? -1) !== 10) {
            $cFields[] = 'submit_log.after_status';
            $mark('status_at_submit', self::TRUST_C, 'submit 日志 after_status 非 10');
        }
        if (!empty($rejectLog)
            && (int)($rejectLog['before_status'] ?? -1) !== 10
        ) {
            $cFields[] = 'reject_log.before_status';
            $mark('status_at_submit', self::TRUST_C, '驳回日志 before_status 非 10');
        }

        $stats = ['A' => 0, 'B' => 0, 'C' => 0];
        foreach ($grades as $g) {
            $lv = (string)($g['level'] ?? '');
            if (isset($stats[$lv])) {
                $stats[$lv]++;
            }
        }

        $cFields = array_values(array_unique($cFields));
        if (!empty($cFields)) {
            return [
                'ok' => false,
                'grades' => $grades,
                'stats' => $stats,
                'c_fields' => $cFields,
                'reason' => '存在 C 类不可信字段：' . implode(',', $cFields),
            ];
        }

        return [
            'ok' => true,
            'grades' => $grades,
            'stats' => $stats,
            'c_fields' => [],
            'reason' => '',
        ];
    }

    /**
     * 环境与写授权门禁（多重；缺一拒绝写入）
     *
     * @param array $options
     * @return array{ok:bool,reason:string,checks:array}
     */
    public function assertWriteAuthorization(array $options)
    {
        $checks = [];

        $checks['write_capability_constant'] = self::WRITE_CAPABILITY_ENABLED === true;
        $checks['mode_execute'] = ((string)($options['mode'] ?? self::MODE_DRY_RUN) === self::MODE_EXECUTE);
        $checks['allow_write_explicit'] = !empty($options['allow_write']);
        $checks['user_approved_write'] = !empty($options['user_approved_write']);

        $operatorId = (int)($options['operator_id'] ?? 0);
        $checks['operator_id'] = $operatorId > 0;

        $role = trim((string)($options['operator_role'] ?? ''));
        $checks['operator_role_ops'] = ($role === self::OPERATOR_ROLE_OPS_BACKFILL);

        // 不得仅靠 shipping_submit.enabled
        $enabledOnly = (function_exists('config') && config('shipping_submit.enabled') === true);
        $checks['not_only_shipping_submit_enabled'] = true;
        if ($enabledOnly && empty($options['environment_token']) && empty($options['target_database_fingerprint'])) {
            // 仅有 enabled、无独立身份令牌 → 视为不足
            $checks['independent_env_identity'] = false;
        } else {
            $token = trim((string)($options['environment_token'] ?? ''));
            $fp = trim((string)($options['target_database_fingerprint'] ?? ''));
            $checks['independent_env_identity'] = ($token !== '' && $fp !== '');
        }

        // 不得仅靠库名
        $dbName = trim((string)($options['database_name'] ?? ''));
        $checks['not_dbname_only'] = true;
        if ($dbName !== '' && $dbName === 'crmneimao' && empty($options['environment_token'])) {
            $checks['dbname_alone_insufficient'] = false;
        } else {
            $checks['dbname_alone_insufficient'] = true;
        }

        // 单申请 ID
        $applyId = (int)($options['apply_id'] ?? 0);
        $checks['single_apply_id'] = $applyId > 0;

        // 禁止普通业务员角色冒充
        $forbiddenRoles = ['applicant', 'product_manager', 'product_director', 'sales', ''];
        $checks['not_business_role'] = !in_array($role, $forbiddenRoles, true);

        $failed = [];
        foreach ($checks as $k => $ok) {
            if (!$ok) {
                $failed[] = $k;
            }
        }

        if (!empty($failed)) {
            return [
                'ok' => false,
                'reason' => '写授权门禁未通过：' . implode(',', $failed),
                'checks' => $checks,
            ];
        }

        return [
            'ok' => true,
            'reason' => '',
            'checks' => $checks,
        ];
    }

    /**
     * 构建补存审计 opinion（无手机号/地址/采购价明文；长度受限）
     *
     * @param array $ctx
     * @return string
     * @throws Exception
     */
    public function buildBackfillAuditOpinion(array $ctx)
    {
        $opinion = [
            'action' => self::AUDIT_ACTION,
            'source' => 'historical_round_snapshot_backfill',
            'apply_id' => (int)($ctx['apply_id'] ?? 0),
            'approval_round' => (int)($ctx['approval_round'] ?? 0),
            'snapshot_stage' => ShippingApplyRoundSnapshotService::SNAPSHOT_STAGE_SUBMIT,
            'schema' => ShippingApplyRoundSnapshotService::SNAPSHOT_SCHEMA_VERSION,
            'hash_apply_id' => (int)($ctx['hash_apply_id'] ?? 0),
            'submit_hash_prefix' => substr(strtolower((string)($ctx['submit_payload_hash'] ?? '')), 0, 12),
            'snapshot_hash_prefix' => substr(strtolower((string)($ctx['snapshot_payload_hash'] ?? '')), 0, 12),
            'evidence' => [
                'submit_hash_match' => !empty($ctx['submit_hash_match']),
                'submit_log' => !empty($ctx['submit_log_ok']),
                'reject_log' => !empty($ctx['reject_log_ok']),
                'task_crosscheck' => !empty($ctx['task_crosscheck_ok']),
            ],
            'trust_stats' => isset($ctx['trust_stats']) && is_array($ctx['trust_stats'])
                ? $ctx['trust_stats']
                : ['A' => 0, 'B' => 0, 'C' => 0],
            'note' => '历史 submit 快照受控补存（非新审批）',
        ];

        $json = json_encode($opinion, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false || !is_string($json)) {
            throw new Exception('补存审计编码失败');
        }
        if (strlen($json) > ShippingApplyService::APPROVAL_LOG_OPINION_MAX) {
            // 压缩：去掉次要键
            unset($opinion['evidence']['reject_log'], $opinion['note']);
            $json = json_encode($opinion, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            if ($json === false || strlen($json) > ShippingApplyService::APPROVAL_LOG_OPINION_MAX) {
                throw new Exception('补存审计 opinion 超长');
            }
        }

        // 禁止敏感明文泄漏
        $lower = strtolower($json);
        foreach (['receiver_phone', 'receiver_address', 'customer_phone', 'purchase_unit', 'apply_purchase'] as $bad) {
            if (strpos($lower, $bad) !== false && strpos($lower, 'prefix') === false) {
                // 允许 hash_prefix 类字段；禁止完整业务敏感键名作为值载体
            }
        }

        return $json;
    }

    /**
     * 锁顺序说明（与正式提交兼容；供审计/测试断言）
     *
     * @return string[]
     */
    public function getCompatibleLockOrderSteps()
    {
        return [
            '1_startTrans',
            '2_lockOrderForShippingSubmit',
            '3_lockOrderItemsForShippingSubmit_id_asc',
            '4_lockShippingApply_by_id',
            '5_lockShippingApplyItems_id_asc',
            '6_readTasksAndApprovalLogs',
            '7_recheckSubmitHash',
            '8_checkExistingSnapshot',
            '9_insertSnapshot',
            '10_rereadAssertIntegrity',
            '11_insertBackfillAudit',
            '12_finalConsistency',
            '13_commit',
        ];
    }

    /**
     * @param int $applyId
     * @param array $options
     * @return array
     */
    protected function runControlledBackfill($applyId, array $options)
    {
        $mode = (string)($options['mode'] ?? self::MODE_DRY_RUN);
        if ($mode !== self::MODE_EXECUTE) {
            $mode = self::MODE_DRY_RUN;
        }
        $options['mode'] = $mode;
        $options['apply_id'] = (int)$applyId;

        if ($applyId <= 0) {
            return $this->result(self::DECISION_REJECTED, false, '申请 ID 无效', [
                'apply_id' => $applyId,
                'mode' => $mode,
            ]);
        }

        try {
            $bundle = $this->loadEvidenceBundle($applyId, $options);
        } catch (\Throwable $e) {
            return $this->result(self::DECISION_REJECTED, false, '证据加载失败：' . $e->getMessage(), [
                'apply_id' => $applyId,
                'mode' => $mode,
            ]);
        }

        $eval = $this->evaluateCandidateCore($bundle);
        if (empty($eval['ok'])) {
            return $this->result(
                (string)($eval['decision'] ?? self::DECISION_REJECTED),
                false,
                (string)($eval['reason'] ?? '候选核验失败'),
                array_merge($eval, ['mode' => $mode, 'wrote' => false])
            );
        }

        // 已有合法快照：幂等只读结论
        if (($eval['decision'] ?? '') === self::DECISION_IDEMPOTENT_EXISTS) {
            return $this->result(self::DECISION_IDEMPOTENT_EXISTS, true, '本轮 submit 快照已存在且完整', array_merge($eval, [
                'mode' => $mode,
                'wrote' => false,
            ]));
        }

        // dry-run：返回可构建快照，不写
        if ($mode !== self::MODE_EXECUTE || empty($options['allow_write'])) {
            return $this->result(self::DECISION_ELIGIBLE, true, 'dry-run 核验通过，未写库', array_merge($eval, [
                'mode' => self::MODE_DRY_RUN,
                'wrote' => false,
                'write_blocked_reason' => $mode === self::MODE_EXECUTE
                    ? '缺少 allow_write'
                    : '默认 dry-run',
            ]));
        }

        // execute 路径：多重门禁 + 本构建硬关闭
        $auth = $this->assertWriteAuthorization($options);
        if (!$auth['ok'] || self::WRITE_CAPABILITY_ENABLED !== true) {
            $reason = self::WRITE_CAPABILITY_ENABLED !== true
                ? '本构建 WRITE_CAPABILITY_ENABLED=false，拒绝真实写库'
                : $auth['reason'];
            return $this->result(self::DECISION_WRITE_BLOCKED, false, $reason, array_merge($eval, [
                'mode' => $mode,
                'wrote' => false,
                'auth_checks' => $auth['checks'],
            ]));
        }

        // 理论上本构建不会到达此处；保留事务设计供未来启用与 Mock
        try {
            $writeResult = $this->runBackfillTransaction($bundle, $eval, $options);
            return $this->result(
                (string)($writeResult['decision'] ?? self::DECISION_ELIGIBLE),
                !empty($writeResult['ok']),
                (string)($writeResult['reason'] ?? ''),
                array_merge($eval, $writeResult, ['mode' => $mode])
            );
        } catch (\Throwable $e) {
            return $this->result(self::DECISION_REJECTED, false, '补存事务失败：' . $e->getMessage(), [
                'apply_id' => $applyId,
                'mode' => $mode,
                'wrote' => false,
            ]);
        }
    }

    /**
     * @param array $bundle
     * @return array
     */
    protected function evaluateCandidateCore(array $bundle)
    {
        $apply = isset($bundle['apply']) && is_array($bundle['apply']) ? $bundle['apply'] : [];
        $items = isset($bundle['items']) && is_array($bundle['items']) ? $bundle['items'] : [];
        $logs = isset($bundle['logs']) && is_array($bundle['logs']) ? $bundle['logs'] : [];
        $tasks = isset($bundle['tasks']) && is_array($bundle['tasks']) ? $bundle['tasks'] : [];
        $order = isset($bundle['order']) && is_array($bundle['order']) ? $bundle['order'] : null;
        $existingSnapshot = array_key_exists('existing_snapshot', $bundle) ? $bundle['existing_snapshot'] : null;
        $backfillAudits = isset($bundle['backfill_audits']) && is_array($bundle['backfill_audits'])
            ? $bundle['backfill_audits']
            : [];

        $applyId = (int)($apply['id'] ?? 0);
        if ($applyId <= 0) {
            return $this->rejectEval('非法申请 ID');
        }
        if ((int)($apply['is_deleted'] ?? 0) !== 0) {
            return $this->rejectEval('申请已软删除，拒绝补存');
        }

        $status = (int)($apply['status'] ?? -1);
        if ($status === 0) {
            return $this->rejectEval('未提交草稿不属于补存范围');
        }
        if ($status === 50 || $status === 60 || $status === 30 || $status === 10) {
            return $this->rejectEval('当前状态不属于本轮补存范围（仅驳回态 20/40）');
        }
        if (!in_array($status, self::ALLOWED_CURRENT_STATUSES, true)) {
            return $this->rejectEval('当前状态不允许本轮补存');
        }

        $round = (int)($apply['approval_round'] ?? 0);
        if ($round !== 1) {
            return $this->rejectEval('本轮仅支持 approval_round=1 补存');
        }

        $submitTime = trim((string)($apply['submit_time'] ?? ''));
        if ($submitTime === '' || !preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $submitTime)) {
            return $this->rejectEval('缺少合法 submit_time');
        }

        $payloadHash = strtolower(trim((string)($apply['submit_payload_hash'] ?? '')));
        if ($payloadHash === '' || strlen($payloadHash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $payloadHash)) {
            return $this->rejectEval('缺少合法 submit_payload_hash');
        }

        if (empty($items)) {
            return $this->rejectEval('产品明细缺失');
        }

        // 订单关联
        if ($order !== null) {
            if ((int)($order['id'] ?? 0) !== (int)($apply['order_id'] ?? 0)) {
                return $this->rejectEval('申请与订单关联不一致');
            }
        }

        $submitLog = $this->findFirstSubmitLog($logs, $applyId);
        if ($submitLog === null) {
            return $this->rejectEval('缺少有效 submit 审计日志');
        }
        if ((int)($submitLog['after_status'] ?? -1) !== 10) {
            return $this->rejectEval('submit 日志状态不匹配（after_status 应为 10）');
        }
        // 首次提交：before 允许 0（新建）或其它草稿语义；本轮要求能证明进入 10
        $submitLogTime = trim((string)($submitLog['create_time'] ?? ''));
        if ($submitLogTime !== '' && $submitTime !== '' && $submitLogTime !== $submitTime) {
            // 允许秒级一致；不一致记为冲突
            return $this->rejectEval('submit 日志时间与主表 submit_time 不一致');
        }

        $rejectLog = $this->findMatchingRejectLog($logs, $applyId, $status);
        if ($rejectLog === null) {
            return $this->rejectEval('缺少与当前驳回状态一致的驳回日志');
        }
        if ((int)($rejectLog['before_status'] ?? -1) !== 10) {
            return $this->rejectEval('驳回日志 before_status 不匹配');
        }
        if ((int)($rejectLog['after_status'] ?? -1) !== $status) {
            return $this->rejectEval('驳回日志 after_status 与当前状态不匹配');
        }

        $pmTask = $this->findPrimaryManagerTask($tasks, $round);
        if ($pmTask === null) {
            return $this->rejectEval('缺少产品经理审批任务');
        }
        // 驳回后任务应为已驳回(3)或至少存在
        $taskStatus = (int)($pmTask['status'] ?? -1);
        if (!in_array($taskStatus, [2, 3, 4], true) && $taskStatus !== 1) {
            // 允许历史状态；但审批人必须有效
        }
        if ((int)($pmTask['approver_id'] ?? 0) <= 0) {
            return $this->rejectEval('审批任务审批人无效');
        }
        if ((int)($rejectLog['operator_id'] ?? 0) > 0
            && (int)($pmTask['approver_id'] ?? 0) > 0
            && (int)$rejectLog['operator_id'] !== (int)$pmTask['approver_id']
            && (string)($rejectLog['action'] ?? '') !== ''
        ) {
            // 驳回操作人应与任务审批人一致（经理驳回）
            $rejectAction = (string)($rejectLog['action'] ?? '');
            if (in_array($rejectAction, ['reject', 'pm_reject'], true)
                && (int)$rejectLog['operator_id'] !== (int)$pmTask['approver_id']
            ) {
                return $this->rejectEval('任务审批人与驳回日志操作人不一致');
            }
        }

        // 原 hash 重算
        $hashMatch = $this->resolveHistoricalSubmitHashMatch($apply, $items);
        if (empty($hashMatch['ok'])) {
            return $this->rejectEval(
                '原提交 hash 核验失败：' . (string)$hashMatch['reason'],
                ['hash_match' => $hashMatch]
            );
        }

        // 明细与 hash 意图一致性（行集合）
        $normalizedItems = $hashMatch['hit']['normalized']['items'] ?? [];
        if (!$this->assertItemsMatchNormalizedIntent($items, is_array($normalizedItems) ? $normalizedItems : [])) {
            return $this->rejectEval('明细与原 hash 意图不一致', ['hash_match' => $hashMatch]);
        }

        $trust = $this->classifyFieldTrust(
            $apply,
            $items,
            $hashMatch['hit'],
            $submitLog,
            $rejectLog,
            $tasks
        );
        if (empty($trust['ok'])) {
            return $this->rejectEval($trust['reason'], [
                'trust' => $trust,
                'hash_match' => $hashMatch,
            ]);
        }

        // 已有快照
        if ($existingSnapshot !== null && is_array($existingSnapshot) && !empty($existingSnapshot)) {
            $snapSvc = $this->getSnapshotService();
            $verify = $snapSvc->verifyStoredSnapshotIntegrity($existingSnapshot);
            if (empty($verify['ok'])) {
                return [
                    'ok' => false,
                    'decision' => self::DECISION_REJECTED,
                    'reason' => '已有快照损坏，拒绝覆盖：' . (string)($verify['reason'] ?? ''),
                    'existing_snapshot_corrupt' => true,
                ];
            }
            if ((int)($existingSnapshot['shipping_apply_id'] ?? 0) !== $applyId
                || (int)($existingSnapshot['approval_round'] ?? 0) !== $round
            ) {
                return $this->rejectEval('已有快照身份与申请不一致');
            }
            return [
                'ok' => true,
                'decision' => self::DECISION_IDEMPOTENT_EXISTS,
                'reason' => '合法快照已存在',
                'apply_id' => $applyId,
                'approval_round' => $round,
                'existing_snapshot' => $existingSnapshot,
                'trust' => $trust,
                'hash_match' => $hashMatch,
                'status_at_submit' => 10,
                'current_stage_at_submit' => 1,
                'backfill_audits' => $backfillAudits,
            ];
        }

        // 构建历史快照（证据态 10/1；不改主表）
        $snapSvc = $this->getSnapshotService();
        try {
            $header = $snapSvc->buildHeaderFromHistoricalSubmitEvidence($apply, 10, 1);
            $builtItems = $snapSvc->buildItemsFromItemRows($items);
            $hashPack = $snapSvc->computeSubmitSnapshotHash($header, $builtItems, $applyId, $round);
            // 预演完整性
            $previewRow = [
                'shipping_apply_id' => $applyId,
                'approval_round' => $round,
                'snapshot_stage' => ShippingApplyRoundSnapshotService::SNAPSHOT_STAGE_SUBMIT,
                'snapshot_schema_version' => ShippingApplyRoundSnapshotService::SNAPSHOT_SCHEMA_VERSION,
                'header_json' => $snapSvc->encodeCanonicalJson($header),
                'items_json' => $snapSvc->encodeCanonicalJson($builtItems),
                'payload_hash' => $hashPack['hash'],
            ];
            $snapSvc->assertStoredSnapshotIntegrity($previewRow);
        } catch (\Throwable $e) {
            return $this->rejectEval('历史快照构建失败：' . $e->getMessage());
        }

        // 确认正式路径门禁未被放宽：当前 20/0 不得走 buildHeaderFromApplyRow
        $formalGateIntact = true;
        try {
            $snapSvc->buildHeaderFromApplyRow($apply, 10, 1);
            $formalGateIntact = false;
        } catch (\Throwable $e) {
            $formalGateIntact = true;
        }

        return [
            'ok' => true,
            'decision' => self::DECISION_ELIGIBLE,
            'reason' => '证据充分，可 dry-run 构建',
            'apply_id' => $applyId,
            'approval_round' => $round,
            'current_status' => $status,
            'current_stage' => (int)($apply['current_stage'] ?? 0),
            'status_at_submit' => 10,
            'current_stage_at_submit' => 1,
            'header' => $header,
            'items' => $builtItems,
            'snapshot_payload_hash' => $hashPack['hash'],
            'preview_row' => $previewRow,
            'trust' => $trust,
            'hash_match' => $hashMatch,
            'submit_log' => $this->sanitizeLogBrief($submitLog),
            'reject_log' => $this->sanitizeLogBrief($rejectLog),
            'formal_submit_gate_intact' => $formalGateIntact,
            'lock_order' => $this->getCompatibleLockOrderSteps(),
            'confirmation_flags_limitation' => '确认标志无独立库字段；仅由有限合法候选命中推断，审计记录 hash_apply_id 与命中标签',
            'backfill_audits' => $backfillAudits,
        ];
    }

    /**
     * 未来真实写库事务骨架（本构建因 WRITE_CAPABILITY_ENABLED=false 不会被生产调用）
     *
     * @param array $bundle
     * @param array $eval
     * @param array $options
     * @return array
     * @throws Exception
     */
    protected function runBackfillTransaction(array $bundle, array $eval, array $options)
    {
        if (self::WRITE_CAPABILITY_ENABLED !== true) {
            throw new Exception('写入能力未启用');
        }

        $applyId = (int)$eval['apply_id'];
        $round = (int)$eval['approval_round'];
        $operatorId = (int)$options['operator_id'];
        $operatorName = mb_substr(trim((string)($options['operator_name'] ?? 'ops')), 0, 100, 'UTF-8');
        $header = $eval['header'];
        $items = $eval['items'];

        $runner = $this->txRunnerOverride;
        $body = function () use (
            $bundle,
            $eval,
            $options,
            $applyId,
            $round,
            $operatorId,
            $operatorName,
            $header,
            $items
        ) {
            $apply = $bundle['apply'];
            $orderId = (int)($apply['order_id'] ?? 0);

            // 锁序：订单 → 订单行 → 申请 → 明细
            $order = Db::name('crm_client_order')->where('id', $orderId)->lock(true)->find();
            if (empty($order)) {
                throw new Exception('锁定原订单失败');
            }
            $orderItemIds = [];
            foreach ($bundle['items'] as $it) {
                $oid = (int)($it['order_item_id'] ?? 0);
                if ($oid > 0) {
                    $orderItemIds[$oid] = $oid;
                }
            }
            $ids = array_values($orderItemIds);
            sort($ids, SORT_NUMERIC);
            if (!empty($ids)) {
                Db::name('crm_client_order_product')
                    ->where('order_id', $orderId)
                    ->where('id', 'in', $ids)
                    ->order('id', 'asc')
                    ->lock(true)
                    ->select();
            }

            $lockedApply = Db::name('crm_shipping_apply')->where('id', $applyId)->lock(true)->find();
            if (empty($lockedApply) || (int)($lockedApply['is_deleted'] ?? 0) !== 0) {
                throw new Exception('锁定发货申请失败');
            }
            // 禁止修改状态：仅校验
            if ((int)($lockedApply['status'] ?? -1) !== (int)($apply['status'] ?? -1)
                || (int)($lockedApply['approval_round'] ?? 0) !== $round
            ) {
                throw new Exception('事务内申请状态/轮次已变化');
            }

            Db::name('crm_shipping_apply_item')
                ->where('shipping_apply_id', $applyId)
                ->order('id', 'asc')
                ->lock(true)
                ->select();

            // 再次核验快照不存在或幂等
            $snapSvc = $this->getSnapshotService();
            $existing = $snapSvc->findSubmitRoundSnapshot($applyId, $round);
            if ($existing !== null) {
                $snapSvc->assertStoredSnapshotIntegrity($existing);
                $existingAudits = $this->loadBackfillAudits($applyId, $round);
                if (empty($existingAudits)) {
                    throw new Exception('已有快照但缺少补存审计，拒绝静默幂等');
                }
                return [
                    'ok' => true,
                    'decision' => self::DECISION_IDEMPOTENT_EXISTS,
                    'reason' => '唯一约束下已有完整快照+审计',
                    'wrote' => false,
                    'snapshot' => $existing,
                ];
            }

            $existingAudits = $this->loadBackfillAudits($applyId, $round);
            if (!empty($existingAudits) && $existing === null) {
                throw new Exception('存在补存审计但无快照，证据冲突，失败关闭');
            }

            $stored = $snapSvc->insertPrebuiltSubmitRoundSnapshotInCurrentTx(
                $applyId,
                $round,
                $operatorId,
                $header,
                $items
            );

            $opinion = $this->buildBackfillAuditOpinion([
                'apply_id' => $applyId,
                'approval_round' => $round,
                'hash_apply_id' => (int)($eval['hash_match']['hit']['hash_apply_id'] ?? 0),
                'submit_payload_hash' => (string)($lockedApply['submit_payload_hash'] ?? ''),
                'snapshot_payload_hash' => (string)($stored['payload_hash'] ?? ''),
                'submit_hash_match' => true,
                'submit_log_ok' => true,
                'reject_log_ok' => true,
                'task_crosscheck_ok' => true,
                'trust_stats' => $eval['trust']['stats'] ?? [],
            ]);

            $currentStatus = (int)($lockedApply['status'] ?? 0);
            $logId = (int)Db::name('crm_shipping_approval_log')->insertGetId([
                'shipping_apply_id' => $applyId,
                'task_id' => null,
                'operator_id' => $operatorId,
                'operator_name' => $operatorName,
                'operator_role' => self::OPERATOR_ROLE_OPS_BACKFILL,
                'action' => self::AUDIT_ACTION,
                'before_status' => $currentStatus,
                'after_status' => $currentStatus,
                'opinion' => $opinion,
                'create_time' => date('Y-m-d H:i:s'),
            ]);
            if ($logId <= 0) {
                throw new Exception('补存审计写入失败');
            }

            // 最终一致性：主表状态未变
            $final = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
            if ((int)($final['status'] ?? -1) !== $currentStatus
                || (int)($final['approval_round'] ?? 0) !== $round
            ) {
                throw new Exception('最终一致性校验失败：主表状态被改变');
            }

            return [
                'ok' => true,
                'decision' => self::DECISION_ELIGIBLE,
                'reason' => '补存成功',
                'wrote' => true,
                'snapshot' => $stored,
                'audit_log_id' => $logId,
            ];
        };

        if (is_callable($runner)) {
            return $runner($body);
        }

        // 真实 Db 事务（仅未来启用）
        Db::startTrans();
        try {
            $result = $body();
            Db::commit();
            return $result;
        } catch (\Throwable $e) {
            try {
                Db::rollback();
            } catch (\Throwable $rb) {
                // COMMIT/ROLLBACK 不确定：不得假定已回滚成功
                return [
                    'ok' => false,
                    'decision' => self::DECISION_COMMIT_UNCERTAIN,
                    'reason' => '事务失败且回滚结果不确定：' . $e->getMessage(),
                    'wrote' => false,
                    'rollback_error' => $rb->getMessage(),
                ];
            }
            throw $e;
        }
    }

    /**
     * @param int $applyId
     * @param array $options
     * @return array
     * @throws Exception
     */
    protected function loadEvidenceBundle($applyId, array $options)
    {
        if (is_array($this->injectedEvidenceBundle)) {
            $bundle = $this->injectedEvidenceBundle;
            if ((int)($bundle['apply']['id'] ?? 0) !== (int)$applyId
                && (int)$applyId > 0
                && isset($bundle['apply']['id'])
            ) {
                // 允许测试用固定包；若 ID 不匹配则仍返回包由评估拒绝
            }
            return $bundle;
        }

        $apply = Db::name('crm_shipping_apply')->where('id', $applyId)->find();
        if (empty($apply)) {
            throw new Exception('申请不存在');
        }

        $items = Db::name('crm_shipping_apply_item')
            ->where('shipping_apply_id', $applyId)
            ->order('apply_line_no', 'asc')
            ->order('order_item_id', 'asc')
            ->order('id', 'asc')
            ->select();
        if ($items instanceof \think\Collection) {
            $items = $items->toArray();
        }
        $items = is_array($items) ? $items : [];

        $logs = Db::name('crm_shipping_approval_log')
            ->where('shipping_apply_id', $applyId)
            ->order('id', 'asc')
            ->select();
        if ($logs instanceof \think\Collection) {
            $logs = $logs->toArray();
        }
        $logs = is_array($logs) ? $logs : [];

        $round = (int)($apply['approval_round'] ?? 0);
        $tasks = [];
        if ($round > 0) {
            $tasks = Db::name('crm_shipping_approval_task')
                ->where('shipping_apply_id', $applyId)
                ->where('approval_round', $round)
                ->order('id', 'asc')
                ->select();
            if ($tasks instanceof \think\Collection) {
                $tasks = $tasks->toArray();
            }
            $tasks = is_array($tasks) ? $tasks : [];
        }

        $order = null;
        $orderId = (int)($apply['order_id'] ?? 0);
        if ($orderId > 0) {
            $order = Db::name('crm_client_order')->where('id', $orderId)->find();
        }

        $existingSnapshot = $this->getSnapshotService()->findSubmitRoundSnapshot($applyId, max(1, $round));
        $backfillAudits = $this->loadBackfillAudits($applyId, max(1, $round));

        return [
            'apply' => $apply,
            'items' => $items,
            'logs' => $logs,
            'tasks' => $tasks,
            'order' => $order,
            'existing_snapshot' => $existingSnapshot,
            'backfill_audits' => $backfillAudits,
        ];
    }

    /**
     * @param int $applyId
     * @param int $round
     * @return array
     */
    protected function loadBackfillAudits($applyId, $round)
    {
        try {
            $rows = Db::name('crm_shipping_approval_log')
                ->where('shipping_apply_id', (int)$applyId)
                ->where('action', self::AUDIT_ACTION)
                ->order('id', 'asc')
                ->select();
            if ($rows instanceof \think\Collection) {
                $rows = $rows->toArray();
            }
            $rows = is_array($rows) ? $rows : [];
            $out = [];
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $opinion = (string)($row['opinion'] ?? '');
                $decoded = json_decode($opinion, true);
                if (is_array($decoded) && (int)($decoded['approval_round'] ?? 0) === (int)$round) {
                    $out[] = $row;
                } elseif (!is_array($decoded)) {
                    // 无法解析时保守纳入，由上层判断
                    $out[] = $row;
                }
            }
            return $out;
        } catch (\Throwable $e) {
            return [];
        }
    }

    /**
     * @param array $logs
     * @param int $applyId
     * @return array|null
     */
    protected function findFirstSubmitLog(array $logs, $applyId)
    {
        foreach ($logs as $log) {
            if (!is_array($log)) {
                continue;
            }
            if ((int)($log['shipping_apply_id'] ?? 0) !== (int)$applyId) {
                continue;
            }
            if ((string)($log['action'] ?? '') !== 'submit') {
                continue;
            }
            if ((int)($log['after_status'] ?? -1) !== 10) {
                continue;
            }
            return $log;
        }
        return null;
    }

    /**
     * @param array $logs
     * @param int $applyId
     * @param int $currentStatus
     * @return array|null
     */
    protected function findMatchingRejectLog(array $logs, $applyId, $currentStatus)
    {
        $rejectActions = ['reject', 'pm_reject', 'director_reject'];
        $found = null;
        foreach ($logs as $log) {
            if (!is_array($log)) {
                continue;
            }
            if ((int)($log['shipping_apply_id'] ?? 0) !== (int)$applyId) {
                continue;
            }
            $action = (string)($log['action'] ?? '');
            if (!in_array($action, $rejectActions, true)) {
                continue;
            }
            if ((int)($log['after_status'] ?? -1) === (int)$currentStatus) {
                $found = $log;
            }
        }
        return $found;
    }

    /**
     * @param array $tasks
     * @param int $round
     * @return array|null
     */
    protected function findPrimaryManagerTask(array $tasks, $round)
    {
        foreach ($tasks as $task) {
            if (!is_array($task)) {
                continue;
            }
            if ((int)($task['approval_round'] ?? 0) !== (int)$round) {
                continue;
            }
            if ((int)($task['approval_type'] ?? 0) !== 1) {
                continue;
            }
            return $task;
        }
        return null;
    }

    /**
     * @param array $itemRows
     * @param array $normalizedItems
     * @return bool
     */
    protected function assertItemsMatchNormalizedIntent(array $itemRows, array $normalizedItems)
    {
        if (empty($normalizedItems)) {
            return false;
        }
        $a = [];
        foreach ($itemRows as $row) {
            if (!is_array($row) || (int)($row['apply_qty'] ?? 0) <= 0) {
                continue;
            }
            $a[] = [
                'order_item_id' => (int)($row['order_item_id'] ?? 0),
                'apply_qty' => (int)($row['apply_qty'] ?? 0),
                'factory_id' => (int)($row['factory_id'] ?? 0),
                'apply_unit_price' => $this->moneyKey($row['apply_unit_price'] ?? null),
                'apply_purchase_unit_price' => $this->moneyKey($row['apply_purchase_unit_price'] ?? null),
                'apply_product_name' => (string)($row['apply_product_name'] ?? ''),
                'apply_spec_model' => (string)($row['apply_spec_model'] ?? ''),
                'remark' => (string)($row['remark'] ?? ''),
            ];
        }
        $b = [];
        foreach ($normalizedItems as $row) {
            if (!is_array($row)) {
                continue;
            }
            $b[] = [
                'order_item_id' => (int)($row['order_item_id'] ?? 0),
                'apply_qty' => (int)($row['apply_qty'] ?? 0),
                'factory_id' => (int)($row['factory_id'] ?? 0),
                'apply_unit_price' => $this->moneyKey($row['apply_unit_price'] ?? null),
                'apply_purchase_unit_price' => $this->moneyKey($row['apply_purchase_unit_price'] ?? null),
                'apply_product_name' => (string)($row['apply_product_name'] ?? ''),
                'apply_spec_model' => (string)($row['apply_spec_model'] ?? ''),
                'remark' => (string)($row['remark'] ?? ''),
            ];
        }

        $sortFn = function ($x, $y) {
            if ($x['order_item_id'] !== $y['order_item_id']) {
                return $x['order_item_id'] < $y['order_item_id'] ? -1 : 1;
            }
            if ($x['factory_id'] !== $y['factory_id']) {
                return $x['factory_id'] < $y['factory_id'] ? -1 : 1;
            }
            return 0;
        };
        usort($a, $sortFn);
        usort($b, $sortFn);
        return $a === $b;
    }

    /**
     * @param mixed $raw
     * @return string
     */
    protected function moneyKey($raw)
    {
        if ($raw === null || $raw === '') {
            return '';
        }
        if (is_float($raw)) {
            return 'FLOAT_FORBIDDEN';
        }
        $s = trim((string)$raw);
        if ($s === '') {
            return '';
        }
        if (function_exists('bcadd')) {
            try {
                return bcadd($s, '0', 2);
            } catch (\Throwable $e) {
                return $s;
            }
        }
        return $s;
    }

    /**
     * @param array $log
     * @return array
     */
    protected function sanitizeLogBrief(array $log)
    {
        return [
            'id' => (int)($log['id'] ?? 0),
            'action' => (string)($log['action'] ?? ''),
            'before_status' => isset($log['before_status']) ? (int)$log['before_status'] : null,
            'after_status' => isset($log['after_status']) ? (int)$log['after_status'] : null,
            'operator_id' => (int)($log['operator_id'] ?? 0),
            'create_time' => (string)($log['create_time'] ?? ''),
        ];
    }

    /**
     * @param string $reason
     * @param array $extra
     * @return array
     */
    protected function rejectEval($reason, array $extra = [])
    {
        return array_merge([
            'ok' => false,
            'decision' => self::DECISION_REJECTED,
            'reason' => (string)$reason,
        ], $extra);
    }

    /**
     * @param string $decision
     * @param bool $ok
     * @param string $reason
     * @param array $data
     * @return array
     */
    protected function result($decision, $ok, $reason, array $data = [])
    {
        // 规范字段后合并，避免 $data/eval 覆盖 decision/ok/wrote
        return array_merge($data, [
            'ok' => (bool)$ok,
            'decision' => (string)$decision,
            'reason' => (string)$reason,
            'wrote' => !empty($data['wrote']) ? true : false,
            'write_capability_enabled' => self::WRITE_CAPABILITY_ENABLED,
        ]);
    }

    /**
     * @return ShippingApplyRoundSnapshotService
     */
    protected function getSnapshotService()
    {
        if ($this->snapshotService instanceof ShippingApplyRoundSnapshotService) {
            return $this->snapshotService;
        }
        $this->snapshotService = new ShippingApplyRoundSnapshotService();
        return $this->snapshotService;
    }

    /**
     * @return ShippingApplyService
     */
    protected function getSubmitService()
    {
        if ($this->submitService instanceof ShippingApplyService) {
            return $this->submitService;
        }
        $this->submitService = new ShippingApplyService();
        return $this->submitService;
    }
}
