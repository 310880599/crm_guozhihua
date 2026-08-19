<?php

namespace app\admin\service;

use think\Db;
use think\facade\Log;

/**
 * 客户负责人任职阶段生命周期记录（第一阶段）
 *
 * 职责：
 * - 维护 crm_client_owner_history 的“任职阶段”记录（一段时间内某个负责人负责某个客户）
 * - 在负责人发生转移时：校验当前阶段与旧负责人一致 -> 关闭旧阶段 -> 新增新阶段
 *
 * 明确不负责：
 * - 不是 crm_leads.pr_user / pr_user_id 的替代数据源，当前负责人始终以 crm_leads 为准
 * - 不自动补写历史初始化数据（history_init / baseline_init），历史缺失时直接阻断并抛异常
 * - 不管理事务：调用方（Controller）负责 Db::startTrans() / commit() / rollback()
 */
class ClientOwnerHistoryService
{
    const TABLE = 'crm_client_owner_history';

    /**
     * 判断新旧负责人是否为同一人（优先按 user_id 判断）
     *
     * @param array $oldOwner ['user_id' => int, 'user_name' => string]
     * @param array $newOwner ['user_id' => int, 'user_name' => string]
     * @return bool
     */
    public function isSameOwner(array $oldOwner, array $newOwner)
    {
        $oldId = (int)($oldOwner['user_id'] ?? 0);
        $newId = (int)($newOwner['user_id'] ?? 0);
        if ($oldId > 0 && $newId > 0) {
            return $oldId === $newId;
        }

        // 旧数据 user_id 缺失时，谨慎地用用户名兜底判断，避免误判
        $oldName = trim((string)($oldOwner['user_name'] ?? ''));
        $newName = trim((string)($newOwner['user_name'] ?? ''));

        return $oldName !== '' && $oldName === $newName;
    }

    /**
     * 负责人转移：校验当前历史阶段 -> 关闭旧阶段 -> 新增新阶段
     *
     * 重要约束：
     * - 本方法不判断“转给自己是否跳过”，该判断由调用方在修改 crm_leads 之前完成
     * - 本方法不调用 Db::startTrans()/commit()/rollback()，事务由调用方统一控制
     * - 任何数据不一致场景均以抛出 \RuntimeException 的方式交由调用方回滚
     *
     * @param int $leadsId 客户ID（crm_leads.id）
     * @param array $oldOwner 旧负责人快照 ['user_id' => int, 'user_name' => string]，必须是 crm_leads 更新前读取的值
     * @param array $newOwner 新负责人 ['user_id' => int, 'user_name' => string]
     * @param string $sourceType 来源类型，如 manual_transfer / baseline_init
     * @param array $operatorInfo 操作人 ['admin_id' => int, 'username' => string]
     * @param int $sourceLogId crm_operation_log 的日志ID
     * @param string $changeTime 统一变更时间（Y-m-d H:i:s），旧阶段 end_time 与新阶段 start_time 均使用该值
     * @param string $remark 备注
     * @return bool
     * @throws \RuntimeException 历史未初始化 / 历史多条当前阶段 / 历史与crm_leads不一致 / 写入失败
     */
    public function changeOwner(
        $leadsId,
        array $oldOwner,
        array $newOwner,
        $sourceType,
        array $operatorInfo,
        $sourceLogId,
        $changeTime,
        $remark = ''
    ) {
        $leadsId = (int)$leadsId;
        if ($leadsId <= 0) {
            throw new \RuntimeException('客户ID无效，无法记录负责人历史');
        }

        $changeTime = trim((string)$changeTime);
        if ($changeTime === '') {
            throw new \RuntimeException('负责人变更时间无效，无法记录负责人历史');
        }

        $currentStage = $this->getCurrentStage($leadsId);
        $this->assertStageMatchesOldOwner($leadsId, $currentStage, $oldOwner, $newOwner, $operatorInfo);
        $this->closeCurrentStage($leadsId, $currentStage, $changeTime);
        $this->createNewStage($leadsId, $newOwner, (string)$sourceType, $operatorInfo, (int)$sourceLogId, $changeTime, (string)$remark);

        return true;
    }

    /**
     * 新客户创建首条负责人任职阶段（第0阶段）
     *
     * 与 changeOwner() 的区别：
     * - changeOwner() 针对“已存在旧阶段”的负责人转移场景：关闭旧阶段 -> 新增新阶段
     * - 本方法针对“全新客户、尚无任何历史记录”的场景，只新增，不关闭任何阶段
     *
     * 重要约束：
     * - 本方法不调用 Db::startTrans()/commit()/rollback()，事务由调用方（Client::add()）统一控制
     * - 调用前必须保证 leads_id 在 crm_client_owner_history 中不存在任何记录，否则视为异常并阻断
     * - owner/operator 必须由调用方基于 crm_leads 落库的最终数据快照传入，本方法不做任何猜测或二次查询
     *
     * @param int $leadsId 客户ID（crm_leads.id）
     * @param array $owner 负责人快照 ['user_id' => int, 'user_name' => string]，须与 crm_leads.pr_user_id / pr_user 一致
     * @param array $operatorInfo 操作人 ['admin_id' => int, 'username' => string]
     * @param int $sourceLogId crm_operation_log 的日志ID（本次新增客户操作日志）
     * @param string $startTime 起始时间（Y-m-d H:i:s），须与 crm_leads.at_time 一致
     * @param string $sourceType 来源类型，新增客户固定为 create
     * @param string $remark 备注
     * @return int 新阶段ID
     * @throws \RuntimeException 参数无效 / 历史已存在 / 写入失败
     */
    public function createInitialOwnerStage(
        $leadsId,
        array $owner,
        array $operatorInfo,
        $sourceLogId,
        $startTime,
        $sourceType = 'create',
        $remark = ''
    ) {
        $leadsId = (int)$leadsId;
        if ($leadsId <= 0) {
            throw new \RuntimeException('客户ID无效，无法创建负责人初始阶段');
        }

        $ownerUserId = (int)($owner['user_id'] ?? 0);
        if ($ownerUserId <= 0) {
            throw new \RuntimeException('负责人ID无效，无法创建负责人初始阶段');
        }

        $ownerUserName = trim((string)($owner['user_name'] ?? ''));
        if ($ownerUserName === '') {
            throw new \RuntimeException('负责人姓名为空，无法创建负责人初始阶段');
        }

        $startTime = trim((string)$startTime);
        if ($startTime === '') {
            throw new \RuntimeException('起始时间无效，无法创建负责人初始阶段');
        }

        $sourceType = trim((string)$sourceType);
        if ($sourceType === '') {
            throw new \RuntimeException('来源类型无效，无法创建负责人初始阶段');
        }

        $this->assertNoExistingHistory($leadsId);

        return $this->createNewStage(
            $leadsId,
            ['user_id' => $ownerUserId, 'user_name' => $ownerUserName],
            $sourceType,
            $operatorInfo,
            (int)$sourceLogId,
            $startTime,
            (string)$remark
        );
    }

    /**
     * 校验该客户当前在 crm_client_owner_history 中不存在任何记录（含已结束阶段）
     *
     * 新客户必须是0条历史，一旦存在任意记录（无论是否已结束），说明重复初始化或数据异常，
     * 禁止静默跳过、禁止再插一条、禁止覆盖或删除旧记录。
     *
     * @param int $leadsId
     * @return void
     * @throws \RuntimeException
     */
    private function assertNoExistingHistory($leadsId)
    {
        $existing = Db::table(self::TABLE)
            ->where('leads_id', $leadsId)
            ->lock(true)
            ->find();

        if ($existing) {
            Log::error('[ClientOwnerHistoryService] 新客户负责人历史已存在，禁止重复初始化', [
                'leads_id' => $leadsId,
                'existing_stage_id' => (int)($existing['id'] ?? 0),
            ]);
            throw new \RuntimeException('新客户负责人历史已存在，禁止重复初始化');
        }
    }

    /**
     * 读取当前负责人任职阶段（end_time IS NULL），并加行锁
     *
     * 必须严格等于1条：
     * - 0条：baseline初始化未完成或历史数据异常，禁止自动补历史
     * - >1条：脏数据，禁止自动修复
     *
     * @param int $leadsId
     * @return array 当前阶段记录
     * @throws \RuntimeException
     */
    private function getCurrentStage($leadsId)
    {
        $stages = Db::table(self::TABLE)
            ->where('leads_id', $leadsId)
            ->whereNull('end_time')
            ->lock(true)
            ->select();

        if (is_object($stages) && method_exists($stages, 'toArray')) {
            $stages = $stages->toArray();
        } elseif (!is_array($stages)) {
            $stages = [];
        }

        $count = count($stages);

        if ($count === 0) {
            throw new \RuntimeException('客户负责人历史未初始化，请先完成baseline初始化');
        }

        if ($count > 1) {
            Log::error('[ClientOwnerHistoryService] 负责人历史异常：存在多个未结束负责人阶段', [
                'leads_id' => $leadsId,
                'stage_ids' => array_column($stages, 'id'),
            ]);
            throw new \RuntimeException('客户负责人历史异常，存在多个未结束负责人阶段');
        }

        return $stages[0];
    }

    /**
     * 校验历史当前阶段负责人与 crm_leads 旧负责人（调用方传入的快照）是否一致
     *
     * @param int $leadsId
     * @param array $stage 当前历史阶段记录
     * @param array $oldOwner
     * @param array $newOwner 仅用于日志记录，不参与判断
     * @param array $operatorInfo 仅用于日志记录
     * @return void
     * @throws \RuntimeException
     */
    private function assertStageMatchesOldOwner($leadsId, array $stage, array $oldOwner, array $newOwner, array $operatorInfo)
    {
        $stageOwnerId = (int)($stage['owner_user_id'] ?? 0);
        $oldOwnerId = (int)($oldOwner['user_id'] ?? 0);

        if ($stageOwnerId > 0 && $oldOwnerId > 0) {
            $matched = $stageOwnerId === $oldOwnerId;
        } else {
            // ID缺失的历史兼容场景下，谨慎地用用户名辅助判断
            $stageOwnerName = trim((string)($stage['owner_user_name'] ?? ''));
            $oldOwnerName = trim((string)($oldOwner['user_name'] ?? ''));
            $matched = $stageOwnerName !== '' && $stageOwnerName === $oldOwnerName;
        }

        if ($matched) {
            return;
        }

        Log::error('[ClientOwnerHistoryService] 负责人历史与crm_leads当前负责人不一致', [
            'leads_id' => $leadsId,
            'crm_leads_old_owner' => [
                'user_id' => $oldOwnerId,
                'user_name' => (string)($oldOwner['user_name'] ?? ''),
            ],
            'owner_history_current_stage' => [
                'stage_id' => (int)($stage['id'] ?? 0),
                'owner_user_id' => $stageOwnerId,
                'owner_user_name' => (string)($stage['owner_user_name'] ?? ''),
            ],
            'new_owner' => [
                'user_id' => (int)($newOwner['user_id'] ?? 0),
                'user_name' => (string)($newOwner['user_name'] ?? ''),
            ],
            'operator' => [
                'admin_id' => (int)($operatorInfo['admin_id'] ?? 0),
                'username' => (string)($operatorInfo['username'] ?? ''),
            ],
        ]);
        throw new \RuntimeException('负责人历史与crm_leads当前负责人不一致');
    }

    /**
     * 关闭当前负责人任职阶段（end_time = $changeTime）
     *
     * 更新条件必须同时包含 id + leads_id + end_time IS NULL，避免误关闭历史记录
     *
     * @param int $leadsId
     * @param array $stage 当前历史阶段记录
     * @param string $changeTime
     * @return void
     * @throws \RuntimeException
     */
    private function closeCurrentStage($leadsId, array $stage, $changeTime)
    {
        $stageId = (int)($stage['id'] ?? 0);
        if ($stageId <= 0) {
            throw new \RuntimeException('负责人历史阶段数据异常，无法关闭当前阶段');
        }

        $affected = Db::table(self::TABLE)
            ->where('id', $stageId)
            ->where('leads_id', $leadsId)
            ->whereNull('end_time')
            ->update(['end_time' => $changeTime]);

        if ($affected !== 1) {
            Log::error('[ClientOwnerHistoryService] 关闭负责人历史阶段失败', [
                'leads_id' => $leadsId,
                'stage_id' => $stageId,
                'affected' => $affected,
            ]);
            throw new \RuntimeException('负责人历史阶段关闭失败');
        }
    }

    /**
     * 新增新负责人任职阶段（start_time = $changeTime，end_time = NULL）
     *
     * @param int $leadsId
     * @param array $newOwner ['user_id' => int, 'user_name' => string]
     * @param string $sourceType
     * @param array $operatorInfo ['admin_id' => int, 'username' => string]
     * @param int $sourceLogId
     * @param string $changeTime
     * @param string $remark
     * @return int 新阶段ID
     * @throws \RuntimeException
     */
    private function createNewStage($leadsId, array $newOwner, $sourceType, array $operatorInfo, $sourceLogId, $changeTime, $remark)
    {
        $insertData = [
            'leads_id' => (int)$leadsId,
            'owner_user_id' => (int)($newOwner['user_id'] ?? 0),
            'owner_user_name' => (string)($newOwner['user_name'] ?? ''),
            'start_time' => $changeTime,
            'end_time' => null,
            'source_type' => (string)$sourceType,
            'operator_id' => (int)($operatorInfo['admin_id'] ?? 0),
            'operator_name' => (string)($operatorInfo['username'] ?? ''),
            'source_log_id' => (int)$sourceLogId,
            'remark' => (string)$remark,
            'created_at' => $changeTime,
        ];

        $newId = Db::table(self::TABLE)->insertGetId($insertData);
        if (!$newId) {
            Log::error('[ClientOwnerHistoryService] 新增负责人历史阶段失败', [
                'leads_id' => $leadsId,
                'insert_data' => $insertData,
            ]);
            throw new \RuntimeException('负责人历史阶段新增失败');
        }

        return (int)$newId;
    }
}
