<?php

namespace app\admin\service;

use think\Db;
use think\facade\Log;

/**
 * 客户负责人（可转移/可分配）统一资格规则
 *
 * 单向读取 ClientConfigService（禁止反向调用，避免循环依赖）。
 * - 无正式配置：fallback 旧岗位白名单 + is_open=1
 * - 有正式配置：仅允许 allowed_owner_user_ids，并按 exclude_disabled_users 处理停用账号
 * - 硬规则：group_id=1（超管）、group_id=12（运营）永远禁止
 */
class ClientOwnerCandidateService
{
    /**
     * 允许拥有客户的销售相关岗位（legacy fallback，与历史协同人筛选一致）
     *
     * 10 普通员工（业务员）
     * 11 主管
     * 14 产品经理（业务上可作负责人/协同人）
     * 17 普工加检委
     * 18 产经加检委
     * 19 主管加产经
     * 21 副主管
     * 22 副主管加产经
     *
     * @return int[]
     */
    public function getAllowedOwnerGroupIds()
    {
        return [10, 11, 14, 17, 18, 19, 21, 22];
    }

    /**
     * 当前生效的负责人资格策略
     *
     * @return array{
     *   mode:string,
     *   allowed_group_ids?:int[],
     *   allowed_owner_user_ids?:int[],
     *   exclude_disabled_users?:int
     * }
     */
    public function getEffectiveOwnerPolicy()
    {
        try {
            $config = (new ClientConfigService())->getLatestConfig();
        } catch (\Throwable $e) {
            Log::error('[ClientOwnerCandidateService] read crm_client_config failed, fallback legacy', [
                'error' => $e->getMessage(),
            ]);
            return $this->buildLegacyPolicy();
        }

        if ($config === null || !is_array($config)) {
            return $this->buildLegacyPolicy();
        }

        $allowedIds = (new ClientConfigService())->parseAllowedOwnerUserIds(
            $config['allowed_owner_user_ids'] ?? ''
        );

        // 非 0/1 按安全值 1 处理
        $excludeRaw = $config['exclude_disabled_users'] ?? 1;
        if (is_bool($excludeRaw)) {
            $excludeDisabled = $excludeRaw ? 1 : 0;
        } else {
            $excludeStr = trim((string)$excludeRaw);
            $excludeDisabled = ($excludeStr === '0') ? 0 : 1;
        }

        if (empty($allowedIds)) {
            // 已有正式配置但白名单为空：脏配置，禁止 fallback 旧规则
            Log::warning('[ClientOwnerCandidateService] dirty config: empty allowed_owner_user_ids', [
                'config_id' => (int)($config['id'] ?? 0),
            ]);
        }

        return [
            'mode' => 'config',
            'allowed_owner_user_ids' => $allowedIds,
            'exclude_disabled_users' => $excludeDisabled,
        ];
    }

    /**
     * 候选列表与 validate 共用的资格判断
     *
     * @param array $adminRow 至少含 admin_id/username/group_id/is_open
     * @param array $policy getEffectiveOwnerPolicy() 返回值
     * @return bool
     */
    public function isEligibleOwnerRow(array $adminRow, array $policy)
    {
        $adminId = (int)($adminRow['admin_id'] ?? 0);
        $username = trim((string)($adminRow['username'] ?? ''));
        $groupId = (int)($adminRow['group_id'] ?? 0);
        $isOpen = (int)($adminRow['is_open'] ?? 0);

        if ($adminId <= 0 || $username === '') {
            return false;
        }

        // 硬规则：超管 / 运营永远禁止
        if ($groupId === 1 || $groupId === 12) {
            return false;
        }

        $mode = (string)($policy['mode'] ?? 'legacy');
        if ($mode === 'config') {
            $allowedIds = isset($policy['allowed_owner_user_ids']) && is_array($policy['allowed_owner_user_ids'])
                ? $policy['allowed_owner_user_ids']
                : [];
            if (!in_array($adminId, $allowedIds, true)) {
                return false;
            }
            $excludeDisabled = (int)($policy['exclude_disabled_users'] ?? 1) === 0 ? 0 : 1;
            if ($excludeDisabled === 1 && $isOpen !== 1) {
                return false;
            }
            return true;
        }

        // legacy：旧岗位 + 必须启用
        if ($isOpen !== 1) {
            return false;
        }
        $allowedGroups = isset($policy['allowed_group_ids']) && is_array($policy['allowed_group_ids'])
            ? $policy['allowed_group_ids']
            : $this->getAllowedOwnerGroupIds();

        return in_array($groupId, $allowedGroups, true);
    }

    /**
     * 转移/分配弹窗候选人列表（与 validate 同一套 policy）
     *
     * @return array<int,array{admin_id:int,username:string}>
     */
    public function getTransferableOwnerCandidates()
    {
        $policy = $this->getEffectiveOwnerPolicy();
        $rows = [];

        try {
            if (($policy['mode'] ?? '') === 'config') {
                $allowedIds = isset($policy['allowed_owner_user_ids']) && is_array($policy['allowed_owner_user_ids'])
                    ? $policy['allowed_owner_user_ids']
                    : [];
                if (empty($allowedIds)) {
                    return [];
                }
                $rows = Db::name('admin')
                    ->where('admin_id', 'in', $allowedIds)
                    ->where('username', '<>', '')
                    ->field('admin_id,username,group_id,is_open')
                    ->order('username', 'asc')
                    ->select();
            } else {
                $rows = Db::name('admin')
                    ->where('is_open', 1)
                    ->where('group_id', 'in', $this->getAllowedOwnerGroupIds())
                    ->where('username', '<>', '')
                    ->field('admin_id,username,group_id,is_open')
                    ->order('username', 'asc')
                    ->select();
            }
        } catch (\Throwable $e) {
            Log::error('[ClientOwnerCandidateService] query admin candidates failed', [
                'mode' => $policy['mode'] ?? '',
                'error' => $e->getMessage(),
            ]);
            return [];
        }

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        }
        if (!is_array($rows)) {
            $rows = [];
        }

        $list = [];
        $seen = [];
        foreach ($rows as $row) {
            if (!is_array($row) || !$this->isEligibleOwnerRow($row, $policy)) {
                continue;
            }
            $username = trim((string)($row['username'] ?? ''));
            if ($username === '' || isset($seen[$username])) {
                continue;
            }
            $seen[$username] = true;
            $list[] = [
                'admin_id' => (int)($row['admin_id'] ?? 0),
                'username' => $username,
            ];
        }

        return $list;
    }

    /**
     * 校验目标负责人是否允许接收客户（与候选列表同一套资格）
     *
     * @param string $username
     * @return array{ok:bool,code?:int,msg?:string,admin_id?:int,username?:string}
     */
    public function validateTransferTargetOwner($username)
    {
        $username = trim((string)$username);
        if ($username === '') {
            return [
                'ok' => false,
                'code' => 500,
                'msg' => '负责人不能为空',
            ];
        }

        $admin = Db::name('admin')
            ->where('username', $username)
            ->field('admin_id,username,group_id,is_open')
            ->find();

        if (empty($admin)) {
            return [
                'ok' => false,
                'code' => 500,
                'msg' => '未找到该负责人账号，请重新选择',
            ];
        }

        $groupId = (int)($admin['group_id'] ?? 0);
        if ($groupId === 1) {
            return [
                'ok' => false,
                'code' => 500,
                'msg' => '超级管理员不能作为客户负责人',
            ];
        }
        if ($groupId === 12) {
            return [
                'ok' => false,
                'code' => 500,
                'msg' => '运营人员不能作为客户负责人',
            ];
        }

        $policy = $this->getEffectiveOwnerPolicy();
        $mode = (string)($policy['mode'] ?? 'legacy');

        if ($mode === 'config') {
            $allowedIds = isset($policy['allowed_owner_user_ids']) && is_array($policy['allowed_owner_user_ids'])
                ? $policy['allowed_owner_user_ids']
                : [];
            $adminId = (int)($admin['admin_id'] ?? 0);
            if (!in_array($adminId, $allowedIds, true)) {
                return [
                    'ok' => false,
                    'code' => 500,
                    'msg' => '该账号不在允许接收客户的人员名单中',
                ];
            }

            $excludeDisabled = (int)($policy['exclude_disabled_users'] ?? 1) === 0 ? 0 : 1;
            if ($excludeDisabled === 1 && (int)($admin['is_open'] ?? 0) !== 1) {
                return [
                    'ok' => false,
                    'code' => 500,
                    'msg' => '该账号已停用，不能作为客户负责人',
                ];
            }
        } else {
            if ((int)($admin['is_open'] ?? 0) !== 1) {
                return [
                    'ok' => false,
                    'code' => 500,
                    'msg' => '该账号已停用，不能作为客户负责人',
                ];
            }
            if (!in_array($groupId, $this->getAllowedOwnerGroupIds(), true)) {
                return [
                    'ok' => false,
                    'code' => 500,
                    'msg' => '该账号不是有效销售负责人，不能接收客户',
                ];
            }
        }

        // 最终再走一遍共用资格判断，防止候选/校验分叉
        if (!$this->isEligibleOwnerRow($admin, $policy)) {
            return [
                'ok' => false,
                'code' => 500,
                'msg' => '该账号当前不能作为客户负责人，请重新选择',
            ];
        }

        return [
            'ok' => true,
            'admin_id' => (int)$admin['admin_id'],
            'username' => trim((string)$admin['username']),
        ];
    }

    /**
     * @return array{mode:string,allowed_group_ids:int[]}
     */
    protected function buildLegacyPolicy()
    {
        return [
            'mode' => 'legacy',
            'allowed_group_ids' => $this->getAllowedOwnerGroupIds(),
        ];
    }
}
