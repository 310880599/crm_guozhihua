<?php

namespace app\admin\service;

use app\admin\model\Admin;
use think\Db;

/**
 * 待办跟进列表的数据可见范围（负责人 + 协同人）。
 *
 * 支持两个权限场景：
 * - todo_manage（客户跟进管理）：超管可看全部，其他仅本人负责人（+ 协同人）
 * - team_todo（客户跟进团队）：超管可看全部，主管看本团队，员工仅本人（+ 协同人）
 */
class ClientFollowTodoPermissionService
{
    const SCENE_TODO_MANAGE = 'todo_manage';
    const SCENE_TEAM_TODO = 'team_todo';

    /**
     * 获取当前登录管理员基础身份信息。
     *
     * @return array
     */
    private function getCurrentAdminIdentity()
    {
        $current = (array)Admin::getMyInfo();
        $adminId = (int)($current['admin_id'] ?? 0);
        $username = trim((string)($current['username'] ?? ''));
        $groupId = (int)($current['group_id'] ?? 0);
        $teamName = trim((string)($current['team_name'] ?? ''));
        $position = (int)($current['position'] ?? 0);
        $parentId = (int)($current['parent_id'] ?? 0);
        $roleId = (int)($current['role_id'] ?? 0);

        return [
            'admin_id' => $adminId,
            'username' => $username,
            'group_id' => $groupId,
            'team_name' => $teamName,
            'position' => $position,
            'parent_id' => $parentId,
            'role_id' => $roleId,
            'is_super' => ($adminId === 1 || $groupId === 1),
        ];
    }

    /**
     * 组装标准 scope 返回结构。
     *
     * @param bool $unrestricted
     * @param array $usernames
     * @param int $adminId
     * @param string $username
     * @return array{unrestricted:bool,pr_usernames:string[],admin_id:int,username:string}
     */
    private function buildScopeResult($unrestricted, array $usernames, $adminId, $username)
    {
        $cleanUsernames = array_values(array_unique(array_filter(array_map('trim', (array)$usernames))));
        if (empty($cleanUsernames) && trim((string)$username) !== '') {
            $cleanUsernames = [trim((string)$username)];
        }

        return [
            'unrestricted' => (bool)$unrestricted,
            'pr_usernames' => $cleanUsernames,
            'admin_id' => (int)$adminId,
            'username' => trim((string)$username),
        ];
    }

    /**
     * 客户跟进管理页面权限范围：
     * - 超管：全部
     * - 主管/员工：仅自己负责人（协同人可见逻辑在 apply 中统一保留）
     *
     * @return array{unrestricted:bool,pr_usernames:string[],admin_id:int,username:string}
     */
    public function getScopeForTodoManage()
    {
        $identity = $this->getCurrentAdminIdentity();
        $isTeamLeader = $this->isTeamLeader($identity);

        if (!empty($identity['is_super'])) {
            $scope = $this->buildScopeResult(true, [], $identity['admin_id'], $identity['username']);
            $this->debugScope(self::SCENE_TODO_MANAGE, $identity, $scope, $isTeamLeader);
            return $scope;
        }

        $usernames = [];
        if ($identity['username'] !== '') {
            $usernames[] = $identity['username'];
        }

        $scope = $this->buildScopeResult(false, $usernames, $identity['admin_id'], $identity['username']);
        $this->debugScope(self::SCENE_TODO_MANAGE, $identity, $scope, $isTeamLeader);
        return $scope;
    }

    /**
     * 客户跟进团队页面权限范围：
     * - 超管：全部
     * - 主管：优先 team_name 同团队；team_name 为空回退 parent_id（自己+下属）
     * - 员工：仅自己负责人（协同人可见逻辑在 apply 中统一保留）
     *
     * @return array{unrestricted:bool,pr_usernames:string[],admin_id:int,username:string}
     */
    public function getScopeForTeamTodo()
    {
        $identity = $this->getCurrentAdminIdentity();
        $isTeamLeader = $this->isTeamLeader($identity);

        if (!empty($identity['is_super'])) {
            $scope = $this->buildScopeResult(true, [], $identity['admin_id'], $identity['username']);
            $this->debugScope(self::SCENE_TEAM_TODO, $identity, $scope, $isTeamLeader);
            return $scope;
        }

        $usernames = [];
        if ($isTeamLeader) {
            $usernames = $this->getTeamUsernames($identity);
        } elseif ((string)$identity['username'] !== '') {
            $usernames = [(string)$identity['username']];
        }

        $scope = $this->buildScopeResult(
            false,
            (array)$usernames,
            (int)$identity['admin_id'],
            (string)$identity['username']
        );
        $this->debugScope(self::SCENE_TEAM_TODO, $identity, $scope, $isTeamLeader);
        return $scope;
    }

    /**
     * @return array{
     *   unrestricted: bool,
     *   pr_usernames: string[],
     *   admin_id: int,
     *   username: string
     * }
     */
    public function getScopeForCurrentAdmin()
    {
        // 保留兼容：历史默认口径仍按“团队待办”处理。
        return $this->getScopeForTeamTodo();
    }

    /**
     * 在已 alias 的 crm_leads 查询上追加可见范围（负责人池 OR 协同人）。
     *
     * @param \think\db\Query $query
     * @param string $alias 主表别名，如 l
     * @param string $scene todo_manage|team_todo
     */
    public function applyLeadsVisibilityScope($query, $alias = 'l', $scene = self::SCENE_TODO_MANAGE)
    {
        $scope = ($scene === self::SCENE_TEAM_TODO)
            ? $this->getScopeForTeamTodo()
            : $this->getScopeForTodoManage();

        if (!empty($scope['unrestricted'])) {
            return;
        }

        $usernames = (array)$scope['pr_usernames'];
        $adminId = (int)$scope['admin_id'];

        $query->where(function ($q) use ($usernames, $adminId, $alias) {
            $hasOwnerScope = false;
            if (!empty($usernames)) {
                $q->whereIn($alias . '.pr_user', $usernames);
                $hasOwnerScope = true;
            }

            if ($adminId > 0) {
                if ($hasOwnerScope) {
                    $q->whereOr(function ($sub) use ($alias, $adminId) {
                        $this->whereJointPersonMatchesAdmin($sub, $alias, $adminId);
                    });
                } else {
                    $this->whereJointPersonMatchesAdmin($q, $alias, $adminId);
                }
                return;
            }

            if (!$hasOwnerScope) {
                // 安全兜底：未知身份不返回任何数据。
                $q->where($alias . '.id', '=', 0);
            }
        });
    }

    /**
     * 是否具备团队主管视角（用于 team_todo 场景）。
     *
     * 判定规则：
     * 1) 超管不走“主管”逻辑（超管直接 unrestricted）
     * 2) 同 team_name 存在其他员工 -> 具备团队范围
     * 3) 存在 parent_id = 当前 admin_id 的员工 -> 具备团队范围
     * 4) position=2 作为兼容补充（非唯一依据）
     *
     * @param array $identity
     * @return bool
     */
    private function isTeamLeader(array $identity)
    {
        if (!empty($identity['is_super'])) {
            return false;
        }

        $adminId = (int)($identity['admin_id'] ?? 0);
        $teamName = trim((string)($identity['team_name'] ?? ''));
        $position = (int)($identity['position'] ?? 0);

        $hasTeamPeers = false;
        if ($teamName !== '') {
            $peerQuery = Db::name('admin')
                ->where('team_name', $teamName)
                ->where('username', '<>', '');
            if ($adminId > 0) {
                $peerQuery->where('admin_id', '<>', $adminId);
            }
            $hasTeamPeers = ((int)$peerQuery->count('admin_id') > 0);
        }

        $hasChildren = false;
        if ($adminId > 0) {
            $hasChildren = ((int)Db::name('admin')
                    ->where('parent_id', $adminId)
                    ->where('username', '<>', '')
                    ->count('admin_id') > 0);
        }

        // 兼容历史结构：position=2 常表示主管，但不是唯一判定依据。
        $matchesLegacyPosition = ($position === 2);

        return $hasTeamPeers || $hasChildren || $matchesLegacyPosition;
    }

    /**
     * 获取团队可见负责人用户名（自己 + team_name + parent_id 下属）。
     *
     * @param array $identity
     * @return string[]
     */
    private function getTeamUsernames(array $identity)
    {
        $adminId = (int)($identity['admin_id'] ?? 0);
        $username = trim((string)($identity['username'] ?? ''));
        $teamName = trim((string)($identity['team_name'] ?? ''));

        $usernames = [];
        if ($username !== '') {
            $usernames[] = $username;
        }

        if ($teamName !== '') {
            $teamUsers = Db::name('admin')
                ->where('team_name', $teamName)
                ->where('username', '<>', '')
                ->column('username');
            $usernames = array_merge($usernames, (array)$teamUsers);
        }

        if ($adminId > 0) {
            $childUsers = Db::name('admin')
                ->where('parent_id', $adminId)
                ->where('username', '<>', '')
                ->column('username');
            $usernames = array_merge($usernames, (array)$childUsers);
        }

        $usernames = array_values(array_unique(array_filter(array_map('trim', $usernames))));
        if (empty($usernames) && $username !== '') {
            $usernames = [$username];
        }

        return $usernames;
    }

    /**
     * 协同人匹配：兼容 JSON 数组与逗号分隔两种存储格式。
     *
     * @param \think\db\Query $q
     * @param string $alias
     * @param int $adminId
     */
    private function whereJointPersonMatchesAdmin($q, $alias, $adminId)
    {
        $id = (int)$adminId;
        if ($id <= 0) {
            return;
        }

        $sid = (string)$id;
        $field = $alias . '.joint_person';
        $q->where(function ($sub) use ($field, $sid) {
            // JSON 字符串数组：["12","13"]
            $sub->where($field, 'like', '%"' . $sid . '"%')
                // JSON 数字数组：[12,13] / [12]
                ->whereOr($field, 'like', '%[' . $sid . ',%')
                ->whereOr($field, 'like', '%,' . $sid . ',%')
                ->whereOr($field, 'like', '%,' . $sid . ']%')
                ->whereOr($field, 'like', '%[' . $sid . ']%')
                // 逗号分隔：12,13 / 12 / 13,12
                ->whereOr($field, 'like', $sid . ',%')
                ->whereOr($field, 'like', '%,' . $sid)
                ->whereOr($field, '=', $sid);
        });
    }

    /**
     * 调试输出：记录当前 scene 的权限计算结果。
     *
     * @param string $scene
     * @param array $identity
     * @param array $scope
     * @param bool $isTeamLeader
     * @return void
     */
    private function debugScope($scene, array $identity, array $scope, $isTeamLeader)
    {
        if (!$this->isDebugLogEnabled()) {
            return;
        }

        $payload = [
            'scene' => (string)$scene,
            'admin_id' => (int)($identity['admin_id'] ?? 0),
            'username' => (string)($identity['username'] ?? ''),
            'group_id' => (int)($identity['group_id'] ?? 0),
            'team_name' => (string)($identity['team_name'] ?? ''),
            'position' => (int)($identity['position'] ?? 0),
            'role_id' => (int)($identity['role_id'] ?? 0),
            'parent_id' => (int)($identity['parent_id'] ?? 0),
            'is_super' => !empty($identity['is_super']),
            'is_team_leader' => (bool)$isTeamLeader,
            'unrestricted' => !empty($scope['unrestricted']),
            'pr_usernames' => array_values((array)($scope['pr_usernames'] ?? [])),
        ];

        $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            $json = '{}';
        }

        $this->writeDebugLog('todo permission scene=' . (string)$scene . ' scope=' . $json);
    }

    /**
     * 调试日志开关：
     * - 优先读取 config('todo_permission_debug')
     * - 未配置时回退 config('app_debug')
     *
     * @return bool
     */
    private function isDebugLogEnabled()
    {
        $switch = config('todo_permission_debug');
        if ($switch === null) {
            $switch = config('app_debug');
        }

        if ($switch === true || $switch === 1 || $switch === '1') {
            return true;
        }
        if (is_string($switch)) {
            $s = strtolower(trim($switch));
            return in_array($s, ['true', 'yes', 'on'], true);
        }

        return false;
    }

    /**
     * 兼容 TP5.1 的日志写法（facade / think\Log）。
     *
     * @param string $message
     * @return void
     */
    private function writeDebugLog($message)
    {
        try {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write((string)$message, 'debug');
                return;
            }
        } catch (\Throwable $e) {
        }

        try {
            if (class_exists('\\think\\Log')) {
                \think\Log::record((string)$message, 'debug');
            }
        } catch (\Throwable $e) {
        }
    }
}
