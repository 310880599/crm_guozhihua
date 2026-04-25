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
     * @return array{
     *   admin_id:int,
     *   username:string,
     *   group_id:int,
     *   team_name:string,
     *   position:int,
     *   is_super:bool
     * }
     */
    private function getCurrentAdminIdentity()
    {
        $current = Admin::getMyInfo();
        $adminId = (int)($current['admin_id'] ?? 0);
        $username = trim((string)($current['username'] ?? ''));
        $groupId = (int)($current['group_id'] ?? 0);
        $teamName = trim((string)($current['team_name'] ?? ''));
        $position = (int)($current['position'] ?? 0);

        return [
            'admin_id' => $adminId,
            'username' => $username,
            'group_id' => $groupId,
            'team_name' => $teamName,
            'position' => $position,
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
        if (!empty($identity['is_super'])) {
            return $this->buildScopeResult(true, [], $identity['admin_id'], $identity['username']);
        }

        $usernames = [];
        if ($identity['username'] !== '') {
            $usernames[] = $identity['username'];
        }

        return $this->buildScopeResult(false, $usernames, $identity['admin_id'], $identity['username']);
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
        if (!empty($identity['is_super'])) {
            return $this->buildScopeResult(true, [], $identity['admin_id'], $identity['username']);
        }

        $adminId = (int)$identity['admin_id'];
        $username = (string)$identity['username'];
        $teamName = (string)$identity['team_name'];
        $position = (int)$identity['position'];

        $usernames = [];
        if ($position === 2) {
            if ($teamName !== '') {
                $usernames = Db::name('admin')
                    ->where('team_name', $teamName)
                    ->where('username', '<>', '')
                    ->column('username');
            } elseif ($adminId > 0) {
                $usernames = Db::name('admin')
                    ->where(function ($q) use ($adminId) {
                        $q->where('admin_id', $adminId)
                            ->whereOr('parent_id', $adminId);
                    })
                    ->where('username', '<>', '')
                    ->column('username');
            }
        } elseif ($username !== '') {
            $usernames = [$username];
        }

        return $this->buildScopeResult(false, (array)$usernames, $adminId, $username);
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
}
