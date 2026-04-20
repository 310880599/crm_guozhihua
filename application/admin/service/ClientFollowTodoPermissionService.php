<?php

namespace app\admin\service;

use app\admin\model\Admin;
use think\Db;

/**
 * 待办跟进列表的数据可见范围（负责人 + 协同人）。
 *
 * 规则：
 * - 超级管理员（group_id=1）：可看全部
 * - 主管（position=2）：本团队负责人 + 自己作为协同人的客户
 * - 普通组员（其他角色）：本人负责人 + 自己作为协同人的客户
 */
class ClientFollowTodoPermissionService
{
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
        $current = Admin::getMyInfo();
        if (empty($current)) {
            return [
                'unrestricted' => false,
                'pr_usernames' => [],
                'admin_id' => 0,
                'username' => '',
            ];
        }

        $adminId = (int)($current['admin_id'] ?? 0);
        $username = trim((string)($current['username'] ?? ''));
        $groupId = (int)($current['group_id'] ?? 0);
        $teamName = trim((string)($current['team_name'] ?? ''));
        $position = (int)($current['position'] ?? 0);

        $usernames = $username !== '' ? [$username] : [];

        // 超级管理员直接放行
        if ($groupId === 1 || $adminId === 1) {
            return [
                'unrestricted' => true,
                'pr_usernames' => [],
                'admin_id' => $adminId,
                'username' => $username,
            ];
        }

        // 主管：优先按 team_name 看团队；team_name 为空时回退 parent_id 关系
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
            // 普通成员：只能看自己
            $usernames = [$username];
        }

        $usernames = array_values(array_unique(array_filter(array_map('trim', (array)$usernames))));

        if (empty($usernames) && $username !== '') {
            $usernames = [$username];
        }

        return [
            'unrestricted' => false,
            'pr_usernames' => $usernames,
            'admin_id' => $adminId,
            'username' => $username,
        ];
    }

    /**
     * 在已 alias 的 crm_leads 查询上追加可见范围（负责人池 OR 协同人）。
     *
     * @param \think\db\Query $query
     * @param string $alias 主表别名，如 l
     */
    public function applyLeadsVisibilityScope($query, $alias = 'l')
    {
        $scope = $this->getScopeForCurrentAdmin();

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
