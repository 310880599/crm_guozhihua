<?php

namespace app\admin\service;

use app\admin\controller\Client as ClientController;
use app\admin\model\Admin;
use think\Db;

/**
 * 待办跟进列表的数据可见范围（与「客户列表」buildClientSearchAllBaseQuery 口径对齐，并补充协同人）。
 *
 * - 超管（group_id=1）：无 org 限制时等价于可看全部线索；有 org 时仅看该 org 下业务员名下 + 协同
 * - 有 team_name：同团队业务员名下 + 协同
 * - 其他：本人名下 + 协同
 *
 * 团队主管看全团队：若账号已配置 team_name，与「客户列表」一致，本服务已覆盖。
 * 若后续需与 getCheckClientAllowedUsernames 等特殊名单合并，可在此类集中扩展。
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
        $org = (string)($current['org'] ?? '');

        $aWhere = [];
        if (strpos($org, 'admin') === false) {
            $aWhere = [(new ClientController())->getOrgWhere($org)];
        }

        $usernames = $username !== '' ? [$username] : [];

        if ($groupId === 1) {
            $usernames = [];
            if (!empty($aWhere)) {
                $usernames = Db::name('admin')->where($aWhere)->column('username');
            }
        } elseif ($teamName !== '') {
            $usernames = Db::name('admin')->where('team_name', $teamName)->where($aWhere)->column('username');
        }

        $usernames = array_values(array_unique(array_filter(array_map('trim', (array)$usernames))));

        $unrestricted = ($groupId === 1 && empty($aWhere));

        return [
            'unrestricted' => $unrestricted,
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

        $usernames = $scope['pr_usernames'];
        $adminId = (int)$scope['admin_id'];

        $query->where(function ($q2) use ($usernames, $adminId, $alias) {
            if (empty($usernames)) {
                $q2->where($alias . '.id', '=', 0);
                return;
            }
            $q2->where(function ($q3) use ($usernames, $adminId, $alias) {
                $q3->whereIn($alias . '.pr_user', $usernames);
                if ($adminId > 0) {
                    $q3->whereOr(function ($q4) use ($adminId, $alias) {
                        $this->whereJointPersonMatchesAdmin($q4, $alias, $adminId);
                    });
                }
            });
        });
    }

    /**
     * 协同人匹配：与 ClientFollowService::isJointPerson 存储格式兼容（JSON 数组或逗号分隔 ID）。
     *
     * @param \think\db\Query $q
     */
    private function whereJointPersonMatchesAdmin($q, $alias, $adminId)
    {
        $id = (int)$adminId;
        if ($id <= 0) {
            return;
        }
        $s = (string)$id;
        $field = $alias . '.joint_person';
        $q->where(function ($sub) use ($field, $s) {
            $sub->where($field, 'like', '%"' . $s . '"%')
                ->whereOr($field, 'like', '%,' . $s . ',%')
                ->whereOr($field, 'like', $s . ',%')
                ->whereOr($field, 'like', '%,' . $s)
                ->whereOr($field, '=', $s)
                ->whereOrRaw('FIND_IN_SET(?, ' . $field . ')', [$s]);
        });
    }
}
