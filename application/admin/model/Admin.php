<?php

namespace app\admin\model;

use think\Model;
use think\Db;

class Admin extends Model
{
    protected $pk = 'admin_id';

    public function login($data)
    {
        $user = Db::name('admin')->where('username', $data['username'])->find();
        if ($user) {
            if ($user['is_open'] == 1 && $user['pwd'] == md5($data['password'])) {
                session('username', $user['username']);
                session('aid', $user['admin_id']);
                session('gid', $user['group_id']);
                $avatar = $user['avatar'] == '' ? '/static/admin/images/0.jpg' : $user['avatar'];
                session('avatar', $avatar);
                //记录团队名称
                session('team_name', $user['team_name']);
                session('user_info', $user);
                return ['code' => 1, 'msg' => '登录成功!']; //信息正确
            } else {
                return ['code' => 0, 'msg' => '用户名或者密码错误，重新输入!']; //密码错误
            }
        } else {
            return ['code' => 0, 'msg' => '用户名或者密码错误，重新输入!']; //用户不存在
        }
    }

    public function getInfo($userId)
    {
        $info = Db::name('admin')->where('admin_id', $userId)->find();
        return $info;
    }

    public function getMyInfo($refresh = false)
    {
        if (!$refresh) {
            $info = session('user_info');
            if ($info) {
                return $info;
            }
        }
        $admin_id = session('aid');
        $info = Db::name('admin')->where('admin_id', $admin_id)->find();
        if ($info) {
            session('user_info', $info);
        }
        return $info;
    }

    /**
     * 检查订单下拉：按用户名白名单取 admin_id、username
     *
     * @param string[] $usernames
     * @return array
     */
    public static function listByUsernamesForSelect(array $usernames): array
    {
        if (empty($usernames)) {
            return [];
        }
        return Db::name('admin')
            ->field('admin_id,username')
            ->where('username', 'in', $usernames)
            ->order('admin_id', 'asc')
            ->select();
    }

    /**
     * 在可见用户名池内按团队、组织条件筛出仍可见的用户名（检查订单权限收窄）
     *
     * @param string[] $visibleUsernames
     * @param string|null $teamName
     * @param array<int, \Closure> $orgWhereClosures getOrgWhere 等闭包条件
     * @return string[]
     */
    public static function columnUsernamesAfterTeamOrgFilter(array $visibleUsernames, ?string $teamName, array $orgWhereClosures): array
    {
        if (empty($visibleUsernames)) {
            return [];
        }
        $q = Db::name('admin')->where('username', 'in', $visibleUsernames);
        if ($teamName !== null && $teamName !== '') {
            $q->where('team_name', $teamName);
        }
        if (!empty($orgWhereClosures)) {
            $q->where($orgWhereClosures);
        }
        return $q->column('username');
    }

    // 验证码检查方法已移除
}
