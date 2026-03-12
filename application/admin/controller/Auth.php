<?php

namespace app\admin\controller;

use function MongoDB\BSON\toJSON;
use think\Db;
use clt\Leftnav;
use app\admin\model\Admin;
use app\admin\model\AuthGroup;
use app\admin\model\authRule;
use think\facade\Request;
use think\Validate;

class Auth extends Common
{
    //增删改权限判断
    private  function checkAuth($rule)
    {
        $current_admin = Admin::getMyInfo();
        // 统一：超级管理员、运营主管、业务主管类账号拥有增删改管理员权限
        //超级管理员
        if ($current_admin['group_id'] == 1) {
            return true;
        }
        //运营主管
        // $where = [['group_id', $this->yygid], ['position', '<>', 0]];
        if ($current_admin['group_id'] == $this->yygid && $current_admin['position'] != 0) {
            return true;
        }

        //业务主管类（包含：11 业务主管；19 主管加产经）
        //说明：如果某个账号业务上应为“主管加产经”，数据库中应设置为 group_id = 19；
        //     若误设置为其它组（例如 18 产经加检委），则这里不会被识别为业务主管类，权限范围会异常。
        if ($this->isBusinessLeader($current_admin)) {
            return true;
        }
        return false;
    }

    /**
     * 业务主管类 group_id 列表统一维护
     *
     * 当前约定：
     *  - 11：业务主管（$this->ywzgid）
     *  - 19：主管加产经
     *
     * 注意：
     *  - 18 为“产经加检委”，不属于业务主管类；
     *    如果某个账号业务上应为“主管加产经”，应在数据库中将该账号的 group_id 调整为 19，
     *    否则其在管理员列表中的可见范围仍然只能是“普通员工/非主管”。
     */
    private function getBusinessLeaderGroupIds()
    {
        return [$this->ywzgid, 19];
    }

    /**
     * 是否为业务主管类账号（统一入口）
     *
     * 之前代码中仅判断 group_id == 11，导致“主管加产经”（19）账号如“李营”只能看到自己，
     * 这里统一改为：只要 group_id 属于 getBusinessLeaderGroupIds()，都视为业务主管类账号。
     */
    private function isBusinessLeader($admin)
    {
        $groupIds = $this->getBusinessLeaderGroupIds();
        return in_array($admin['group_id'], $groupIds);
    }


    //管理员列表
    public function adminList()
    {
        if (Request::isAjax()) {
            $val = input('val');
            $url['val'] = $val;
            $this->assign('testval', $val);
            
            // 获取当前用户信息
            $admin_id = session('aid');
            $current_admin = Admin::get($admin_id);
            if(!$current_admin){
                return $result = ['code' => 0, 'msg' => '用户不存在'];
            }
            // 构建查询条件
            $map = [];
            $where = [];
            if ($val) {
                $map['username'] = ['like', "%" . $val . "%"];
            }
            if ($current_admin['group_id'] != 1 && $current_admin['org'] != 'admin') {
                $where[]=$this->getOrgWhere($current_admin['org']);
            }
            // 权限控制逻辑（统一业务主管类判断）
            if ($this->isBusinessLeader($current_admin)) {
                // 业务主管类账号（包含：11 主管；19 主管加产经），按团队维度查看
                if (!empty($current_admin['team_name'])) {
                    $map['team_name'] = $current_admin['team_name'];
                } else {
                    // 如果team_name为空，只显示自己
                    $map['admin_id'] = $admin_id;
                }
            } else if ($current_admin['group_id'] == $this->yygid) {
                //运营主管相关逻辑（保持原有行为不变）
                if (!empty($current_admin['team_name'])) {
                    $map['team_name'] = $current_admin['team_name'];
                    if ($current_admin['position'] == 0) {
                        $map['admin_id'] = $admin_id;
                    } elseif ($current_admin['position'] == 2) {
                        $map['channel'] = $current_admin['channel'];
                    }
                } else {
                    // 如果team_name为空，只显示自己
                    $map['admin_id'] = $admin_id;
                }
            } else if ($current_admin['group_id'] != 1) {
                // 非超级管理员只能查看自己的信息
                $map['admin_id'] = $admin_id;
            }
            // 查询数据
            $list = Db::table(config('database.prefix') . 'admin')->alias('a')
                ->join(config('database.prefix') . 'auth_group ag', 'a.group_id = ag.group_id', 'left')
                ->field('a.*,ag.title')
                ->where($map)
                ->where($where)
                ->select();

            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list, 'rel' => 1];
        }
        return view();
    }

    public function adminAdd()
    {
        if (Request::isAjax()) {
            //判断是否有添加权限
            if (!$this->checkAuth('adminAdd')) {
                return $result = ['code' => 0, 'msg' => '您无此操作权限'];
            }

            $data = input('post.');
            $check_user = Admin::get(['username' => $data['username']]);
            if ($check_user) {
                return $result = ['code' => 0, 'msg' => '用户已存在，请重新输入用户名!'];
            }
            $data['pwd'] = input('post.pwd', '', 'md5');
            //默认开启
            $data['is_open'] = 1;
            $data['add_time'] = time();
            $data['ip'] = request()->ip();
            if($data['org'])$data['org'] = $this->org_fgx . implode($this->org_fgx , $data['org']) .$this->org_fgx  ;
            
            // 处理运营端口数据（现在存储的是店铺名称，不是ID）
            if (isset($data['port_id']) && !empty($data['port_id'])) {
                // 支持数组或逗号分隔字符串两种格式
                $ports = is_array($data['port_id'])
                    ? array_filter(array_map('trim', $data['port_id']))
                    : array_filter(array_map('trim', explode(',', $data['port_id'])));
                if (!empty($ports)) {
                    $inquiryId = $data['inquiry_id'] ?? 0;
                    if (!empty($inquiryId)) {
                        $used = $this->checkPortsUsed($ports, $inquiryId, 0);
                        if (!empty($used)) {
                            // 查询端口名称用于错误提示
                            $portNames = Db::name('crm_inquiry_port')->whereIn('id', $used)->column('port_name');
                            return ['code' => 0, 'msg' => '以下端口已被其他管理员使用：' . implode('、', $portNames)];
                        }
                    }
                    $data['port_id'] = implode(',', $ports);
                } else {
                    $data['port_id'] = '';
                }
            } else {
                $data['port_id'] = '';
            }
            
            // 新增: 若选择了渠道，将渠道名称赋值到旧 channel 字段（兼容旧逻辑）
            if (!empty($data['inquiry_id'])) {
                $data['channel'] = Db::name('crm_inquiry')->where('id', $data['inquiry_id'])->value('inquiry_name') ?? '';
            }

            //验证
            $msg = $this->validate($data, 'app\admin\validate\Admin');
            if ($msg != 'true') {
                return $result = ['code' => 0, 'msg' => $msg];
            }
            //单独验证密码
            $checkPwd = Validate::make([input('post.pwd') => 'require']);
            if (false === $checkPwd) {
                return $result = ['code' => 0, 'msg' => '密码不能为空！'];
            }
            //添加
            if (Admin::create($data)) {
                //判断是否是新增的团队
                if (!empty($data['team_name'])) {
                    $teamList = $this->getTeamList();
                    if (!in_array($data['team_name'], $teamList)) {
                        //缓存更新
                        $this->getTeamList(true);
                    }
                }
                return ['code' => 1, 'msg' => '管理员添加成功!', 'url' => url('adminList')];
            } else {
                return ['code' => 0, 'msg' => '管理员添加失败!'];
            }
        } else {
            // 获取当前用户信息
            $admin_id = session('aid');
            $current_admin = Admin::get($admin_id);
            if (!$current_admin) {
                return $this->error('用户不存在');
            }
            $result = [];
            // 组织列表
            $orgList = self::ORG;
            // 查询用户组
            if ($current_admin['group_id'] == 1 || strpos($current_admin['org'],'admin') !== false) {
                // 超级管理员可以显示所有用户组
                $auth_group = AuthGroup::all();
            } else {
                $orgList = $this->getOrg($current_admin['org']);
                if ($current_admin->group_id == $this->yygid) {
                    //运营只能看到运营
                    $auth_group = AuthGroup::where('group_id', '=', $this->yygid)->select();
                    $result['is_yy'] = 1;
                } else {
                    // 其他用户只能看到普通员工组
                    $auth_group = AuthGroup::where('group_id', '=', 10)->select();
                }
            }
            $result['channel'] = $current_admin['channel'] ?? "";
            $result['username'] = $current_admin['username'] ?? "";
            $result['is_admin'] = $current_admin['group_id'] == 1 ? 1 : 0;
            $result['is_position'] = $current_admin['position'] == 1 ? 1 : 0;


            // 获取主管列表（包含主管与主管加产经）
            $leaderList = $this->getLeaderAndLeaderProductManagerList($current_admin['group_id']);
            $info['org'] = $this->getOrg($current_admin['org']);
            $this->assign('team_name', $current_admin['team_name']);
            $this->assign('orgList', $orgList);
            $this->assign('leaderList', $leaderList);
            $this->assign('authGroup', $auth_group);
            $this->assign('title', lang('add') . lang('admin'));
            $this->assign('info', json_encode($info, true));
            $this->assign('selected', 'null');
            // 新增时默认无上级
            $this->assign('parent_id', 0);
            $this->assign('result', $result);
            return view('adminForm');
        }
    }

    //获取主管列表
    private function getLeaderList($group_id)
    {
        if ($group_id == $this->yygid) {
            $leaderList = \app\admin\model\Admin::where('group_id', $group_id)->where('position', '<>', 0)
                ->field('admin_id, username')->select();
        } else {
            // 业务端主管列表：使用统一的业务主管类 group_id 列表（11 业务主管；19 主管加产经）
            $leaderList = \app\admin\model\Admin::whereIn('group_id', $this->getBusinessLeaderGroupIds())
                ->field('admin_id, username')->select();
        }
        return $leaderList;
    }

    //获取主管和主管加产经列表
    private function getLeaderAndLeaderProductManagerList($group_id)  
    {
        if ($group_id == $this->yygid) {
            $leaderList = \app\admin\model\Admin::where('group_id', $group_id)->where('position', '<>', 0)
                ->field('admin_id, username')->select();
        } else {
            $leaderList = \app\admin\model\Admin::whereIn('group_id', [11, 19])
                ->field('admin_id, username')->select();
        }
        return $leaderList;
    }

    //删除管理员
    public function adminDel()
    {
        //判断是否有删除权限
        if (!$this->checkAuth('adminDel')) {
            return $result = ['code' => 0, 'msg' => '您无此操作权限'];
        }

        $admin_id = input('post.admin_id');
        if (session('aid') == 1) {
            Admin::where('admin_id', '=', $admin_id)->delete();
            return $result = ['code' => 1, 'msg' => '删除成功!'];
        } else {
            return $result = ['code' => 0, 'msg' => '您没有删除管理员的权限!'];
        }
    }
    //修改管理员状态
    public function adminState()
    {
        $id = input('post.id');
        $is_open = input('post.is_open');
        if (empty($id)) {
            $result['status'] = 0;
            $result['info'] = '用户ID不存在!';
            $result['url'] = url('adminList');
            return $result;
        }
        db('admin')->where('admin_id=' . $id)->update(['is_open' => $is_open]);
        $result['status'] = 1;
        $result['info'] = '用户状态修改成功!';
        $result['url'] = url('adminList');
        return $result;
    }
    //更新管理员信息
    public function adminEdit()
    {
        if (request()->isPost()) {
            // 判断是否有修改权限
            if (!$this->checkAuth('adminEdit')) {
                return $result = ['code' => 0, 'msg' => '您无此操作权限'];
            }

            $data = input('post.');
            $pwd = input('post.pwd');
            $map[] = ['admin_id', '<>', $data['admin_id']];
            $where['admin_id'] = $data['admin_id'];
            $info = Admin::getInfo(input('admin_id'));

            if (!$info) {
                return $result = ['code' => 0, 'msg' => '用户不存在!'];
            }

            // 保存旧的用户名
            $oldUsername = $info['username'];

            if ($data['username']) {
                $map[] = ['username', '=', $data['username']];
                $check_user = Admin::where($map)->find();
                if ($check_user) {
                    return $result = ['code' => 0, 'msg' => '用户已存在，请重新输入用户名!'];
                }
            }

            if ($pwd && $pwd != $info['pwd']) {
                $data['pwd'] = input('post.pwd', '', 'md5');
            } else {
                unset($data['pwd']);
            }

            // 处理运营端口数据（现在存储的是店铺名称，不是ID）
            if (isset($data['port_id']) && $data['port_id'] !== '') {
                $ports = is_array($data['port_id'])
                    ? array_filter(array_map('trim', $data['port_id']))
                    : array_filter(array_map('trim', explode(',', $data['port_id'])));
                if (!empty($ports)) {
                    $inquiryId = $data['inquiry_id'] ?? 0;
                    if (!empty($inquiryId)) {
                        $used = $this->checkPortsUsed($ports, $inquiryId, $data['admin_id']);
                        if (!empty($used)) {
                            $portNames = Db::name('crm_inquiry_port')->whereIn('id', $used)->column('port_name');
                            return ['code' => 0, 'msg' => '以下端口已被其他管理员使用：' . implode('、', $portNames)];
                        }
                    }
                    $data['port_id'] = implode(',', $ports);
                } else {
                    $data['port_id'] = '';
                }
            } else {
                $data['port_id'] = '';
            }

            // 新增: 若提交了渠道ID，查询渠道名称并赋值到 channel 字段
            if (!empty($data['inquiry_id'])) {
                $data['channel'] = Db::name('crm_inquiry')->where('id', $data['inquiry_id'])->value('inquiry_name') ?? '';
            }

            $msg = $this->validate($data, 'app\admin\validate\Admin');
            if ($msg != 'true') {
                return $result = ['code' => 0, 'msg' => $msg];
            }
            if($data['org'])$data['org'] = $this->org_fgx . implode($this->org_fgx , $data['org']) .$this->org_fgx  ;
            // 开启事务（可选）
            Db::startTrans();
            try {
                Admin::update($data, $where);

                // 如果 username 发生变化，同步更新 crm_leads 表
                if (isset($data['username']) && $data['username'] !== $oldUsername) {
                    Db::name('crm_leads')
                        ->where('pr_user', $oldUsername)
                        ->update(['pr_user' => $data['username']]);
                }

                Db::commit();
            } catch (\Exception $e) {
                Db::rollback();
                return ['code' => 0, 'msg' => '更新失败，请重试'];
            }

            if ($data['admin_id'] == session('aid')) {
                session('username', $data['username']);
                $avatar = $data['avatar'] == '' ? '/static/admin/images/0.jpg' : $data['avatar'];
                session('avatar', $avatar);
            }

            return $result = ['code' => 1, 'msg' => '管理员修改成功!', 'url' => url('adminList')];
        } else {
            // $auth_group = AuthGroup::all();
            // $admin = new Admin();
            // $info = $admin->getInfo(input('admin_id'));
            // $this->assign('info', json_encode($info,true));
            // $this->assign('authGroup',$auth_group);
            // $this->assign('title',lang('edit').lang('admin'));
            // return view('adminForm');

            $info = Admin::getInfo(input('admin_id'));

            //当前用户信息
            $current_admin = Admin::getInfo(session('aid'));
            // 获取主管列表供下拉选择（包含主管与主管加产经）
            $leaderList = $this->getLeaderAndLeaderProductManagerList($current_admin['group_id']);
            $this->assign('leaderList', $leaderList);
            // 当前编辑管理员的上级ID
            $this->assign('parent_id', isset($info['parent_id']) ? $info['parent_id'] : 0);
            // 组织列表
            $orgList = self::ORG;
            if ($current_admin['group_id'] == 1 || strpos($current_admin['org'],'admin') !== false) {
                // 超级管理员可以显示所有用户组
                $auth_group = AuthGroup::all();
            } else {
                $orgList = $this->getOrg($current_admin['org']);
                if ($current_admin['group_id'] == $this->yygid) {
                    //运营只能看到运营
                    $auth_group = AuthGroup::where('group_id', '=', $this->yygid)->select();
                } else {
                    $auth_group = AuthGroup::where('group_id', '<>', $this->yygid)->where('group_id', '<>', 1)->select();
                }
            }
            $result['is_yy'] = $current_admin['group_id'] == $this->yygid ? 1 : 0;
            $result['channel'] = '';  // 默认空渠道名
            if (!empty($info['inquiry_id'])) {
                // 根据 inquiry_id 获取渠道名称
                $result['channel'] = Db::name('crm_inquiry')->where('id', $info['inquiry_id'])->value('inquiry_name') ?? '';
            } elseif (!empty($info['channel'])) {
                // 退一步使用旧 channel 字段
                $result['channel'] = $info['channel'];
            }
            $result['inquiry_id'] = $info['inquiry_id'] ?? 0;
            $result['is_admin'] = $current_admin['group_id'] == 1 ? 1 : 0;
            $result['is_position'] = $current_admin['position'] == 1 ? 1 : 0;
            $info['org'] = $this->getOrg($info['org']);
            $this->assign('orgList', $orgList);
            $this->assign('info', json_encode($info, true));
            $this->assign('authGroup', $auth_group);
            $this->assign('is_edit', 1);
            $this->assign('title', lang('edit') . lang('admin'));
            $this->assign('result', $result);
            return view('adminForm');
        }
    }
    /*-----------------------用户组管理----------------------*/
    //用户组管理
    public function adminGroup()
    {
        if (request()->isPost()) {
            $list = AuthGroup::all();
            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list, 'rel' => 1];
        }
        return view();
    }
    //删除管理员分组
    public function groupDel()
    {
        AuthGroup::where('group_id', '=', input('id'))->delete();
        return $result = ['code' => 1, 'msg' => '删除成功!'];
    }
    //添加分组
    public function groupAdd()
    {
        if (request()->isPost()) {
            $data = input('post.');
            $data['addtime'] = time();
            AuthGroup::create($data);
            $result['msg'] = '用户组添加成功!';
            $result['url'] = url('adminGroup');
            $result['code'] = 1;
            return $result;
        } else {
            $this->assign('title', '添加用户组');
            $this->assign('info', 'null');
            return $this->fetch('groupForm');
        }
    }
    //修改分组
    public function groupEdit()
    {
        if (request()->isPost()) {
            $data = input('post.');
            $where['group_id'] = $data['group_id'];
            AuthGroup::update($data, $where);
            $result = ['code' => 1, 'msg' => '用户组修改成功!', 'url' => url('adminGroup')];
            return $result;
        } else {
            $id = input('id');
            $info = AuthGroup::get(['group_id' => $id]);
            $this->assign('info', json_encode($info, true));
            $this->assign('title', '编辑用户组');
            return $this->fetch('groupForm');
        }
    }
    //分组配置规则
    public function groupAccess()
    {
        $nav = new Leftnav();
        $admin_rule = db('auth_rule')->field('id,pid,title')->order('sort asc')->select();
        $rules = db('auth_group')->where('group_id', input('id'))->value('rules');
        $arr = $nav->auth($admin_rule, $pid = 0, $rules);
        $arr[] = array(
            "id" => 0,
            "pid" => 0,
            "title" => "全部",
            "open" => true
        );
        $this->assign('data', json_encode($arr, true));
        return $this->fetch();
    }
    public function groupSetaccess()
    {
        $rules = input('post.rules');
        if (empty($rules)) {
            return array('msg' => '请选择权限!', 'code' => 0);
        }
        $data = input('post.');
        $where['group_id'] = $data['group_id'];
        if (AuthGroup::update($data, $where)) {
            return array('msg' => '权限配置成功!', 'url' => url('adminGroup'), 'code' => 1);
        } else {
            return array('msg' => '保存错误', 'code' => 0);
        }
    }

    /********************************权限管理*******************************/
    public function adminRule()
    {
        if (request()->isPost()) {
            $arr = cache('authRuleList');
            if (!$arr) {
                $arr = Db::name('authRule')->order('pid asc,sort asc')->select();
                foreach ($arr as $k => $v) {
                    $arr[$k]['lay_is_open'] = false;
                }
                cache('authRuleList', $arr, 3600);
            }
            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $arr, 'is' => true, 'tip' => '操作成功'];
        }
        return view();
    }
    public function clear()
    {
        $arr = Db::name('authRule')->where('pid', 'neq', 0)->select();
        foreach ($arr as $k => $v) {
            $p = Db::name('authRule')->where('id', $v['pid'])->find();
            if (!$p) {
                Db::name('authRule')->where('id', $v['id'])->delete();
            }
        }
        cache('authRule', NULL);
        cache('authRuleList', NULL);
        $this->success('清除成功');
    }
    public function ruleAdd()
    {
        if (request()->isPost()) {
            $data = input('post.');
            $data['addtime'] = time();
            authRule::create($data);
            cache('authRule', NULL);
            cache('authRuleList', NULL);
            cache('addAuthRuleList', NULL);
            return $result = ['code' => 1, 'msg' => '权限添加成功!', 'url' => url('adminRule')];
        } else {
            $nav = new Leftnav();
            $arr = cache('addAuthRuleList');
            if (!$arr) {
                $authRule = authRule::all(function ($query) {
                    $query->order('sort', 'asc');
                });
                $arr = $nav->menu($authRule);
                cache('addAuthRuleList', $arr, 3600);
            }
            $this->assign('admin_rule', $arr); //权限列表
            return $this->fetch();
        }
    }
    public function ruleOrder()
    {
        $auth_rule = db('auth_rule');
        $data = input('post.');
        if ($auth_rule->update($data) !== false) {
            cache('authRuleList', NULL);
            cache('authRule', NULL);
            cache('addAuthRuleList', NULL);
            return $result = ['code' => 1, 'msg' => '排序更新成功!', 'url' => url('adminRule')];
        } else {
            return $result = ['code' => 0, 'msg' => '排序更新失败!'];
        }
    }
    //设置权限菜单显示或者隐藏
    public function ruleState()
    {
        $id = input('post.id');
        $menustatus = input('post.menustatus');
        if (db('auth_rule')->where('id=' . $id)->update(['menustatus' => $menustatus]) !== false) {
            cache('authRule', NULL);
            cache('authRuleList', NULL);
            cache('addAuthRuleList', NULL);
            return ['status' => 1, 'msg' => '设置成功!'];
        } else {
            return ['status' => 0, 'msg' => '设置失败!'];
        }
    }
    //设置权限是否验证
    public function ruleTz()
    {
        $id = input('post.id');
        $authopen = input('post.authopen');
        if (db('auth_rule')->where('id=' . $id)->update(['authopen' => $authopen]) !== false) {
            cache('authRule', NULL);
            cache('authRuleList', NULL);
            cache('addAuthRuleList', NULL);
            return ['status' => 1, 'msg' => '设置成功!'];
        } else {
            return ['status' => 0, 'msg' => '设置失败!'];
        }
    }
    public function ruleDel()
    {
        authRule::destroy(['id' => input('param.id')]);
        cache('authRule', NULL);
        cache('authRuleList', NULL);
        cache('addAuthRuleList', NULL);
        return $result = ['code' => 1, 'msg' => '删除成功!'];
    }

    public function ruleEdit()
    {
        if (request()->isPost()) {
            $datas = input('post.');
            if (authRule::update($datas)) {
                cache('authRule', NULL);
                cache('authRuleList', NULL);
                cache('addAuthRuleList', NULL);
                return json(['code' => 1, 'msg' => '保存成功!', 'url' => url('adminRule')]);
            } else {
                return json(['code' => 0, 'msg' => '保存失败！']);
            }
        } else {
            $admin_rule = authRule::get(function ($query) {
                $query->where(['id' => input('id')])->field('id,href,title,icon,sort,menustatus');
            });
            $this->assign('rule', $admin_rule);
            return $this->fetch();
        }
    }


    public function getChannels() {
        // 查询 crm_inquiry 表中 status=0 的渠道
        $list = Db::name('crm_inquiry')->where('status', 0)->field('id, inquiry_name')->select();
        $channels = [];
        foreach ($list as $li) {
            $channels[] = [
                'id'   => $li['id'],
                'name' => $li['inquiry_name']
            ];
        }
        return json(['code' => 1, 'msg' => '获取成功!', 'data' => $channels]);
    }

    // 根据渠道获取店铺列表（从crm_client_status表的shop_names字段读取）
    public function getShops() {
        $inquiry_id = input('inquiry_id/d', 0);   // 获取渠道ID
        $admin_id   = input('admin_id/d', 0);     // 当前操作的管理员ID（编辑时）
        if (empty($inquiry_id)) {
            return json(['code' => 0, 'msg' => '渠道ID不能为空', 'data' => []]);
        }
        try {
            // 查询指定渠道下 status=0 的端口列表
            $portList = Db::name('crm_inquiry_port')
                ->where('inquiry_id', $inquiry_id)
                ->where('status', 0)
                ->field('id, port_name')
                ->select();
            if (!$portList) {
                return json(['code' => 0, 'msg' => '该渠道暂无端口数据', 'data' => []]);
            }
            // 获取当前管理员已选的端口ID列表（编辑时用来解禁自己已选的端口）
            $currentPortIds = [];
            if ($admin_id) {
                $currentAdmin = Admin::get($admin_id);
                if ($currentAdmin && !empty($currentAdmin['port_id'])) {
                    $currentPortIds = array_filter(explode(',', $currentAdmin['port_id']));
                }
            }
            // 收集同渠道下其他管理员已占用的端口ID
            $usedPortIds = [];
            $otherAdminsQuery = Db::name('admin')
                ->where('group_id', $this->yygid)           // 只针对运营组用户检查
                ->where('inquiry_id', $inquiry_id)
                ->where('port_id', '<>', '')
                ->where('port_id', '<>', null);
            if ($admin_id) {
                $otherAdminsQuery->where('admin_id', '<>', $admin_id);
            }
            $otherList = $otherAdminsQuery->field('port_id')->select();
            foreach ($otherList as $admin) {
                $ids = array_filter(explode(',', $admin['port_id']));
                foreach ($ids as $pid) {
                    if ($pid !== '' && !in_array($pid, $usedPortIds)) {
                        $usedPortIds[] = $pid;
                    }
                }
            }
            // 整理输出数据，标记哪些端口已被占用（当前管理员已拥有的端口不算占用）
            $data = [];
            foreach ($portList as $port) {
                $id = (string)$port['id'];
                $is_used = in_array($id, $usedPortIds) ? 1 : 0;
                if ($is_used && in_array($id, $currentPortIds)) {
                    $is_used = 0;  // 自己已选的端口视为可用
                }
                $data[] = [
                    'id'        => $port['id'],
                    'port_name' => $port['port_name'],
                    'is_used'   => $is_used
                ];
            }
            return json(['code' => 1, 'msg' => '获取成功!', 'data' => $data]);
        } catch (\Exception $e) {
            return json(['code' => 0, 'msg' => '获取端口列表失败:' . $e->getMessage(), 'data' => []]);
        }
    }

    // 检查店铺（店铺名称）是否已被使用
    private function checkPortsUsed($portIds, $inquiry_id, $exclude_admin_id = 0) {
        if (empty($portIds) || empty($inquiry_id)) {
            return [];
        }
        // 确保是数组并去除空白
        $portIds = is_array($portIds) ? array_filter(array_map('trim', $portIds)) : array();
        if (empty($portIds)) {
            return [];
        }
        $used = [];
        // 查询同渠道下其他管理员的端口占用情况
        $query = Db::name('admin')
            ->where('group_id', $this->yygid)
            ->where('inquiry_id', $inquiry_id)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null);
        if ($exclude_admin_id > 0) {
            $query->where('admin_id', '<>', $exclude_admin_id);
        }
        $otherAdmins = $query->field('port_id')->select();
        foreach ($otherAdmins as $admin) {
            $ids = array_filter(explode(',', $admin['port_id']));
            foreach ($ids as $pid) {
                if ($pid !== '' && in_array($pid, $portIds)) {
                    $used[] = $pid;
                }
            }
        }
        return $used;
    }
}
