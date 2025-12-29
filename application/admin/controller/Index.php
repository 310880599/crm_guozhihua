<?php

namespace app\admin\controller;

use app\admin\model\Admin;
use think\Db;
use think\facade\Env;
use think\facade\Request;
use think\facade\Session;

class Index extends Common
{
    public function initialize()
    {
        parent::initialize();
    }
    public function index()
    {
        //导航
        // 获取缓存数据
        //$authRule = cache('authRuleGuoNei');
        //if (!$authRule) {
            //$authRule = db('auth_rule')->where('menustatus=1')->order('sort')->select();
            //cache('authRuleGuoNei', $authRule, 3600);
        //}

        $authRule = db('auth_rule')->where('menustatus=1')->order('sort')->select();

        //声明数组
        $menus = array();
        foreach ($authRule as $key => $val) {
            $authRule[$key]['href'] = url($val['href']);
            if ($val['pid'] == 0) {
                if (session('aid') != 1) {
                    if (in_array($val['id'], $this->adminRules)) {
                        $menus[] = $val;
                    }
                } else {
                    $menus[] = $val;
                }
            }
        }
        foreach ($menus as $k => $v) {
            foreach ($authRule as $kk => $vv) {
                if ($v['id'] == $vv['pid']) {
                    if (session('aid') != 1) {
                        if (in_array($vv['id'], $this->adminRules)) {
                            $menus[$k]['children'][] = $vv;
                        }
                    } else {
                        $menus[$k]['children'][] = $vv;
                    }
                }
            }
        }
        $curget = Admin::getMyInfo(1);
        $curgetnum = $curget['curgetnum'];
        $sysinfo = Db::table('system')->where(['id' => 1])->field('maxgetnum,custlimit')->find();
        $maxgetnum = $sysinfo['maxgetnum'];
        //判断组织是否已写入
        if (empty($curget['org'])) {
            $organizations = self::ORG;
            if ($curget['group_id'] != 1) {
                array_shift($organizations);
            }
        } else $organizations = [];
        $main ='main';
        if($curget['org'] !='admin' && in_array($curget['group_id'],[ $this->yygid,1])){
             $main ='Operator/main';
        }
        if($curget['org'] !='admin' && in_array($curget['group_id'],[ $this->pdgid])){
            $main ='Products/main';
        }
        $this->assign('organizations', json_encode($organizations));
        $this->assign('admin_id', $curget['admin_id']);
        $this->assign('main', $main);
        $this->assign('curgetnum', ($maxgetnum - $curgetnum));
        $this->assign('menus', json_encode($menus, true));
        return $this->fetch();
    }
    public function main()
    {
        //当前时间
        $time = time();
        $end_time = date('Y-m-d', $time);
        $start_time = date('Y-m-01', $time);
        $user_info = Admin::getMyInfo();
        $where = [['pr_user' ,'=', Session::get('username')],['at_time','between',[$start_time.' 00:00:00',$end_time.' 23:59:59']]];
        $whereOrder = [['create_time','between',[$start_time.' 00:00:00',$end_time.' 23:59:59']]];
        $whereAdmin =[['group_id', 'in',[$this->ywzgid,$this->ywgid]],['org','=',$user_info['org']]];


        //订单
        $orderCount = Db::table('crm_client_order')->where($whereOrder)->count();
        $myOrderCount = Db::table('crm_client_order')->where($whereOrder)->where('pr_user' ,'=', Session::get('username'))->count();

        $this->assign('orderCount', $orderCount);
        $this->assign('myOrderCount', $myOrderCount);


        //0 线索，1客户，2公海
        // $cluesCount = Db::table('crm_leads')->where(['status' => 0])->count();
        $clientCount = Db::table('crm_leads')->where($where)->where(['status' => 1])->count();
        $liberumCount = Db::table('crm_leads')->where('to_gh_time','between',[$start_time.' 00:00:00',$end_time.' 23:59:59'])->where('pr_user_bef' ,'=', Session::get('username'))->where(['status' => 2])->count();
        // 区别管理员和业务员


        // $this->assign('cluesCount', $cluesCount);
        $this->assign('clientCount', $clientCount);
        $this->assign('liberumCount', $liberumCount);


        //获取本周线索 ->whereTime('at_time', 'week') 
        // $cluesCount_week = Db::table('crm_leads')->where(['status' => 0, 'pr_user' => Session::get('username')])->count();
        //获取本月转客户数据 ->whereTime('to_kh_time', 'month') 
        $clientCount_month = Db::table('crm_leads')->where($where)->where(['status' => 1, 'issuccess' => -1, 'pr_user' => Session::get('username')])->count();

        //获取今年公海数据 ->whereTime('to_gh_time', 'year') 
        $liberumCount_year = Db::table('crm_leads')->where('to_gh_time','between',[$start_time.' 00:00:00',$end_time.' 23:59:59'])->where(['status' => 2])->count();


        // $clientCount_cj = Db::table('crm_leads')->where($where)->where(['status' => 1, 'issuccess' => 1, 'pr_user' => Session::get('username')])->count();
        // $clientCount_cj = Db::table('crm_leads')->where(['status' => 1, 'issuccess' => 1, 'pr_user' => Session::get('username')])->count();
        $clientCount_cj = Db::table('crm_client_order')->where($whereOrder)->count();

        $this->assign('clientCount_cj', $clientCount_cj);

        //月度排名（名）、月目标（元）、已成交（元）、完成率（%）、已成交（单）、提成点（%），
        //管理员添加业绩设置权限。
        $userlist = Db::name('admin')->where($whereAdmin)->field('admin_id,username,mubiao,ticheng')->select();



        //所有业务员
        foreach ($userlist as $key => $value) {
            $wheretoday = [];
            $wheretoday['pr_user'] = $value['username'];
            $wheretoday['status'] = '审核通过';
            $money_month = Db::name('crm_client_order')
                ->where($wheretoday)
                ->whereTime('create_time', 'month')
                ->sum('money');

            $value['money_month'] = $money_month;
            if ($value['mubiao'] > 0) {
                $value['wanchenglv'] = round($money_month / $value['mubiao'] * 100, 2);
            } else {
                $value['wanchenglv'] = 0;
            }

            $number_month = Db::table('crm_client_order')->where($wheretoday)->whereTime('create_time', 'month')->count('id');
            $value['number_month'] = $number_month;
            $userlist[$key] = $value;
        }
        // 获取所有业务员
        $userlist = Db::name('admin')->where($whereAdmin)->field('admin_id,username,mubiao,ticheng')->select();


        foreach ($userlist as $key => $value) {
            $username = $value['username'];

            // ✅ 查询当月客户数量
            $clientCountMonth = Db::table('crm_leads')
                ->where(['pr_user' => $username, 'status' => 1])
                ->whereTime('at_time', 'month') // 根据你的创建时间字段调整
                ->count();

            // 添加字段
            $value['client_count_month'] = $clientCountMonth;

            // 已有逻辑保留
            $wheretoday = [];
            $wheretoday['pr_user'] = $username;
            $wheretoday['status'] = '审核通过';

            $money_month = Db::name('crm_client_order')
                ->where($wheretoday)
                ->whereTime('create_time', 'month')
                ->sum('money');

            $value['money_month'] = $money_month ?: 0;
            $value['wanchenglv'] = $value['mubiao'] > 0 ? round($money_month / $value['mubiao'] * 100, 2) : 0;

            $number_month = Db::table('crm_client_order')
                ->where($wheretoday)
                ->whereTime('create_time', 'month')
                ->count('id');

            $value['number_month'] = $number_month;

            $userlist[$key] = $value;
        }

        // 排序
        array_multisort(array_column($userlist, 'money_month'), SORT_DESC, $userlist);

        $this->assign('userlist', $userlist);
        // 数组排序
        array_multisort(array_column($userlist, 'money_month'), SORT_DESC, $userlist);

        $this->assign('userlist', $userlist);

        //本人跟进动态
        //最近跟进动态  
        $result = Db::table('crm_leads')
            ->alias('l')
            ->join('crm_comment c', 'c.leads_id = l.id')
            ->join('admin a', 'c.user_id = a.admin_id')
            ->field('l.id,a.username,a.avatar,l.kh_name,c.reply_msg,c.create_date')
            ->order('c.create_date desc')
            ->where(['l.pr_user' => Session::get('username')])
            ->limit(10)->select();
        $this->assign('result', $result);


        $strTimeToString = "000111222334455556666667";
        $strWenhou = array('夜深了，', '凌晨了，', '早上好！', '上午好！', '中午好！', '下午好！', '晚上好！', '夜深了，');
        //echo $strWenhou[(int)$strTimeToString[(int)date('G',time())]];
        $this->assign('wenhou', '尊敬的管理员' . $strWenhou[(int)$strTimeToString[(int)date('G', time())]]);



        // $this->assign('cluesCount_week', $cluesCount_week);
        $this->assign('clientCount_month', $clientCount_month);
        $this->assign('liberumCount_year', $liberumCount_year);
        // 获取待办事项
        //今日已跟进客户*个，未跟进*个，跟进率*%
        //last_up_time

        $wheretoday = [];
        $wheretoday['pr_user'] = Session::get('username');
        $wheretoday['status'] = 1;
        $wheretoday['issuccess'] = -1;
        $all_count = Db::table('crm_leads')->where($wheretoday)->count();
        $today_count = Db::table('crm_leads')->where($wheretoday)->whereTime('last_up_time', 'today')->count();
        if ($all_count > 0) {
            $genjinlv = $today_count / $all_count * 100;
        } else {
            $genjinlv = 0;
        }

        $this->assign('all_count', $all_count - $today_count);
        $this->assign('today_count', $today_count);
        $this->assign('genjinlv', round($genjinlv, 2));

        $version = Db::query('SELECT VERSION() AS ver');
        $config  = [
            'url'             => $_SERVER['HTTP_HOST'],
            'document_root'   => $_SERVER['DOCUMENT_ROOT'],
            'server_os'       => PHP_OS,
            'server_port'     => $_SERVER['SERVER_PORT'],
            'server_ip'       => $_SERVER['SERVER_ADDR'],
            'server_soft'     => $_SERVER['SERVER_SOFTWARE'],
            'php_version'     => PHP_VERSION,
            'mysql_version'   => $version[0]['ver'],
            'max_upload_size' => ini_get('upload_max_filesize')
        ];
        $this->assign('config', $config);



        // ★ 修改：订单审核通知拆分为【未读】和【已读】
        $currentUsername = Session::get('username');

        // ★ 先执行自动归档/软删除处理（维护未读上限10条、已读上限5条）
        $this->autoTrimNotifications($currentUsername);

        // 未读：最多 10 条
        $unreadNotifications = Db::name('crm_order_notifications')
            ->where('target_user', $currentUsername)
            ->where('is_deleted', 0)
            ->where('is_read', 0)
            ->order('create_time desc')
            ->limit(10)
            ->select();

        // 已读：最多 5 条
        $readNotifications = Db::name('crm_order_notifications')
            ->where('target_user', $currentUsername)
            ->where('is_deleted', 0)
            ->where('is_read', 1)
            ->order('create_time desc')
            ->limit(5)
            ->select();

        $this->assign('unreadNotifications', $unreadNotifications);
        $this->assign('readNotifications', $readNotifications);

        
        return $this->fetch();
    }

    // ========================= 【新增】标记通知为已读 + 已读区超5条自动软删除最早的 =========================
    public function markNotificationRead()
    {
        if (!request()->isPost()) {
            // 【修改】统一返回结构为 code/msg/data
            return json(['code' => 0, 'msg' => '非法请求', 'data' => []]);
        }

        $id = (int)Request::param('id', 0);
        $currentUsername = Session::get('username');

        if ($id <= 0 || empty($currentUsername)) {
            // 【修改】统一返回结构为 code/msg/data
            return json(['code' => 0, 'msg' => '参数错误', 'data' => []]);
        }

        Db::startTrans();
        try {
            // 1) 查这条通知（必须是当前用户的、未删除）
            $notice = Db::name('crm_order_notifications')
                ->where('id', $id)
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->find();

            if (!$notice) {
                Db::rollback();
                // 【修改】统一返回结构为 code/msg/data
                return json(['code' => 0, 'msg' => '通知不存在或无权限', 'data' => []]);
            }

            // 2) 如果未读，则更新为已读（写入 read_time），但auto_read必须保持为0（手动已读不算系统归档）
            if ((int)$notice['is_read'] === 0) {
                Db::name('crm_order_notifications')
                    ->where('id', $id)
                    ->update([
                        'is_read'   => 1,
                        'read_time' => date('Y-m-d H:i:s'),
                        // 注意：不设置auto_read，保持原值（如果之前为0则保持0，这是手动已读）
                    ]);
            }

            // 3) 已读区最多保留 5 条：超过则软删除最早的已读消息
            $readCount = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->where('is_read', 1)
                ->count();

            $deletedIds = [];
            if ($readCount > 5) {
                $needDel = $readCount - 5;

                // 取"最早"的已读消息（按 create_time 正序）
                $oldest = Db::name('crm_order_notifications')
                    ->where('target_user', $currentUsername)
                    ->where('is_deleted', 0)
                    ->where('is_read', 1)
                    ->order('create_time asc')
                    ->limit($needDel)
                    ->column('id');

                if (!empty($oldest)) {
                    Db::name('crm_order_notifications')
                        ->where('id', 'in', $oldest)
                        ->update([
                            'is_deleted' => 1,
                            'deleted_time' => date('Y-m-d H:i:s')
                        ]);

                    $deletedIds = $oldest;
                }
            }

            // 4) 返回给前端：用于"挤到已读区"的展示数据
            // 说明：优先返回 read_time，否则返回 create_time，格式化为完整时间 Y-m-d H:i:s
            $timeField = '';
            if (!empty($notice['read_time'])) {
                $timeField = date('Y-m-d H:i:s', strtotime($notice['read_time']));
            } else if (!empty($notice['audit_time'])) {
                $timeField = date('Y-m-d H:i:s', strtotime($notice['audit_time']));
            } else {
                $timeField = date('Y-m-d H:i:s', strtotime($notice['create_time']));
            }
            
            $payload = [
                'id'          => $id,
                'message'     => $notice['message'],
                'type'        => (int)$notice['type'],
                'create_time' => $notice['create_time'],
                'read_time'   => !empty($notice['read_time']) ? $notice['read_time'] : '',
                'audit_time'  => !empty($notice['audit_time']) ? $notice['audit_time'] : '',
                'time_text'   => $timeField, // 完整时间格式
                'deleted_ids' => $deletedIds,
            ];

            Db::commit();
            // 【修改】统一返回结构为 code/msg/data，成功返回 code=1
            return json(['code' => 1, 'msg' => '已标记为已读', 'data' => $payload]);
        } catch (\Throwable $e) {
            Db::rollback();
            // 【修改】统一返回结构为 code/msg/data，失败返回 code=0
            return json(['code' => 0, 'msg' => '操作失败：' . $e->getMessage(), 'data' => []]);
        }
    }

    // ========================= 【新增】删除已读通知 =========================
    public function deleteNotification()
    {
        if (!request()->isPost()) {
            return json(['code' => 0, 'msg' => '非法请求']);
        }

        $id = (int)Request::param('id', 0);
        $currentUsername = Session::get('username');

        if ($id <= 0 || empty($currentUsername)) {
            return json(['code' => 0, 'msg' => '参数错误']);
        }

        try {
            // 1) 查这条通知（必须是当前用户的、未删除）
            $notice = Db::name('crm_order_notifications')
                ->where('id', $id)
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->find();

            if (!$notice) {
                return json(['code' => 0, 'msg' => '无权限或记录不存在']);
            }

            // 2) 软删除：更新 is_deleted = 1，并写入 deleted_time
            $updateData = [
                'is_deleted' => 1,
                'deleted_time' => date('Y-m-d H:i:s')
            ];

            // 4) 执行更新
            $result = Db::name('crm_order_notifications')
                ->where('id', $id)
                ->update($updateData);

            if ($result) {
                return json(['code' => 1, 'msg' => '删除成功']);
            } else {
                return json(['code' => 0, 'msg' => '删除失败']);
            }
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => '删除失败：' . $e->getMessage()]);
        }
    }

    // ========================= 【新增】轮询心跳接口：轻量级变化检测 =========================
    /**
     * 轮询心跳接口，用于检测通知是否有变化
     * 只返回变化标识（stamp），不返回完整列表，降低带宽和负载
     * 访问路径：/admin/index/notifyHeartbeat
     */
    public function notifyHeartbeat()
    {
        if (!request()->isAjax()) {
            return json(['code' => 0, 'msg' => '非法请求', 'data' => []]);
        }

        $currentUsername = Session::get('username');
        if (empty($currentUsername)) {
            return json(['code' => 0, 'msg' => '未登录', 'data' => []]);
        }

        try {
            // 只查询统计信息，不查询完整列表（轻量查询）
            // 未读数量
            $unreadCount = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->where('is_read', 0)
                ->count();

            // 已读数量（最多统计5条范围内的，因为已读区只显示5条）
            $readCount = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->where('is_read', 1)
                ->count();

            // 获取最大ID（用于变化检测）
            $maxId = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->max('id');

            $maxId = $maxId ? (int)$maxId : 0;

            // 生成变化标识 stamp（使用 md5 生成轻量标识）
            $stamp = md5($unreadCount . '|' . $readCount . '|' . $maxId);

            return json([
                'code' => 1,
                'msg' => 'success',
                'data' => [
                    'stamp' => $stamp,
                    'unread_count' => (int)$unreadCount,
                    'read_count' => (int)$readCount
                ]
            ]);
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => '查询失败：' . $e->getMessage(), 'data' => []]);
        }
    }

    // ========================= 【新增】获取通知列表接口（用于轮询检测到变化后拉取完整列表） =========================
    /**
     * 获取当前用户的通知列表（未读10条 + 已读5条）
     * 访问路径：/admin/index/getNotifications
     */
    public function getNotifications()
    {
        if (!request()->isAjax()) {
            return json(['code' => 0, 'msg' => '非法请求', 'data' => []]);
        }

        $currentUsername = Session::get('username');
        if (empty($currentUsername)) {
            return json(['code' => 0, 'msg' => '未登录', 'data' => []]);
        }

        try {
            // 先执行自动归档/软删除处理（维护未读上限10条、已读上限5条）
            $this->autoTrimNotifications($currentUsername);

            // 未读：最多 10 条
            $unreadNotifications = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->where('is_read', 0)
                ->order('create_time desc')
                ->limit(10)
                ->select();

            // 已读：最多 5 条
            $readNotifications = Db::name('crm_order_notifications')
                ->where('target_user', $currentUsername)
                ->where('is_deleted', 0)
                ->where('is_read', 1)
                ->order('create_time desc')
                ->limit(5)
                ->select();

            return json([
                'code' => 1,
                'msg' => 'success',
                'data' => [
                    'unread' => $unreadNotifications ?: [],
                    'read' => $readNotifications ?: []
                ]
            ]);
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => '查询失败：' . $e->getMessage(), 'data' => []]);
        }
    }
    

    public function navbar()
    {
        return $this->fetch();
    }
    public function nav()
    {
        return $this->fetch();
    }
    public function clear()
    {
        $R = Env::get('runtime_path');
        if ($this->_deleteDir($R)) {
            $result['info'] = '清除缓存成功!';
            $result['status'] = 1;
        } else {
            $result['info'] = '清除缓存失败!';
            $result['status'] = 0;
        }
        $result['url'] = url('admin/index/index');
        return $result;
    }
    private function _deleteDir($R)
    {
        $handle = opendir($R);
        while (($item = readdir($handle)) !== false) {
            if ($item != '.' and $item != '..') {
                if (is_dir($R . '/' . $item)) {
                    $this->_deleteDir($R . '/' . $item);
                } else {
                    if (!unlink($R . '/' . $item))
                        die('error!');
                }
            }
        }
        closedir($handle);
        return rmdir($R);
    }

    //退出登陆
    public function logout()
    {
        session(null);
        $this->redirect('login/index');
    }


    //保存组织
    public function saveorg()
    {
        $org = input('post.org');
        if (!$org) return ['code' => 0, 'msg' => '保存失败!'];
        $admin_id = input('post.admin_id');
        $res = Db::table('admin')->where('admin_id', $admin_id)->update(['org' => $org]);
        if ($res) {
            session('user_info', null);
            return ['code' => 1, 'msg' => '保存成功!'];
        } else {
            return ['code' => 0, 'msg' => '保存失败!'];
        }
    }

    //修改密码
    public function editPasswd()
    {
        if (Request::isAjax()) {
            $admin_id = Session::get('aid');
            $password = input('post.old_password', '', 'md5');
            $data['pwd'] = input('post.new_password', '', 'md5');
            $res = Db::table('admin')->where('admin_id', $admin_id)->where('pwd', $password)->update($data);
            if ($res) {
                session(null);
                return ['code' => 1, 'msg' => '修改成功!'];
            } else {
                return ['code' => 0, 'msg' => '修改失败:原密码错误或原密码与新密码相同!'];

            }
        }
        return $this->fetch('index/modify_password');
    }
}
