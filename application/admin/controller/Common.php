<?php

namespace app\admin\controller;

use think\Db;
use think\Controller;
use app\admin\model\Admin;
use think\facade\Request;

class Common extends Controller
{
    const ORG = [
        0 => 'admin',
        1 => '豫工',
        // 2 => '2s',
        3 => '3s'
    ];
    public  $channel_map = [
        'c端' => 'C端',
        '抖音' => '抖音',
        'sem' => '竞价',
        'SEM' => '竞价',
        '竞价' => '竞价',
    ];

    public $yygid = 12; //运营id
    public $ywzgid = 11; //业务主管
    public $ywgid = 10; //业务员
    public $pdgid = 13; //产品总监
    public $org_fgx = ',';


    protected $mod, $role, $system, $nav, $menudata, $cache_model, $categorys, $module, $moduleid, $adminRules, $HrefId;
    public function initialize()
    {
        //判断管理员是否登录
        if (!session('aid')) {
            $this->redirect('admin/login/index');
        }
        define('MODULE_NAME', strtolower(request()->controller()));
        define('ACTION_NAME', strtolower(request()->action()));
        //权限管理
        //当前操作权限ID
        if (session('aid') != 1) {
            if (!$this->shouldBypassClientEditAuth() && !$this->shouldBypassFirstTimeoutChildAuth()) {
                $this->HrefId = db('auth_rule')->where('href', MODULE_NAME . '/' . ACTION_NAME)->value('id');
                //当前管理员权限
                $map['a.admin_id'] = session('aid');
                $rules = Db::table(config('database.prefix') . 'admin')->alias('a')
                    ->join(config('database.prefix') . 'auth_group ag', 'a.group_id = ag.group_id', 'left')
                    ->where($map)
                    ->value('ag.rules');
                $this->adminRules = explode(',', $rules);
                if ($this->HrefId) {
                    if (!in_array($this->HrefId, $this->adminRules)) {
                        $this->error('您无此操作权限');
                    }
                }
            }
        }
        $this->cache_model = array('Module', 'AuthRule', 'Category', 'Posid', 'Field', 'System', 'cm');
        foreach ($this->cache_model as $r) {
            if (!cache($r)) {
                savecache($r);
            }
        }
        $this->system = cache('System');
        $this->categorys = cache('Category');
        $this->module = cache('Module');
        $this->mod = cache('Mod');
        $this->rule = cache('AuthRule');
        $this->cm = cache('cm');
    }

    /**
     * 统一权限拦截放行：
     * 财务专员(group_id=15)允许访问 Client/edit（仅编辑客户，不扩大删除等权限）。
     *
     * @return bool
     */
    protected function shouldBypassClientEditAuth()
    {
        $currentController = strtolower((string)request()->controller());
        $currentAction = strtolower((string)request()->action());

        if ($currentController !== 'client' || $currentAction !== 'edit') {
            return false;
        }

        $currentAdmin = Admin::getMyInfo();
        $gid = (int)($currentAdmin['group_id'] ?? session('gid'));

        return $gid === 15;
    }

    /**
     * 首次超时分配子接口权限放行：
     * 只要拥有 Liberum/firstTimeout 页面权限，即可访问其配套接口。
     * 注意：仅放行节点权限拦截，业务数据范围仍由 Service 层控制。
     *
     * @return bool
     */
    protected function shouldBypassFirstTimeoutChildAuth()
    {
        $currentController = strtolower((string)request()->controller());
        $currentAction = strtolower((string)request()->action());
        if ($currentController !== 'liberum') {
            return false;
        }

        $allowedActions = [
            'getfirsttimeoutlist',
            'getfirsttimeoutconfigstatus',
            'getassignuseroptions',
            'assignfirsttimeout',
            'batchassignfirsttimeout',
            'firsttimeoutdetail',
        ];
        if (!in_array($currentAction, $allowedActions, true)) {
            return false;
        }

        $pageRuleId = db('auth_rule')
            ->where('href', 'Liberum/firstTimeout')
            ->value('id');
        if (!$pageRuleId) {
            $pageRuleId = db('auth_rule')
                ->where('href', 'liberum/firsttimeout')
                ->value('id');
        }
        $pageRuleId = (int)$pageRuleId;
        if ($pageRuleId <= 0) {
            return false;
        }

        $rules = Db::table(config('database.prefix') . 'admin')->alias('a')
            ->join(config('database.prefix') . 'auth_group ag', 'a.group_id = ag.group_id', 'left')
            ->where('a.admin_id', (int)session('aid'))
            ->value('ag.rules');
        if (!is_string($rules) || trim($rules) === '') {
            return false;
        }

        $ruleList = array_values(array_filter(array_map('trim', explode(',', $rules)), function ($item) {
            return $item !== '';
        }));
        return in_array((string)$pageRuleId, $ruleList, true);
    }
    //空操作
    public function _empty()
    {
        return $this->error('空操作，返回上次访问页面中...');
    }


    //使用缓存记录团队数据

    public function getTeamList($flush = false)
    {

        //清除缓存
        // if ($flush) cache('teamList', null);
        // $teamList = cache('teamList');
        // if ($teamList) {
        //     return $teamList;
        // }
        $teamList = Db::name('admin')->group('team_name')->column('team_name');
        //cache('teamList', $teamList);
        return $teamList;
    }


    //redis 连点锁定
    public function redisLock()
    {
        $redis_name  = md5(request()->path() . json_encode(request()->param()));
        $redis = new \Redis();
        $redis->connect('127.0.0.1', 26739);
        $redis->auth('csE88ifakDGC8PfH');   // 如有密码请取消注释
        if ($redis->get($redis_name)) return $this->result([], 500, '操作过于频繁，请稍后再试');
        $redis->setex($redis_name, 30, 1);
    }

    //redis 解锁
    public function redisUnLock()
    {
        $redis_name  = md5(request()->path() . json_encode(request()->param()));
        $redis = new \Redis();
        $redis->connect('127.0.0.1', 26739);
        $redis->auth('csE88ifakDGC8PfH');   // 如有密码请取消注释
        $redis->del($redis_name);
    }

    //客户来源列表
    public function getSoruceList()
    {
        if ($source_list = cache('sourceList')) {
            return $source_list;
        }
        $list = DB::table('crm_client_status')->field('id,status_name as name')->select();

        $source_list = [];
        foreach ($list as $v) {
            $source_list[$v['name']] = $v['id'];
        }
        cache('sourceList', $source_list);
        return $source_list;
    }



    //运营人员列表
    public function getYyList($channel = null)
    {
        $current_admin = Admin::getMyInfo();
        $where = [['group_id', '=', $this->yygid], ['is_open', '=', 1]];
        if ($current_admin['org']  &&  $current_admin['org'] != 'admin') {
            $where[] = $this->getOrgWhere($current_admin['org']);
        }

        if ($channel) $where[] = ['channel', '=', $channel];
        $yyList = [];
        $_yyList = [];
        $list = Admin::where($where)->order('org')->order('channel', 'asc')->field('admin_id,username,channel')->select();
        $channel_list = array_intersect_key($this->channel_map, $this->getSoruceList());
        $channel_list = array_flip($channel_list);

        foreach ($list as $v) {
            $_yyList[] = ['id' => $v['admin_id'], 'name' => $v['username']];
            $yyList[$channel_list[$v['channel']]][] = ['id' => $v['admin_id'], 'name' => $v['username']];
        }
        return ['yyList' => $yyList, '_yyList' => $_yyList];
    }

    //新增产品
    public function addProduct($product_name)
    {
        $current_admin = Admin::getMyInfo();
        $data['org'] = $current_admin['org'];
        $data['product_name'] = $product_name;
        $res = Db::name('crm_products')->insert($data);
        // cache($current_admin['org'] . '_product_list', null);
        return $res;
    }

    //判断是否存在商品
    public function checkProduct($product_name)
    {
        if (!$product_name) return true;
        $current_admin = Admin::getMyInfo();
        $where = [['product_name', '=', $product_name]];
        if ($current_admin['org'] && $current_admin['org'] != 'admin') $where[] = $this->getOrgWhere($current_admin['org']);
        $res = Db::name('crm_products')->where($where)->find();
        return $res;
    }

    //判断是否存在商品,条件是指定的分类
    public function checkProductCategory($product_name,$category_id)
    {
        if (!$product_name) return true;
        $current_admin = Admin::getMyInfo();
        $where = [['product_name', '=', $product_name]];
        if ($current_admin['org'] && $current_admin['org'] != 'admin') $where[] = $this->getOrgWhere($current_admin['org']);
        // 只检查启用状态的产品（status = 0），不检查已删除的（status = -1）
        $res = Db::name('crm_products')
            ->where($where)
            ->where('category_id','=',$category_id)
            ->where('status', '=', 0)
            ->find();
        return $res;
    }


    //判断是否存在运营端口,条件是指定的询盘来源
    public function checkPortInquiry($port_name, $inquiry_id)
    {
        if (!$port_name) return true;
        $current_admin = Admin::getMyInfo();
        $where = [['port_name', '=', $port_name]];
        if ($current_admin['org'] && $current_admin['org'] != 'admin') $where[] = $this->getOrgWhere($current_admin['org']);
        $res = Db::name('crm_inquiry_port')->where($where)->where('inquiry_id','=',$inquiry_id)->find();
        return $res;
    }


    //产品列表
    public function getProductList()
    {
        $current_admin = Admin::getMyInfo();
        $where = [];
        if ($current_admin['org'] && strpos($current_admin['org'], 'admin') === false) $where[] = $this->getOrgWhere($current_admin['org']);
        // 只获取启用状态的产品（status = 0）
        $list = Db::name('crm_products')
            ->where($where)
            ->where('status', '=', 0)
            ->group('product_name')
            ->field('product_name')
            ->select();
        $list = array_column($list, 'product_name');
        return json_encode($list);
    }


    //产品列表(我的客户使用)
    public function getProductListClient()
    {
        $current_admin = Admin::getMyInfo();
        $where = [];
        if ($current_admin['org'] && strpos($current_admin['org'], 'admin') === false) {
            $where[] = $this->getOrgWhere($current_admin['org'], 'p');
        }

        // 只获取启用状态的产品（status = 0）
        $rows = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id')
            ->where($where)
            ->where('p.status', '=', 0)
            ->group('p.product_name, c.category_name')
            ->field('p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();

        $result = [];
        foreach ($rows as $r) {
            $cat = $r['category_name'] ?: '无';
            $result[] = $r['product_name'].' --('.$cat.')';
        }
        return json_encode($result, JSON_UNESCAPED_UNICODE);
    }




    public function _search($params, $model, $callback = null)
    {
        $size = $params['limit'] ?? config('pageSize');
        $page = $params['page'] ?? 1;
        $table = $model->getTable();
        $_model = $model->getModel();
        if ($callback) $model = call_user_func($callback, $model, $params);

        $model->order($table . '.id', 'desc');
        $list = $model->paginate(array('list_rows' => $size, 'page' => $page))->toArray();
        if (method_exists($_model, '_formatData')) {
            foreach ($list['data'] as &$item) {
                $_model->_formatData($item);
            }
        }
        return $list;
    }

    public function buildTimeWhere($timebucket, $field = 'create_time')
    {
        if (!$timebucket) {
            return ['1','=','1'];
        }
        $raw = trim($timebucket);
        $lower = strtolower($raw);
        $lower = preg_replace('/\s+/', ' ', $lower);
        // 相对时间（如 -2 hours）保持原样，不替换空格，用于 strtotime
        if (preg_match('/^\-\d+/', $lower)) {
            $bucket = $lower;
        } else {
            $bucket = str_replace(' ', '_', $lower);
        }

        $timeRanges = [
            'today' => ['today', 'today'],
            'yesterday' => ['yesterday', 'yesterday'],
            'week' => ['monday this week', 'sunday this week'],
            'last_week' => ['monday last week', 'sunday last week'],
            'month' => ['first day of this month', 'last day of this month'],
            'last_month' => ['first day of last month', 'last day of last month'],
            'year' => ['first day of january this year', 'last day of december this year'],
            'last_year' => ['first day of january last year', 'last day of december last year'],
            '-2 hours' => [date('Y-m-d H:i:s', strtotime('-2 hours')), null]
        ];

        if (isset($timeRanges[$bucket])) {
            list($start, $end) = $timeRanges[$bucket];
            if ($bucket === '-2 hours') {
                return [$field, '>=', $start];
            }

            return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime($start)), date('Y-m-d 23:59:59', strtotime($end))]];
        }

        // 相对时间：如 -3 days, -2 hours（未在 timeRanges 中预定义的）
        if (preg_match('/^\-\d+/', $bucket)) {
            $start = date('Y-m-d H:i:s', strtotime($bucket));
            return [$field, '>=', $start];
        }

        if (strpos($lower, ' - ') !== false) {
            list($start, $end) = explode(' - ', $lower);
            return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime(trim($start))), date('Y-m-d 23:59:59', strtotime(trim($end)))]];
        }

        // 自定义日期
        return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime($lower)), date('Y-m-d 23:59:59', strtotime($lower . '+1 day'))]];
    }

    public function getOrg($org)
    {
        return explode($this->org_fgx, trim($org, $this->org_fgx));
    }

    public function getOrgWhere($org, $alias = '')
    {
        return function ($query) use ($org, $alias) {
            $org_list = $this->getOrg($org);
            $alias = $alias ? $alias . '.' : '';
            $query->where($alias . 'org', 'in', $org_list);
            foreach ($org_list as $v) {
                $query->whereOr($alias . 'org', 'like', '%' . $this->org_fgx . $v . $this->org_fgx . '%');
            }
        };
    }

    //每个月的询盘数=当月录入询盘数-当月录入的询盘丢入公海数（仅当月）+当月从公海拾取数
    public function getClientimeWhere($timebucket, $alias = '')
    {
        $alias = $alias ? $alias . '.' : '';
        return function ($query) use ($timebucket, $alias) {
            $query->where([$this->buildTimeWhere($timebucket, $alias . 'at_time')])
                ->whereOr([$this->buildTimeWhere($timebucket, $alias . 'to_kh_time')]);
        };
    }

    //产品分类列表
    public function getCategoryList()
    {
        $current_admin = Admin::getMyInfo();
        $where = [];
        if ($current_admin['org'] && strpos($current_admin['org'], 'admin') === false) $where[] = $this->getOrgWhere($current_admin['org']);
        $list = Db::name('crm_product_category')->where($where)->select();
        return $list;
    }

    //询盘来源列表
    public function getInquiryList()
    {
        $current_admin = Admin::getMyInfo();
        $where = [];
        if ($current_admin['org'] && strpos($current_admin['org'], 'admin') === false) $where[] = $this->getOrgWhere($current_admin['org']);
        $list = Db::name('crm_inquiry')->where($where)->select();
        return $list;
    }

    // 自定义过滤器：先转义后去空格
    function htmlentities_trim($value)
    {
        return trim(htmlentities($value, ENT_QUOTES, 'UTF-8'));
    }

    // 根据渠道获取店铺列表（从数据库读取，关联crm_client_status表）
    // channel参数是映射后的渠道名称（如：C端、抖音、竞价）
    // status_name是crm_client_status表的原始status_name（如：c端、抖音、SEM）
    public function getShopsByChannel($channel, $status_name = null)
    {
        if (empty($channel)) {
            return [];
        }
        
        try {
            // 检查表是否存在（考虑表前缀）
            $prefix = config('database.prefix');
            $tableName = $prefix . 'crm_operation_shops';
            // 使用参数化查询避免SQL注入
            $tables = Db::query("SHOW TABLES LIKE ?", [$tableName]);
            if (empty($tables)) {
                // 表不存在，返回空数组
                return [];
            }
            
            // shops表的channel字段存储的是crm_client_status表的status_name值
            // 所以需要通过status_name来查询，而不是映射后的channel名称
            $queryChannel = $status_name;
            
            // 如果没有提供status_name，尝试通过channel_map反向查找
            if (empty($queryChannel)) {
                foreach ($this->channel_map as $key => $value) {
                    if ($value === $channel) {
                        $queryChannel = $key;
                        break;
                    }
                }
                // 如果还是找不到，尝试直接使用channel（可能是小写）
                if (empty($queryChannel)) {
                    $queryChannel = strtolower($channel);
                }
            }
            
            // 通过channel字段（存储的是status_name值）查询店铺
            $shops = Db::name('crm_operation_shops')
                ->where('channel', $queryChannel)
                ->where('is_active', 1)
                ->order('sort', 'asc')
                ->order('id', 'asc')
                ->field('id, shop_name as name, channel')
                ->select();
            
            // 转换为统一格式
            $result = [];
            if ($shops) {
                foreach ($shops as $shop) {
                    $result[] = [
                        'id' => $shop['id'],
                        'name' => $shop['name'],
                        'channel' => $shop['channel']
                    ];
                }
            }
            
            return $result;
        } catch (\Exception $e) {
            // 如果查询出错，返回空数组（错误信息会在调用处处理）
            return [];
        }
    }

    /**
     * 自动归档/软删除通知（自动维护未读上限10条、已读上限5条）
     * @param string $targetUser 目标用户（通知的target_user字段值）
     * @return bool 是否执行成功
     */
    protected function autoTrimNotifications($targetUser)
    {
        if (empty($targetUser)) {
            return false;
        }

        Db::startTrans();
        try {
            // 检测是否使用create_time字段（只查询一次）
            $tableColumns = Db::query("SHOW COLUMNS FROM `crm_order_notifications` LIKE 'create_time'");
            $useCreateTime = !empty($tableColumns);
            $orderField = $useCreateTime ? 'create_time' : 'id';

            // 1. 未读上限 10 条：如果超过 10 条，把最早的多余未读自动标记为已读
            $unreadCount = Db::name('crm_order_notifications')
                ->where('target_user', $targetUser)
                ->where('is_deleted', 0)
                ->where('is_read', 0)
                ->count();

            if ($unreadCount > 10) {
                $needAutoRead = $unreadCount - 10;

                // 查询最早的多余未读通知ID（按create_time升序，如果没有则用id ASC）
                $oldestUnreadIds = Db::name('crm_order_notifications')
                    ->where('target_user', $targetUser)
                    ->where('is_deleted', 0)
                    ->where('is_read', 0)
                    ->order($orderField, 'asc')
                    ->limit($needAutoRead)
                    ->column('id');

                if (!empty($oldestUnreadIds)) {
                    // 批量更新：标记为已读，并设置auto_read=1
                    Db::name('crm_order_notifications')
                        ->where('id', 'in', $oldestUnreadIds)
                        ->update([
                            'is_read' => 1,
                            'auto_read' => 1,
                            'read_time' => date('Y-m-d H:i:s'),
                        ]);
                }
            }

            // 2. 已读上限 5 条：如果超过 5 条，把最早的多余已读软删除
            $readCount = Db::name('crm_order_notifications')
                ->where('target_user', $targetUser)
                ->where('is_deleted', 0)
                ->where('is_read', 1)
                ->count();

            if ($readCount > 5) {
                $needSoftDelete = $readCount - 5;

                // 查询最早的多余已读通知ID（按create_time升序，如果没有则用id ASC）
                $oldestReadIds = Db::name('crm_order_notifications')
                    ->where('target_user', $targetUser)
                    ->where('is_deleted', 0)
                    ->where('is_read', 1)
                    ->order($orderField, 'asc')
                    ->limit($needSoftDelete)
                    ->column('id');

                if (!empty($oldestReadIds)) {
                    // 批量更新：软删除，并设置deleted_time
                    Db::name('crm_order_notifications')
                        ->where('id', 'in', $oldestReadIds)
                        ->update([
                            'is_deleted' => 1,
                            'deleted_time' => date('Y-m-d H:i:s'),
                        ]);
                }
            }

            Db::commit();
            return true;
        } catch (\Throwable $e) {
            Db::rollback();
            // 静默失败，避免影响主流程
            return false;
        }
    }

    // =========================================================================
    // 团队业绩表公共排除逻辑（统计排除，非权限限制）
    // 被排除的人仍可登录和进入页面，只是数据不参与团队业绩表统计和导出
    // =========================================================================

    /**
     * 团队业绩表统计排除名单（公共方法，Operator 与 DataStatistics 共用）。
     *
     * 说明：
     * - 这里只是"统计排除"，不是"权限限制"。
     * - 被排除的人自己仍然可以登录、进入页面查看。
     * - 只是他们的数据不参与团队业绩表三连屏（第一/二/三屏）和导出。
     *
     * 后续维护：只需修改此处一个数组，Operator 和 DataStatistics 两边自动同步生效。
     */
    protected function getExcludedTeamPerformanceUsernames(): array
    {
        $items = [
            // '张三',
            // '李四',
            '范文清',
            '郭志华',
            '郭志华2',
            '付淑雅',
            '叶诗龙',
            '于喜英',
            '李鹏',
            // 按实际需要继续补充
        ];

        $items = array_map(function ($v) {
            return trim((string)$v);
        }, $items);

        $items = array_filter($items, function ($v) {
            return $v !== '';
        });

        return array_values(array_unique($items));
    }

    /**
     * 在 query 上追加"排除指定业务员"的 WHERE 条件（公共方法，Operator 与 DataStatistics 共用）。
     *
     * @param mixed  $query         ThinkPHP Query 对象
     * @param array  $excludedUsers 排除名单（空数组则不追加条件）
     * @param string $usernameField 字段名，默认 pr_user；admin 表查询时传 'username'
     * @return mixed 原 query 对象（已链式追加条件）
     */
    protected function applyTeamPerformanceExcludedUsers($query, array $excludedUsers = [], string $usernameField = 'pr_user')
    {
        if (!empty($excludedUsers)) {
            $query->where($usernameField, 'not in', $excludedUsers);
        }
        return $query;
    }

    // =========================================================================
    // 业务人员业绩表公共排除逻辑（统计排除，非权限限制）
    // 被排除的人仍可登录和进入页面，只是数据不参与业务人员业绩表统计展示和导出
    // 后续维护：只需修改此处一个数组，Operator 和 DataStatistics 两边自动同步生效
    // =========================================================================

    /**
     * 业务人员业绩表统计排除名单（公共方法，Operator 与 DataStatistics 共用）。
     *
     * 说明：
     * - 这里只是"统计排除"，不是"权限限制"。
     * - 被排除的人自己仍然可以登录、进入页面查看数据。
     * - 只是他们的数据不参与业务人员业绩表（页面展示行、汇总 summary、导出）。
     *
     * 后续维护：只需修改此处一个数组，两侧页面自动同步生效。
     */
    protected function getPerformanceExcludeUsernames(): array
    {
        $items = [
            '范文清',
            '郭志华',
            '郭志华2',
            '付淑雅',
            '叶诗龙',
            '于喜英',
            '李鹏',
        ];

        return array_values(array_unique(array_filter(array_map(function ($name) {
            return trim((string)$name);
        }, $items))));
    }

    /**
     * 返回业务人员业绩表排除名单的 map 格式（key 为用户名，value 为 true）。
     * 方便业务代码直接用 isset($map[$username]) 判断，无需遍历数组。
     *
     * 仅影响展示数据，不影响账号权限。
     */
    protected function getPerformanceExcludeUsernameMap(): array
    {
        return array_fill_keys($this->getPerformanceExcludeUsernames(), true);
    }

}
