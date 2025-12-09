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
        // cache('teamList', $teamList);
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
        $timeRanges = [
            'today' => ['today', 'today'],
            'yesterday' => ['yesterday', 'yesterday'],
            'week' => ['monday this week', 'sunday this week'],
            'last week' => ['monday last week', 'sunday last week'],
            'month' => ['first day of this month', 'last day of this month'],
            'last month' => ['first day of last month', 'last day of last month'],
            'year' => ['first day of january this year', 'last day of december this year'],
            'last year' => ['first day of january last year', 'last day of december last year'],
            '-2 hours' => [date('Y-m-d H:i:s', strtotime('-2 hours')), null]
        ];

        if (isset($timeRanges[$timebucket])) {
            list($start, $end) = $timeRanges[$timebucket];
            if ($timebucket === '-2 hours') {
                return [[$field, '>=', $start]];
            }

            return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime($start)), date('Y-m-d 23:59:59', strtotime($end))]];
        }

        if (strpos($timebucket, '-') !== false) {
            list($start, $end) = explode(' - ', $timebucket);
            return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime($start)), date('Y-m-d 23:59:59', strtotime($end))]];
        }

        // 自定义日期
        return [$field, 'between time', [date('Y-m-d 00:00:00', strtotime($timebucket)), date('Y-m-d 23:59:59', strtotime($timebucket . '+1 day'))]];
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


}
