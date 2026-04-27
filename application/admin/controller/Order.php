<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use think\Container;
use app\admin\service\OrderImageService;
use app\admin\service\OrderService;

class Order extends Common
{

    const CUSTOMER_TYPE = [
        '个人用户（无公司）',
        '小型私企（终端用户）',
        '大型私企（上市/集团公司）',
        '经销商（商贸非自用）',
        '国企央企（含境外）',
        '政府事业单位',
        '部队/军企',
        '国外客户(外国人/公司)',
        '其他',
    ];

    /**
     * 利润达到阈值时，必须上传两类凭证
     *
     * @param mixed $profit
     * @return bool
     */
    private function isVoucherRequired($profit)
    {
        return floatval($profit) >= 2000;
    }

    //订单列表
    public function index()
    {
        if (request()->isPost()) {
            //获取函数的所有方法
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            $params['keyword']['timebucket'] = 'month';
            Request::merge($params);
            return $this->clientSearch();
            // $key = input('post.key');
            // $page = input('page') ? input('page') : 1;
            // $pageSize = input('limit') ? input('limit') : config('pageSize');
            // $list = db('crm_client_order')
            //     ->order('create_time desc')
            //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
            //     ->toArray();
            // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }

        // $total_money = db('crm_client_order')->sum('money');
        // $total_profit = db('crm_client_order')->sum('profit');
        // $this->assign('total_money', number_format($total_money, 2));
        // $this->assign('total_profit', number_format($total_profit, 2));


        //查询所有管理员（去除admin）
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        $this->assign('adminResult', $adminResult);

        //查询所有团队
        $teamList = $this->getTeamList();
        $this->assign('teamList', $teamList);

        //查询所有公司
        $user = \app\admin\model\Admin::getMyInfo();
        $orgList = self::ORG;
        if ($user['org']) {
            $orgList = $this->getOrg($user['org']);
        }
        $this->assign('orgList', $orgList);

        //查询所有客户来源
        $sourceList = Db::name('crm_client_status')->distinct(true)->column('status_name');
        $this->assign('sourceList', $sourceList);
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        return $this->fetch();
    }

    // ================= 列宽记忆：读取 =================
    // POST: page_key, table_id
    public function getColWidths()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey = trim((string)input('post.page_key', ''));
        $tableId = trim((string)input('post.table_id', ''));

        if ($pageKey === '' || $tableId === '') {
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }

        try {
            $rows = Db::name('crm_table_colwidth')
                ->where('admin_id', intval($aid))
                ->where('page_key', $pageKey)
                ->where('table_id', $tableId)
                ->field('field,width')
                ->select();

            $map = [];
            foreach ($rows as $r) {
                if (!empty($r['field'])) {
                    $map[$r['field']] = intval($r['width']);
                }
            }
            return json(['code' => 0, 'msg' => 'ok', 'data' => $map]);
        } catch (\Throwable $e) {
            // 表不存在/异常：直接返回空，不影响页面
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }
    }

    // ================= 列宽记忆：保存 =================
    // POST: page_key, table_id, widths(JSON字符串 或 数组)
    public function saveColWidths()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey = trim((string)input('post.page_key', ''));
        $tableId = trim((string)input('post.table_id', ''));
        $widths  = input('post.widths');

        if ($pageKey === '' || $tableId === '') {
            return json(['code' => 0, 'msg' => 'ok']);
        }

        // widths 兼容：数组 / JSON字符串
        if (is_string($widths)) {
            $decoded = json_decode($widths, true);
            if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                $widths = $decoded;
            } else {
                $widths = [];
            }
        }
        if (!is_array($widths)) $widths = [];

        $rows = [];
        $now  = time();

        foreach ($widths as $field => $w) {
            $field = trim((string)$field);
            if ($field === '' || !preg_match('/^[a-zA-Z0-9_]+$/', $field)) continue;

            $w = intval($w);
            if ($w <= 0 || $w > 3000) continue; // 上限与前端一致

            $rows[] = [
                'admin_id'   => intval($aid),
                'page_key'   => $pageKey,
                'table_id'   => $tableId,
                'field'      => $field,
                'width'      => $w,
                'updated_at' => $now,
            ];
        }

        if (empty($rows)) {
            return json(['code' => 0, 'msg' => 'ok']);
        }

        try {
            // 批量 upsert：依赖唯一索引 (admin_id,page_key,table_id,field)
            $values = [];
            foreach ($rows as $r) {
                $values[] = "(" .
                    intval($r['admin_id']) . "," .
                    "'" . addslashes($r['page_key']) . "'," .
                    "'" . addslashes($r['table_id']) . "'," .
                    "'" . addslashes($r['field']) . "'," .
                    intval($r['width']) . "," .
                    intval($r['updated_at']) .
                ")";
            }

            $sql = "INSERT INTO `crm_table_colwidth` (`admin_id`,`page_key`,`table_id`,`field`,`width`,`updated_at`) VALUES "
                . implode(',', $values)
                . " ON DUPLICATE KEY UPDATE `width`=VALUES(`width`), `updated_at`=VALUES(`updated_at`)";

            Db::execute($sql);

            return json(['code' => 0, 'msg' => 'ok']);
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => 'ok']);
        }
    }

    // ================= 列配置记忆（显示/隐藏）：读取 =================
    // POST: page_key
    public function getColConfig()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey = trim((string)input('post.page_key', ''));
        if ($pageKey === '') {
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }

        try {
            $row = Db::name('crm_table_col_config')
                ->where('uid', intval($aid))
                ->where('page_key', $pageKey)
                ->value('config_json');

            if (empty($row)) {
                return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
            }

            $arr = json_decode($row, true);
            if (json_last_error() !== JSON_ERROR_NONE || !is_array($arr)) {
                $arr = [];
            }

            return json(['code' => 0, 'msg' => 'ok', 'data' => $arr]);
        } catch (\Throwable $e) {
            // 表不存在/异常：直接返回空，不影响页面
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }
    }

    // ================= 列配置记忆（显示/隐藏）：保存 =================
    // POST: page_key, config_json(JSON字符串 或 数组)
    public function saveColConfig()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey    = trim((string)input('post.page_key', ''));
        $configJson = input('post.config_json');

        if ($pageKey === '') {
            return json(['code' => 0, 'msg' => 'ok']);
        }

        // 兼容：数组 / JSON 字符串；最终转为 JSON 字符串（对象/数组），非法则 {}
        $configArr = [];
        if (is_string($configJson) && $configJson !== '') {
            $decoded = json_decode($configJson, true);
            if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                $configArr = $decoded;
            }
        } elseif (is_array($configJson)) {
            $configArr = $configJson;
        }

        if (!is_array($configArr)) {
            $configArr = [];
        }

        $json = json_encode($configArr, JSON_UNESCAPED_UNICODE);
        if ($json === false || $json === null) {
            $json = '{}';
        }

        // 防御性长度限制，避免异常大数据
        if (strlen($json) > 200000) {
            $json = '{}';
        }

        $now      = time();
        $uid      = intval($aid);
        $pageSafe = addslashes($pageKey);
        $cfgSafe  = addslashes($json);

        try {
            $sql = "INSERT INTO `crm_table_col_config` (`uid`,`page_key`,`config_json`,`create_time`,`ut_time`) VALUES "
                . "(" . $uid . ",'" . $pageSafe . "','" . $cfgSafe . "'," . $now . "," . $now . ")"
                . " ON DUPLICATE KEY UPDATE `config_json`=VALUES(`config_json`), `ut_time`=VALUES(`ut_time`)";

            Db::execute($sql);

            return json(['code' => 0, 'msg' => 'ok']);
        } catch (\Throwable $e) {
            // 任意异常都不影响页面：仍返回 code=0
            return json(['code' => 0, 'msg' => 'ok']);
        }
    }

    // ================= 列顺序记忆：读取 =================
    // POST: page_key, table_key
    public function getColOrder()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey  = trim((string)input('post.page_key', ''));
        $tableKey = trim((string)input('post.table_key', ''));

        if ($pageKey === '' || $tableKey === '') {
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }

        try {
            $row = Db::name('crm_table_column_order')
                ->where('admin_id', intval($aid))
                ->where('page_key', $pageKey)
                ->where('table_key', $tableKey)
                ->value('column_order');

            if (empty($row)) {
                return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
            }

            $arr = json_decode($row, true);
            if (json_last_error() !== JSON_ERROR_NONE || !is_array($arr)) {
                $arr = [];
            }

            return json(['code' => 0, 'msg' => 'ok', 'data' => $arr]);
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => 'ok', 'data' => []]);
        }
    }

    // ================= 列顺序记忆：保存 =================
    // POST: page_key, table_key, column_order(JSON字符串 或 数组)
    public function saveColOrder()
    {
        $aid = Session::get('aid');
        if (empty($aid)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $pageKey     = trim((string)input('post.page_key', ''));
        $tableKey    = trim((string)input('post.table_key', ''));
        $columnOrder = input('post.column_order');

        if ($pageKey === '' || $tableKey === '') {
            return json(['code' => 0, 'msg' => 'ok']);
        }

        if (is_string($columnOrder)) {
            $arr = json_decode($columnOrder, true);
            if (json_last_error() !== JSON_ERROR_NONE || !is_array($arr)) {
                $arr = [];
            }
        } elseif (is_array($columnOrder)) {
            $arr = $columnOrder;
        } else {
            $arr = [];
        }

        $arr = array_values(array_unique(array_filter($arr, function ($f) {
            $f = trim((string)$f);
            return $f !== '' && preg_match('/^[a-zA-Z0-9_]+$/', $f);
        })));
        if (count($arr) > 500) {
            $arr = array_slice($arr, 0, 500);
        }

        $json = json_encode($arr, JSON_UNESCAPED_UNICODE);
        if ($json === false || $json === null) {
            $json = '[]';
        }
        if (strlen($json) > 200000) {
            $json = '[]';
        }

        $now = time();
        try {
            $sql = "INSERT INTO `crm_table_column_order` (`admin_id`,`page_key`,`table_key`,`column_order`,`create_time`,`update_time`) VALUES "
                . "(" . intval($aid) . ",'" . addslashes($pageKey) . "','" . addslashes($tableKey) . "','" . addslashes($json) . "'," . $now . "," . $now . ")"
                . " ON DUPLICATE KEY UPDATE `column_order`=VALUES(`column_order`), `update_time`=VALUES(`update_time`)";
            Db::execute($sql);
            return json(['code' => 0, 'msg' => 'ok']);
        } catch (\Throwable $e) {
            return json(['code' => 0, 'msg' => 'ok']);
        }
    }

    //导出订单
    public function exportindex()
    {
        if (request()->isPost()) {
            //获取函数的所有方法
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            $params['keyword']['timebucket'] = 'month';
            Request::merge($params);
            return $this->clientSearch();
            // $key = input('post.key');
            // $page = input('page') ? input('page') : 1;
            // $pageSize = input('limit') ? input('limit') : config('pageSize');
            // $list = db('crm_client_order')
            //     ->order('create_time desc')
            //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
            //     ->toArray();
            // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }

        // $total_money = db('crm_client_order')->sum('money');
        // $total_profit = db('crm_client_order')->sum('profit');
        // $this->assign('total_money', number_format($total_money, 2));
        // $this->assign('total_profit', number_format($total_profit, 2));


        //查询所有管理员（去除admin）
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        $this->assign('adminResult', $adminResult);

        //查询所有团队
        $teamList = $this->getTeamList();
        $this->assign('teamList', $teamList);

        //查询所有公司
        $user = \app\admin\model\Admin::getMyInfo();
        $orgList = self::ORG;
        if ($user['org']) {
            $orgList = $this->getOrg($user['org']);
        }
        $this->assign('orgList', $orgList);

        //查询所有客户来源
        $sourceList = Db::name('crm_client_status')->distinct(true)->column('status_name');
        $this->assign('sourceList', $sourceList);
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        return $this->fetch();
    }


    //（我的订单）列表
    public function personindex()
    {

        if (request()->isPost()) {
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            $params['keyword']['timebucket'] = 'month';
            Request::merge($params);
            return $this->personClientSearch();
            // $key = input('post.key');
            // $page = input('page') ? input('page') : 1;
            // $pageSize = input('limit') ? input('limit') : config('pageSize');
            // $list = db('crm_client_order')
            //     ->where(['pr_user' => Session::get('username')])
            //     ->order('create_time desc')
            //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
            //     ->toArray();
            // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        $this->assign('sourceList', Db::name('crm_client_status')->distinct(true)->column('status_name'));
        return $this->fetch();
    }

        //（订单草稿）列表
        public function draftindex()
        {
    
            if (request()->isPost()) {
                $params = Request::param();
                if (!isset($params['keyword'])) {
                    $params['keyword'] = [];
                }
                // 只有未选择时间时才默认「本月」，否则使用表单提交的 timebucket
                if (!isset($params['keyword']['timebucket']) || $params['keyword']['timebucket'] === '') {
                    $params['keyword']['timebucket'] = 'month';
                }
                Request::merge($params);
                return $this->draftClientSearch();
                // $key = input('post.key');
                // $page = input('page') ? input('page') : 1;
                // $pageSize = input('limit') ? input('limit') : config('pageSize');
                // $list = db('crm_client_order')
                //     ->where(['pr_user' => Session::get('username')])
                //     ->order('create_time desc')
                //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
                //     ->toArray();
                // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
            }
            $this->assign('customer_type', self::CUSTOMER_TYPE);
            $this->assign('sourceList', Db::name('crm_client_status')->distinct(true)->column('status_name'));
            return $this->fetch();
        }


    // 协同人订单页面入口（新增）
    public function collabIndex()
    {
        if (request()->isPost()) {
            return $this->collabSearch();
        }
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        $this->assign('sourceList', Db::name('crm_client_status')->distinct(true)->column('status_name'));
        return $this->fetch('order/collabindex');
    }

    // 协同人订单搜索接口（新增，返回 JSON）
    public function collabSearch()
    {
        $params = Request::param();
        if (!isset($params['keyword'])) {
            $params['keyword'] = [];
        }
        // 如果前端没有传 timebucket，默认使用本月
        if (!isset($params['keyword']['timebucket']) || empty($params['keyword']['timebucket'])) {
            $params['keyword']['timebucket'] = 'month';
        }
        Request::merge($params);
        return $this->collaboratorClientSearch();
    }

    //（待审核订单）列表
    public function pendingindex()
    {

        if (request()->isPost()) {
            $params = Request::param();
            if (!isset($params['keyword']) || !is_array($params['keyword'])) {
                $params['keyword'] = [];
            }
            if (!isset($params['keyword']['timebucket']) || $params['keyword']['timebucket'] === '') {
                $params['keyword']['timebucket'] = 'month';
            }
            Request::merge($params);
            return $this->pendingClientSearch();
            // $key = input('post.key');
            // $page = input('page') ? input('page') : 1;
            // $pageSize = input('limit') ? input('limit') : config('pageSize');
            // $list = db('crm_client_order')
            //     ->where(['pr_user' => Session::get('username')])
            //     ->order('create_time desc')
            //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
            //     ->toArray();
            // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        $this->assign('sourceList', Db::name('crm_client_status')->distinct(true)->column('status_name'));
        return $this->fetch();
    }
    
    
    //（审核失败订单）列表
    public function failedindex()
    {

        if (request()->isPost()) {
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            $params['keyword']['timebucket'] = 'month';
            Request::merge($params);
            return $this->failedClientSearch();
            // $key = input('post.key');
            // $page = input('page') ? input('page') : 1;
            // $pageSize = input('limit') ? input('limit') : config('pageSize');
            // $list = db('crm_client_order')
            //     ->where(['pr_user' => Session::get('username')])
            //     ->order('create_time desc')
            //     ->paginate(array('list_rows' => $pageSize, 'page' => $page))
            //     ->toArray();
            // return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        $this->assign('sourceList', Db::name('crm_client_status')->distinct(true)->column('status_name'));
        return $this->fetch();
    }
    
    

    // 新建订单第3版
    public function add()
    {
        if (request()->isPost()) {
            // ✅新增：区分“提交保存 / 保存草稿”
            $saveType = Request::param('save_type', 'submit');
            $isDraft = ($saveType === 'draft');
            $adminInfo = \app\admin\model\Admin::getMyInfo();
            $canManageAllOrders = OrderService::canManageAllOrders($adminInfo);

            // ====== 验证客户是否属于当前用户或协同人 ======
            $contact = Request::param('contact');
            $leadsId = null; // 保存客户ID，用于后续更新成交状态
            $custinfo = null; // 保存客户信息，用于后续填充cname等字段
            if (!empty($contact)) {
                $currentUsername = Session::get('username');
                $currentAdminId = Session::get('aid');
                
                // 查找客户信息
                $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($contact) {
                    $_contact = trim(preg_replace('/[+\-\s]/', '', $contact));
                    $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$contact}'")
                        ->whereOr('contact_value', $contact);
                    if ($contact != $_contact) {
                        $query->whereOr('contact_value', $_contact)
                            ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_contact}'");
                    }
                })->find();
                
                if ($coninfo) {
                    $custinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
                    if ($custinfo) {
                        $leadsId = $custinfo['id']; // 保存客户ID
                        $isMyCustomer = ($custinfo['pr_user'] == $currentUsername);
                        
                        // 检查是否是协同人客户
                        $isCollaboratorCustomer = false;
                        if (!empty($custinfo['joint_person'])) {
                            $jp = $custinfo['joint_person'];
                            $jointPersonIds = [];
                            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                                $tmp = json_decode($jp, true);
                                if (is_array($tmp)) {
                                    $jointPersonIds = $tmp;
                                }
                            } else {
                                $jointPersonIds = array_values(array_filter(explode(',', $jp)));
                            }
                            if (in_array($currentAdminId, $jointPersonIds)) {
                                $isCollaboratorCustomer = true;
                            }
                        }
                        
                        // 如果客户既不是我的客户，也不是协同人客户，则不允许添加订单
                        if (!$canManageAllOrders && !$isMyCustomer && !$isCollaboratorCustomer) {
                            return fail('该客户不属于您的客户或协同人客户，无法添加订单');
                        }

                        // 订单正式提交前：客户电话职位身份必须完整（后端兜底）
                        if (!$isDraft) {
                            $checkResult = OrderService::checkClientPhonePositionTitleComplete($leadsId);
                            if (!$checkResult['ok']) {
                                return json(['code' => 0, 'msg' => '客户电话职位身份未完善']);
                            }
                        }
                    }
                }
            }
            
            // ====== 读取主表字段 ======
            $data = [];
            $data['contact']          = Request::param('contact');        // 客户联系方式
            $data['cname']            = Request::param('cname', '');       // 客户名称
            
            // 如果cname为空，尝试从已查询的客户信息中获取
            if (empty($data['cname']) && $custinfo && !empty($custinfo['kh_name'])) {
                $data['cname'] = $custinfo['kh_name'];
            }
            
            // 如果cname仍然为空，尝试再次查询（防止前面验证失败的情况）
            if (empty($data['cname']) && !empty($contact)) {
                // 查找客户信息
                $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($contact) {
                    $_contact = trim(preg_replace('/[+\-\s]/', '', $contact));
                    $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$contact}'")
                        ->whereOr('contact_value', $contact);
                    if ($contact != $_contact) {
                        $query->whereOr('contact_value', $_contact)
                            ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_contact}'");
                    }
                })->find();
                
                if ($coninfo && !empty($coninfo['leads_id'])) {
                    $tempCustinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
                    if ($tempCustinfo && !empty($tempCustinfo['kh_name'])) {
                        $data['cname'] = $tempCustinfo['kh_name'];
                    }
                }
            }
            
            // 如果cname仍然为空，返回错误
            if (empty($data['cname'])) {
                // 草稿：允许不完整（但前端已做弱校验 contact/cname 至少一个）
                if (!$isDraft) {
                    return json(['code' => -200, 'msg' => '客户名称不能为空，请先输入联系方式并验证客户信息']);
                }
                $data['cname'] = '';
            }
            
            // 用户属性：0=公司，1=个人
            $data['customer_type_flag'] = Request::param('customer_type_flag', 0);
            $data['customer_type_flag'] = in_array($data['customer_type_flag'], ['0', '1']) ? (int)$data['customer_type_flag'] : 0;
            
            // 客户公司：如果是公司（0）则必填，如果是个人（1）则可以为空
            $clientCompany = Request::param('client_company', '');
            if ($data['customer_type_flag'] == 0) {
                $clientCompany = trim($clientCompany);
                // 公司类型：提交保存保持强校验；草稿允许为空/不校验格式
                if (!$isDraft) {
                    if ($clientCompany === '') {
                        return json(['code' => 0, 'msg' => '客户公司名称不能为空']);
                    }
                    // 校验：允许中文 + 中文括号（），且不少于2个字符
                    if (!preg_match('/^[\x{4e00}-\x{9fa5}（）]{2,}$/u', $clientCompany)) {
                        return json(['code' => 0, 'msg' => '客户公司名称只能填写中文（可包含中文括号），且不少于2个字']);
                    }
                }
                $data['client_company'] = $clientCompany; // 草稿可为空
            } else {
                // 个人类型，客户公司可以为空
                $data['client_company'] = '';
            }
            
            $data['province']         = Request::param('province', '');  // 省份
            $data['city']             = Request::param('city', '');       // 城市
            $data['country']          = Request::param('country');        // 发货地址
            $data['customer_type']    = Request::param('customer_type');  // 客户性质
            $data['source']           = Request::param('source');         // 询盘来源（运营渠道，存储为文字）
            // 强制覆盖 pr_user 为当前登录人，无论前端传什么值
            $data['pr_user']          = Session::get('username');
            $data['pr_user_id']       = (int)Session::get('aid');
            $data['oper_user']        = Request::param('oper_user');      // 运营人员
            $data['bank_account']     = Request::param('bank_account');   // 收款账户 ID
            // 【收款账户快照模式】根据 bank_account ID 查询账户名称并写入快照字段
            if (!empty($data['bank_account'])) {
                $data['bank_account_name'] = $this->resolveBankAccountName($data['bank_account']);
            } else {
                $data['bank_account_name'] = '';  // 如果为空，快照字段也置空
            }
            // ✅审核状态：草稿=0，提交=1（待审核）
            $data['check_status']     = $isDraft ? 0 : 1;

            // 处理运营端口：优先识别端口ID并反查名称；查不到时按名称兜底
            $sourcePortInput = trim((string)Request::param('source_port', ''));
            $sourcePortName = '';
            $data['source_port'] = '';
            if ($sourcePortInput !== '') {
                $isNumericId = ctype_digit($sourcePortInput) || (is_numeric($sourcePortInput) && (int)$sourcePortInput > 0);
                if ($isNumericId) {
                    $portInfo = Db::name('crm_inquiry_port')->where('id', (int)$sourcePortInput)->field('id, port_name')->find();
                    if (!empty($portInfo['port_name'])) {
                        $sourcePortName = trim((string)$portInfo['port_name']);
                    }
                }
                if ($sourcePortName === '') {
                    $sourcePortName = mb_substr($sourcePortInput, 0, 100, 'UTF-8');
                }
                $data['source_port'] = $sourcePortName;
            }

            // 返单强规则（仅正式提交校验，草稿不拦截）
            if (!$isDraft) {
                $returnRuleCheck = OrderService::validateReturnOrderSelection(
                    $data['contact'],
                    $data['source'],
                    $sourcePortInput,
                    0,
                    $sourcePortName
                );
                if (empty($returnRuleCheck['ok'])) {
                    $this->redisUnLock();
                    return json([
                        'code' => 0,
                        'msg' => (string)($returnRuleCheck['message'] ?? '该客户已有审核通过订单，本次订单必须选择返单来源和对应返单运营端口，请勿选择非返单来源。'),
                    ]);
                }
            }
            // 最终入库前以后端规则统一修正 source / order_type
            $data = OrderService::applyOrderTypeByReturnRule($data, 0);
            // 强制覆盖 team_name 为当前登录人的团队名称
            $currentUsername = Session::get('username');
            $loginTeamName = '';
            // 从 admin 表读取登录人的团队名称
            $adminInfo = Db::name('admin')->where('username', $currentUsername)->field('team_name')->find();
            if ($adminInfo && !empty($adminInfo['team_name'])) {
                $loginTeamName = $adminInfo['team_name'];
            } else {
                // fallback: 如果查不到，尝试从 session 获取
                $loginTeamName = Session::get('team_name') ?: '';
            }
            $data['team_name']        = $loginTeamName;  // 强制使用登录人团队名称
            $data['at_user']          = Session::get('username');         // 创建人
            $data['at_user_id']       = (int)Session::get('aid');
            // 成交时间：提交必填；草稿允许为空（写 NULL，避免 datetime 插入 '' 报错）
            $orderTime = trim((string)Request::param('order_time', ''));

            if ($isDraft) {
                // 草稿：允许不填，空则写 NULL
                $data['order_time'] = ($orderTime === '') ? null : $orderTime;
            } else {
                // 提交：必须填写
                if ($orderTime === '') {
                    $this->redisUnLock();
                    return json(['code' => 0, 'msg' => '成交时间不能为空']);
                }
                // 可选：格式校验（yyyy-MM-dd HH:mm:ss）
                if (!preg_match('/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/', $orderTime)) {
                    $this->redisUnLock();
                    return json(['code' => 0, 'msg' => '成交时间格式不正确，请选择正确的日期时间']);
                }
                $data['order_time'] = $orderTime;
            }
            $data['shipping_cost']    = Request::param('shipping_cost');  // 估算运费
            // 票种性质（普票、专票、不开票）- 验证并保存
            $invoiceType = Request::param('invoice_type', '');
            if (in_array($invoiceType, ['普票', '专票', '不开票'])) {
                $data['invoice_type'] = $invoiceType;
            } else {
                $data['invoice_type'] = ''; // 如果值不正确，设为空
            }
            $data['invoice_amount']   = Request::param('invoice_amount'); // 开票金额
            $data['tax_amount']       = Request::param('tax_amount');     // 税费金额
            $data['debugging_cost']   = Request::param('debugging_cost'); // 调试费
            $data['sales_commission'] = Request::param('sales_commission'); // 佣金
            $data['split_remarks']    = Request::param('split_remarks');  // 分成备注
            $data['amount_received']  = Request::param('amount_received'); // 已收款金额
            //$data['remark']           = Request::param('remark');         // 备注
            
            // ====== 后端统一重算利润（禁止使用前端传入 profit / margin_rate）======
            // 口径：purchase_price = 【进价合计】（整行，非单价），详见 OrderService::recalculateOrderProfit 注释
            // 这里只准备 params，供下方 buildOrderSaveData 与凭证校验复用
            $profitCalcParams = [
                'qty' => Request::param('qty/a'),
                'unit_price' => Request::param('unit_price/a'),
                'purchase_price' => Request::param('purchase_price/a'),
                'shipping_cost' => $data['shipping_cost'],
                'tax_amount' => $data['tax_amount'],
                'debugging_cost' => $data['debugging_cost'],
                'sales_commission' => $data['sales_commission'],
            ];

            // ✅草稿：可不传凭证；正式提交必须通过统一凭证校验
            if ($isDraft) {
                $data['wechat_receipt_image'] = '';
                $data['inquiry_assign_image'] = '';
            } else {
                $MAX_WECHAT_RECEIPT_IMAGES = 10;
                $MAX_INQUIRY_ASSIGN_IMAGES = 10;

                $wechatReceiptRaw = Request::param('wechat_receipt_image', '');
                $wechatReceiptParsed = OrderService::parseImageList($wechatReceiptRaw);
                if (count($wechatReceiptParsed) > $MAX_WECHAT_RECEIPT_IMAGES) {
                    $this->redisUnLock();
                    return json(['code' => 0, 'msg' => '微信沟通凭证图片数量不能超过 ' . $MAX_WECHAT_RECEIPT_IMAGES . ' 张']);
                }
                $wechatReceiptUrls = OrderService::normalizeVoucherImages($wechatReceiptParsed, $MAX_WECHAT_RECEIPT_IMAGES);

                $inquiryAssignRaw = Request::param('inquiry_assign_image', '');
                $inquiryAssignParsed = OrderService::parseImageList($inquiryAssignRaw);
                if (count($inquiryAssignParsed) > $MAX_INQUIRY_ASSIGN_IMAGES) {
                    $this->redisUnLock();
                    return json(['code' => 0, 'msg' => '询盘来源凭证图片数量不能超过 ' . $MAX_INQUIRY_ASSIGN_IMAGES . ' 张']);
                }
                $inquiryAssignUrls = OrderService::normalizeVoucherImages($inquiryAssignParsed, $MAX_INQUIRY_ASSIGN_IMAGES);

                $voucherValidationParams = $profitCalcParams;
                $voucherValidationParams['wechat_receipt_image'] = $wechatReceiptRaw;
                $voucherValidationParams['inquiry_assign_image'] = $inquiryAssignRaw;
                $voucherValidation = OrderService::validateVoucherRequirement($voucherValidationParams, []);
                if (empty($voucherValidation['ok'])) {
                    $this->redisUnLock();
                    return json(['code' => 0, 'msg' => $voucherValidation['message']]);
                }

                $data['wechat_receipt_image'] = json_encode($wechatReceiptUrls, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                $data['inquiry_assign_image'] = json_encode($inquiryAssignUrls, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            }
            $managerIds   = Request::param('product_manager/a'); // ★ 产品经理（管理员）ID 数组
            // ✅文本状态（如表里有 status 字段则同步）
            $data['status']           = $isDraft ? '草稿' : '待审核';
            $data['create_time']      = date("Y-m-d H:i:s");
            $data['order_no']         = date("YmdHis") . rand(1000, 9999);


            // 3) 解析并写入协同人（joint_person），支持 数组 / JSON / 逗号分隔
            $jpRaw = Request::param('joint_person');
            $jpIds = [];
            if (is_array($jpRaw)) {
                $jpIds = $jpRaw;
            } else if (is_string($jpRaw)) {
                $jpRaw = trim($jpRaw);
                if ($jpRaw !== '') {
                    if ($jpRaw[0] === '[') {
                        $tmp = json_decode($jpRaw, true);
                        if (is_array($tmp)) $jpIds = $tmp;
                    } else {
                        $jpIds = explode(',', $jpRaw);
                    }
                }
            }
            // 仅保留数字、去空去重
            $jpIds = array_values(array_unique(array_filter(array_map(function ($v) {
                return preg_replace('/\D/', '', (string)$v);
            }, $jpIds), function ($v) {
                return $v !== '';
            })));
            $jpStr = implode(',', $jpIds);
            // 若你的 joint_person 仍为 varchar(30)，做长度保护（推荐把字段扩为 varchar(255)）
            if (strlen($jpStr) > 30) {
                $this->redisUnLock();
                return fail('协同人过多，超出存储限制（请减少选择或扩大 joint_person 字段长度）');
            }
            $data['joint_person'] = $jpStr;



            // ====== 明细字段（注意：product_name[] 现在是【产品ID】）======
            $productIds     = Request::param('product_name/a'); // <-- 产品ID数组
            $specModels     = Request::param('spec_model/a');
            $units          = Request::param('unit/a');
            $qtys           = Request::param('qty/a');
            $unitPrices     = Request::param('unit_price/a');
            $totalPrices    = Request::param('total_price/a');
            $purchasePrices = Request::param('purchase_price/a');
            $subProfits     = Request::param('sub_profit/a');
            $itemRemarks    = Request::param('item_remark/a');

            // 汇总要查询的产品ID
            $idArr = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $pid) {
                    $pid = (int)$pid;
                    if ($pid > 0) $idArr[] = $pid;
                }
                $idArr = array_values(array_unique($idArr));
            }

            // 【订单快照改造】批量查询产品信息，构建产品名称、供应商ID、供应商名称的映射
            // 注意：这里不过滤 status，因为历史订单可能引用已删除的产品，需要保留产品名称
            $prodMap = [];      // product_id => product_name
            $supIdMap = [];     // product_id => category_id (供应商ID)
            $supNameMap = [];   // product_id => category_name (供应商名称)
            if (!empty($idArr)) {
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, p.category_id, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    $prodMap[$r['id']] = $r['product_name'];
                    $supIdMap[$r['id']] = $r['category_id'] ?? 0;
                    $supNameMap[$r['id']] = $r['category_name'] ?? '';
                }
                
                // 【兼容校验】如果某些产品ID查询不到（异常/被删/数据脏），直接返回错误
                $missingPids = [];
                foreach ($idArr as $pid) {
                    if (!isset($prodMap[$pid])) {
                        $missingPids[] = $pid;
                    }
                }
                if (!empty($missingPids)) {
                    if (!$isDraft) {
                        $this->redisUnLock();
                        return json(['code' => -200, 'msg' => '产品ID ' . implode(', ', $missingPids) . ' 不存在或已删除，无法保存']);
                    }
                }
            }

            // 统一由 Service 构造明细行 + 主表金额（唯一口径，杜绝 Controller 自己再算一遍）
            $saveBundle = OrderService::buildOrderSaveData(
                [
                    'product_ids'      => $productIds,
                    'manager_ids'      => $managerIds,
                    'spec_models'      => $specModels,
                    'units'            => $units,
                    'qty'              => $qtys,
                    'unit_price'       => $unitPrices,
                    'purchase_price'   => $purchasePrices,
                    'item_remarks'     => $itemRemarks,
                    'shipping_cost'    => $data['shipping_cost'],
                    'tax_amount'       => $data['tax_amount'],
                    'debugging_cost'   => $data['debugging_cost'],
                    'sales_commission' => $data['sales_commission'],
                ],
                $prodMap,
                $supIdMap,
                $supNameMap,
                0 // 主表 insert 后再回填 order_id
            );
            $itemsData = $saveBundle['items'];

            // ★ 防御性补丁：无论 $data 之前从哪里取过值，在落库前强制剥掉
            // profit / margin_rate / money，杜绝前端 / 其他开发者后续改动带入污染值。
            unset($data['profit'], $data['margin_rate'], $data['money']);

            // 主表金额字段一律以 Service 计算结果为准（禁止信任前端 profit / margin_rate / money）
            $data['money']       = $saveBundle['money'];
            $data['profit']      = $saveBundle['profit'];
            $data['margin_rate'] = $saveBundle['margin_rate'];

            // 主表 product_name（存第一个产品名称，非ID）
            if ($saveBundle['product_name_summary'] !== '') {
                $data['product_name'] = $saveBundle['product_name_summary'];
            }

            // 开启事务，插入订单主表和明细表
            Db::startTrans();
            try {
                $orderId = null;
                // ====== 提交保存时：草稿转正（update），不 insert ======
                if (!$isDraft) {
                    $draftId = (int)Request::param('draft_id', 0);
                    $draftOrder = null;
                    // 优先A：前端传了 draft_id，校验存在/草稿态/权限
                    if ($draftId > 0) {
                        $draftOrder = Db::name('crm_client_order')->where('id', $draftId)->find();
                        if ($draftOrder && (int)$draftOrder['check_status'] === 0) {
                            $pr_user = Session::get('username') ?? '';
                            if ($pr_user && ($draftOrder['at_user'] === $pr_user || $draftOrder['pr_user'] === $pr_user)) {
                                // 校验通过，使用该草稿转正
                            } else {
                                $draftOrder = null;
                            }
                        } else {
                            $draftOrder = null;
                        }
                    }
                    // 兜底B：前端没传 draft_id，按 contact 匹配当前用户最新草稿
                    if (!$draftOrder && $draftId <= 0) {
                        $username = Session::get('username') ?? '';
                        $contactRaw = trim((string)Request::param('contact', ''));
                        $contactNorm = trim(preg_replace('/[+\-\s]/', '', $contactRaw));
                        if ($username !== '' && $contactNorm !== '') {
                            $drafts = Db::name('crm_client_order')
                                ->where('check_status', 0)
                                ->where(function ($q) use ($username) {
                                    $q->where('at_user', $username)->whereOr('pr_user', $username);
                                })
                                ->orderRaw('COALESCE(ut_time, create_time) DESC')
                                ->order('id', 'DESC')
                                ->select();
                            foreach ($drafts as $d) {
                                $dContact = trim((string)($d['contact'] ?? ''));
                                $dNorm = trim(preg_replace('/[+\-\s]/', '', $dContact));
                                if ($dNorm === $contactNorm) {
                                    $draftOrder = $d;
                                    $draftId = (int)$d['id'];
                                    break;
                                }
                            }
                        }
                    }
                    if ($draftOrder && $draftId > 0) {
                        // 草稿转正：update 主表，重建明细表
                        unset($data['create_time']);
                        $data['check_status'] = 1;
                        $data['status'] = '待审核';
                        $data['ut_time'] = date('Y-m-d H:i:s');
                        Db::name('crm_client_order')->where('id', $draftId)->update($data);
                        Db::name('crm_order_item')->where('order_id', $draftId)->delete();
                        if (!empty($itemsData)) {
                            foreach ($itemsData as &$item) {
                                $item['order_id'] = $draftId;
                            }
                            unset($item);
                            $res = Db::name('crm_order_item')->insertAll($itemsData);
                            if ($res === false || $res != count($itemsData)) {
                                throw new \Exception('订单明细插入失败');
                            }
                        }
                        if (!empty($leadsId)) {
                            Db::name('crm_leads')->where('id', $leadsId)->update(['issuccess' => 1]);
                        }
                        Db::commit();
                        return json(['code' => 0, 'msg' => '添加成功！', 'data' => ['id' => $draftId, 'from_draft' => 1]]);
                    }
                }

                // ====== 原有逻辑：insert 新订单（草稿保存 or 非草稿恢复的正常提交）======
                $orderId = Db::name('crm_client_order')->insertGetId($data);
                if (!$orderId) {
                    throw new \Exception('主订单插入失败');
                }
                // 插入明细行
                if (!empty($itemsData)) {
                    // 回填每个明细的 order_id
                    foreach ($itemsData as &$item) {
                        $item['order_id'] = $orderId;
                    }
                    unset($item);
                    $res = Db::name('crm_order_item')->insertAll($itemsData);
                    if ($res === false || $res != count($itemsData)) {
                        throw new \Exception('订单明细插入失败');
                    }
                }
                
                // 订单添加成功后，更新客户的成交状态为已成交
                if (!$isDraft && !empty($leadsId)) {
                    // 更新客户的成交状态为已成交
                    Db::name('crm_leads')->where('id', $leadsId)->update(['issuccess' => 1]);
                }
                
                Db::commit();
                if ($isDraft) {
                    return json(['code' => 0, 'msg' => '草稿保存成功', 'draft_id' => $orderId, 'data' => ['id' => $orderId]]);
                }
                return json(['code' => 0, 'msg' => '添加成功！']);
            } catch (\Exception $e) {
                Db::rollback();
                return json(['code' => -200, 'msg' => '添加失败！' . $e->getMessage()]);
            }
        }

        // ====== GET：渲染页面下拉等 ======

        // 团队/来源/客户性质/运营
        $teamList = $this->getTeamList();
        $this->assign('teamList', $teamList);

        // 从 crm_inquiry 表获取询盘来源列表（客户渠道）
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $inquiryWhere = [];
        if ($currentAdmin['org'] && strpos($currentAdmin['org'], 'admin') === false) {
            $inquiryWhere[] = $this->getOrgWhere($currentAdmin['org']);
        }
        // 只获取启用状态的询盘来源（status = 0）
        $inquiryQuery = Db::name('crm_inquiry');
        if (!empty($inquiryWhere)) {
            $inquiryQuery->where($inquiryWhere);
        }
        $inquiryList = $inquiryQuery
            ->where('status', '=', 0)
            ->field('id, inquiry_name')
            ->order('inquiry_name', 'asc')
            ->select();
        
        // 获取询盘来源名称列表（用于下拉框）
        $sourceList = array_column($inquiryList, 'inquiry_name');
        $this->assign('sourceList', $sourceList);

        $this->assign('customer_type', self::CUSTOMER_TYPE);

        $userlist = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        // 【收款账户快照模式】只显示未删除的账户（is_deleted=0）
        $accountList = Db::name('crm_receive_account')->where('is_deleted', 0)->field('id, account')->select();
        //var_dump($bankaccount);
        $this->assign('userlist', $userlist);
        $this->assign('accountList', $accountList);
        $this->assign('username', Session::get('username'));
        $this->assign('team_name', Session::get('team_name'));

        $yyData = $this->getYyList();
        $operUserList = $yyData['_yyList'];
        $this->assign('operUserList', $operUserList);
        $this->assign('yyList', json_encode($yyData['yyList'], JSON_UNESCAPED_UNICODE));

        // 从 crm_inquiry_port 表获取端口列表，按询盘来源（inquiry_id）分组
        $shopList = [];
        foreach ($inquiryList as $inquiry) {
            $inquiryName = $inquiry['inquiry_name'];
            $inquiryId = $inquiry['id'];
            
            // 查询该询盘来源对应的所有端口
            $ports = Db::name('crm_inquiry_port')
                ->where('inquiry_id', $inquiryId)
                ->where('status', '=', 0) // 只获取启用状态的端口
                ->field('id, port_name')
                ->order('port_name', 'asc')
                ->select();
            
            $shops = [];
            foreach ($ports as $port) {
                $shops[] = [
                    'id' => $port['id'],
                    'name' => $port['port_name']
                ];
            }
            
            if (!empty($shops)) {
                $shopList[$inquiryName] = $shops;
            }
        }
        $this->assign('shopList', json_encode($shopList, JSON_UNESCAPED_UNICODE));

        // 产品列表（与客户新增页一致，分组、取最小ID、带分类名）
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $productRows = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if ($currentAdmin['org'] && strpos($currentAdmin['org'], 'admin') === false) {
            $productRows->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        // 使用新的软删除字段：只获取未删除的产品和供应商（is_deleted = 0）
        $productRows->where([
            'p.is_deleted' => 0,
            'c.is_deleted' => 0,
        ]);
        $productRows = $productRows
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        $this->assign('productList', $productRows);

        // 协同人 xmSelect
        $teamName = session('team_name') ?: '';
        $adminList = Db::name('admin')
            ->where('group_id', '<>', 1)
            ->whereIn('group_id', [10, 11, 14, 17, 18, 19, 21, 22])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        foreach ($adminList as $admin) {
            $collaboratorData[] = ['name' => $admin['username'], 'value' => $admin['admin_id']];
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        // 查询所有产品经理（admin表中 group_id=14），按用户名升序
        $extraManagerIds = []; // 李营，可为空数组 [] 表示不额外包含任何人
        $managerList = Db::name('admin')
            ->where(function($q) use ($extraManagerIds){
                $q->whereIn('group_id', [13, 14, 18, 19, 22]);
                if (!empty($extraManagerIds)) {
                    $q->whereOr('admin_id', 'in', $extraManagerIds);   // ✅ TP5.1 兼容
                }
            })
            ->field('admin_id, username')
            ->order('username', 'asc')
            ->select();
        
        $this->assign('managerList', $managerList);
        

        return $this->fetch('order/add');
    }





    public function changeyewu()
    {
        $data  = Request::param();
        $custphone = $data['contact'];
        $adminInfo = \app\admin\model\Admin::getMyInfo();
        $canManageAllOrders = OrderService::canManageAllOrders($adminInfo);
        // $where=[];
        // $where['phone'] = $custphone;
        // $custinfo = Db::name('crm_leads')->where($where)->find();
        $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($custphone) {
            $_custphone = trim(preg_replace('/[+\-\s]/', '', $custphone));
            $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$custphone}'")
                ->whereOr('contact_value', $custphone);
            if ($custphone != $_custphone) {
                $query->whereOr('contact_value', $_custphone)
                    ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_custphone}'");
            }
        })->find();
        if (!$coninfo) {
            $res['code'] = 0;
            $res['msg'] = "该客户信息没用找到";
        } else {
            $custinfo =  Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
            if ($custinfo) {
                // if ($custinfo['pr_user'] != Session::get('username')) {
                //     $res['code'] = 0;
                //     $res['msg'] = "该客户在" . $custinfo['pr_user'] . "业务员下";
                //     return $this->success($res);
                // }
                // 检查客户是否属于当前用户或协同人
                $currentUsername = Session::get('username');
                $currentAdminId = Session::get('aid');
                $isMyCustomer = ($custinfo['pr_user'] == $currentUsername);
                
                // 检查是否是协同人客户
                $isCollaboratorCustomer = false;
                if (!empty($custinfo['joint_person'])) {
                    $jp = $custinfo['joint_person'];
                    $jointPersonIds = [];
                    if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                        // JSON 数组格式
                        $tmp = json_decode($jp, true);
                        if (is_array($tmp)) {
                            $jointPersonIds = $tmp;
                        }
                    } else {
                        // 逗号分隔格式
                        $jointPersonIds = array_values(array_filter(explode(',', $jp)));
                    }
                    // 检查当前用户的 admin_id 是否在协同人列表中
                    if (in_array($currentAdminId, $jointPersonIds)) {
                        $isCollaboratorCustomer = true;
                    }
                }
                
                // 如果客户既不是我的客户，也不是协同人客户，则不允许添加订单
                if (!$canManageAllOrders && !$isMyCustomer && !$isCollaboratorCustomer) {
                    $res['code'] = 0;
                    $res['msg'] = "该客户不属于您的客户或协同人客户，无法添加订单";
                    $this->success($res);
                    return;
                }
                
                // 获取来源端口和询盘来源渠道（通过端口反查渠道）
                $res['source_port'] = '';
                $portName = ''; // 用于显示在消息中的端口名称
                $sourceName = ''; // 询盘来源渠道名称
                
                // 优先从 source_port 字段获取端口名称
                try {
                    $columns = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
                    if (!empty($columns) && isset($custinfo['source_port']) && !empty($custinfo['source_port'])) {
                        $res['source_port'] = $custinfo['source_port'];
                        $portName = $custinfo['source_port'];
                    }
                } catch (\Exception $e) {
                    // 忽略错误
                }
                
                // 如果 source_port 字段为空，尝试从 port_id 获取端口名称和渠道信息
                if (empty($portName) && !empty($custinfo['port_id'])) {
                    $portIds = array_filter(explode(',', $custinfo['port_id']));
                    if (!empty($portIds)) {
                        $firstPortId = trim($portIds[0]);
                        if ($firstPortId) {
                            // 查询端口信息，同时获取端口名称和渠道ID
                            $portInfo = Db::name('crm_inquiry_port')
                                ->where('id', $firstPortId)
                                ->field('port_name, inquiry_id')
                                ->find();
                            if ($portInfo) {
                                if (!empty($portInfo['port_name'])) {
                                    $portName = $portInfo['port_name'];
                                    $res['source_port'] = $portName;
                                }
                                // 通过端口的 inquiry_id 获取渠道名称
                                if (!empty($portInfo['inquiry_id'])) {
                                    $inquiryInfo = Db::name('crm_inquiry')
                                        ->where('id', $portInfo['inquiry_id'])
                                        ->field('inquiry_name')
                                        ->find();
                                    if ($inquiryInfo && !empty($inquiryInfo['inquiry_name'])) {
                                        $sourceName = $inquiryInfo['inquiry_name'];
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 如果通过端口没有获取到渠道名称，尝试从 inquiry_id 字段获取
                if (empty($sourceName) && !empty($custinfo['inquiry_id'])) {
                    $inquiryInfo = Db::name('crm_inquiry')
                        ->where('id', $custinfo['inquiry_id'])
                        ->field('inquiry_name')
                        ->find();
                    if ($inquiryInfo && !empty($inquiryInfo['inquiry_name'])) {
                        $sourceName = $inquiryInfo['inquiry_name'];
                    }
                }
                
                // 如果还是没有渠道名称，尝试从 kh_status 获取（兼容旧数据）
                if (empty($sourceName)) {
                    $khStatusValue = $custinfo['kh_status'] ?? '';
                    if (!empty($khStatusValue)) {
                        // 先尝试作为 ID 查找
                        if (is_numeric($khStatusValue)) {
                            $inquiryInfo = Db::name('crm_inquiry')->where('id', $khStatusValue)->find();
                            if ($inquiryInfo) {
                                $sourceName = $inquiryInfo['inquiry_name'];
                            } else {
                                // 如果 crm_inquiry 表中找不到，尝试从 crm_client_status 表查找（兼容旧数据）
                                $statusInfo = Db::name('crm_client_status')->where('id', $khStatusValue)->find();
                                if ($statusInfo) {
                                    $sourceName = $statusInfo['status_name'];
                                }
                            }
                        } else {
                            // 如果已经是名称，先尝试从 crm_inquiry 表验证
                            $inquiryInfo = Db::name('crm_inquiry')->where('inquiry_name', $khStatusValue)->find();
                            if ($inquiryInfo) {
                                $sourceName = $inquiryInfo['inquiry_name'];
                            } else {
                                // 如果 crm_inquiry 表中找不到，直接使用原值（可能是 crm_client_status 的名称，兼容旧数据）
                                $sourceName = $khStatusValue;
                            }
                        }
                    }
                }
                
                // 如果还是没有渠道名称，使用默认值
                if (empty($sourceName)) {
                    $sourceName = '未设置';
                }
                
                // 如果还是没有端口名称，使用默认值
                if (empty($portName)) {
                    $portName = '未设置';
                }
                
                $res['code'] = 1;
                $res['leads_id'] = (int)$custinfo['id'];
                $titleCheck = OrderService::checkClientPhonePositionTitleComplete($custinfo['id']);
                $res['position_title_completed'] = !empty($titleCheck['ok']) ? 1 : 0;
                $res['custname'] = $custinfo['kh_name'];
                $res['kh_username'] = $custinfo['kh_username'];
                $res['source'] = $sourceName;  // 返回来源名称（通过端口反查的渠道）
                $res['pr_user'] = $custinfo['pr_user'];
                $res['country'] = $custinfo['xs_area'];
                $res['oper_user'] = $custinfo['oper_user'];
                
                // 获取原负责人的 admin_id
                $prUserId = 0;
                if (!empty($custinfo['pr_user'])) {
                    $prUserAdminInfo = Db::name('admin')->where('username', $custinfo['pr_user'])->field('admin_id')->find();
                    if ($prUserAdminInfo && !empty($prUserAdminInfo['admin_id'])) {
                        $prUserId = $prUserAdminInfo['admin_id'];
                    }
                }
                $res['pr_user_id'] = $prUserId;
                
                // 返回当前登录人是否是协同人
                $res['is_login_collaborator'] = $isCollaboratorCustomer ? 1 : 0;
                
                // 获取协同人（joint_person）字段，解析为数组格式
                $jointPersonIds = [];
                if (!empty($custinfo['joint_person'])) {
                    $jp = $custinfo['joint_person'];
                    if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                        // JSON 数组格式
                        $tmp = json_decode($jp, true);
                        if (is_array($tmp)) {
                            $jointPersonIds = $tmp;
                        }
                    } else {
                        // 逗号分隔格式
                        $jointPersonIds = array_values(array_filter(explode(',', $jp)));
                    }
                }
                $res['joint_person'] = $jointPersonIds;
                
                // 获取团队名称（通过负责人 pr_user 查找）
                $teamName = '';
                if (!empty($custinfo['pr_user'])) {
                    $adminInfo = Db::name('admin')->where('username', $custinfo['pr_user'])->field('team_name')->find();
                    if ($adminInfo && !empty($adminInfo['team_name'])) {
                        $teamName = $adminInfo['team_name'];
                    }
                }
                $res['team_name'] = $teamName;
                
                // 无论客户是否成交，都不返回历史订单产品信息，让用户手动选择产品（创建新订单）
                $isSuccess = ($custinfo['issuccess'] == 1);
                $res['is_success'] = $isSuccess; // 标记客户是否已成交
                
                // 始终返回空的历史产品数组，不自动填充历史产品信息
                $res['history_products'] = [];
                
                // 构建提示信息（将"所属运营"改为"所属端口"）
                if ($isSuccess) {
                    $res['msg'] = "【该客户已成交，将创建新订单】客户名称:" . $custinfo['kh_name'] . "询盘来源：" . $sourceName . ",所属业务员:" . $custinfo['pr_user'] . ",所属端口:" . $portName;
                } else {
                    $res['msg'] = "客户名称:" . $custinfo['kh_name'] . "询盘来源：" . $sourceName . ",所属业务员:" . $custinfo['pr_user'] . ",所属端口:" . $portName;
                }
            } else {
                $res['code'] = 0;
                $res['msg'] = "该客户信息没用找到";
            }
        }


        $this->success($res);
    }

    /**
     * 接口：校验客户电话职位身份完整性
     * 入参：client_id（crm_leads.id）
     * 返回：
     * {
     *   code: 1/0,
     *   msg: '',
     *   data: { can_submit: true/false }
     * }
     */
    public function checkClientPhoneTitles()
    {
        $clientId = (int)Request::param('client_id/d', 0);
        if ($clientId <= 0) {
            return json([
                'code' => 0,
                'msg' => 'client_id不能为空',
                'data' => [
                    'can_submit' => false,
                ],
            ]);
        }

        $checkResult = OrderService::checkClientPhonePositionTitleComplete($clientId);
        return json([
            'code' => !empty($checkResult['ok']) ? 1 : 0,
            'msg' => (string)($checkResult['message'] ?? ''),
            'data' => [
                'can_submit' => !empty($checkResult['ok']),
            ],
        ]);
    }

    /**
     * AJAX：根据联系方式检查是否必须返单（添加/编辑页共用）
     *
     * 入参：
     * - contact: 联系方式
     * - order_id: 当前订单ID（编辑页传，排除自身）
     *
     * 返回：
     * {
     *   code: 1,
     *   msg: '',
     *   data: {...}
     * }
     */
    public function checkReturnOrderRule()
    {
        $contact = trim((string)Request::param('contact', ''));
        $orderId = (int)Request::param('order_id/d', 0);
        if ($contact === '') {
            return json([
                'code' => 1,
                'msg' => '联系方式为空',
                'data' => [
                    'must_return' => false,
                    'leads_id' => 0,
                    'phones' => [],
                    'matched_contact' => '',
                    'suggest_source_name' => '返单',
                    'suggest_port_name' => '',
                    'suggest_source_id' => 0,
                    'suggest_port_id' => 0,
                    'message' => '联系方式为空，无法判定返单规则',
                ],
            ]);
        }

        $rule = OrderService::getReturnOrderRuleByContact($contact, $orderId);
        return json([
            'code' => 1,
            'msg' => (string)($rule['message'] ?? ''),
            'data' => $rule,
        ]);
    }

    // 根据 pr_user 获取团队名称
    public function getTeamByPrUser()
    {
        $prUser = Request::param('pr_user', '');
        if (empty($prUser)) {
            return json(['code' => -1, 'msg' => '']);
        }
        
        // 从 admin 表查询团队名称
        $adminInfo = Db::name('admin')->where('username', $prUser)->field('team_name')->find();
        $teamName = '';
        if ($adminInfo && !empty($adminInfo['team_name'])) {
            $teamName = $adminInfo['team_name'];
        } else {
            // fallback: 如果查不到，尝试从 session 获取
            $teamName = Session::get('team_name') ?: '';
        }
        
        return json(['code' => 0, 'msg' => $teamName]);
    }


    //编辑客户第3版
    public function edit()
    {
        if (request()->isPost()) {
            // 获取订单ID
            $id = Request::param('id/d');
            if (!$id) {
                return json(['code' => -200, 'msg' => '缺少订单ID参数']);
            }

            // ====== 验证客户是否属于当前用户或协同人（与新增逻辑保持一致） ======
            $contact = Request::param('contact');
            $leadsId = null;
            $custinfo = null;
            $adminInfo = \app\admin\model\Admin::getMyInfo();
            $canManageAllOrders = OrderService::canManageAllOrders($adminInfo);
            if (!empty($contact)) {
                $currentUsername = Session::get('username');
                $currentAdminId = Session::get('aid');
                $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($contact) {
                    $_contact = trim(preg_replace('/[+\-\s]/', '', $contact));
                    $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$contact}'")
                        ->whereOr('contact_value', $contact);
                    if ($contact != $_contact) {
                        $query->whereOr('contact_value', $_contact)
                            ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_contact}'");
                    }
                })->find();

                if ($coninfo) {
                    $custinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
                    if ($custinfo) {
                        $leadsId = $custinfo['id'];
                        $isMyCustomer = ($custinfo['pr_user'] == $currentUsername);

                        $isCollaboratorCustomer = false;
                        if (!empty($custinfo['joint_person'])) {
                            $jp = $custinfo['joint_person'];
                            $jointPersonIds = [];
                            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                                $tmp = json_decode($jp, true);
                                if (is_array($tmp)) {
                                    $jointPersonIds = $tmp;
                                }
                            } else {
                                $jointPersonIds = array_values(array_filter(explode(',', $jp)));
                            }
                            if (in_array($currentAdminId, $jointPersonIds)) {
                                $isCollaboratorCustomer = true;
                            }
                        }

                        if (!$canManageAllOrders && !$isMyCustomer && !$isCollaboratorCustomer) {
                            return fail('该客户不属于您的客户或协同人客户，无法添加订单');
                        }

                        // 编辑订单保存前：客户电话职位身份必须完整（后端兜底）
                        $checkResult = OrderService::checkClientPhonePositionTitleComplete($leadsId);
                        if (!$checkResult['ok']) {
                            return json(['code' => 0, 'msg' => '客户电话职位身份未完善']);
                        }
                    }
                }
            }
            // ====== 读取并整理主表字段 ======
            $data = [];
            $data['contact']          = Request::param('contact');        // 客户联系方式
            $data['cname']            = Request::param('cname', '');          // 客户名称

            if (empty($data['cname']) && $custinfo && !empty($custinfo['kh_name'])) {
                $data['cname'] = $custinfo['kh_name'];
            }

            if (empty($data['cname']) && !empty($contact)) {
                $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($contact) {
                    $_contact = trim(preg_replace('/[+\-\s]/', '', $contact));
                    $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$contact}'")
                        ->whereOr('contact_value', $contact);
                    if ($contact != $_contact) {
                        $query->whereOr('contact_value', $_contact)
                            ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_contact}'");
                    }
                })->find();

                if ($coninfo && !empty($coninfo['leads_id'])) {
                    $tempCustinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
                    if ($tempCustinfo && !empty($tempCustinfo['kh_name'])) {
                        $data['cname'] = $tempCustinfo['kh_name'];
                    }
                }
            }

            if (empty($data['cname'])) {
                return json(['code' => -200, 'msg' => '客户名称不能为空，请先输入联系方式并验证客户信息']);
            }

            // 用户属性：0=公司，1=个人
            $data['customer_type_flag'] = Request::param('customer_type_flag', 0);
            $data['customer_type_flag'] = in_array($data['customer_type_flag'], ['0', '1']) ? (int)$data['customer_type_flag'] : 0;
            
            // 客户公司：如果是公司（0）则必填，如果是个人（1）则可以为空
            $clientCompany = Request::param('client_company', '');
            if ($data['customer_type_flag'] == 0) {
                // 公司类型，客户公司必填
                $clientCompany = trim($clientCompany);
                if ($clientCompany === '') {
                    return json(['code' => 0, 'msg' => '客户公司名称不能为空']);
                }
                // 校验：允许中文 + 中文括号（），且不少于2个字符
                if (!preg_match('/^[\x{4e00}-\x{9fa5}（）]{2,}$/u', $clientCompany)) {
                    return json(['code' => 0, 'msg' => '客户公司名称只能填写中文（可包含中文括号），且不少于2个字']);
                }
                $data['client_company'] = $clientCompany;
            } else {
                // 个人类型，客户公司可以为空
                $data['client_company'] = '';
            }
            
            $data['province']         = Request::param('province', '');
            $data['city']             = Request::param('city', '');
            $data['country']          = Request::param('country');        // 发货地址
            $data['customer_type']    = Request::param('customer_type');  // 客户性质
            $data['source']           = Request::param('source');         // 询盘来源（运营渠道，存储为文字）
            $data['bank_account']     = Request::param('bank_account');  // 收款账户 ID (as string)
            
            // 【收款账户快照模式】根据 bank_account ID 查询账户名称并更新快照字段
            // 先查出当前订单原来的快照字段做兜底
            $originalOrder = Db::name('crm_client_order')->where('id', $id)->field('bank_account_name')->find();
            $originalBankAccountName = $originalOrder['bank_account_name'] ?? '';
            
            if (!empty($data['bank_account'])) {
                // 根据本次提交的 bank_account 查询账户名称
                $data['bank_account_name'] = $this->resolveBankAccountName($data['bank_account']);
                // 若查不到（账户被删除/异常）：不要把快照写空，保留原来的 bank_account_name
                if (empty($data['bank_account_name']) && !empty($originalBankAccountName)) {
                    $data['bank_account_name'] = $originalBankAccountName;
                }
            } else {
                // 如果 bank_account 为空（用户清空）：bank_account_name 也置空
                $data['bank_account_name'] = '';
            }
            
            $data['pr_user']          = Request::param('pr_user') ?: Session::get('username'); // 客户负责人（默认当前用户）
            $data['team_name']        = Request::param('team_name');      // 团队名称
            
            // 处理运营端口：优先识别端口ID并反查名称；查不到时按名称兜底
            $sourcePortInput = trim((string)Request::param('source_port', ''));
            $sourcePortName = '';
            $data['source_port'] = '';
            if ($sourcePortInput !== '') {
                $isNumericId = ctype_digit($sourcePortInput) || (is_numeric($sourcePortInput) && (int)$sourcePortInput > 0);
                if ($isNumericId) {
                    $portInfo = Db::name('crm_inquiry_port')
                        ->where('id', (int)$sourcePortInput)
                        ->field('id, port_name')
                        ->find();
                    if (!empty($portInfo['port_name'])) {
                        $sourcePortName = trim((string)$portInfo['port_name']);
                    }
                }
                if ($sourcePortName === '') {
                    $sourcePortName = mb_substr($sourcePortInput, 0, 100, 'UTF-8');
                }
                $data['source_port'] = $sourcePortName;
            }

            // 编辑保存返单强规则（排除当前订单自身）
            $returnRuleCheck = OrderService::validateReturnOrderSelection(
                $data['contact'],
                $data['source'],
                $sourcePortInput,
                $id,
                $sourcePortName
            );
            if (empty($returnRuleCheck['ok'])) {
                return json([
                    'code' => 0,
                    'msg' => (string)($returnRuleCheck['message'] ?? '该客户已有审核通过订单，本次订单必须选择返单来源和对应返单运营端口，请勿选择非返单来源。'),
                ]);
            }
            // 最终入库前以后端规则统一修正 source / order_type
            $data = OrderService::applyOrderTypeByReturnRule($data, $id);
            
            $data['order_time']       = Request::param('order_time');     // 成交时间
            $data['shipping_cost']    = Request::param('shipping_cost');  // 估算运费
            // 票种性质（普票、专票、不开票）- 验证并保存
            $invoiceType = Request::param('invoice_type', '');
            if (in_array($invoiceType, ['普票', '专票', '不开票'])) {
                $data['invoice_type'] = $invoiceType;
            } else {
                $data['invoice_type'] = ''; // 如果值不正确，设为空
            }
            $data['invoice_amount']   = Request::param('invoice_amount'); // 开票金额
            $data['tax_amount']       = Request::param('tax_amount');     // 税费金额
            $data['debugging_cost']   = Request::param('debugging_cost'); // 调试费
            $data['sales_commission'] = Request::param('sales_commission'); // 佣金
            $data['split_remarks']    = Request::param('split_remarks');  // 分成备注
            $data['amount_received']  = Request::param('amount_received'); // 已收款金额
            //$data['remark']           = Request::param('remark');         // 备注
            
            // ====== 编辑场景：读取原订单并统一重算利润 + 凭证校验 ======
            $originalOrder = Db::name('crm_client_order')
                ->where('id', $id)
                ->field('wechat_receipt_image, inquiry_assign_image')
                ->find();
            if (!$originalOrder) {
                $originalOrder = [];
            }

            // 口径：purchase_price = 【进价合计】（整行，非单价），统一走 Service
            $profitCalcParams = [
                'qty' => Request::param('qty/a'),
                'unit_price' => Request::param('unit_price/a'),
                'purchase_price' => Request::param('purchase_price/a'),
                'shipping_cost' => $data['shipping_cost'],
                'tax_amount' => $data['tax_amount'],
                'debugging_cost' => $data['debugging_cost'],
                'sales_commission' => $data['sales_commission'],
            ];

            $MAX_WECHAT_RECEIPT_IMAGES = 10;
            $MAX_INQUIRY_ASSIGN_IMAGES = 10;

            $clearWechatReceipt = (int)Request::param('clear_wechat_receipt_image', 0);
            $clearInquiryAssign = (int)Request::param('clear_inquiry_assign_image', 0);
            $hasWechatInput = request()->has('wechat_receipt_image');
            $hasInquiryInput = request()->has('inquiry_assign_image');

            $voucherValidationParams = $profitCalcParams;
            $voucherValidationParams['clear_wechat_receipt_image'] = $clearWechatReceipt;
            $voucherValidationParams['clear_inquiry_assign_image'] = $clearInquiryAssign;

            if ($clearWechatReceipt === 1) {
                $data['wechat_receipt_image'] = '';
            } elseif ($hasWechatInput) {
                $wechatReceiptRaw = Request::param('wechat_receipt_image', '');
                $wechatReceiptParsed = OrderService::parseImageList($wechatReceiptRaw);
                if (count($wechatReceiptParsed) > $MAX_WECHAT_RECEIPT_IMAGES) {
                    return json(['code' => 0, 'msg' => '微信沟通凭证图片数量不能超过 ' . $MAX_WECHAT_RECEIPT_IMAGES . ' 张']);
                }
                $wechatReceiptUrls = OrderService::normalizeVoucherImages($wechatReceiptParsed, $MAX_WECHAT_RECEIPT_IMAGES);
                if (!empty($wechatReceiptUrls)) {
                    $data['wechat_receipt_image'] = json_encode($wechatReceiptUrls, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                }
                $voucherValidationParams['wechat_receipt_image'] = $wechatReceiptRaw;
            }

            if ($clearInquiryAssign === 1) {
                $data['inquiry_assign_image'] = '';
            } elseif ($hasInquiryInput) {
                $inquiryAssignRaw = Request::param('inquiry_assign_image', '');
                $inquiryAssignParsed = OrderService::parseImageList($inquiryAssignRaw);
                if (count($inquiryAssignParsed) > $MAX_INQUIRY_ASSIGN_IMAGES) {
                    return json(['code' => 0, 'msg' => '询盘来源凭证图片数量不能超过 ' . $MAX_INQUIRY_ASSIGN_IMAGES . ' 张']);
                }
                $inquiryAssignUrls = OrderService::normalizeVoucherImages($inquiryAssignParsed, $MAX_INQUIRY_ASSIGN_IMAGES);
                if (!empty($inquiryAssignUrls)) {
                    $data['inquiry_assign_image'] = json_encode($inquiryAssignUrls, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                }
                $voucherValidationParams['inquiry_assign_image'] = $inquiryAssignRaw;
            }

            $voucherValidation = OrderService::validateVoucherRequirement($voucherValidationParams, $originalOrder);
            if (empty($voucherValidation['ok'])) {
                return json(['code' => 0, 'msg' => $voucherValidation['message']]);
            }
            $data['ut_time']          = date("Y-m-d H:i:s");              // 更新操作时间

            // 解析协同人 joint_person 字段（支持数组/JSON/逗号分隔字符串）
            $jpRaw = Request::param('joint_person');
            $jpIds = [];
            if (is_array($jpRaw)) {
                $jpIds = $jpRaw;
            } else if (is_string($jpRaw)) {
                $jpRaw = trim($jpRaw);
                if ($jpRaw !== '') {
                    if ($jpRaw[0] === '[') {
                        // JSON 字符串
                        $tmp = json_decode($jpRaw, true);
                        if (is_array($tmp)) $jpIds = $tmp;
                    } else {
                        // 逗号分隔字符串
                        $jpIds = explode(',', $jpRaw);
                    }
                }
            }
            // 保留数字字符并去重
            $jpIds = array_values(array_unique(array_filter(array_map(function ($v) {
                return preg_replace('/\D/', '', (string)$v);
            }, $jpIds), function ($v) {
                return $v !== '';
            })));
            $jpStr = implode(',', $jpIds);
            // 若协同人超出字段长度限制则报错
            if (strlen($jpStr) > 30) {
                return json(['code' => -200, 'msg' => '协同人选择过多，超出存储限制']);
            }
            $data['joint_person'] = $jpStr;

            // ====== 获取并处理明细表字段（产品明细多行） ======
            $productIds     = Request::param('product_name/a');    // ★ 产品ID数组（对应每行产品）
            $managerIds     = Request::param('product_manager/a'); // ★ 产品经理ID数组（对应每行产品）
            $specModels     = Request::param('spec_model/a');
            $units          = Request::param('unit/a');
            $qtys           = Request::param('qty/a');
            $unitPrices     = Request::param('unit_price/a');
            $totalPrices    = Request::param('total_price/a');
            $purchasePrices = Request::param('purchase_price/a');
            $subProfits     = Request::param('sub_profit/a');
            $itemRemarks    = Request::param('item_remark/a');

            // 【订单快照改造】批量查询产品信息，构建产品名称、供应商ID、供应商名称的映射
            $idArr = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $pid) {
                    $pid = (int)$pid;
                    if ($pid > 0) $idArr[] = $pid;
                }
                $idArr = array_values(array_unique($idArr));
            }
            $prodMap = [];      // product_id => product_name
            $supIdMap = [];     // product_id => category_id (供应商ID)
            $supNameMap = [];   // product_id => category_name (供应商名称)
            if (!empty($idArr)) {
                // 从产品表获取名称和分类，用于快照保存
                // 注意：这里不过滤 status，因为历史订单可能引用已删除的产品，需要保留产品名称
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, p.category_id, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    $prodMap[$r['id']] = $r['product_name'];
                    $supIdMap[$r['id']] = $r['category_id'] ?? 0;
                    $supNameMap[$r['id']] = $r['category_name'] ?? '';
                }
                
                // 【兼容校验】如果某些产品ID查询不到（异常/被删/数据脏），直接返回错误
                $missingPids = [];
                foreach ($idArr as $pid) {
                    if (!isset($prodMap[$pid])) {
                        $missingPids[] = $pid;
                    }
                }
                if (!empty($missingPids)) {
                    return json(['code' => -200, 'msg' => '产品ID ' . implode(', ', $missingPids) . ' 不存在或已删除，无法保存']);
                }
            }

            // 统一由 Service 构造明细行 + 主表金额（唯一口径，禁止 Controller 自己再算一遍）
            $saveBundle = OrderService::buildOrderSaveData(
                [
                    'product_ids'      => $productIds,
                    'manager_ids'      => $managerIds,
                    'spec_models'      => $specModels,
                    'units'            => $units,
                    'qty'              => $qtys,
                    'unit_price'       => $unitPrices,
                    'purchase_price'   => $purchasePrices,
                    'item_remarks'     => $itemRemarks,
                    'shipping_cost'    => $data['shipping_cost'],
                    'tax_amount'       => $data['tax_amount'],
                    'debugging_cost'   => $data['debugging_cost'],
                    'sales_commission' => $data['sales_commission'],
                ],
                $prodMap,
                $supIdMap,
                $supNameMap,
                $id
            );
            $itemsData = $saveBundle['items'];

            // ★ 防御性补丁：无论 $data 之前从哪里取过值，在落库前强制剥掉
            // profit / margin_rate / money，杜绝前端 / 其他开发者后续改动带入污染值。
            unset($data['profit'], $data['margin_rate'], $data['money']);

            // 主表金额字段一律以 Service 计算结果为准（禁止信任前端 profit / margin_rate / money）
            $data['money']       = $saveBundle['money'];
            $data['profit']      = $saveBundle['profit'];
            $data['margin_rate'] = $saveBundle['margin_rate'];

            // 更新主表产品名称摘要（存入第一个产品名称，多个则加"等"字样）
            if ($saveBundle['product_name_summary'] !== '') {
                $data['product_name'] = $saveBundle['product_name_summary'];
            }

            // ====== 写入数据库（使用事务处理） ======
            Db::startTrans();
            try {
                // 更新订单主表数据
                $resMain = Db::name('crm_client_order')->where('id', $id)->update($data);
                if ($resMain === false) {
                    throw new \Exception('主订单更新失败');
                }
                // 清除旧的明细行记录
                Db::name('crm_order_item')->where('order_id', $id)->delete();
                // 批量插入新的明细行数据
                if (!empty($itemsData)) {
                    $resItems = Db::name('crm_order_item')->insertAll($itemsData);
                    if ($resItems === false || $resItems != count($itemsData)) {
                        throw new \Exception('订单明细更新失败');
                    }
                }
                Db::commit();

                // ====== 草稿来源：保存成功后自动提交审核（check_status=1） ======
                $from = Request::param('from', '');
                if ($from === 'draft') {
                    $orderRow = Db::name('crm_client_order')->where('id', $id)->field('check_status')->find();
                    if ($orderRow && (int)$orderRow['check_status'] === 0) {
                        // 最小兜底校验：成交时间/客户/产品/金额
                        $orderTime = trim((string)($data['order_time'] ?? ''));
                        $cname = trim((string)($data['cname'] ?? ''));
                        $money = isset($data['money']) ? (float)$data['money'] : 0;
                        $hasProduct = !empty($productIds) && is_array($productIds) && count(array_filter($productIds, function ($pid) { return (int)$pid > 0; })) > 0;
                        if ($orderTime === '' || $cname === '' || $money <= 0 || !$hasProduct) {
                            return json(['code' => 1, 'msg' => '请先完善必填项后再提交审核']);
                        }
                        Db::name('crm_client_order')->where('id', $id)->update([
                            'check_status' => 1,
                            'status'       => '待审核',
                            'ut_time'      => date('Y-m-d H:i:s'),
                        ]);
                        return json(['code' => 0, 'msg' => '保存成功，已提交审核']);
                    }
                }

                return json(['code' => 0, 'msg' => '编辑成功！']);
            } catch (\Exception $e) {
                Db::rollback();
                return json(['code' => -200, 'msg' => '编辑失败！' . $e->getMessage()]);
            }
        }

        // ====== GET 请求：加载编辑页面 ======
        $orderId = Request::param('id/d');
        $order = Db::name('crm_client_order')->where('id', $orderId)->find();
        if (!$order) {
            $this->error('订单不存在或已删除');
        }
        // 读取该订单的所有产品明细行
        $items = Db::name('crm_order_item')->where('order_id', $orderId)->select();

        // 准备下拉选项数据（团队列表、来源列表、客户性质列表、运营人员列表等）
        $teamList   = $this->getTeamList();
        
        // ====== 解析收款账户展示名（与 details() 方法保持一致） ======
        // 优先使用 crm_receive_account 的最新账户名，而不是订单快照
        // 先保存原始快照名，避免在更新后被覆盖
        $originalSnapshotName = $order['bank_account_name'] ?? '';
        $currentAccountInfoForDisplay = null;
        if (!empty($order['bank_account'])) {
            $currentAccountId = $order['bank_account'];
            // 按 ID 查询账户表（不加 is_deleted 条件），获取最新账户信息
            $currentAccountInfoForDisplay = Db::name('crm_receive_account')
                ->where('id', $currentAccountId)
                ->field('id, account, is_deleted')
                ->find();
            
            $displayName = '';
            if ($currentAccountInfoForDisplay) {
                // 查到账户记录：使用最新的账户名，并根据 is_deleted 决定是否追加"（已删除）"
                $displayName = $currentAccountInfoForDisplay['account'] ?? '';
                if (isset($currentAccountInfoForDisplay['is_deleted']) && $currentAccountInfoForDisplay['is_deleted'] == 1) {
                    $displayName .= '（已删除）';
                }
            } else {
                // 查不到账户记录（被物理删除/异常）：回退使用订单快照名，并显示"（已删除）"
                $snapshotName = $originalSnapshotName;
                // 去掉可能已有的"（已删除）"标记，避免重复
                $snapshotName = str_replace('（已删除）', '', $snapshotName);
                $displayName = $snapshotName . '（已删除）';
            }
            
            // 覆盖订单的 bank_account_name 为计算后的展示名，供模板使用
            $order['bank_account_name'] = $displayName;
        }
        
        // ====== 构建 accountList（与 details() 方法保持一致） ======
        // 【收款账户快照模式】只显示未删除的账户（is_deleted=0）
        $accountList = Db::name('crm_receive_account')->where('is_deleted', 0)->field('id, account')->select();
        
        // 【收款账户快照模式】兼容已删除账户：如果当前订单的 bank_account 对应账户不在列表中，补充到列表头部
        if (!empty($order['bank_account'])) {
            $currentAccountId = $order['bank_account'];
            $foundInList = false;
            foreach ($accountList as $acc) {
                if ($acc['id'] == $currentAccountId) {
                    $foundInList = true;
                    break;
                }
            }
            // 如果当前订单的账户不在列表中（因为 is_deleted=1 或查不到），需要补充一个选项
            if (!$foundInList) {
                $displayAccountName = '';
                $statusSuffix = '';
                
                // 复用上面查询的结果，避免重复查询
                if ($currentAccountInfoForDisplay) {
                    // 若能按 ID 查到：用其最新 account 名，并按 is_deleted 决定是否加"（已删除）"
                    $displayAccountName = $currentAccountInfoForDisplay['account'] ?? '';
                    if (isset($currentAccountInfoForDisplay['is_deleted']) && $currentAccountInfoForDisplay['is_deleted'] == 1) {
                        $statusSuffix = '（已删除）';
                    }
                } else {
                    // 若查不到：用订单原始快照 bank_account_name（去掉可能已有的标记） + "（已删除）"
                    $snapshotName = $originalSnapshotName;
                    // 去掉可能已有的"（已删除）"标记，避免重复
                    $displayAccountName = str_replace('（已删除）', '', $snapshotName);
                    $statusSuffix = '（已删除）';
                }
                
                // 如果最终显示名称为空，使用订单原始快照名作为最后兜底
                if (empty($displayAccountName)) {
                    $snapshotName = $originalSnapshotName;
                    // 去掉可能已有的"（已删除）"标记，重新追加
                    $displayAccountName = str_replace('（已删除）', '', $snapshotName);
                    $statusSuffix = '（已删除）';
                }
                
                // 将该选项添加到列表头部，保证下拉框能正常选中
                if (!empty($displayAccountName)) {
                    array_unshift($accountList, [
                        'id' => $currentAccountId,
                        'account' => $displayAccountName . $statusSuffix
                    ]);
                }
            }
        }
        
        $this->assign('accountList', $accountList);
        $this->assign('teamList', $teamList);
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        // 当前登录用户信息
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $this->assign('username', $currentAdmin['username'] ?? Session::get('username'));
        $this->assign('team_name', $currentAdmin['team_name'] ?? Session::get('team_name'));

        // 从 crm_inquiry 表获取询盘来源列表（客户渠道）
        $inquiryWhere = [];
        if ($currentAdmin['org'] && strpos($currentAdmin['org'], 'admin') === false) {
            $inquiryWhere[] = $this->getOrgWhere($currentAdmin['org']);
        }
        $inquiryQuery = Db::name('crm_inquiry');
        if (!empty($inquiryWhere)) {
            $inquiryQuery->where($inquiryWhere);
        }
        $inquiryList = $inquiryQuery
            ->where('status', '=', 0)
            ->field('id, inquiry_name')
            ->order('inquiry_name', 'asc')
            ->select();
        $sourceList = array_column($inquiryList, 'inquiry_name');
        $this->assign('sourceList', $sourceList);

        // 获取运营人员列表（以及按询盘来源分类的映射，用于联动下拉）
        $yyData = $this->getYyList();
        $operUserList = $yyData['_yyList'];
        $this->assign('operUserList', $operUserList);
        $this->assign('yyList', json_encode($yyData['yyList'], JSON_UNESCAPED_UNICODE));

        // 产品列表（含分类名）。无组织限制时查询所有产品
        $productQuery = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            // 有组织限制时构造过滤条件
            $productQuery->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        // 使用新的软删除字段：只获取未删除的产品和供应商（is_deleted = 0）
        $productQuery->where([
            'p.is_deleted' => 0,
            'c.is_deleted' => 0,
        ]);
        $productRows = $productQuery
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        
        // 处理已删除产品的回显：如果当前订单绑定的产品已删除，需要追加到列表中
        if (isset($items) && !empty($items)) {
            $existingProductIds = [];
            foreach ($items as $item) {
                if (!empty($item['product_id'])) {
                    $existingProductIds[] = (int)$item['product_id'];
                }
            }
            $existingProductIds = array_unique($existingProductIds);
            
            // 检查订单中的产品是否在未删除列表中
            if (!empty($existingProductIds)) {
                // 获取未删除列表中的产品ID
                $activeProductIds = array_column($productRows, 'id');
                
                // 查询订单中所有产品（包括已删除的），不加 is_deleted 限制
                $allProducts = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $existingProductIds)
                    ->field('p.id, p.product_name, c.category_name, p.is_deleted as p_deleted, c.is_deleted as c_deleted')
                    ->select();
                
                // 找出已删除的产品（is_deleted = 1），追加到列表中用于回显
                foreach ($allProducts as $product) {
                    $productId = $product['id'];
                    // 如果产品不在未删除列表中，说明已被删除
                    if (!in_array($productId, $activeProductIds)) {
                        // 标记为已删除，并在分类名称中添加标识
                        $categoryName = $product['category_name'] ?: '无';
                        if (isset($product['p_deleted']) && $product['p_deleted'] == 1) {
                            $categoryName = '【已删除】' . $categoryName;
                        } elseif (isset($product['c_deleted']) && $product['c_deleted'] == 1) {
                            $categoryName = '【已删除】' . $categoryName;
                        }
                        $product['category_name'] = $categoryName;
                        $product['is_deleted'] = true; // 标记为已删除
                        // 将已删除的产品插入到列表开头，确保能回显
                        array_unshift($productRows, $product);
                    }
                }
                
                // 对于在订单中存在但查询不到的产品（可能已被物理删除），从订单明细中获取产品名称
                $foundProductIds = array_column($allProducts, 'id');
                foreach ($items as $item) {
                    if (!empty($item['product_id']) && !in_array($item['product_id'], $foundProductIds)) {
                        // 产品不存在于产品表中，但从订单明细中获取信息
                        if (!empty($item['product_name'])) {
                            $deletedProduct = [
                                'id' => $item['product_id'],
                                'product_name' => $item['product_name'],
                                'category_name' => '【已删除】无',
                                'is_deleted' => true
                            ];
                            // 将已删除的产品插入到列表开头
                            array_unshift($productRows, $deletedProduct);
                        }
                    }
                }
            }
        }
        
        $this->assign('productList', $productRows);

        // 协同人列表（xm-select 数据格式）
        $teamName = $currentAdmin['team_name'] ?? Session::get('team_name') ?: '';
        $adminList = Db::name('admin')
            ->where('group_id', '<>', 1)
            ->whereIn('group_id', [10, 11, 14, 17, 18, 19, 21, 22])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        $currentJpIds = [];
        if (!empty($order['joint_person'])) {
            $currentJpIds = explode(',', $order['joint_person']);
        }
        foreach ($adminList as $admin) {
            $item = ['name' => $admin['username'], 'value' => $admin['admin_id']];
            if (in_array($admin['admin_id'], $currentJpIds)) {
                $item['selected'] = true;  // 默认选中已有协同人
            }
            $collaboratorData[] = $item;
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        // 产品经理列表（group_id = 13/14）
        $extraManagerIds = []; // 李营，可为空数组 [] 表示不额外包含任何人
        $managerList = Db::name('admin')
            ->where(function($q) use ($extraManagerIds){
                $q->whereIn('group_id', [13, 14, 18, 19, 22]);
                if (!empty($extraManagerIds)) {
                    $q->whereOr('admin_id', 'in', $extraManagerIds);   // ✅ TP5.1 兼容
                }
            })
            ->field('admin_id, username')
            ->order('username', 'asc')
            ->select();
        
        $this->assign('managerList', $managerList);


        // 获取来源端口列表（按来源名称分组，与订单新增页一致）
        $shopList = [];
        foreach ($inquiryList as $inquiry) {
            $inquiryName = $inquiry['inquiry_name'];
            $inquiryId = $inquiry['id'];

            $ports = Db::name('crm_inquiry_port')
                ->where('inquiry_id', $inquiryId)
                ->where('status', '=', 0)
                ->field('id, port_name')
                ->order('port_name', 'asc')
                ->select();

            $shops = [];
            foreach ($ports as $port) {
                $shops[] = [
                    'id' => $port['id'],
                    'name' => $port['port_name']
                ];
            }

            if (!empty($shops)) {
                $shopList[$inquiryName] = $shops;
            }
        }
        $this->assign('shopList', json_encode($shopList, JSON_UNESCAPED_UNICODE));

        // 根据订单的 contact 字段，从 crm_leads 表获取 source_port 值
        $orderSourcePort = '';
        if (!empty($order['contact'])) {
            try {
                // 通过联系方式查找 crm_contacts 表
                $custphone = trim($order['contact']);
                $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($custphone) {
                    $_custphone = trim(preg_replace('/[+\-\s]/', '', $custphone));
                    $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$custphone}'")
                        ->whereOr('contact_value', $custphone);
                    if ($custphone != $_custphone) {
                        $query->whereOr('contact_value', $_custphone)
                            ->whereOrRaw("CONCAT(contact_extra, contact_value) = '{$_custphone}'");
                    }
                })->find();
                
                if ($coninfo && !empty($coninfo['leads_id'])) {
                    // 从 crm_leads 表获取 source_port
                    $custinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
                    if ($custinfo) {
                        // 检查 source_port 字段是否存在
                        $columns = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
                        if (!empty($columns) && isset($custinfo['source_port']) && !empty($custinfo['source_port'])) {
                            $orderSourcePort = $custinfo['source_port'];
                        }
                    }
                }
            } catch (\Exception $e) {
                // 忽略错误，保持为空
            }
        }
        
        // 如果订单表本身有 source_port 字段，优先使用订单表的（如果订单表字段存在且不为空）
        if (empty($orderSourcePort)) {
            try {
                $columns = Db::query("SHOW COLUMNS FROM `crm_client_order` LIKE 'source_port'");
                if (!empty($columns) && isset($order['source_port']) && !empty($order['source_port'])) {
                    $orderSourcePort = $order['source_port'];
                }
            } catch (\Exception $e) {
                // 忽略错误
            }
        }
        
        // 如果 source_port 是端口名称（文字），需要找到对应的端口ID以便前端下拉框能正确选中
        $orderSourcePortId = '';
        if (!empty($orderSourcePort)) {
            // 尝试通过端口名称查找端口ID
            $portInfo = Db::name('crm_inquiry_port')
                ->where('port_name', $orderSourcePort)
                ->field('id')
                ->find();
            if ($portInfo && !empty($portInfo['id'])) {
                $orderSourcePortId = $portInfo['id'];
            }
        }
        
        // 将 source_port 值（端口名称）和 source_port_id（端口ID，用于前端回显）添加到订单信息中
        $order['source_port'] = $orderSourcePort;  // 端口名称（文字）
        $order['source_port_id'] = $orderSourcePortId;  // 端口ID（用于前端下拉框选中）

        // 将订单主表和明细数据分配给模板
        $this->assign('orderInfo', $order);
        $this->assign('orderItems', $items);
        return $this->fetch('order/edit');
    }


    // 显示订单详情
    public function details()
    {
        if (request()->isPost()) {
            // 获取订单ID
            $id = Request::param('id/d');
            if (!$id) {
                return json(['code' => -200, 'msg' => '缺少订单ID参数']);
            }
            // ====== 读取并整理主表字段 ======
            $data = [];
            $data['contact']          = Request::param('contact');        // 客户联系方式
            $data['cname']            = Request::param('cname');          // 客户名称
            $data['client_company']   = Request::param('client_company'); // 客户公司
            $data['country']          = Request::param('country');        // 发货地址
            $data['customer_type']    = Request::param('customer_type');  // 客户性质
            $data['source']           = Request::param('source');         // 询盘来源
            $data['bank_account']     = Request::param('bank_account');  // 收款账户 ID (as string)
            // 【收款账户快照模式】根据 bank_account ID 查询账户名称并更新快照字段
            // 先获取原始订单数据，用于在查询失败时保留原值
            $originalOrder = Db::name('crm_client_order')->where('id', $id)->field('bank_account_name')->find();
            if (!empty($data['bank_account'])) {
                $data['bank_account_name'] = $this->resolveBankAccountName($data['bank_account']);
                // 如果查不到（例如账户被删除或异常），保留原值
                if (empty($data['bank_account_name']) && !empty($originalOrder['bank_account_name'])) {
                    $data['bank_account_name'] = $originalOrder['bank_account_name'];
                }
            } else {
                $data['bank_account_name'] = '';  // 如果为空，快照字段也置空
            }
            $data['pr_user']          = Request::param('pr_user') ?: Session::get('username'); // 客户负责人（默认当前用户）
            $data['oper_user']        = Request::param('oper_user');      // 运营人员
            $data['team_name']        = Request::param('team_name');      // 团队名称
            $data['order_time']       = Request::param('order_time');     // 成交时间
            $data['shipping_cost']    = Request::param('shipping_cost');  // 估算运费
            // 票种性质（普票、专票、不开票）- 验证并保存
            $invoiceType = Request::param('invoice_type', '');
            if (in_array($invoiceType, ['普票', '专票', '不开票'])) {
                $data['invoice_type'] = $invoiceType;
            } else {
                $data['invoice_type'] = ''; // 如果值不正确，设为空
            }
            $data['invoice_amount']   = Request::param('invoice_amount'); // 开票金额
            $data['tax_amount']       = Request::param('tax_amount');     // 税费金额
            $data['debugging_cost']   = Request::param('debugging_cost'); // 调试费
            $data['sales_commission'] = Request::param('sales_commission'); // 佣金
            $data['split_remarks']    = Request::param('split_remarks');  // 分成备注
            $data['amount_received']  = Request::param('amount_received'); // 已收款金额
            //$data['remark']           = Request::param('remark');         // 备注
            $data['ut_time']          = date("Y-m-d H:i:s");              // 更新操作时间

            // 解析协同人 joint_person 字段（支持数组/JSON/逗号分隔字符串）
            $jpRaw = Request::param('joint_person');
            $jpIds = [];
            if (is_array($jpRaw)) {
                $jpIds = $jpRaw;
            } else if (is_string($jpRaw)) {
                $jpRaw = trim($jpRaw);
                if ($jpRaw !== '') {
                    if ($jpRaw[0] === '[') {
                        // JSON 字符串
                        $tmp = json_decode($jpRaw, true);
                        if (is_array($tmp)) $jpIds = $tmp;
                    } else {
                        // 逗号分隔字符串
                        $jpIds = explode(',', $jpRaw);
                    }
                }
            }
            // 保留数字字符并去重
            $jpIds = array_values(array_unique(array_filter(array_map(function ($v) {
                return preg_replace('/\D/', '', (string)$v);
            }, $jpIds), function ($v) {
                return $v !== '';
            })));
            $jpStr = implode(',', $jpIds);
            // 若协同人超出字段长度限制则报错
            if (strlen($jpStr) > 255) {
                return json(['code' => -200, 'msg' => '协同人选择过多，超出存储限制']);
            }
            $data['joint_person'] = $jpStr;

            // ====== 获取并处理明细表字段（产品明细多行） ======
            $productIds     = Request::param('product_name/a');    // ★ 产品ID数组（对应每行产品）
            $managerIds     = Request::param('product_manager/a'); // ★ 产品经理ID数组（对应每行产品）
            $specModels     = Request::param('spec_model/a');
            $units          = Request::param('unit/a');
            $qtys           = Request::param('qty/a');
            $unitPrices     = Request::param('unit_price/a');
            $totalPrices    = Request::param('total_price/a');
            $purchasePrices = Request::param('purchase_price/a');
            $subProfits     = Request::param('sub_profit/a');
            $itemRemarks    = Request::param('item_remark/a');

            // 查询涉及的产品名称/供应商快照信息
            $idArr = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $pid) {
                    $pid = (int)$pid;
                    if ($pid > 0) $idArr[] = $pid;
                }
                $idArr = array_values(array_unique($idArr));
            }
            $prodMap = [];      // pid => product_name
            $supIdMap = [];     // pid => category_id
            $supNameMap = [];   // pid => category_name
            if (!empty($idArr)) {
                // 从产品表获取名称和分类，用于展示和计算；已删除产品不过滤 status
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, p.category_id, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    $prodMap[$r['id']]    = $r['product_name'];
                    $supIdMap[$r['id']]   = $r['category_id'] ?? 0;
                    $supNameMap[$r['id']] = $r['category_name'] ?? '';
                }

                // 如果某些产品ID查询不到（可能已被删除），尝试从订单明细表中获取产品名称
                foreach ($idArr as $pid) {
                    if (!isset($prodMap[$pid])) {
                        $item = Db::name('crm_order_item')
                            ->where('product_id', $pid)
                            ->where('product_name', '<>', '')
                            ->order('id desc')
                            ->field('product_name')
                            ->find();
                        if ($item && !empty($item['product_name'])) {
                            $prodMap[$pid] = $item['product_name'];
                        }
                    }
                }
            }

            // 统一由 Service 构造明细行 + 主表金额（唯一口径，禁止 Controller 自己再算一遍）
            $saveBundle = OrderService::buildOrderSaveData(
                [
                    'product_ids'      => $productIds,
                    'manager_ids'      => $managerIds,
                    'spec_models'      => $specModels,
                    'units'            => $units,
                    'qty'              => $qtys,
                    'unit_price'       => $unitPrices,
                    'purchase_price'   => $purchasePrices,
                    'item_remarks'     => $itemRemarks,
                    'shipping_cost'    => $data['shipping_cost'] ?? 0,
                    'tax_amount'       => $data['tax_amount'] ?? 0,
                    'debugging_cost'   => $data['debugging_cost'] ?? 0,
                    'sales_commission' => $data['sales_commission'] ?? 0,
                ],
                $prodMap,
                $supIdMap,
                $supNameMap,
                $id
            );
            $itemsData = $saveBundle['items'];

            // ★ 防御性补丁：无论 $data 之前从哪里取过值，在落库前强制剥掉
            // profit / margin_rate / money，杜绝前端 / 其他开发者后续改动带入污染值。
            unset($data['profit'], $data['margin_rate'], $data['money']);

            // 主表金额字段一律以 Service 计算结果为准（禁止信任前端 profit / margin_rate / money）
            $data['money']       = $saveBundle['money'];
            $data['profit']      = $saveBundle['profit'];
            $data['margin_rate'] = $saveBundle['margin_rate'];

            // 更新主表产品名称摘要
            if ($saveBundle['product_name_summary'] !== '') {
                $data['product_name'] = $saveBundle['product_name_summary'];
            }

            // ====== 写入数据库（使用事务处理） ======
            Db::startTrans();
            try {
                // 更新订单主表数据
                $resMain = Db::name('crm_client_order')->where('id', $id)->update($data);
                if ($resMain === false) {
                    throw new \Exception('主订单更新失败');
                }
                // 清除旧的明细行记录
                Db::name('crm_order_item')->where('order_id', $id)->delete();
                // 批量插入新的明细行数据
                if (!empty($itemsData)) {
                    $resItems = Db::name('crm_order_item')->insertAll($itemsData);
                    if ($resItems === false || $resItems != count($itemsData)) {
                        throw new \Exception('订单明细更新失败');
                    }
                }
                Db::commit();
                return json(['code' => 0, 'msg' => '编辑成功！']);
            } catch (\Exception $e) {
                Db::rollback();
                return json(['code' => -200, 'msg' => '编辑失败！' . $e->getMessage()]);
            }
        }

        // ====== GET 请求：加载编辑页面 ======
        $orderId = Request::param('id/d');
        $order = Db::name('crm_client_order')->where('id', $orderId)->find();
        if (!$order) {
            $this->error('订单不存在或已删除');
        }
        // 询盘来源等字段规范化，避免前后空格导致下拉无法选中
        $order['source'] = trim((string)($order['source'] ?? ''));
        if (isset($order['source_port'])) {
            $order['source_port'] = trim((string)($order['source_port'] ?? ''));
        }
        if (isset($order['province'])) {
            $order['province'] = trim((string)($order['province'] ?? ''));
        }
        if (isset($order['city'])) {
            $order['city'] = trim((string)($order['city'] ?? ''));
        }

        $order = OrderImageService::enrichOrderForEditView($order);

        // 读取该订单的所有产品明细行
        $items = Db::name('crm_order_item')->where('order_id', $orderId)->select();

        // 准备下拉选项数据（团队列表、来源列表、客户性质列表、运营人员列表等）
        $teamList   = $this->getTeamList();
        $sourceList = Db::name('crm_client_status')->distinct(true)->column('status_name');
        // 使用 array_map 和 trim 去除每个值的前后空格
        $sourceList = array_map('trim', $sourceList);
        // 兜底：当前订单的询盘来源若不在列表中（被删/改名），塞入列表头部，保证详情页能正确选中
        if (!empty($order['source']) && !in_array($order['source'], $sourceList, true)) {
            array_unshift($sourceList, $order['source']);
        }
        //var_dump($sourceList);
        // ====== 解析收款账户展示名（与 edit() 方法保持一致） ======
        // 优先使用 crm_receive_account 的最新账户名，而不是订单快照
        // 先保存原始快照名，避免在更新后被覆盖
        $originalSnapshotName = $order['bank_account_name'] ?? '';
        $currentAccountInfoForDisplay = null;
        if (!empty($order['bank_account'])) {
            $currentAccountId = $order['bank_account'];
            // 按 ID 查询账户表（不加 is_deleted 条件），获取最新账户信息
            $currentAccountInfoForDisplay = Db::name('crm_receive_account')
                ->where('id', $currentAccountId)
                ->field('id, account, is_deleted')
                ->find();
            
            $displayName = '';
            if ($currentAccountInfoForDisplay) {
                // 查到账户记录：使用最新的账户名，并根据 is_deleted 决定是否追加"（已删除）"
                $displayName = $currentAccountInfoForDisplay['account'] ?? '';
                if (isset($currentAccountInfoForDisplay['is_deleted']) && $currentAccountInfoForDisplay['is_deleted'] == 1) {
                    $displayName .= '（已删除）';
                }
            } else {
                // 查不到账户记录（被物理删除/异常）：回退使用订单快照名，并显示"（已删除）"
                $snapshotName = $originalSnapshotName;
                // 去掉可能已有的"（已删除）"标记，避免重复
                $snapshotName = str_replace('（已删除）', '', $snapshotName);
                $displayName = $snapshotName . '（已删除）';
            }
            
            // 覆盖订单的 bank_account_name 为计算后的展示名，供模板使用
            $order['bank_account_name'] = $displayName;
        }
        
        // ====== 构建 accountList（与 edit() 方法保持一致） ======
        // 【收款账户快照模式】只显示未删除的账户（is_deleted=0）
        $accountList = Db::name('crm_receive_account')->where('is_deleted', 0)->field('id, account')->select();
        
        // 【收款账户快照模式】兼容已删除账户：如果当前订单的 bank_account 对应账户不在列表中，补充到列表头部
        if (!empty($order['bank_account'])) {
            $currentAccountId = $order['bank_account'];
            $foundInList = false;
            foreach ($accountList as $acc) {
                if ($acc['id'] == $currentAccountId) {
                    $foundInList = true;
                    break;
                }
            }
            // 如果当前订单的账户不在列表中（因为 is_deleted=1 或查不到），需要补充一个选项
            if (!$foundInList) {
                $displayAccountName = '';
                $statusSuffix = '';
                
                // 复用上面查询的结果，避免重复查询
                if ($currentAccountInfoForDisplay) {
                    // 若能按 ID 查到：用其最新 account 名，并按 is_deleted 决定是否加"（已删除）"
                    $displayAccountName = $currentAccountInfoForDisplay['account'] ?? '';
                    if (isset($currentAccountInfoForDisplay['is_deleted']) && $currentAccountInfoForDisplay['is_deleted'] == 1) {
                        $statusSuffix = '（已删除）';
                    }
                } else {
                    // 若查不到：用订单原始快照 bank_account_name（去掉可能已有的标记） + "（已删除）"
                    $snapshotName = $originalSnapshotName;
                    // 去掉可能已有的"（已删除）"标记，避免重复
                    $displayAccountName = str_replace('（已删除）', '', $snapshotName);
                    $statusSuffix = '（已删除）';
                }
                
                // 如果最终显示名称为空，使用订单原始快照名作为最后兜底
                if (empty($displayAccountName)) {
                    $snapshotName = $originalSnapshotName;
                    // 去掉可能已有的"（已删除）"标记，重新追加
                    $displayAccountName = str_replace('（已删除）', '', $snapshotName);
                    $statusSuffix = '（已删除）';
                }
                
                // 将该选项添加到列表头部，保证下拉框能正常选中
                if (!empty($displayAccountName)) {
                    array_unshift($accountList, [
                        'id' => $currentAccountId,
                        'account' => $displayAccountName . $statusSuffix
                    ]);
                }
            }
        }
        
        $this->assign('accountList', $accountList);
        $this->assign('teamList', $teamList);
        $this->assign('sourceList', $sourceList);
        $this->assign('customer_type', self::CUSTOMER_TYPE);
        // 当前登录用户信息
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $this->assign('username', $currentAdmin['username'] ?? Session::get('username'));
        $this->assign('team_name', $currentAdmin['team_name'] ?? Session::get('team_name'));
        // 获取运营人员列表（以及按询盘来源分类的映射，用于联动下拉）
        $yyData = $this->getYyList();
        $operUserList = $yyData['_yyList'];
        $this->assign('operUserList', $operUserList);
        $this->assign('yyList', json_encode($yyData['yyList'], JSON_UNESCAPED_UNICODE));

        // 产品列表（含分类名）。详情页需要显示所有产品（包括已删除的），用于展示历史订单
        // 详情页查询所有产品（不加 is_deleted 限制），用于显示历史数据
        $productQuery = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            // 有组织限制时构造过滤条件
            $productQuery->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        $productRows = $productQuery
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name, MAX(p.is_deleted) as p_deleted, MAX(c.is_deleted) as c_deleted')
            ->order('p.product_name', 'asc')
            ->select();
        
        // 标记已删除的产品（is_deleted = 1）
        foreach ($productRows as &$product) {
            if ((isset($product['p_deleted']) && $product['p_deleted'] == 1) || 
                (isset($product['c_deleted']) && $product['c_deleted'] == 1)) {
                $product['is_deleted'] = true; // 标记为已删除
            }
        }
        unset($product); // 释放引用
        
        // 获取订单中已有的产品ID，检查是否有已被物理删除的产品
        // 如果产品不存在于产品表中，从订单明细中获取产品名称
        if (isset($items) && !empty($items)) {
            $existingProductIds = [];
            foreach ($items as $item) {
                if (!empty($item['product_id'])) {
                    $existingProductIds[] = (int)$item['product_id'];
                }
            }
            $existingProductIds = array_unique($existingProductIds);
            
            // 检查订单中的产品是否都在产品列表中
            if (!empty($existingProductIds)) {
                $foundProductIds = array_column($productRows, 'id');
                foreach ($items as $item) {
                    if (!empty($item['product_id']) && !in_array($item['product_id'], $foundProductIds)) {
                        // 产品不存在于产品表中（可能已被物理删除），从订单明细中获取信息
                        if (!empty($item['product_name'])) {
                            $productRows[] = [
                                'id' => $item['product_id'],
                                'product_name' => $item['product_name'],
                                'category_name' => '无',
                                'p_deleted' => 1,
                                'c_deleted' => 1,
                                'is_deleted' => true
                            ];
                        }
                    }
                }
            }
        }
        
        $this->assign('productList', $productRows);

        // 协同人列表（xm-select 数据格式）
        $teamName = $currentAdmin['team_name'] ?? Session::get('team_name') ?: '';
        $adminList = Db::name('admin')
            ->where('group_id', '<>', 1)
            ->whereIn('group_id', [10, 11, 14, 17, 18, 19, 21, 22])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        $currentJpIds = [];
        if (!empty($order['joint_person'])) {
            $currentJpIds = explode(',', $order['joint_person']);
        }
        foreach ($adminList as $admin) {
            $item = ['name' => $admin['username'], 'value' => $admin['admin_id']];
            if (in_array($admin['admin_id'], $currentJpIds)) {
                $item['selected'] = true;  // 默认选中已有协同人
            }
            $collaboratorData[] = $item;
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        // 产品经理列表（group_id = 14）
        $extraManagerIds = []; // 李营，可为空数组 [] 表示不额外包含任何人
        $managerList = Db::name('admin')
            ->where(function($q) use ($extraManagerIds){
                $q->whereIn('group_id', [13, 14, 18, 19, 22]);
                if (!empty($extraManagerIds)) {
                    $q->whereOr('admin_id', 'in', $extraManagerIds);   // ✅ TP5.1 兼容
                }
            })
            ->field('admin_id, username')
            ->order('username', 'asc')
            ->select();
        
        $this->assign('managerList', $managerList);


        // 将订单主表和明细数据分配给模板
        $this->assign('orderInfo', $order);
        $this->assign('orderItems', $items);
        return $this->fetch('order/details');
    }


    /**
     * 删除订单：删除指定ID的订单，并级联删除相关子项
     */
    public function del()
    {
        $id = (int)Request::param('id');
        if ($id <= 0) {
            return json(['code' => -200, 'msg' => '参数错误']);
        }
        $adminInfo = \app\admin\model\Admin::getMyInfo();
        $canManageAllOrders = OrderService::canManageAllOrders($adminInfo);
        $order = Db::table('crm_client_order')
            ->where('id', $id)
            ->field('id,pr_user,at_user')
            ->find();
        if (empty($order)) {
            return json(['code' => -200, 'msg' => '订单不存在']);
        }
        if (!$canManageAllOrders) {
            $currentUsername = (string)Session::get('username');
            $canDeleteCurrent = $currentUsername !== ''
                && (
                    ($order['pr_user'] ?? '') === $currentUsername
                    || ($order['at_user'] ?? '') === $currentUsername
                );
            if (!$canDeleteCurrent) {
                return json(['code' => -200, 'msg' => '无权限删除该订单']);
            }
        }

        // 开启事务
        Db::startTrans();
        try {
            // 查询订单信息，获取客户手机号
            // $order = Db::table('crm_client_order')->where('id', $id)->find();
            // if (!$order) {
            //     // 未找到订单记录，抛出异常以回滚事务
            //     throw new \Exception('订单不存在');
            // }
            // $custPhone = $order['cphone'];
            // 将对应线索表 (crm_leads) 中该客户的状态更新为 -1（未成交）
            //Db::table('crm_leads')->where('phone', $custPhone)->update(['issuccess' => -1]);
            // 删除订单关联的所有明细记录 (crm_order_item 表)
            Db::table('crm_order_item')->where('order_id', $id)->delete();
            // 删除订单主表记录 (crm_client_order 表)
            $result = Db::table('crm_client_order')->where('id', $id)->delete();
            if (!$result) {
                // 如果删除失败，抛出异常回滚事务
                throw new \Exception('删除订单失败');
            }
            // 提交事务
            Db::commit();
            return json(['code' => 0, 'msg' => '删除成功！']);
        } catch (\Exception $e) {
            // 捕获异常，回滚事务
            Db::rollback();
            return json(['code' => -200, 'msg' => '删除失败！']);
        }
    }


    //订单审核通过的处理
    public function passIndex()
    {
        $id = (int)Request::param('id');
        $auditUserId = (int)Session::get('aid');
        $res = $this->doPassOrder($id, $auditUserId);
        return json([
            'code' => $res['success'] ? 0 : -200,
            'msg'  => $res['msg'],
            'data' => $res['data'] ?? [],
        ]);
    }

    /**
     * 批量审核通过（待审核订单）
     * 支持 ids=[1,2,3] 或 ids="1,2,3"
     */
    public function passBatchIndex()
    {
        if (!request()->isPost()) {
            return json(['code' => 0, 'msg' => '非法请求']);
        }

        $ids = input('post.ids');
        $idsArr = [];
        if (is_array($ids)) {
            $idsArr = $ids;
        } elseif (is_string($ids)) {
            $idsArr = explode(',', $ids);
        }

        $idsArr = array_values(array_unique(array_filter(array_map(function ($v) {
            $v = is_string($v) ? trim($v) : $v;
            if ($v === '' || $v === null) return null;
            if (is_numeric($v)) return (int)$v;
            return null;
        }, $idsArr), function ($v) {
            return !empty($v) && (int)$v > 0;
        })));

        if (empty($idsArr)) {
            return json(['code' => 0, 'msg' => '请选择要审核的订单']);
        }

        $auditUserId = (int)session('aid');
        $succ = 0;
        $fail = 0;
        $failReasons = [];

        foreach ($idsArr as $orderId) {
            $r = $this->doPassOrder((int)$orderId, $auditUserId);
            if (!empty($r['success'])) {
                $succ++;
            } else {
                $fail++;
                $reason = $r['msg'] ?? '失败';
                if (!isset($failReasons[$reason])) $failReasons[$reason] = 0;
                $failReasons[$reason]++;
            }
        }

        if ($succ > 0 && $fail === 0) {
            return json(['code' => 0, 'msg' => '批量审核成功，共通过 ' . $succ . ' 条订单']);
        }
        if ($succ > 0 && $fail > 0) {
            $reasonText = '';
            if (!empty($failReasons)) {
                $parts = [];
                foreach ($failReasons as $k => $n) $parts[] = $k . ' ' . $n . ' 条';
                $reasonText = '（失败原因：' . implode('；', $parts) . '）';
            }
            return json(['code' => 0, 'msg' => '成功通过 ' . $succ . ' 条，失败 ' . $fail . ' 条' . $reasonText]);
        }

        $reasonText = '';
        if (!empty($failReasons)) {
            $parts = [];
            foreach ($failReasons as $k => $n) $parts[] = $k . ' ' . $n . ' 条';
            $reasonText = '（' . implode('；', $parts) . '）';
        }
        return json(['code' => 0, 'msg' => '批量审核失败，全部失败' . $reasonText]);
    }

    /**
     * 单条订单审核通过的通用逻辑（供 passIndex / passBatchIndex 复用）
     * @return array ['success'=>bool,'msg'=>string,'data'=>array]
     */
    private function doPassOrder($orderId, $auditUserId)
    {
        $orderId = (int)$orderId;
        $auditUserId = (int)$auditUserId;

        if ($orderId <= 0) {
            return ['success' => false, 'msg' => '参数错误', 'data' => []];
        }

        $orderinfo = Db::table('crm_client_order')->where('id', $orderId)->find();
        if (!$orderinfo) {
            return ['success' => false, 'msg' => '订单不存在', 'data' => []];
        }

        // 权限校验：与 pendingClientSearch 可见规则保持一致
        $aid = (int)session('aid');
        $username = session('username') ?? '';
        $groupId = (int)Db::name('admin')->where('admin_id', $aid)->value('group_id');
        $canViewAll = ($aid === 1) || in_array($groupId, [13, 15], true);
        if (!$canViewAll) {
            if (empty($username) || (($orderinfo['pr_user'] ?? '') != $username && ($orderinfo['at_user'] ?? '') != $username)) {
                return ['success' => false, 'msg' => '无权限审核该订单', 'data' => []];
            }
        }

        // 只允许处理待审核订单
        if ((int)($orderinfo['check_status'] ?? 0) !== 1) {
            return ['success' => false, 'msg' => '订单状态不是待审核', 'data' => []];
        }

        // 根据联系方式找到线索并更新成交状态
        $custphone = trim(preg_replace('/[+\-\s]/', '', (string)($orderinfo['contact'] ?? '')));
        if ($custphone === '') {
            return ['success' => false, 'msg' => '联系方式为空', 'data' => []];
        }

        $coninfo = Db::name('crm_contacts')
            ->where('is_delete', 0)
            ->where('contact_value', $custphone)
            ->find();

        if (!$coninfo) {
            return ['success' => false, 'msg' => '该客户信息没有找到', 'data' => []];
        }

        $custinfo = Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
        if (!$custinfo) {
            return ['success' => false, 'msg' => '客户线索不存在', 'data' => []];
        }

        $now = date('Y-m-d H:i:s');

        Db::startTrans();
        try {
            // 防并发：带上 check_status=1，避免重复审核
            $result = Db::table('crm_client_order')
                ->where('id', $orderId)
                ->where('check_status', 1)
                ->update([
                    'check_status'  => 2,
                    'audit_user_id' => $auditUserId,
                    'audit_time'    => $now,
                    'audit_remark'  => '审核通过',
                ]);

            if (!$result) {
                Db::rollback();
                return ['success' => false, 'msg' => '订单通过失败', 'data' => []];
            }

            Db::table('crm_leads')->where('id', $custinfo['id'])->update(['issuccess' => 1]);

            // 插入审核通过通知（保持与现有 passIndex 风格一致）
            Db::name('crm_order_notifications')->insert([
                'order_id'     => $orderId,
                'order_no'     => $orderinfo['order_no'],
                'contact'      => $orderinfo['contact'],
                'type'         => 1, // 1=审核通过
                'message'      => '订单编号为' . $orderinfo['order_no'] . '的订单已经通过审核',
                'target_user'  => $orderinfo['pr_user'], // 订单负责人
                'is_read'      => 0,
                'create_time'  => $now,
            ]);

            Db::commit();
        } catch (\Throwable $e) {
            Db::rollback();
            return ['success' => false, 'msg' => '批量审核异常：' . $e->getMessage(), 'data' => []];
        }

        // 插入通知后，执行自动归档/软删除（避免影响主事务）
        try {
            $this->autoTrimNotifications($orderinfo['pr_user']);
        } catch (\Throwable $e) {
            // 静默
        }

        return ['success' => true, 'msg' => '订单通过成功', 'data' => []];
    }

    //订单审核拒绝的处理
    public function rejectIndex()
    {
        $id = input('post.id/d', 0);
        $reason = input('post.audit_remark/s', '');

        if (!$id || $reason === '') {
            return json(['code' => 0, 'msg' => '参数错误']);
        }

        $adminId = session('aid');

        // 先查询订单信息，获取联系方式
        $orderinfo = Db::name('crm_client_order')->where('id', $id)->find();
        if (!$orderinfo) {
            return json(['code' => 0, 'msg' => '订单不存在']);
        }

        $res = Db::name('crm_client_order')
            ->where('id', $id)
            ->update([
                'check_status'  => 3,
                'audit_user_id' => $adminId,
                'audit_time'    => date('Y-m-d H:i:s'),
                'audit_remark'  => $reason,
            ]);

        if ($res !== false) {

            // ★ 新增：插入审核拒绝通知
            Db::name('crm_order_notifications')->insert([
                'order_id' => $id,
                'order_no' => $orderinfo['order_no'], 
                'contact' => $orderinfo['contact'],
                'type' => 2, // 2=审核拒绝
                'message' => '订单编号为' . $orderinfo['order_no'] . '的订单审核被拒绝',
                'target_user' => $orderinfo['pr_user'], // 订单负责人
                'is_read' => 0,
                'create_time' => date('Y-m-d H:i:s')
            ]);

            // ★ 插入通知后，立即执行自动归档/软删除处理
            $this->autoTrimNotifications($orderinfo['pr_user']);
        


            return json(['code' => 1, 'msg' => '已拒绝该订单']);
        } else {
            return json(['code' => 0, 'msg' => '操作失败，请重试']);
        }
       
    }

    
    /**
     * ★★★ 新增：审核失败订单重新提交到“待审核” ★★★
     */
    public function resubmitIndex()
    {
        if (!request()->isPost()) {
            return json(['code' => 0, 'msg' => '非法请求']);
        }

        $id = input('post.id/d', 0);
        if (!$id) {
            return json(['code' => 0, 'msg' => '参数错误']);
        }

        // 【修改】A. 获取当前登录用户信息
        $adminId = (int)session('aid');
        $currentUsername = (string)Session::get('username');  // 当前登录用户名，用于对比 pr_user/at_user

        // 【修改】B. 查询订单时补充 pr_user 和 at_user 字段
        // 先确认这条订单确实是"审核失败"状态，并获取 pr_user 和 at_user
        $order = Db::name('crm_client_order')
            ->where('id', $id)
            ->field('id,check_status,pr_user,at_user,create_time,first_create_time')
            ->find();

        if (!$order) {
            return json(['code' => 0, 'msg' => '订单不存在或已删除']);
        }

        if ((int)$order['check_status'] !== 3) {
            return json(['code' => 0, 'msg' => '只有审核失败的订单才能重新提交']);
        }

        // 【修改】C. 权限判断逻辑（替换原 admin/group 判断）
        // 【删除】原来的判断：if ($adminId !== 1 && $groupId !== 15)
        // 【新增】新的权限规则：
        // 1. 超级管理员(aid==1)可以放行
        // 2. 否则必须满足：order.pr_user == 当前用户 或 order.at_user == 当前用户（或包含当前用户）
        $hasPermission = false;
        if ($adminId === 1) {
            // 超级管理员可以放行
            $hasPermission = true;
        } else {
            // 检查 pr_user 是否等于当前用户
            if (!empty($order['pr_user']) && $order['pr_user'] == $currentUsername) {
                $hasPermission = true;
            }
            // 检查 at_user 是否等于当前用户或包含当前用户（支持多人字符串，如逗号分隔）
            if (!$hasPermission && !empty($order['at_user'])) {
                if ($order['at_user'] == $currentUsername) {
                    $hasPermission = true;
                } elseif (strpos($order['at_user'], $currentUsername) !== false) {
                    // 支持 at_user 为多人字符串的情况（如："user1,user2,user3"）
                    // 使用精确匹配避免部分匹配问题（例如 "user1" 不应该匹配 "user10"）
                    $atUserArray = array_map('trim', explode(',', $order['at_user']));
                    if (in_array($currentUsername, $atUserArray)) {
                        $hasPermission = true;
                    }
                }
            }
        }

        if (!$hasPermission) {
            return json(['code' => 0, 'msg' => '您没有重新提交该订单的权限']);
        }

        // 【修改】D. 重新提交更新逻辑，加入 create_time / first_create_time 处理
        // 生成当前时间
        $now = date('Y-m-d H:i:s');
        
        // 构造更新数据
        $updateData = [
            'check_status'  => 1,                 // 待审核
            'status'        => '待审核',          // 文本字段，兼容历史逻辑
            // 重新提交时，可以清空审核人和审核时间，让下一次审核更干净
            'audit_user_id' => null,
            'audit_time'    => null,
            // 是否清空上一次的审核意见，看你业务需求：
            // 想保留历史就不要清空；想让下一次审核写新的理由就清空
            'audit_remark'  => null,
            'create_time'   => $now,              // 更新为当前时间
        ];
        
        // 处理 first_create_time：如果为空，则把旧的 create_time 写入 first_create_time
        if (empty($order['first_create_time'])) {
            // 如果 first_create_time 为空，把旧的 create_time 写入 first_create_time
            if (!empty($order['create_time'])) {
                $updateData['first_create_time'] = $order['create_time'];
            } else {
                // 如果旧的 create_time 也为空，则使用当前时间
                $updateData['first_create_time'] = $now;
            }
        }
        // 如果 first_create_time 不为空，则不修改它（只更新 create_time）

        // 使用数据库事务包裹更新操作
        Db::startTrans();
        try {
            $res = Db::name('crm_client_order')
                ->where('id', $id)
                ->update($updateData);

            if ($res !== false) {
                Db::commit();
                return json(['code' => 1, 'msg' => '重新提交成功，该订单已回到待审核列表']);
            } else {
                Db::rollback();
                return json(['code' => 0, 'msg' => '重新提交失败，请重试']);
            }
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 0, 'msg' => '重新提交失败：' . $e->getMessage()]);
        }
    }

    /**
     * 【新增】获取审核失败信息（给“原因”按钮用）
     */
    public function auditInfo()
    {
        if (!request()->isAjax()) {
            return json(['code' => 0, 'msg' => '非法请求']);
        }

        $id = input('id/d', 0);
        if (!$id) {
            return json(['code' => 0, 'msg' => '参数错误']);
        }

        // 关联 admin 表，取审核人名字
        $info = Db::name('crm_client_order')
            ->alias('o')
            ->leftJoin('admin a', 'a.admin_id = o.audit_user_id')
            ->field('o.check_status,o.audit_remark,o.audit_time,a.username as audit_user_name')
            ->where('o.id', $id)
            ->find();

        if (!$info) {
            return json(['code' => 0, 'msg' => '订单不存在或已删除']);
        }

        // 状态文字
        $statusMap = [
            1 => '待审核',
            2 => '审核通过',
            3 => '审核失败',
        ];
        $info['check_status_text'] = isset($statusMap[$info['check_status']])
            ? $statusMap[$info['check_status']]
            : '未知状态';

        return json([
            'code' => 1,
            'msg'  => '获取成功',
            'data' => $info,
        ]);
    }


    

    public function shenhe()
    {
        $id = Request::param('id');

        $orderinfo = Db::table('crm_client_order')->where('id', $id)->find();
        $custphone = $orderinfo['cphone'];
        $custphone = trim(preg_replace('/[+\-\s]/', '', $custphone));
        $coninfo = Db::name('crm_contacts')->where('is_delete', 0)->where(function ($query) use ($custphone) {
            $query->whereRaw("CONCAT(contact_extra, contact_value) = '{$custphone}'")
                ->whereOr('contact_value', $custphone);
        })->find();
        if (!$coninfo) {
            $msg['code'] = -200;
            $msg['msg'] = "该客户信息没用找到";
            return json($msg);
        }
        $custinfo =  Db::name('crm_leads')->where('id', $coninfo['leads_id'])->find();
        // $custinfo = Db::table('crm_leads')->where('phone', $custphone)->find();
        if ($custinfo['issuccess'] == 1) {
            $msg = ['code' => -200, 'msg' => '该客户已成交,业绩请勿重复添加', 'data' => []];
            return json($msg);
        }
        $updatearr = [];
        $updatearr['issuccess'] = 1;

        Db::table('crm_leads')->where('id', $custinfo['id'])->update($updatearr);
        $result = Db::table('crm_client_order')->where('id', $id)->update(['status' => '审核通过']);

        if ($result) {
            $msg = ['code' => 0, 'msg' => '审核成功', 'data' => []];
            return json($msg);
        } else {
            $msg = ['code' => -200, 'msg' => '审核已成功', 'data' => []];
            return json($msg);
        }
    }

    //客户搜索
    public function clientSearch()
    {
        $where = [];
        $client_where = [];
        //判断权限
        $user = \app\admin\model\Admin::getMyInfo();
        $canManageAllOrders = OrderService::canManageAllOrders($user);
        $team_name = $canManageAllOrders ? '' : ($user['team_name'] ?? '');
        if ($team_name) $where[] = ['team_name', '=', $team_name];
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);
        
        // 兼容旧的时间查询字段（timebucket/at_time），优先使用新的字段
        if (isset($keyword['timebucket']) || isset($keyword['at_time'])) {
            // 如果没有设置新的成交时间查询，则使用旧的字段
            if (!isset($keyword['order_timebucket']) && !isset($keyword['order_time'])) {
                $keyword['order_timebucket'] = isset($keyword['timebucket']) ? $keyword['timebucket'] : $keyword['at_time'];
            }
        }
        
        $where[] = ['check_status', '=', 2];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        
        // 处理成交时间查询（order_timebucket 或 order_time）
        if (isset($keyword['order_timebucket']) && $keyword['order_timebucket'] !== '') {
            // 使用快捷时间选择
            $where[] = $this->buildTimeWhere($keyword['order_timebucket'], 'order_time');
            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['order_timebucket'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['order_timebucket'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        } elseif (isset($keyword['order_time']) && $keyword['order_time'] !== '') {
            // 使用自定义时间范围
            $where[] = $this->buildTimeWhere($keyword['order_time'], 'order_time');
            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['order_time'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['order_time'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }
        
        // 处理创建时间查询（create_timebucket 或 create_time）
        if (isset($keyword['create_timebucket']) && $keyword['create_timebucket'] !== '') {
            // 使用快捷时间选择
            $where[] = $this->buildTimeWhere($keyword['create_timebucket'], 'create_time');
        } elseif (isset($keyword['create_time']) && $keyword['create_time'] !== '') {
            // 使用自定义时间范围
            $where[] = $this->buildTimeWhere($keyword['create_time'], 'create_time');
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
            // $client_where[] = ['kh_name', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['customer_type_flag']) && $keyword['customer_type_flag'] !== '') {
            $where[] = ['customer_type_flag', '=', $keyword['customer_type_flag']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
            $client_where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (!$team_name && isset($keyword['team_name'])) {
            $where[] = ['team_name', '=', $keyword['team_name']];
            $team_name = $keyword['team_name'];
        }
        $org_where = [];
        if (!$canManageAllOrders && $user['org']) {
            $org_where[] =  $this->getOrgWhere($user['org']);
        }
        if (!empty($keyword['org'])) {
            $org_where[] =  $this->getOrgWhere($keyword['org']);
        }
        if ($team_name) {
            $usernames = Db::table('admin')->where('team_name', $team_name)->where($org_where)->column('username');
        } else {
            if (!empty($org_where)) {
                $usernames = Db::table('admin')->where($org_where)->column('username');
            }
        }
        if (isset($usernames)) {
            if (!$usernames) {
                $client_where[] = ['pr_user', '=', time()];
                $where[] = ['pr_user', '=', time()];
            } else {
                $client_where[] = ['pr_user', 'in', $usernames];
                $client_where[] = ['oper_user', 'in', $usernames];

                $where[] = ['pr_user', 'in', $usernames];
            }
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        if (isset($keyword['pr_user'])) {
            $where[] = ['pr_user', '=', $keyword['pr_user']];
            $client_where[] = ['pr_user', '=', $keyword['pr_user']];
        }

        $list = Db::table('crm_client_order')
            ->where($where)
            ->order('create_time desc,id desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();

        // 收集所有需要查询的admin_id（收款账户和协同人）
        $allAdminIds = [];
        foreach ($list['data'] as $order) {
            // 收集协同人ID
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (is_numeric($id)) {
                        $allAdminIds[] = $id;
                    }
                }
            }
        }
        $allAdminIds = array_unique($allAdminIds);

        // 批量查询admin表获取用户名映射
        $adminMap = [];
        if (!empty($allAdminIds)) {
            $admins = Db::name('admin')
                ->whereIn('admin_id', $allAdminIds)
                ->column('username', 'admin_id');
            $adminMap = $admins;
        }

        // 【订单快照模式】查询订单对应的产品明细，统一使用快照字段
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = $this->buildOrderItemsSnapshotMap($orderIds);
        
        // 【收款账户快照模式】优先使用 bank_account_name 快照字段，仅在为空时补齐
        // 转换协同人ID为用户名
        foreach ($list['data'] as &$order) {
            // 【收款账户快照模式】优先使用 bank_account_name（快照），仅在 bank_account 有值且 bank_account_name 为空时实时查询补齐
            if (!empty($order['bank_account']) && empty($order['bank_account_name'])) {
                $accountName = $this->resolveBankAccountName($order['bank_account']);
                if (!empty($accountName)) {
                    $order['bank_account_name'] = $accountName;
                    // 可选：写回数据库补齐（一次性补齐老数据）
                    // Db::name('crm_client_order')->where('id', $order['id'])->update(['bank_account_name' => $accountName]);
                }
            }
            
            // 转换协同人ID为用户名
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                $names = [];
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (isset($adminMap[$id])) {
                        $names[] = $adminMap[$id];
                    }
                }
                if (!empty($names)) {
                    $order['joint_person_names'] = implode(',', $names);
                }
            }

            // 绑定订单的产品明细（快照数据），便于前端一次渲染
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            // 如果订单主表的 product_name 为空，从明细快照中取第一个产品名称
            if (empty($order['product_name']) && !empty($order['order_items'])) {
                $order['product_name'] = $order['order_items'][0]['product_name'] ?? '';
            }
        }
        unset($order);

        $list['data'] = OrderImageService::appendOrderImageFields($list['data'] ?? []);

        //成单率

        //询盘数 
        //每个月的询盘数=当月录入询盘数-当月录入的询盘丢入公海数（仅当月）+当月从公海拾取数
        $totalInquiries = Db::table('crm_leads')->where('status', 1)->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        return $result = [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
            'totalProfitRate' => $totalMoney > 0 ? number_format($totalProfit / $totalMoney * 100, 2) : 0,
            'totalCount' => $successOrders,
        ];
    }


    /**
     * 获取指定字段的总和
     * @param array $where 查询条件
     * @param string $field 要统计的字段
     * @return float 字段总和
     */
    private function getSum($where, $field)
    {
        return Db::table('crm_client_order')
            ->where($where)
            ->sum($field);
    }

    /**
     * 订单明细快照聚合方法（核心）
     * 统一从 crm_order_item 表读取快照字段，禁止从 crm_products、crm_product_category 动态 JOIN
     * @param array $orderIds 订单ID数组
     * @return array 返回格式：$map[order_id] = [ ['product_name'=>..., 'supplier_name'=>..., ...], ... ]
     */
    private function buildOrderItemsSnapshotMap(array $orderIds): array
    {
        if (empty($orderIds)) {
            return [];
        }

        // 只查询 crm_order_item 表，使用快照字段
        $items = Db::table('crm_order_item')
            ->whereIn('order_id', $orderIds)
            ->order('order_id asc, line_no asc, id asc')
            ->field('order_id, product_name, supplier_name, spec_model, unit, qty, unit_price, total_price, purchase_price, remark, manager_id, line_no')
            ->select();
        // 组装产品经理映射
        $managerIds = [];
        foreach ($items as $item) {
            if (!empty($item['manager_id'])) {
                $managerIds[] = $item['manager_id'];
            }
        }
        $managerIds = array_unique($managerIds);
        $managerMap = [];
        if (!empty($managerIds)) {
            $managers = Db::table('admin')
                ->whereIn('admin_id', $managerIds)
                ->field('admin_id, username')
                ->select();
            foreach ($managers as $manager) {
                $managerMap[$manager['admin_id']] = $manager['username'];
            }
        }

        // 按 order_id 聚合，并添加产品经理名称
        $orderItemsMap = [];
        foreach ($items as $item) {
            $item['manager_name'] = isset($managerMap[$item['manager_id']]) ? $managerMap[$item['manager_id']] : '';
            // 为了兼容前端，将 supplier_name 也映射为 supplier
            $item['supplier'] = $item['supplier_name'] ?? '';
            $orderItemsMap[$item['order_id']][] = $item;
        }

        return $orderItemsMap;
    }

    /**
     * 根据收款账户ID获取账户名称（用于快照保存）
     * @param int|string $bankAccountId 收款账户ID
     * @return string 账户名称，如果查不到或ID为空则返回空字符串
     */
    private function resolveBankAccountName($bankAccountId)
    {
        if (empty($bankAccountId)) {
            return '';
        }
        
        $accountInfo = Db::name('crm_receive_account')
            ->where('id', $bankAccountId)
            ->field('account')
            ->find();
        
        return $accountInfo ? ($accountInfo['account'] ?? '') : '';
    }


    //（我的客户）搜索
    public function personClientSearchOld()
    {
        $page = input('page') ? input('page') : 1;
        $limit = input('limit') ? input('limit') : config('pageSize');
        $keyword = Request::param('keyword');

        $mapAtTime = []; //添加时间
        $mapKhName = []; //客户名称
        $mapXsSource = []; //线索/客户来源
        $mapPrUser = []; //业务员/负责人
        if ($keyword['create_time'] != '') {
            $at = $keyword['create_time']; //日期
            $end_at = date('Y-m-d', strtotime("$at+1day"));
            $mapAtTime = [['create_time', 'between time', [strtotime($at), strtotime($end_at)]]];
        }
        if ($keyword['cname'] != '') {
            $mapKhName = [['cname', 'like', '%' . $keyword['cname'] . '%']];
        }

        if ($keyword['status'] != '') {

            $mapXsSource =  ['status' => $keyword['status']];
        }

        // if ($keyword['uname'] != ''){
        //     $mapPrUser = [['uname','like','%'.$keyword['uname'].'%']];
        // }
        $mapPrUser['pr_user'] =  Session::get('username');
        $list  = Db::table('crm_client_order')
            ->where($mapKhName)
            ->where($mapXsSource)
            ->where($mapPrUser)
            ->where($mapAtTime)
            ->whereTime('create_time', $keyword['timebucket'] ? $keyword['timebucket'] : null)
            ->order('create_time desc')
            ->paginate(array('list_rows' => $limit, 'page' => $page))
            ->toArray();
        //var_dump($list);
        return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }

    public function personClientSearch()
    {
        $where = [];
        $client_where = [];
        $pr_user = Session::get('username') ?? '';
        
        // 确保只显示当前用户的订单：自己创建的或自己是负责人的
        if (!empty($pr_user)) {
            $where[] = function($query) use ($pr_user) {
                $query->where('at_user', '=', $pr_user)
                      ->whereOr('pr_user', '=', $pr_user);
            };
        } else {
            // 如果没有用户名，返回空结果
            $where[] = ['id', '=', 0];
        }
        
        $client_where[] = ['pr_user', '=', $pr_user];
        //判断权限
        // $team_name = session('team_name') ?? '';
        // if ($team_name) {
        //     $where[] = ['team_name', '=', $team_name];
        //     $usernames = Db::table('admin')->where('team_name', $team_name)->column('username');
        //     $client_where[] = ['pr_user', 'in', $usernames];
        // }
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        $where[] = ['check_status', '=', 2];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        if (isset($keyword['timebucket'])) {
            $where[] = $this->buildTimeWhere($keyword['timebucket'], 'order_time');

            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
            // $client_where[] = ['kh_name', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        $list = Db::table('crm_client_order')
            ->where($where)
            ->order('create_time desc,id desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();
        
        // 收集所有需要查询的admin_id（协同人）
        $allAdminIds = [];
        foreach ($list['data'] as $order) {
            // 收集协同人ID
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (is_numeric($id)) {
                        $allAdminIds[] = $id;
                    }
                }
            }
        }
        $allAdminIds = array_unique($allAdminIds);

        // 批量查询admin表获取用户名映射
        $adminMap = [];
        if (!empty($allAdminIds)) {
            $admins = Db::name('admin')
                ->whereIn('admin_id', $allAdminIds)
                ->column('username', 'admin_id');
            $adminMap = $admins;
        }

        // 【订单快照模式】查询订单对应的产品明细，统一使用快照字段
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = $this->buildOrderItemsSnapshotMap($orderIds);
        
        // 转换收款账户ID为账户名称 和 协同人ID为用户名
        foreach ($list['data'] as &$order) {
            // 转换收款账户
            if (!empty($order['bank_account'])) {
                $accountInfo = Db::name('crm_receive_account')
                    ->where('id', $order['bank_account'])
                    ->field('account')
                    ->find();
                if ($accountInfo) {
                    $order['bank_account_name'] = $accountInfo['account'];
                }
            }
            
            // 转换协同人ID为用户名
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                $names = [];
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (isset($adminMap[$id])) {
                        $names[] = $adminMap[$id];
                    }
                }
                if (!empty($names)) {
                    $order['joint_person_names'] = implode(',', $names);
                }
            }

            // 绑定订单的产品明细（快照数据）
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            // 如果订单主表的 product_name 为空，从明细快照中取第一个产品名称
            if (empty($order['product_name']) && !empty($order['order_items'])) {
                $order['product_name'] = $order['order_items'][0]['product_name'] ?? '';
            }
        }
        unset($order);


        //成单率

        $totalInquiries = Db::table('crm_leads')->where('status', 1)->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        return $result = [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }

    //订单草稿搜索接口
    public function draftClientSearch()
    {
        $where = [];
        $client_where = [];
        $pr_user = Session::get('username') ?? '';
        
        // 确保只显示当前用户的订单：自己创建的或自己是负责人的
        if (!empty($pr_user)) {
            $where[] = function($query) use ($pr_user) {
                $query->where('at_user', '=', $pr_user)
                      ->whereOr('pr_user', '=', $pr_user);
            };
        } else {
            // 如果没有用户名，返回空结果
            $where[] = ['id', '=', 0];
        }
        
        $client_where[] = ['pr_user', '=', $pr_user];
        //判断权限
        // $team_name = session('team_name') ?? '';
        // if ($team_name) {
        //     $where[] = ['team_name', '=', $team_name];
        //     $usernames = Db::table('admin')->where('team_name', $team_name)->column('username');
        //     $client_where[] = ['pr_user', 'in', $usernames];
        // }
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        $where[] = ['check_status', '=', 0];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        if (isset($keyword['timebucket'])) {
            $where[] = $this->buildTimeWhere($keyword['timebucket'], 'create_time');

            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
            // $client_where[] = ['kh_name', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        $list = Db::table('crm_client_order')
            ->where($where)
            ->order('create_time desc,id desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();

        // #region agent log
        // $lastSql = Db::getLastSql();
        // $logPath = dirname(__DIR__, 5) . DIRECTORY_SEPARATOR . '.cursor' . DIRECTORY_SEPARATOR . 'debug.log';
        // file_put_contents(
        //     $logPath,
        //     json_encode([
        //         'message' => 'draftClientSearch paginate SQL',
        //         'data' => ['sql' => $lastSql, 'page' => $page, 'limit' => $limit],
        //         'timestamp' => round(microtime(true) * 1000),
        //         'location' => 'Order.php:draftClientSearch',
        //         'sessionId' => 'debug-session',
        //         'hypothesisId' => 'SQL_DEBUG'
        //     ], JSON_UNESCAPED_UNICODE) . "\n",
        //     FILE_APPEND
        // );
        // #endregion

        // 收集所有需要查询的admin_id（协同人）
        $allAdminIds = [];
        foreach ($list['data'] as $order) {
            // 收集协同人ID
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (is_numeric($id)) {
                        $allAdminIds[] = $id;
                    }
                }
            }
        }
        $allAdminIds = array_unique($allAdminIds);

        // 批量查询admin表获取用户名映射
        $adminMap = [];
        if (!empty($allAdminIds)) {
            $admins = Db::name('admin')
                ->whereIn('admin_id', $allAdminIds)
                ->column('username', 'admin_id');
            $adminMap = $admins;
        }

        // 【订单快照模式】查询订单对应的产品明细，统一使用快照字段
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = $this->buildOrderItemsSnapshotMap($orderIds);
        
        // 转换收款账户ID为账户名称 和 协同人ID为用户名
        foreach ($list['data'] as &$order) {
            // 转换收款账户
            if (!empty($order['bank_account'])) {
                $accountInfo = Db::name('crm_receive_account')
                    ->where('id', $order['bank_account'])
                    ->field('account')
                    ->find();
                if ($accountInfo) {
                    $order['bank_account_name'] = $accountInfo['account'];
                }
            }
            
            // 转换协同人ID为用户名
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                $names = [];
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (isset($adminMap[$id])) {
                        $names[] = $adminMap[$id];
                    }
                }
                if (!empty($names)) {
                    $order['joint_person_names'] = implode(',', $names);
                }
            }

            // 绑定订单的产品明细（快照数据）
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            // 如果订单主表的 product_name 为空，从明细快照中取第一个产品名称
            if (empty($order['product_name']) && !empty($order['order_items'])) {
                $order['product_name'] = $order['order_items'][0]['product_name'] ?? '';
            }
        }
        unset($order);


        //成单率

        $totalInquiries = Db::table('crm_leads')->where('status', 1)->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        return $result = [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }

    /**
     * 草稿提交审核：仅允许 check_status=0 的订单，更新为待审核（check_status=1）
     * 提交前校验草稿必填项（与 add.html 对齐），缺失则返回 missing 列表与 edit_url
     */
    public function submitDraft()
    {
        $id = (int)Request::param('id');
        if ($id <= 0) {
            return json(['code' => 2, 'msg' => '参数错误']);
        }
        $pr_user = Session::get('username') ?? '';
        $order = Db::table('crm_client_order')->where('id', $id)->find();
        if (!$order) {
            return json(['code' => 2, 'msg' => '订单不存在']);
        }
        if ((int)$order['check_status'] !== 0) {
            return json(['code' => 2, 'msg' => '仅草稿可提交审核']);
        }
        if ($pr_user && $order['at_user'] !== $pr_user && $order['pr_user'] !== $pr_user) {
            return json(['code' => 2, 'msg' => '无权限操作该订单']);
        }

        // 草稿必填校验（与 add.html 的 lay-verify required 及条件必填一致）
        $missing = $this->validateDraftRequired($order, $id);
        if (!empty($missing)) {
            return json([
                'code'     => 1,
                'msg'      => '请先完善必填项后再提交审核',
                'missing'  => $missing,
                'edit_url' => url('Order/edit', ['id' => $id]),
            ]);
        }

        Db::table('crm_client_order')->where('id', $id)->update([
            'check_status' => 1,
            'status'       => '待审核',
        ]);
        return json(['code' => 0, 'msg' => '已提交审核']);
    }

    /**
     * 草稿必填项校验（与 add.html 必填及条件必填对齐）
     * @param array $order 订单主表一条记录
     * @param int   $orderId 订单ID（用于查 crm_order_item）
     * @return array 缺失项中文名称列表，如 ['成交时间','产品明细']
     */
    private function validateDraftRequired($order, $orderId)
    {
        $missing = [];
        $isEmpty = function ($v) {
            if ($v === null) {
                return true;
            }
            $v = trim((string)$v);
            return $v === '';
        };

        // 基础信息必填
        if ($isEmpty($order['contact'] ?? null)) {
            $missing[] = '联系方式';
        }
        if ($isEmpty($order['province'] ?? null)) {
            $missing[] = '所在省';
        }
        if ($isEmpty($order['city'] ?? null)) {
            $missing[] = '所在市';
        }
        if ($isEmpty($order['country'] ?? null)) {
            $missing[] = '收货地址';
        }
        if ($isEmpty($order['customer_type_flag'] ?? null)) {
            $missing[] = '用户属性';
        }
        // 客户公司：仅当 customer_type_flag != '1'（个人）时必填
        $customerTypeFlag = trim((string)($order['customer_type_flag'] ?? ''));
        if ($customerTypeFlag !== '1') {
            $clientCompany = $order['client_company'] ?? $order['cname'] ?? null;
            if ($isEmpty($clientCompany)) {
                $missing[] = '客户公司';
            }
        }
        if ($isEmpty($order['customer_type'] ?? null)) {
            $missing[] = '客户性质';
        }
        if ($isEmpty($order['pr_user'] ?? null)) {
            $missing[] = '客户负责人';
        }
        if ($isEmpty($order['source'] ?? null)) {
            $missing[] = '询盘来源';
        }
        if ($isEmpty($order['source_port'] ?? null)) {
            $missing[] = '运营端口';
        }
        if ($isEmpty($order['bank_account'] ?? null)) {
            $missing[] = '收款账户';
        }
        if ($isEmpty($order['team_name'] ?? null)) {
            $missing[] = '团队名称';
        }
        if ($isEmpty($order['order_time'] ?? null)) {
            $missing[] = '成交时间';
        }
        if ($isEmpty($order['shipping_cost'] ?? null)) {
            $missing[] = '估算运费';
        }
        if ($isEmpty($order['invoice_type'] ?? null)) {
            $missing[] = '票种性质';
        }
        // 条件必填：普票/专票时 invoice_amount 必填
        $invoiceType = trim((string)($order['invoice_type'] ?? ''));
        if ($invoiceType === '普票' || $invoiceType === '专票') {
            if ($isEmpty($order['invoice_amount'] ?? null)) {
                $missing[] = '票种/票金额';
            }
        }
        if ($isEmpty($order['tax_amount'] ?? null)) {
            $missing[] = '税费金额';
        }
        if ($isEmpty($order['debugging_cost'] ?? null)) {
            $missing[] = '调试费';
        }
        if ($isEmpty($order['sales_commission'] ?? null)) {
            $missing[] = '佣金';
        }
        if ($isEmpty($order['split_remarks'] ?? null)) {
            $missing[] = '备注';
        }
        if ($isEmpty($order['amount_received'] ?? null)) {
            $missing[] = '已收款金额';
        }
        if ($isEmpty($order['money'] ?? null)) {
            $missing[] = '订单金额';
        }
        if ($isEmpty($order['profit'] ?? null)) {
            $missing[] = '利润';
        }
        if ($isEmpty($order['margin_rate'] ?? null)) {
            $missing[] = '利润率';
        }
        // 利润>=2000 时，两个凭证必须存在
        $profit = floatval($order['profit'] ?? 0);
        if ($this->isVoucherRequired($profit)) {
            $wechatImages = OrderService::parseImageList($order['wechat_receipt_image'] ?? null);
            if (count($wechatImages) < 1) {
                $missing[] = '微信沟通及付款凭证';
            }
            $inquiryImages = OrderService::parseImageList($order['inquiry_assign_image'] ?? null);
            if (count($inquiryImages) < 1) {
                $missing[] = '询盘来源凭证';
            }
        }

        // 产品明细：至少 1 行，每行含 product_name / product_manager(manager_id) / unit / qty / unit_price / purchase_price
        $items = Db::table('crm_order_item')->where('order_id', $orderId)->order('line_no asc, id asc')->select();
        $productMissing = false;
        if (empty($items)) {
            $productMissing = true;
        } else {
            $requiredItemKeys = ['product_name', 'unit', 'qty', 'unit_price', 'purchase_price'];
            foreach ($items as $row) {
                foreach ($requiredItemKeys as $key) {
                    if ($isEmpty($row[$key] ?? null)) {
                        $productMissing = true;
                        break 2;
                    }
                }
                // 产品经理：表存 manager_id，空则视为缺失
                if ($isEmpty($row['manager_id'] ?? null)) {
                    $productMissing = true;
                    break;
                }
            }
        }
        if ($productMissing) {
            $missing[] = '产品明细';
        }

        return $missing;
    }

    /**
     * 返回当前登录人 id（草稿归属用）
     * @return int 0 表示未登录/参数异常
     */
    private function getDraftOwnerId()
    {
        $aid = Session::get('aid');
        return $aid !== null && $aid !== '' ? (int)$aid : 0;
    }

    /**
     * 查询该用户在 crm_client_order 表中的唯一草稿 id
     * 条件：check_status=0，pr_user_id=$aid，按 COALESCE(ut_time, create_time) 倒序取最新一条
     * @param int $aid 当前用户 id（admin_id）
     * @return int 草稿 id 或 0
     */
    private function findUserDraftId($aid)
    {
        if ($aid <= 0) {
            return 0;
        }
        $row = Db::name('crm_client_order')
            ->where('check_status', 0)
            ->where('pr_user_id', $aid)
            ->orderRaw('COALESCE(ut_time, create_time) DESC, id DESC')
            ->find();
        return $row && !empty($row['id']) ? (int)$row['id'] : 0;
    }

    /**
     * 获取当前用户最新一条草稿（钉钉式断点恢复）
     * 按 aid 唯一草稿：findUserDraftId；没草稿返回 code=0, data=null
     */
    public function getLatestDraft()
    {
        $aid = $this->getDraftOwnerId();
        if ($aid <= 0) {
            return json(['code' => 1, 'msg' => '未登录']);
        }
        $draftId = $this->findUserDraftId($aid);
        if ($draftId <= 0) {
            return json(['code' => 0, 'msg' => 'ok', 'data' => null]);
        }
        $order = Db::table('crm_client_order')->where('id', $draftId)->find();
        if (!$order) {
            return json(['code' => 0, 'msg' => 'ok', 'data' => null]);
        }
        $id = (int)$order['id'];
        // 只返回 add 表单用到的字段
        $formData = [
            'contact'           => $order['contact'] ?? '',
            'cname'             => $order['cname'] ?? '',
            'customer_type_flag'=> (int)($order['customer_type_flag'] ?? 0),
            'client_company'    => $order['client_company'] ?? '',
            'province'          => $order['province'] ?? '',
            'city'              => $order['city'] ?? '',
            'country'           => $order['country'] ?? '',
            'customer_type'     => $order['customer_type'] ?? '',
            'source'            => $order['source'] ?? '',
            'source_port'       => $order['source_port'] ?? '',
            'bank_account'      => $order['bank_account'] ?? '',
            'order_time'        => $order['order_time'] ?? '',
            'shipping_cost'     => $order['shipping_cost'] ?? '',
            'invoice_type'      => $order['invoice_type'] ?? '',
            'invoice_amount'    => $order['invoice_amount'] ?? '',
            'tax_amount'        => $order['tax_amount'] ?? '',
            'debugging_cost'    => $order['debugging_cost'] ?? '',
            'sales_commission'  => $order['sales_commission'] ?? '',
            'split_remarks'     => $order['split_remarks'] ?? '',
            'amount_received'   => $order['amount_received'] ?? '',
            'money'             => $order['money'] ?? '',
            'profit'            => $order['profit'] ?? '',
            'margin_rate'       => $order['margin_rate'] ?? '',
            'remark'            => $order['remark'] ?? '',
            'wechat_receipt_image' => $order['wechat_receipt_image'] ?? '',
            'inquiry_assign_image' => $order['inquiry_assign_image'] ?? '',
        ];
        // joint_person：库中可能是逗号分隔或 JSON，统一转为数组供前端 xmSelect
        $jp = $order['joint_person'] ?? '';
        if (is_string($jp) && $jp !== '') {
            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                $tmp = json_decode($jp, true);
                $formData['joint_person'] = is_array($tmp) ? $tmp : array_filter(explode(',', $jp));
            } else {
                $formData['joint_person'] = array_values(array_filter(array_map('trim', explode(',', $jp))));
            }
        } else {
            $formData['joint_person'] = [];
        }
        // source_port 存的是端口名称，add 页下拉是 id；若有端口表可反查 id，这里先传名称供回填
        $formData['source_port_text'] = $order['source_port'] ?? '';
        // 产品明细行
        $items = Db::name('crm_order_item')->where('order_id', $id)->order('line_no asc')->select();
        $formData['order_items'] = [];
        if ($items) {
            foreach ($items as $row) {
                $formData['order_items'][] = [
                    'product_id'     => $row['product_id'] ?? '',
                    'product_name'   => $row['product_name'] ?? '',
                    'manager_id'     => $row['manager_id'] ?? 0,
                    'spec_model'     => $row['spec_model'] ?? '',
                    'unit'           => $row['unit'] ?? '',
                    'qty'            => $row['qty'] ?? 0,
                    'unit_price'     => $row['unit_price'] ?? '',
                    'total_price'    => $row['total_price'] ?? '',
                    'purchase_price' => $row['purchase_price'] ?? '',
                    'sub_profit'     => $row['sub_profit'] ?? '',
                    'remark'         => $row['remark'] ?? '',
                ];
            }
        }
        return json(['code' => 0, 'msg' => 'ok', 'data' => ['id' => $id, 'formData' => $formData]]);
    }

    /**
     * 创建草稿：先找唯一草稿，有则返回 id（reused=1），没有才 insert；事务内再查一次防并发多草稿
     */
    public function createDraft()
    {
        $aid = $this->getDraftOwnerId();
        if ($aid <= 0) {
            return json(['code' => 1, 'msg' => '未登录']);
        }
        $draftId = $this->findUserDraftId($aid);
        if ($draftId > 0) {
            return json(['code' => 0, 'msg' => 'ok', 'draft_id' => $draftId, 'reused' => 1]);
        }
        $now = date('Y-m-d H:i:s');
        $currentUsername = Session::get('username') ?: '';
        $adminInfo = Db::name('admin')->where('admin_id', $aid)->field('team_name')->find();
        $teamName = ($adminInfo && !empty($adminInfo['team_name'])) ? $adminInfo['team_name'] : '';
        $data = [
            'check_status'       => 0,
            'status'             => '草稿',
            'create_time'        => $now,
            'first_create_time'  => $now,
            'ut_time'            => $now,
            'order_no'           => 'DR' . date('YmdHis') . rand(100, 999),
            'pr_user'            => $currentUsername,
            'pr_user_id'         => $aid,
            'at_user'            => $currentUsername,
            'at_user_id'         => $aid,
            // ★ 空草稿金额字段统一置 0，杜绝任何脏默认值
            'money'              => 0,
            'profit'             => 0,
            'margin_rate'        => 0,
            'source_port'        => '',
            'cname'              => '',
            'customer_type_flag' => 0,
            'team_name'          => $teamName,
            'bank_account'       => '',
            'bank_account_name'  => '',
        ];
        Db::startTrans();
        try {
            $draftId = $this->findUserDraftId($aid);
            if ($draftId > 0) {
                Db::commit();
                return json(['code' => 0, 'msg' => 'ok', 'draft_id' => $draftId, 'reused' => 1]);
            }
            $id = Db::name('crm_client_order')->insertGetId($data);
            if (!$id) {
                Db::rollback();
                return json(['code' => 1, 'msg' => '创建草稿失败']);
            }
            Db::commit();
            return json(['code' => 0, 'msg' => 'ok', 'draft_id' => (int)$id]);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 1, 'msg' => '创建草稿失败']);
        }
    }

    /**
     * 30 秒更新草稿：有 draft_id 更新；没传也能找唯一草稿或创建后更新；严禁改 check_status
     */
    public function autosaveDraft()
    {
        $aid = $this->getDraftOwnerId();
        if ($aid <= 0) {
            return json(['code' => 1, 'msg' => '未登录']);
        }
        $draftId = (int)Request::param('draft_id', 0);
        if ($draftId <= 0) {
            $draftId = $this->findUserDraftId($aid);
            if ($draftId <= 0) {
                Db::startTrans();
                try {
                    $draftId = $this->findUserDraftId($aid);
                    if ($draftId <= 0) {
                        $now = date('Y-m-d H:i:s');
                        $currentUsername = Session::get('username') ?: '';
                        $adminInfo = Db::name('admin')->where('admin_id', $aid)->field('team_name')->find();
                        $teamName = ($adminInfo && !empty($adminInfo['team_name'])) ? $adminInfo['team_name'] : '';
                        $dataInsert = [
                            'check_status' => 0, 'status' => '草稿', 'create_time' => $now,
                            'first_create_time' => $now, 'ut_time' => $now,
                            'order_no' => 'DR' . date('YmdHis') . rand(100, 999),
                            'pr_user' => $currentUsername, 'pr_user_id' => $aid,
                            'at_user' => $currentUsername, 'at_user_id' => $aid,
                            // ★ 空草稿：money/profit/margin_rate 统一置 0
                            'money' => 0, 'profit' => 0, 'margin_rate' => 0,
                            'source_port' => '', 'cname' => '', 'customer_type_flag' => 0,
                            'team_name' => $teamName, 'bank_account' => '', 'bank_account_name' => '',
                        ];
                        $newId = Db::name('crm_client_order')->insertGetId($dataInsert);
                        if ($newId) {
                            $draftId = (int)$newId;
                        }
                    }
                    Db::commit();
                } catch (\Exception $e) {
                    Db::rollback();
                    return json(['code' => 1, 'msg' => '创建草稿失败']);
                }
                if ($draftId <= 0) {
                    return json(['code' => 1, 'msg' => '创建草稿失败']);
                }
            }
        }
        $order = Db::table('crm_client_order')->where('id', $draftId)->find();
        if (!$order || (int)$order['check_status'] !== 0 || (int)($order['pr_user_id'] ?? 0) !== $aid) {
            $draftId = $this->findUserDraftId($aid);
            if ($draftId <= 0) {
                Db::startTrans();
                try {
                    $draftId = $this->findUserDraftId($aid);
                    if ($draftId <= 0) {
                        $now = date('Y-m-d H:i:s');
                        $currentUsername = Session::get('username') ?: '';
                        $adminInfo = Db::name('admin')->where('admin_id', $aid)->field('team_name')->find();
                        $teamName = ($adminInfo && !empty($adminInfo['team_name'])) ? $adminInfo['team_name'] : '';
                        $dataInsert = [
                            'check_status' => 0, 'status' => '草稿', 'create_time' => $now,
                            'first_create_time' => $now, 'ut_time' => $now,
                            'order_no' => 'DR' . date('YmdHis') . rand(100, 999),
                            'pr_user' => $currentUsername, 'pr_user_id' => $aid,
                            'at_user' => $currentUsername, 'at_user_id' => $aid,
                            // ★ 空草稿：money/profit/margin_rate 统一置 0
                            'money' => 0, 'profit' => 0, 'margin_rate' => 0,
                            'source_port' => '', 'cname' => '', 'customer_type_flag' => 0,
                            'team_name' => $teamName, 'bank_account' => '', 'bank_account_name' => '',
                        ];
                        $newId = Db::name('crm_client_order')->insertGetId($dataInsert);
                        if ($newId) {
                            $draftId = (int)$newId;
                        }
                    }
                    Db::commit();
                } catch (\Exception $e) {
                    Db::rollback();
                    return json(['code' => 1, 'msg' => '创建草稿失败']);
                }
            }
            if ($draftId <= 0) {
                return json(['code' => 1, 'msg' => '创建草稿失败']);
            }
        }
        $now = date('Y-m-d H:i:s');
        $data = [];
        $data['contact']          = Request::param('contact', '');
        $data['cname']            = Request::param('cname', '');
        $data['customer_type_flag'] = in_array(Request::param('customer_type_flag'), ['0', '1']) ? (int)Request::param('customer_type_flag') : 0;
        $data['client_company']   = Request::param('client_company', '');
        $data['province']         = Request::param('province', '');
        $data['city']             = Request::param('city', '');
        $data['country']          = Request::param('country', '');
        $data['customer_type']    = Request::param('customer_type', '');
        $data['source']           = Request::param('source', '');
        $data['oper_user']        = Request::param('oper_user', '');
        $data['bank_account']     = Request::param('bank_account', '');
        $sourcePortId = trim((string)Request::param('source_port', ''));
        $data['source_port'] = '';
        if ($sourcePortId !== '') {
            $isNumericId = ctype_digit($sourcePortId) || (is_numeric($sourcePortId) && (int)$sourcePortId > 0);
            if ($isNumericId) {
                $portInfo = Db::name('crm_inquiry_port')->where('id', $sourcePortId)->field('port_name')->find();
                if ($portInfo && !empty($portInfo['port_name'])) $data['source_port'] = $portInfo['port_name'];
            }
            if ($data['source_port'] === '' && $sourcePortId !== '') {
                $data['source_port'] = mb_substr($sourcePortId, 0, 100, 'UTF-8');
            }
        }
        $currentUsername = Session::get('username');
        $adminInfo = Db::name('admin')->where('username', $currentUsername)->field('team_name')->find();
        $data['team_name'] = ($adminInfo && !empty($adminInfo['team_name'])) ? $adminInfo['team_name'] : (Session::get('team_name') ?: '');
        $orderTime = trim((string)Request::param('order_time', ''));
        $data['order_time'] = $orderTime === '' ? null : $orderTime;
        $data['shipping_cost']    = Request::param('shipping_cost', '');
        $invoiceType = Request::param('invoice_type', '');
        $data['invoice_type'] = in_array($invoiceType, ['普票', '专票', '不开票']) ? $invoiceType : '';
        $data['invoice_amount']   = Request::param('invoice_amount', '');
        $data['tax_amount']       = Request::param('tax_amount', '');
        $data['debugging_cost']   = Request::param('debugging_cost', '');
        $data['sales_commission'] = Request::param('sales_commission', '');
        $data['split_remarks']    = Request::param('split_remarks', '');
        $data['amount_received']  = Request::param('amount_received', '');
        // 询盘来源凭证：支持多图 JSON、显式清空、数量限制
        $clearInq = (int)Request::param('clear_inquiry_assign_image', 0);
        if ($clearInq === 1) {
            $data['inquiry_assign_image'] = '';
        } elseif (request()->has('inquiry_assign_image')) {
            $handledInquiryAssignImage = OrderService::handleInquiryImages(Request::param('inquiry_assign_image', ''));
            $inquiryAssignUrls = json_decode($handledInquiryAssignImage, true);
            if (!is_array($inquiryAssignUrls)) {
                $inquiryAssignUrls = [];
            }
            if (count($inquiryAssignUrls) > 10) {
                return json(['code' => 1, 'msg' => '询盘来源凭证图片数量不能超过 10 张']);
            }
            $data['inquiry_assign_image'] = $handledInquiryAssignImage;
        }
        // 微信沟通凭证：支持显式清空，空值不覆盖数据库已有图片
        $wr = Request::param('wechat_receipt_image', null);
        $clearWr = (int)Request::param('clear_wechat_receipt_image', 0);
        if ($clearWr === 1) {
            $data['wechat_receipt_image'] = '';
        } else {
            $wechatReceiptUrls = [];
            if (is_array($wr)) {
                $wechatReceiptUrls = array_values(array_filter($wr, function ($v) { return trim((string)$v) !== ''; }));
            } elseif (is_string($wr)) {
                $wr = trim($wr);
                if ($wr !== '') {
                    if (isset($wr[0]) && $wr[0] === '[') {
                        $tmp = json_decode($wr, true);
                        if (is_array($tmp)) {
                            $wechatReceiptUrls = array_values(array_filter($tmp, function ($v) { return trim((string)$v) !== ''; }));
                        } else {
                            $wechatReceiptUrls = [$wr];
                        }
                    } else {
                        $wechatReceiptUrls = [$wr];
                    }
                }
            }
            if (!empty($wechatReceiptUrls)) {
                $data['wechat_receipt_image'] = json_encode($wechatReceiptUrls, JSON_UNESCAPED_UNICODE);
            }
        }
        $jpRaw = Request::param('joint_person');
        $jpIds = [];
        if (is_array($jpRaw)) {
            $jpIds = $jpRaw;
        } elseif (is_string($jpRaw)) {
            $jpRaw = trim($jpRaw);
            if ($jpRaw !== '') {
                if (isset($jpRaw[0]) && $jpRaw[0] === '[') {
                    $tmp = json_decode($jpRaw, true);
                    if (is_array($tmp)) $jpIds = $tmp;
                } else {
                    $jpIds = explode(',', $jpRaw);
                }
            }
        }
        $jpIds = array_values(array_unique(array_filter(array_map(function ($v) {
            return preg_replace('/\D/', '', (string)$v);
        }, $jpIds), function ($v) { return $v !== ''; })));
        $data['joint_person'] = implode(',', $jpIds);
        if (!empty($data['bank_account'])) {
            $data['bank_account_name'] = $this->resolveBankAccountName($data['bank_account']);
        } else {
            $data['bank_account_name'] = '';
        }
        $productIds = Request::param('product_name/a');
        $specModels = Request::param('spec_model/a');
        $units = Request::param('unit/a');
        $qtys = Request::param('qty/a');
        $unitPrices = Request::param('unit_price/a');
        $purchasePrices = Request::param('purchase_price/a');
        $itemRemarks = Request::param('item_remark/a');
        $managerIds = Request::param('product_manager/a');
        $idArr = [];
        if (!empty($productIds) && is_array($productIds)) {
            foreach ($productIds as $pid) {
                $pid = (int)$pid;
                if ($pid > 0) $idArr[] = $pid;
            }
            $idArr = array_values(array_unique($idArr));
        }
        $prodMap = []; $supIdMap = []; $supNameMap = [];
        if (!empty($idArr)) {
            $rows = Db::name('crm_products')->alias('p')
                ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                ->where('p.id', 'in', $idArr)
                ->field('p.id, p.product_name, p.category_id, c.category_name')
                ->select();
            foreach ($rows as $r) {
                $prodMap[$r['id']] = $r['product_name'];
                $supIdMap[$r['id']] = $r['category_id'] ?? 0;
                $supNameMap[$r['id']] = $r['category_name'] ?? '';
            }
        }
        // 统一由 Service 构造明细行 + 主表金额（与 add/edit/details 同一口径）
        $saveBundle = OrderService::buildOrderSaveData(
            [
                'product_ids'      => $productIds,
                'manager_ids'      => $managerIds,
                'spec_models'      => $specModels,
                'units'            => $units,
                'qty'              => $qtys,
                'unit_price'       => $unitPrices,
                'purchase_price'   => $purchasePrices,
                'item_remarks'     => $itemRemarks,
                'shipping_cost'    => Request::param('shipping_cost', 0),
                'tax_amount'       => Request::param('tax_amount', 0),
                'debugging_cost'   => Request::param('debugging_cost', 0),
                'sales_commission' => Request::param('sales_commission', 0),
            ],
            $prodMap,
            $supIdMap,
            $supNameMap,
            $draftId
        );
        $itemsData = $saveBundle['items'];

        // ★ 防御性补丁：草稿自动保存同样剥掉 profit / margin_rate / money，
        // 确保只有 Service 的计算结果能写进主表，杜绝前端污染。
        unset($data['profit'], $data['margin_rate'], $data['money']);

        $data['money']       = $saveBundle['money'];
        $data['profit']      = $saveBundle['profit'];
        $data['margin_rate'] = $saveBundle['margin_rate'];

        // 主表 product_name 摘要（草稿若无产品行，也写空串保持与旧行为一致）
        $data['product_name'] = $saveBundle['product_name_summary'];
        $data['ut_time'] = $now;
        $data['oper_user'] = Session::get('username');
        $data['status'] = '草稿';
        // 严禁修改 check_status，不写入 $data
        Db::startTrans();
        try {
            Db::name('crm_client_order')->where('id', $draftId)->update($data);
            Db::name('crm_order_item')->where('order_id', $draftId)->delete();
            if (!empty($itemsData)) {
                Db::name('crm_order_item')->insertAll($itemsData);
            }
            Db::commit();
            return json(['code' => 0, 'msg' => 'ok', 'draft_id' => $draftId]);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 1, 'msg' => 'autosave_failed']);
        }
    }

    /**
     * 删除草稿：传 id 时校验 owner=aid 且 check_status=0；未传 id 则删 findUserDraftId 唯一草稿
     */
    public function deleteDraft()
    {
        $aid = $this->getDraftOwnerId();
        if ($aid <= 0) {
            return json(['code' => 1, 'msg' => '未登录']);
        }
        $id = (int)Request::param('id', 0);
        if ($id <= 0) {
            $id = (int)Request::param('draft_id', 0);
        }
        if ($id <= 0) {
            $id = $this->findUserDraftId($aid);
        }
        if ($id <= 0) {
            return json(['code' => 0, 'msg' => '已删除']);
        }
        $order = Db::table('crm_client_order')->where('id', $id)->find();
        if (!$order || (int)$order['check_status'] !== 0 || (int)($order['pr_user_id'] ?? 0) !== $aid) {
            return json(['code' => 1, 'msg' => '无权限操作该草稿']);
        }
        Db::startTrans();
        try {
            Db::table('crm_order_item')->where('order_id', $id)->delete();
            Db::table('crm_client_order')->where('id', $id)->delete();
            Db::commit();
            return json(['code' => 0, 'msg' => '已删除']);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 1, 'msg' => '删除失败']);
        }
    }

    // 协同人订单搜索接口（已改造为只展示协同人订单）
    public function collaboratorClientSearch()
    {
        $where = [];
        
        // 获取当前登录用户的 admin_id
        $aid = (int)Session::get('aid');
        
        // 协同人过滤：只取"我在 joint_person 里"的订单
        $where[] = ['joint_person', '<>', ''];
        $where[] = Db::raw("FIND_IN_SET($aid, joint_person)");
        
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        // 筛选条件复用现有的 keyword 过滤逻辑
        $where[] = ['check_status', '=', 2];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        if (isset($keyword['timebucket'])) {
            // 方案A：timebucket 仅保留订单表字段 order_time 的筛选
            $where[] = $this->buildTimeWhere($keyword['timebucket'], 'order_time');
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
        }
        
        // 列表查询保持不变（crm_client_order）
        $list = Db::table('crm_client_order')
            ->where($where)
            ->order('create_time desc,id desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();
        
        // 收集所有需要查询的admin_id（协同人）
        $allAdminIds = [];
        foreach ($list['data'] as $order) {
            // 收集协同人ID
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (is_numeric($id)) {
                        $allAdminIds[] = $id;
                    }
                }
            }
        }
        $allAdminIds = array_unique($allAdminIds);

        // 批量查询admin表获取用户名映射
        $adminMap = [];
        if (!empty($allAdminIds)) {
            $admins = Db::name('admin')
                ->whereIn('admin_id', $allAdminIds)
                ->column('username', 'admin_id');
            $adminMap = $admins;
        }

        // 【订单快照模式】查询订单对应的产品明细，统一使用快照字段
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = $this->buildOrderItemsSnapshotMap($orderIds);
        
        // 转换收款账户ID为账户名称 和 协同人ID为用户名
        foreach ($list['data'] as &$order) {
            // 转换收款账户
            if (!empty($order['bank_account'])) {
                $accountInfo = Db::name('crm_receive_account')
                    ->where('id', $order['bank_account'])
                    ->field('account')
                    ->find();
                if ($accountInfo) {
                    $order['bank_account_name'] = $accountInfo['account'];
                }
            }
            
            // 转换协同人ID为用户名
            if (!empty($order['joint_person'])) {
                $ids = explode(',', $order['joint_person']);
                $names = [];
                foreach ($ids as $id) {
                    $id = trim($id);
                    if (isset($adminMap[$id])) {
                        $names[] = $adminMap[$id];
                    }
                }
                if (!empty($names)) {
                    $order['joint_person_names'] = implode(',', $names);
                }
            }

            // 绑定订单的产品明细（快照数据）
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            // 如果订单主表的 product_name 为空，从明细快照中取第一个产品名称
            if (empty($order['product_name']) && !empty($order['order_items'])) {
                $order['product_name'] = $order['order_items'][0]['product_name'] ?? '';
            }
        }
        unset($order);

        // 统计字段：只保留金额/利润/订单数（方案A不需要询盘数和成单率）
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        
        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }

    public function pendingClientSearch()
    {
        $where = [];
        $client_where = [];
        $aid = Session::get('aid');
        $pr_user = Session::get('username') ?? '';
        
        // 判断是否"可看全部"
        $groupId = Db::name('admin')->where('admin_id', $aid)->value('group_id');
        $canViewAll = ($aid == 1) || in_array(intval($groupId), [13, 15]);
        
        // 如果 $pr_user 为空：直接让 where 返回空（保持现有逻辑 id=0）
        if (empty($pr_user)) {
            $where[] = ['id', '=', 0];
        } else {
            // 如果不是 $canViewAll（普通用户）：在 $where 里追加闭包条件，限制只能看到自己的订单
            if (!$canViewAll) {
                $where[] = function($q) use ($pr_user) {
                    $q->where('pr_user', '=', $pr_user)
                      ->whereOr('at_user', '=', $pr_user);
                };
                // $client_where 维持只统计自己
                $client_where[] = ['pr_user', '=', $pr_user];
            }
            // 如果是 $canViewAll（aid=1 或 group_id=13/15）：不要加 pr_user/at_user 限制，$client_where 也不要加 pr_user 限制
        }
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        // 表头排序：只允许白名单字段和 asc/desc，否则默认 create_time desc, id desc
        $sortField = input('field/s', '');
        $sortOrder = input('order/s', '');
        $sortFieldWhitelist = ['order_no', 'cname', 'contact', 'money', 'profit', 'margin_rate', 'order_time', 'create_time'];
        $sortOrderWhitelist = ['asc', 'desc'];
        $numericSortFields = ['money', 'profit', 'margin_rate'];

        $sortOrderLower = strtolower($sortOrder);
        $isFieldAllowed = $sortField !== '' && in_array($sortField, $sortFieldWhitelist, true);
        $isOrderAllowed = $sortOrderLower !== '' && in_array($sortOrderLower, $sortOrderWhitelist, true);
        $isNumericField = $isFieldAllowed && in_array($sortField, $numericSortFields, true);

        $where[] = ['check_status', '=', 1];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        if (isset($keyword['timebucket'])) {
            $where[] = $this->buildTimeWhere($keyword['timebucket'], 'order_time');

            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
            // $client_where[] = ['kh_name', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        // 构造基础查询
        $query = Db::table('crm_client_order')->where($where);

        // 应用排序：数值字段使用 CAST + orderRaw，普通字段使用链式 order
        if ($isFieldAllowed && $isOrderAllowed) {
            $sortOrderUpper = strtoupper($sortOrderLower); // ASC / DESC
            if ($isNumericField) {
                // 数值字段：按数值排序，避免字符串比较；字段名来自白名单，避免注入
                $query->orderRaw("CAST(`{$sortField}` AS DECIMAL(18,2)) {$sortOrderUpper}, id DESC");
            } else {
                // 普通字段：正常字段排序，再按 id 降序保证稳定性
                $query->order($sortField, $sortOrderLower)->order('id', 'desc');
            }
        } else {
            // 非法字段或方向：使用默认排序
            $query->order('create_time', 'desc')->order('id', 'desc');
        }

        $list = $query->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])->toArray();

        // 本页订单ID，批量查 crm_order_item 聚合成 item_* 多行字符串（避免 N+1）
        $orderIds = array_column($list['data'], 'id');
        $itemMap = [];
        if (!empty($orderIds)) {
            $itemRows = Db::table('crm_order_item')
                ->whereIn('order_id', $orderIds)
                ->order('line_no asc, id asc')
                ->field('order_id, product_name, spec_model, unit, qty, unit_price, total_price, purchase_price, sub_profit, supplier_name, manager_id')
                ->select();
            $managerIds = array_unique(array_filter(array_column($itemRows, 'manager_id')));
            $managerMap = [];
            if (!empty($managerIds)) {
                $managerMap = Db::name('admin')->whereIn('admin_id', $managerIds)->column('username', 'admin_id');
            }
            foreach ($itemRows as $row) {
                $row['product_manager'] = isset($managerMap[$row['manager_id']]) ? $managerMap[$row['manager_id']] : '';
                $itemMap[$row['order_id']][] = $row;
            }
        }
        
        // 协同人：收集 joint_person 中的 admin_id，批量查 admin 表得到 username 映射
        $allAdminIds = [];
        foreach ($list['data'] as $order) {
            if (!empty($order['joint_person'])) {
                $ids = array_filter(array_map('trim', explode(',', $order['joint_person'])));
                foreach ($ids as $id) {
                    if (is_numeric($id)) $allAdminIds[] = $id;
                }
            }
        }
        $allAdminIds = array_unique($allAdminIds);
        $adminMap = [];
        if (!empty($allAdminIds)) {
            $adminMap = Db::name('admin')->whereIn('admin_id', $allAdminIds)->column('username', 'admin_id');
        }
        
        foreach ($list['data'] as &$order) {
            $rows = $itemMap[$order['id']] ?? [];
            // 待审核列表：补充前端预览图结构字段，不影响原有图片字段
            $order['wechat_images'] = OrderService::buildPreviewImageItems($order['wechat_receipt_image'] ?? '');
            $order['inquiry_images'] = OrderService::buildPreviewImageItems($order['inquiry_assign_image'] ?? '');
            // 与图片字段一一对应的列宽，供前端表格动态渲染
            $order['wechat_width'] = OrderService::calcImageColumnWidth($order['wechat_receipt_image'] ?? '');
            $order['inquiry_width'] = OrderService::calcImageColumnWidth($order['inquiry_assign_image'] ?? '');
            // 回填子表聚合字段（多行用 \n 连接，前端 renderMultiline 换行展示）
            $order['item_product_name'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['product_name'] ?? ''); }, $rows)) : '';
            $order['item_spec_model'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['spec_model'] ?? ''); }, $rows)) : '';
            $order['item_unit'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['unit'] ?? ''); }, $rows)) : '';
            $order['item_qty'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['qty'] ?? ''); }, $rows)) : '';
            $order['item_unit_price'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['unit_price'] ?? ''); }, $rows)) : '';
            $order['item_total_price'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['total_price'] ?? ''); }, $rows)) : '';
            $order['item_purchase_price'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['purchase_price'] ?? ''); }, $rows)) : '';
            $order['item_sub_profit'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['sub_profit'] ?? ''); }, $rows)) : '';
            $order['item_supplier_name'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['supplier_name'] ?? ''); }, $rows)) : '';
            $order['item_product_manager'] = $rows ? implode("\n", array_map(function ($r) { return (string)($r['product_manager'] ?? ''); }, $rows)) : '';

            // 主表 product_name 为空时，用本页已查出的明细第一行产品名（不再 N+1 查库）
            if (empty($order['product_name']) && !empty($rows)) {
                foreach ($rows as $r) {
                    if (!empty($r['product_name'])) {
                        $order['product_name'] = $r['product_name'];
                        break;
                    }
                }
            }
            
            // 转换收款账户ID为账户名称
            if (!empty($order['bank_account'])) {
                $accountInfo = Db::name('crm_receive_account')
                    ->where('id', $order['bank_account'])
                    ->field('account')
                    ->find();
                if ($accountInfo) {
                    $order['bank_account_name'] = $accountInfo['account'];
                }
            }
            
            // 协同人ID转 username（assist_username 供前端“协同人”列展示，空则前端显示 --）
            if (!empty($order['joint_person'])) {
                $names = [];
                foreach (array_filter(array_map('trim', explode(',', $order['joint_person']))) as $id) {
                    if (is_numeric($id) && isset($adminMap[$id])) $names[] = $adminMap[$id];
                }
                $order['assist_username'] = $names ? implode(',', $names) : '';
            }
        }
        unset($order);


        //成单率

        $totalInquiries = Db::table('crm_leads')->where('status', 1)->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        return $result = [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }


    public function failedClientSearch()
    {
        $where = [];
        $client_where = [];
        $aid = Session::get('aid');
        $pr_user = Session::get('username') ?? '';
        
        // 判断是否"可看全部"
        $groupId = Db::name('admin')->where('admin_id', $aid)->value('group_id');
        $canViewAll = ($aid == 1) || in_array(intval($groupId), [13, 15]);
        
        // 如果 $pr_user 为空：直接让 where 返回空（保持现有逻辑 id=0）
        if (empty($pr_user)) {
            $where[] = ['id', '=', 0];
        } else {
            // 如果不是 $canViewAll（普通用户）：在 $where 里追加闭包条件，限制只能看到自己的订单
            if (!$canViewAll) {
                $where[] = function($q) use ($pr_user) {
                    $q->where('pr_user', '=', $pr_user)
                      ->whereOr('at_user', '=', $pr_user);
                };
                // $client_where 维持只统计自己
                $client_where[] = ['pr_user', '=', $pr_user];
            }
            // 如果是 $canViewAll（aid=1 或 group_id=13/15）：不要加 pr_user/at_user 限制，$client_where 也不要加 pr_user 限制
        }
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        $where[] = ['check_status', '=', 3];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        if (isset($keyword['timebucket'])) {
            $where[] = $this->buildTimeWhere($keyword['timebucket'], 'order_time');

            $timeWhere['at_time'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            $client_where[] =  function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }
        if (isset($keyword['min_money'])) $where[] = ['money', '>', $keyword['min_money']];
        if (isset($keyword['max_money'])) $where[] = ['money', '<', $keyword['max_money']];
        if (isset($keyword['min_profit'])) $where[] = ['profit', '>', $keyword['min_profit']];
        if (isset($keyword['max_profit'])) $where[] = ['profit', '<', $keyword['max_profit']];
        if (isset($keyword['min_margin_rate'])) $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        if (isset($keyword['max_margin_rate'])) $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
            // $client_where[] = ['kh_name', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        $list = Db::table('crm_client_order')
            ->where($where)
            ->order('order_time desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();
        
        // 如果订单主表的 product_name 为空，尝试从订单明细表中获取产品名称
        // 这样可以确保即使产品被删除，订单的产品名称仍然可以显示
        foreach ($list['data'] as &$order) {
            if (empty($order['product_name'])) {
                $firstItem = Db::name('crm_order_item')
                    ->where('order_id', $order['id'])
                    ->where('product_name', '<>', '')
                    ->order('line_no asc')
                    ->field('product_name')
                    ->find();
                if ($firstItem && !empty($firstItem['product_name'])) {
                    $order['product_name'] = $firstItem['product_name'];
                }
            }
            
            // 转换收款账户ID为账户名称
            if (!empty($order['bank_account'])) {
                $accountInfo = Db::name('crm_receive_account')
                    ->where('id', $order['bank_account'])
                    ->field('account')
                    ->find();
                if ($accountInfo) {
                    $order['bank_account_name'] = $accountInfo['account'];
                }
            }
        }
        unset($order);


        //成单率

        $totalInquiries = Db::table('crm_leads')->where('status', 1)->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->getSum($where, 'money');
        $totalProfit = $this->getSum($where, 'profit');
        return $result = [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }



    // 获取订单明细数据（用于导出）
    public function getOrderItems()
    {
        $orderIds = Request::param('order_ids/a', []);
        if (empty($orderIds)) {
            return json(['code' => 0, 'msg' => '参数错误', 'data' => []]);
        }
        
        // 【订单快照模式】统一使用快照聚合方法，只从 crm_order_item 读取快照字段
        $orderItemsMap = $this->buildOrderItemsSnapshotMap($orderIds);
        
        // 将 map 转换为扁平数组返回
        $items = [];
        foreach ($orderItemsMap as $orderId => $orderItems) {
            foreach ($orderItems as $item) {
                // 为了兼容前端，将 supplier_name 也映射为 supplier
                $item['supplier'] = $item['supplier_name'] ?? '';
                $items[] = $item;
            }
        }
        
        return json(['code' => 0, 'msg' => '获取成功', 'data' => $items]);
    }

    // 新增：根据关键词通过启信开放平台模糊搜索企业名称
    public function searchCompany()
    {
        // 获取关键词参数
        $keyword = Request::param('keyword', '');
        $keyword = trim($keyword);
        
        // 调试模式：如果传入debug=1参数，返回原始API响应
        $debug = Request::param('debug', 0);
        
        if (empty($keyword)) {
            // 如果关键词为空，返回空列表
            return json(['code' => 0, 'msg' => '无关键词', 'data' => []]);
        }
        
        // 如果关键词长度小于2，返回空列表
        if (mb_strlen($keyword) < 2) {
            return json(['code' => 0, 'msg' => '关键词至少需要2个字符', 'data' => []]);
        }
        
        try {
            // 签名验证：获取当前时间戳并生成签名 Token
            $appkey = config('qixin_appkey');
            $secretKey = config('qixin_secret_key');
            
            // 检查配置是否存在
            if (empty($appkey) || empty($secretKey)) {
                return json(['code' => 500, 'msg' => '启信API配置缺失，请联系管理员', 'data' => []]);
            }
            
            // 生成毫秒级时间戳（13位）
            $timestamp = round(microtime(true) * 1000);
            
            // 生成签名：md5(appkey + timestamp + secret_key)，注意是不带+号的字符串拼接
            $sign = md5($appkey . $timestamp . $secretKey);
            
            // 构造启信开放平台 API 请求 URL（只有keyword参数在URL中）
            $url = "https://api.qixin.com/APIService/v2/search/advSearch?keyword=" . urlencode($keyword);
            
            // 使用 cURL 发起 GET 请求
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $url);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_TIMEOUT, 10); // 增加超时时间到10秒
            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5); // 连接超时5秒
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); // 如果SSL证书有问题，可以设置为false
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
            
            // 设置HTTP Headers（根据文档要求）
            curl_setopt($ch, CURLOPT_HTTPHEADER, [
                'Auth-Version: 2.0',  // 固定值2.0
                'appkey: ' . $appkey,
                'timestamp: ' . $timestamp,
                'sign: ' . $sign,
                'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ]);
            
            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            
            if (curl_errno($ch)) {
                // 请求失败，返回错误信息
                $error = curl_error($ch);
                curl_close($ch);
                return json(['code' => 500, 'msg' => "启信API请求失败: {$error}", 'data' => []]);
            }
            
            curl_close($ch);
            
            // 检查HTTP状态码
            if ($httpCode != 200) {
                return json(['code' => 500, 'msg' => "启信API返回错误，HTTP状态码: {$httpCode}", 'data' => []]);
            }
            
            // 解析 JSON 响应数据
            $data = json_decode($response, true);
            if ($data === null && json_last_error() !== JSON_ERROR_NONE) {
                // 调试：记录原始响应
                \think\facade\Log::write('启信API原始响应: ' . $response, 'error');
                return json(['code' => 500, 'msg' => '启信API返回数据格式错误: ' . json_last_error_msg(), 'data' => []]);
            }
            
            // 调试：记录解析后的数据结构（仅在调试模式下）
            if (config('app.app_debug')) {
                \think\facade\Log::write('启信API响应数据: ' . json_encode($data, JSON_UNESCAPED_UNICODE), 'info');
            }
            
            // 检查API返回的错误
            if (isset($data['code']) && $data['code'] != 0) {
                $errorMsg = isset($data['message']) ? $data['message'] : (isset($data['msg']) ? $data['msg'] : '未知错误');
                return json(['code' => 500, 'msg' => "启信API错误: {$errorMsg}", 'data' => []]);
            }
            
            // 检查是否有status字段且不为成功（根据文档，status是字符串类型）
            if (isset($data['status'])) {
                $status = (string)$data['status'];
                // status为"200"或"0"表示成功
                if ($status !== '200' && $status !== '0') {
                    $errorMsg = isset($data['message']) ? $data['message'] : (isset($data['msg']) ? $data['msg'] : '查询失败');
                    return json(['code' => 500, 'msg' => "启信API错误: {$errorMsg} (status: {$status})", 'data' => []]);
                }
            }
            
            // 提取企业名称列表 - 根据文档，数据在 data.items 中
            $nameList = [];
            
            // 方式1: 检查 data.items 字段（根据文档的标准格式）
            if (isset($data['data']['items']) && is_array($data['data']['items'])) {
                foreach ($data['data']['items'] as $item) {
                    if (isset($item['name']) && !empty(trim($item['name']))) {
                        $nameList[] = trim($item['name']);
                    }
                }
            }
            
            // 方式2: 兼容直接 items 字段（数组格式）
            if (empty($nameList) && isset($data['items']) && is_array($data['items'])) {
                foreach ($data['items'] as $item) {
                    // 支持多种字段名：name, companyName, entName, enterpriseName
                    $companyName = '';
                    if (isset($item['name'])) {
                        $companyName = $item['name'];
                    } elseif (isset($item['companyName'])) {
                        $companyName = $item['companyName'];
                    } elseif (isset($item['entName'])) {
                        $companyName = $item['entName'];
                    } elseif (isset($item['enterpriseName'])) {
                        $companyName = $item['enterpriseName'];
                    } elseif (isset($item['title'])) {
                        $companyName = $item['title'];
                    }
                    
                    if (!empty(trim($companyName))) {
                        $nameList[] = trim($companyName);
                    }
                }
            }
            
            // 方式2: 检查 data 字段（如果items为空）
            if (empty($nameList) && isset($data['data']) && is_array($data['data'])) {
                foreach ($data['data'] as $item) {
                    $companyName = '';
                    if (isset($item['name'])) {
                        $companyName = $item['name'];
                    } elseif (isset($item['companyName'])) {
                        $companyName = $item['companyName'];
                    } elseif (isset($item['entName'])) {
                        $companyName = $item['entName'];
                    } elseif (isset($item['enterpriseName'])) {
                        $companyName = $item['enterpriseName'];
                    } elseif (isset($item['title'])) {
                        $companyName = $item['title'];
                    }
                    
                    if (!empty(trim($companyName))) {
                        $nameList[] = trim($companyName);
                    }
                }
            }
            
            // 方式3: 检查 result 字段
            if (empty($nameList) && isset($data['result']) && is_array($data['result'])) {
                foreach ($data['result'] as $item) {
                    $companyName = '';
                    if (isset($item['name'])) {
                        $companyName = $item['name'];
                    } elseif (isset($item['companyName'])) {
                        $companyName = $item['companyName'];
                    } elseif (isset($item['entName'])) {
                        $companyName = $item['entName'];
                    } elseif (isset($item['enterpriseName'])) {
                        $companyName = $item['enterpriseName'];
                    } elseif (isset($item['title'])) {
                        $companyName = $item['title'];
                    }
                    
                    if (!empty(trim($companyName))) {
                        $nameList[] = trim($companyName);
                    }
                }
            }
            
            // 方式4: 如果data本身就是数组
            if (empty($nameList) && is_array($data) && isset($data[0])) {
                foreach ($data as $item) {
                    if (is_array($item)) {
                        $companyName = '';
                        if (isset($item['name'])) {
                            $companyName = $item['name'];
                        } elseif (isset($item['companyName'])) {
                            $companyName = $item['companyName'];
                        } elseif (isset($item['entName'])) {
                            $companyName = $item['entName'];
                        } elseif (isset($item['enterpriseName'])) {
                            $companyName = $item['enterpriseName'];
                        } elseif (isset($item['title'])) {
                            $companyName = $item['title'];
                        }
                        
                        if (!empty(trim($companyName))) {
                            $nameList[] = trim($companyName);
                        }
                    }
                }
            }
            
            // 去重
            $nameList = array_unique($nameList);
            $nameList = array_values($nameList);
            
            // 限制返回数量，最多返回20条
            $nameList = array_slice($nameList, 0, 20);
            
            // 如果仍然没有数据，记录调试信息
            if (empty($nameList) && config('app.app_debug')) {
                \think\facade\Log::write('启信API未找到企业数据，响应结构: ' . json_encode($data, JSON_UNESCAPED_UNICODE), 'info');
            }
            
            // 调试模式：返回原始响应数据
            if ($debug == 1) {
                return json([
                    'code' => 0,
                    'msg' => '调试模式',
                    'data' => $nameList,
                    'debug' => [
                        'keyword' => $keyword,
                        'api_url' => $url,
                        'headers' => [
                            'Auth-Version' => '2.0',
                            'appkey' => $appkey,
                            'timestamp' => $timestamp,
                            'sign' => $sign
                        ],
                        'raw_response' => $response,
                        'parsed_data' => $data,
                        'http_code' => $httpCode
                    ]
                ]);
            }
            
            // 返回企业名称列表（JSON 格式）
            return json(['code' => 0, 'msg' => empty($nameList) ? '未找到匹配的企业' : '获取成功', 'data' => $nameList]);
            
        } catch (\Exception $e) {
            // 捕获异常
            return json(['code' => 500, 'msg' => '搜索企业名称时发生异常: ' . $e->getMessage(), 'data' => []]);
        }
    }

    
}
