<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use think\Container;

class Order extends Common
{

    const CUSTOMER_TYPE = [
        '终端用户',
        '经销商/批发商',
        '采购商',
        '零售商',
        '采购代理商',
    ];

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



    // 新建订单第3版
    public function add()
    {
        if (request()->isPost()) {
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
                        if (!$isMyCustomer && !$isCollaboratorCustomer) {
                            return fail('该客户不属于您的客户或协同人客户，无法添加订单');
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
                return json(['code' => -200, 'msg' => '客户名称不能为空，请先输入联系方式并验证客户信息']);
            }
            
            // 用户属性：0=公司，1=个人
            $data['customer_type_flag'] = Request::param('customer_type_flag', 0);
            $data['customer_type_flag'] = in_array($data['customer_type_flag'], ['0', '1']) ? (int)$data['customer_type_flag'] : 0;
            
            // 客户公司：如果是公司（0）则必填，如果是个人（1）则可以为空
            $clientCompany = Request::param('client_company', '');
            if ($data['customer_type_flag'] == 0) {
                // 公司类型，客户公司必填
                if (empty(trim($clientCompany))) {
                    return json(['code' => -200, 'msg' => '选择"公司"时，客户公司为必填项']);
                }
                $data['client_company'] = trim($clientCompany);
            } else {
                // 个人类型，客户公司可以为空
                $data['client_company'] = '';
            }
            
            $data['province']         = Request::param('province', '');  // 省份
            $data['city']             = Request::param('city', '');       // 城市
            $data['country']          = Request::param('country');        // 发货地址
            $data['customer_type']    = Request::param('customer_type');  // 客户性质
            $data['source']           = Request::param('source');         // 询盘来源（运营渠道，存储为文字）
            $data['pr_user']          = Request::param('pr_user') ?: Session::get('username');
            $data['oper_user']        = Request::param('oper_user');      // 运营人员
            $data['bank_account']     = Request::param('bank_account');   // 收款账户
            
            // 处理运营端口：将端口ID转换为端口名称（文字）保存
            $sourcePortId = Request::param('source_port', '');
            $data['source_port'] = '';  // 默认为空
            if (!empty($sourcePortId)) {
                // 从 crm_inquiry_port 表获取端口名称
                $portInfo = Db::name('crm_inquiry_port')
                    ->where('id', $sourcePortId)
                    ->field('port_name')
                    ->find();
                if ($portInfo && !empty($portInfo['port_name'])) {
                    $data['source_port'] = $portInfo['port_name'];  // 保存端口名称（文字）
                }
            }
            $data['team_name']        = Request::param('team_name');      // 团队名称
            $data['at_user']          = Session::get('username');         // 创建人
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
            $data['wechat_receipt_image'] = Request::param('wechat_receipt_image', ''); // 客户微信回执图
            $data['inquiry_assign_image'] = Request::param('inquiry_assign_image', ''); // 产品询盘分配图
            $managerIds   = Request::param('product_manager/a'); // ★ 产品经理（管理员）ID 数组
            $data['status']           = '待审核';
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

            // 1次查询构建 id => 产品名称 的映射
            // 注意：这里不过滤 status，因为历史订单可能引用已删除的产品，需要保留产品名称
            $idNameMap = [];
            if (!empty($idArr)) {
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    $idNameMap[$r['id']] = $r['product_name']; // 也可拼分类：$r['product_name'].' ('.$r['category_name'].')'
                }
            }

            // 计算并组装明细数据
            $sumTotal = 0;
            $sumProfit = 0;
            $itemsData = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $index => $pid) {
                    $pid = (int)$pid;
                    if ($pid <= 0) continue;  // 跳过空行
                    // 产品名称文本
                    $pnameText = $idNameMap[$pid] ?? '';
                    // 获取当前行的数量、价格、成本
                    $qty    = isset($qtys[$index]) ? floatval($qtys[$index]) : 0;
                    $price  = isset($unitPrices[$index]) ? floatval($unitPrices[$index]) : 0;
                    $purchase = isset($purchasePrices[$index]) ? floatval($purchasePrices[$index]) : 0;
                    // 计算行合计和利润
                    $lineTotal  = round($qty * $price, 2);
                    $lineProfit = round($lineTotal - $purchase, 2);
                    $sumTotal  += $lineTotal;
                    $sumProfit += $lineProfit;
                    // 获取当前行的产品经理ID（如未选择则默认为0）
                    $managerId = 0;
                    if (!empty($managerIds[$index])) {
                        $managerId = intval($managerIds[$index]);
                    }
                    // 组装该行明细数组，包括 manager_id 字段
                    $itemsData[] = [
                        'order_id'       => 0,                   // 稍后插入主表后会回填
                        'line_no'        => $index + 1,          // 行号
                        'product_id'     => (string)$pid,        // 产品ID
                        'product_name'   => $pnameText,          // 产品名称文本
                        'spec_model'     => $specModels[$index] ?? '',
                        'unit'           => $units[$index] ?? '',
                        'qty'            => (int)$qty,
                        'unit_price'     => number_format($price, 2, '.', ''),    // 保留两位小数的字符串
                        'total_price'    => number_format($lineTotal, 2, '.', ''),
                        'purchase_price' => number_format($purchase, 2, '.', ''),
                        'sub_profit'     => number_format($lineProfit, 2, '.', ''),
                        'remark'         => $itemRemarks[$index] ?? '',
                        'manager_id'     => $managerId           // ★ 新增：产品经理（管理员）ID
                    ];
                }
            }

            // 计算订单总金额（已存在逻辑）
            $data['money'] = round($sumTotal, 2);

            // 将费用字段从字符串转换为浮点数，然后计算最终利润
            $shippingCost    = floatval(Request::param('shipping_cost'));
            $taxAmount       = floatval(Request::param('tax_amount'));
            $debuggingCost   = floatval(Request::param('debugging_cost'));
            $salesCommission = floatval(Request::param('sales_commission'));

            $finalProfit       = $sumProfit - $shippingCost - $taxAmount - $debuggingCost - $salesCommission;
            $data['profit']    = round($finalProfit, 2);
            $data['margin_rate'] = ($sumTotal > 0) ? round($finalProfit / $sumTotal * 100, 2) : 0;


            // 主表 product_name（存第一个产品名称，非ID）
            if (!empty($productIds)) {
                $firstPid   = (int)($productIds[0] ?? 0);
                $firstName  = $idNameMap[$firstPid] ?? '';
                if ($firstName !== '') {
                    $data['product_name'] = $firstName . (count($productIds) > 1 ? ' 等' : '');
                }
            }

            // 开启事务，插入订单主表和明细表
            Db::startTrans();
            try {
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
                if (!empty($leadsId)) {
                    // 更新客户的成交状态为已成交
                    Db::name('crm_leads')->where('id', $leadsId)->update(['issuccess' => 1]);
                }
                
                Db::commit();
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
        $accountList = Db::name('crm_receive_account')->field('id, account')->select();
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
        $where = [];
        if ($currentAdmin['org'] && strpos($currentAdmin['org'], 'admin') === false) {
            $where[] = $this->getOrgWhere($currentAdmin['org'], 'p');
        }
        // 只获取启用状态的产品（status = 0）
        $productRows = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id')
            ->where($where)
            ->where('p.status', '=', 0)
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        $this->assign('productList', $productRows);

        // 协同人 xmSelect
        $teamName = session('team_name') ?: '';
        $adminList = Db::name('admin')
            ->where('group_id', '<>', 1)
            ->whereIn('group_id', [10, 11, 14])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        foreach ($adminList as $admin) {
            $collaboratorData[] = ['name' => $admin['username'], 'value' => $admin['admin_id']];
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        // 查询所有产品经理（admin表中 group_id=14），按用户名升序
        $managerList = Db::name('admin')
            ->whereIn('group_id', [ 13, 14])
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
                if (!$isMyCustomer && !$isCollaboratorCustomer) {
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
                $res['custname'] = $custinfo['kh_name'];
                $res['kh_username'] = $custinfo['kh_username'];
                $res['source'] = $sourceName;  // 返回来源名称（通过端口反查的渠道）
                $res['pr_user'] = $custinfo['pr_user'];
                $res['country'] = $custinfo['xs_area'];
                $res['oper_user'] = $custinfo['oper_user'];
                
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
                
                // 查询协同人名字列表（用于客户负责人下拉框）
                $jointPersonNames = [];
                if (!empty($jointPersonIds)) {
                    $adminList = Db::name('admin')
                        ->whereIn('admin_id', $jointPersonIds)
                        ->field('admin_id,username')
                        ->select();
                    foreach ($adminList as $admin) {
                        $jointPersonNames[] = [
                            'id' => $admin['admin_id'],
                            'name' => $admin['username']
                        ];
                    }
                }
                $res['joint_person_names'] = $jointPersonNames;
                
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

    // 根据客户负责人获取团队名称
    public function getTeamByPrUser()
    {
        if (Request::isAjax()) {
            $prUser = Request::param('pr_user', '');
            
            if (empty($prUser)) {
                return json(['code' => 0, 'msg' => '客户负责人不能为空', 'data' => ['team_name' => '']]);
            }
            
            // 根据客户负责人（username）查询团队名称
            $adminInfo = Db::name('admin')->where('username', $prUser)->field('team_name')->find();
            
            if ($adminInfo && !empty($adminInfo['team_name'])) {
                return json(['code' => 1, 'msg' => '获取成功', 'data' => ['team_name' => $adminInfo['team_name']]]);
            } else {
                return json(['code' => 0, 'msg' => '未找到对应的团队', 'data' => ['team_name' => '']]);
            }
        }
        
        return json(['code' => 500, 'msg' => '请求方式错误', 'data' => ['team_name' => '']]);
    }


    // //编辑客户第3版
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
            if (!empty($contact)) {
                $currentUsername = Session::get('username');
                $currentAdminId = Session::get('aid');
                $currentGroupId = Session::get('gid');
                $isPrivilegedEditor = ($currentAdminId == 1 || $currentGroupId == 15); // 超管/财务专员可编辑所有订单
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

                        if (!$isPrivilegedEditor && !$isMyCustomer && !$isCollaboratorCustomer) {
                            return fail('该客户不属于您的客户或协同人客户，无法添加订单');
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
                if (empty(trim($clientCompany))) {
                    return json(['code' => -200, 'msg' => '选择"公司"时，客户公司为必填项']);
                }
                $data['client_company'] = trim($clientCompany);
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
            $data['pr_user']          = Request::param('pr_user') ?: Session::get('username'); // 客户负责人（默认当前用户）
            $data['team_name']        = Request::param('team_name');      // 团队名称
            
            // 处理运营端口：将端口ID转换为端口名称（文字）保存
            $sourcePortId = Request::param('source_port', '');
            $data['source_port'] = '';  // 默认为空
            if (!empty($sourcePortId)) {
                // 从 crm_inquiry_port 表获取端口名称
                $portInfo = Db::name('crm_inquiry_port')
                    ->where('id', $sourcePortId)
                    ->field('port_name')
                    ->find();
                if ($portInfo && !empty($portInfo['port_name'])) {
                    $data['source_port'] = $portInfo['port_name'];  // 保存端口名称（文字）
                }
            }
            
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
            $data['wechat_receipt_image'] = Request::param('wechat_receipt_image', '');
            $data['inquiry_assign_image'] = Request::param('inquiry_assign_image', '');
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

            // 查询涉及的产品名称（用于获取产品名称文本及分类名）
            $idArr = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $pid) {
                    $pid = (int)$pid;
                    if ($pid > 0) $idArr[] = $pid;
                }
                $idArr = array_values(array_unique($idArr));
            }
            $idNameMap = [];
            if (!empty($idArr)) {
                // 从产品表获取名称和分类，用于展示和计算
                // 注意：这里不过滤 status，因为历史订单可能引用已删除的产品，需要保留产品名称
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    // 拼接名称和分类（如需）：$r['product_name'].' ('.$r['category_name'].')'
                    $idNameMap[$r['id']] = $r['product_name'];
                }
                
                // 如果某些产品ID查询不到（可能已被删除），尝试从订单明细表中获取产品名称
                foreach ($idArr as $pid) {
                    if (!isset($idNameMap[$pid])) {
                        // 尝试从订单明细表中获取该产品ID对应的产品名称（如果有历史记录）
                        $item = Db::name('crm_order_item')
                            ->where('product_id', $pid)
                            ->where('product_name', '<>', '')
                            ->order('id desc')
                            ->field('product_name')
                            ->find();
                        if ($item && !empty($item['product_name'])) {
                            $idNameMap[$pid] = $item['product_name'];
                        }
                    }
                }
            }

            // 计算订单总金额和利润，并构建明细数据数组
            $sumTotal = 0;
            $sumProfit = 0;
            $itemsData = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $index => $pid) {
                    $pid = (int)$pid;
                    if ($pid <= 0) continue;  // 跳过无效行（如空行）
                    // 产品名称文本（用于主表摘要显示）
                    $pnameText = $idNameMap[$pid] ?? '';
                    // 当前行的数量、单价、成本
                    $qty      = isset($qtys[$index]) ? floatval($qtys[$index]) : 0;
                    $price    = isset($unitPrices[$index]) ? floatval($unitPrices[$index]) : 0;
                    $purchase = isset($purchasePrices[$index]) ? floatval($purchasePrices[$index]) : 0;
                    // 计算当前行销售合计和子项利润
                    $lineTotal  = round($qty * $price, 2);
                    $lineProfit = round($lineTotal - $purchase, 2);
                    $sumTotal  += $lineTotal;
                    $sumProfit += $lineProfit;
                    // 当前行对应的产品经理ID（默认为0表示未选择）
                    $managerId = 0;
                    if (!empty($managerIds[$index])) {
                        $managerId = intval($managerIds[$index]);
                    }
                    // 汇总构建当前明细行数据
                    $itemsData[] = [
                        'order_id'       => $id,                  // 关联订单ID
                        'line_no'        => $index + 1,           // 行号
                        'product_id'     => (string)$pid,         // 产品ID（字符串存储）
                        'product_name'   => $pnameText,           // 产品名称文本
                        'spec_model'     => $specModels[$index] ?? '',
                        'unit'           => $units[$index] ?? '',
                        'qty'            => (int)$qty,
                        'unit_price'     => number_format($price, 2, '.', ''),
                        'total_price'    => number_format($lineTotal, 2, '.', ''),
                        'purchase_price' => number_format($purchase, 2, '.', ''),
                        'sub_profit'     => number_format($lineProfit, 2, '.', ''),
                        'remark'         => $itemRemarks[$index] ?? '',
                        'manager_id'     => $managerId
                    ];
                }
            }

            // 汇总订单金额、利润、利润率
            $data['money']       = round($sumTotal, 2);
            $shippingCost        = floatval($data['shipping_cost'] ?? 0);
            $taxAmount           = floatval($data['tax_amount'] ?? 0);
            $debuggingCost       = floatval($data['debugging_cost'] ?? 0);
            $salesCommission     = floatval($data['sales_commission'] ?? 0);
            $finalProfit         = $sumProfit - $shippingCost - $taxAmount - $debuggingCost - $salesCommission;
            $data['profit']      = round($finalProfit, 2);
            $data['margin_rate'] = ($sumTotal > 0) ? round($finalProfit / $sumTotal * 100, 2) : 0;

            // 更新主表产品名称摘要（存入第一个产品名称，多个则加“等”字样）
            if (!empty($productIds)) {
                $firstPid   = (int)($productIds[0] ?? 0);
                $firstName  = $idNameMap[$firstPid] ?? '';
                if ($firstName !== '') {
                    $data['product_name'] = $firstName . (count($productIds) > 1 ? ' 等' : '');
                }
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
        // 读取该订单的所有产品明细行
        $items = Db::name('crm_order_item')->where('order_id', $orderId)->select();

        // 准备下拉选项数据（团队列表、来源列表、客户性质列表、运营人员列表等）
        $teamList   = $this->getTeamList();
        $accountList = Db::name('crm_receive_account')->field('id, account')->select();
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

        // 产品列表（含分类名）。无组织限制时查询所有产品
        $where = [];
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            // 有组织限制时构造过滤条件
            $where[] = $this->getOrgWhere($currentAdmin['org'], 'p');
        }
        // 只获取启用状态的产品（status = 0）
        $productQuery = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($where)) {
            $productQuery->where($where);
        }
        $productRows = $productQuery
            ->where('p.status', '=', 0)
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        
        // 获取订单中已有的产品ID，检查是否有已删除的产品（status=-1）
        // 如果有，需要添加到产品列表中以便显示，但标记为已废弃
        if (isset($items) && !empty($items)) {
            $existingProductIds = [];
            foreach ($items as $item) {
                if (!empty($item['product_id'])) {
                    $existingProductIds[] = (int)$item['product_id'];
                }
            }
            $existingProductIds = array_unique($existingProductIds);
            
            // 检查这些产品是否已被删除（status=-1）
            if (!empty($existingProductIds)) {
                // 先查询所有可能的产品（包括已删除的），不受组织限制（因为历史订单需要显示）
                $allProducts = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $existingProductIds)
                    ->field('p.id, p.product_name, c.category_name, p.status')
                    ->select();
                
                // 找出已删除的产品（status=-1）
                foreach ($allProducts as $product) {
                    if (isset($product['status']) && $product['status'] == -1) {
                        $product['is_deleted'] = true; // 标记为已删除
                        $productRows[] = $product;
                    }
                }
                
                // 对于在订单中存在但查询不到的产品（可能已被物理删除），从订单明细中获取产品名称
                $foundProductIds = array_column($allProducts, 'id');
                foreach ($items as $item) {
                    if (!empty($item['product_id']) && !in_array($item['product_id'], $foundProductIds)) {
                        // 产品不存在于产品表中，但从订单明细中获取信息
                        if (!empty($item['product_name'])) {
                            $productRows[] = [
                                'id' => $item['product_id'],
                                'product_name' => $item['product_name'],
                                'category_name' => '无',
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
            ->whereIn('group_id', [10, 11, 14])
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
        $managerList = Db::name('admin')
            ->whereIn('group_id', [13, 14])
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

            // 查询涉及的产品名称（用于获取产品名称文本及分类名）
            $idArr = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $pid) {
                    $pid = (int)$pid;
                    if ($pid > 0) $idArr[] = $pid;
                }
                $idArr = array_values(array_unique($idArr));
            }
            $idNameMap = [];
            if (!empty($idArr)) {
                // 从产品表获取名称和分类，用于展示和计算
                // 注意：这里不过滤 status，因为历史订单可能引用已删除的产品，需要保留产品名称
                $rows = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', 'in', $idArr)
                    ->field('p.id, p.product_name, c.category_name')
                    ->select();
                foreach ($rows as $r) {
                    // 拼接名称和分类（如需）：$r['product_name'].' ('.$r['category_name'].')'
                    $idNameMap[$r['id']] = $r['product_name'];
                }
                
                // 如果某些产品ID查询不到（可能已被删除），尝试从订单明细表中获取产品名称
                foreach ($idArr as $pid) {
                    if (!isset($idNameMap[$pid])) {
                        // 尝试从订单明细表中获取该产品ID对应的产品名称（如果有历史记录）
                        $item = Db::name('crm_order_item')
                            ->where('product_id', $pid)
                            ->where('product_name', '<>', '')
                            ->order('id desc')
                            ->field('product_name')
                            ->find();
                        if ($item && !empty($item['product_name'])) {
                            $idNameMap[$pid] = $item['product_name'];
                        }
                    }
                }
            }

            // 计算订单总金额和利润，并构建明细数据数组
            $sumTotal = 0;
            $sumProfit = 0;
            $itemsData = [];
            if (!empty($productIds) && is_array($productIds)) {
                foreach ($productIds as $index => $pid) {
                    $pid = (int)$pid;
                    if ($pid <= 0) continue;  // 跳过无效行（如空行）
                    // 产品名称文本（用于主表摘要显示）
                    $pnameText = $idNameMap[$pid] ?? '';
                    // 当前行的数量、单价、成本
                    $qty      = isset($qtys[$index]) ? floatval($qtys[$index]) : 0;
                    $price    = isset($unitPrices[$index]) ? floatval($unitPrices[$index]) : 0;
                    $purchase = isset($purchasePrices[$index]) ? floatval($purchasePrices[$index]) : 0;
                    // 计算当前行销售合计和子项利润
                    $lineTotal  = round($qty * $price, 2);
                    $lineProfit = round($lineTotal - $purchase, 2);
                    $sumTotal  += $lineTotal;
                    $sumProfit += $lineProfit;
                    // 当前行对应的产品经理ID（默认为0表示未选择）
                    $managerId = 0;
                    if (!empty($managerIds[$index])) {
                        $managerId = intval($managerIds[$index]);
                    }
                    // 汇总构建当前明细行数据
                    $itemsData[] = [
                        'order_id'       => $id,                  // 关联订单ID
                        'line_no'        => $index + 1,           // 行号
                        'product_id'     => (string)$pid,         // 产品ID（字符串存储）
                        'product_name'   => $pnameText,           // 产品名称文本
                        'spec_model'     => $specModels[$index] ?? '',
                        'unit'           => $units[$index] ?? '',
                        'qty'            => (int)$qty,
                        'unit_price'     => number_format($price, 2, '.', ''),
                        'total_price'    => number_format($lineTotal, 2, '.', ''),
                        'purchase_price' => number_format($purchase, 2, '.', ''),
                        'sub_profit'     => number_format($lineProfit, 2, '.', ''),
                        'remark'         => $itemRemarks[$index] ?? '',
                        'manager_id'     => $managerId
                    ];
                }
            }

            // 汇总订单金额、利润、利润率
            $data['money']       = round($sumTotal, 2);
            $shippingCost        = floatval($data['shipping_cost'] ?? 0);
            $taxAmount           = floatval($data['tax_amount'] ?? 0);
            $debuggingCost       = floatval($data['debugging_cost'] ?? 0);
            $salesCommission     = floatval($data['sales_commission'] ?? 0);
            $finalProfit         = $sumProfit - $shippingCost - $taxAmount - $debuggingCost - $salesCommission;
            $data['profit']      = round($finalProfit, 2);
            $data['margin_rate'] = ($sumTotal > 0) ? round($finalProfit / $sumTotal * 100, 2) : 0;

            // 更新主表产品名称摘要（存入第一个产品名称，多个则加“等”字样）
            if (!empty($productIds)) {
                $firstPid   = (int)($productIds[0] ?? 0);
                $firstName  = $idNameMap[$firstPid] ?? '';
                if ($firstName !== '') {
                    $data['product_name'] = $firstName . (count($productIds) > 1 ? ' 等' : '');
                }
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
        // 读取该订单的所有产品明细行
        $items = Db::name('crm_order_item')->where('order_id', $orderId)->select();

        // 准备下拉选项数据（团队列表、来源列表、客户性质列表、运营人员列表等）
        $teamList   = $this->getTeamList();
        $sourceList = Db::name('crm_client_status')->distinct(true)->column('status_name');
        // 使用 array_map 和 trim 去除每个值的前后空格
        $sourceList = array_map('trim', $sourceList);
        //var_dump($sourceList);
        $accountList = Db::name('crm_receive_account')->field('id, account')->select();  // fetch all accounts (id and name)
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

        // 产品列表（含分类名）。无组织限制时查询所有产品
        // 不添加status过滤，所有产品数据都进行展示
        $where = [];
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            // 有组织限制时构造过滤条件
            $where[] = $this->getOrgWhere($currentAdmin['org'], 'p');
        }
        // 查询所有产品（不限制status状态）
        $productQuery = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($where)) {
            $productQuery->where($where);
        }
        $productRows = $productQuery
            ->group('p.product_name, c.category_name, p.status')
            ->field('MIN(p.id) as id, p.product_name, c.category_name, p.status')
            ->order('p.product_name', 'asc')
            ->select();
        
        // 标记已删除的产品（status=-1）
        foreach ($productRows as &$product) {
            if (isset($product['status']) && $product['status'] == -1) {
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
                                'status' => -1,
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
            ->whereIn('group_id', [10, 11, 14])
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
        $managerList = Db::name('admin')
            ->where('group_id', 14)
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
        // 开启事务
        Db::startTrans();
        try {
            // 获取请求中的订单ID
            $id = Request::param('id');
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
        $team_name = $user['team_name'] ?? '';
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
        
        // if (isset($keyword['status'])) $where[] = ['status', '=', $keyword['status']];
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
        if ($user['org']) {
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
            ->order('order_time desc')
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

        // 查询订单对应的产品明细，便于前端一次性展示
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = [];
        if (!empty($orderIds)) {
            $items = Db::table('crm_order_item')
                ->alias('oi')
                ->leftJoin('crm_products p', 'oi.product_id = p.id')
                ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                ->whereIn('oi.order_id', $orderIds)
                ->order('oi.order_id asc, oi.line_no asc')
                ->field('oi.*, c.category_name as supplier')
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

            foreach ($items as &$item) {
                $item['manager_name'] = isset($managerMap[$item['manager_id']]) ? $managerMap[$item['manager_id']] : '';
                $item['supplier'] = $item['supplier'] ?? '';
            }
            unset($item);

            foreach ($items as $item) {
                $orderItemsMap[$item['order_id']][] = $item;
            }
        }
        
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

            // 绑定订单的产品明细，便于前端一次渲染
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
            if (empty($order['product_name']) && !empty($order['order_items'])) {
                $order['product_name'] = $order['order_items'][0]['product_name'];
            }
        }
        unset($order);

        // 追加订单明细，便于在列表中展示每个订单的产品信息
        $orderIds = array_column($list['data'], 'id');
        if (!empty($orderIds)) {
            // 查询订单明细并关联产品、品类以获取供应商
            $items = Db::table('crm_order_item')
                ->alias('oi')
                ->leftJoin('crm_products p', 'oi.product_id = p.id')
                ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                ->where('oi.order_id', 'in', $orderIds)
                ->order('oi.order_id asc, oi.line_no asc')
                ->field('oi.*, c.category_name as supplier')
                ->select();

            // 批量获取产品经理名称
            $managerIds = [];
            foreach ($items as $item) {
                if (!empty($item['manager_id'])) {
                    $managerIds[] = $item['manager_id'];
                }
            }
            $managerIds = array_unique($managerIds);

            $managerMap = [];
            if (!empty($managerIds)) {
                $managerMap = Db::table('admin')
                    ->where('admin_id', 'in', $managerIds)
                    ->column('username', 'admin_id');
            }

            $itemsMap = [];
            foreach ($items as &$item) {
                $item['manager_name'] = $managerMap[$item['manager_id']] ?? '';
                // 如果没有从产品表获取到供应商，尝试使用明细中的供应商字段
                if (empty($item['supplier']) && isset($item['supplier'])) {
                    $item['supplier'] = $item['supplier'] ?? '';
                }
                $itemsMap[$item['order_id']][] = $item;
            }
            unset($item);

            foreach ($list['data'] as &$order) {
                $order['order_items'] = $itemsMap[$order['id']] ?? [];
            }
            unset($order);
        }

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

        // if (isset($keyword['status'])) $where[] = ['status', '=', $keyword['status']];
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
        
        // 查询订单明细，关联产品表获取供应商信息（category_name）
        $items = Db::table('crm_order_item')
            ->alias('oi')
            ->leftJoin('crm_products p', 'oi.product_id = p.id')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id')
            ->where('oi.order_id', 'in', $orderIds)
            ->order('oi.order_id asc, oi.line_no asc')
            ->field('oi.*, c.category_name as supplier')
            ->select();
        
        // 获取产品经理名称
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
                ->where('admin_id', 'in', $managerIds)
                ->field('admin_id, username')
                ->select();
            foreach ($managers as $manager) {
                $managerMap[$manager['admin_id']] = $manager['username'];
            }
        }
        
        // 为每个明细添加产品经理名称
        foreach ($items as &$item) {
            $item['manager_name'] = isset($managerMap[$item['manager_id']]) 
                ? $managerMap[$item['manager_id']] 
                : '';
            // 如果没有从产品表获取到供应商，尝试从明细表本身获取
            if (empty($item['supplier']) && isset($item['supplier'])) {
                $item['supplier'] = $item['supplier'] ?? '';
            }
        }
        unset($item);
        
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
