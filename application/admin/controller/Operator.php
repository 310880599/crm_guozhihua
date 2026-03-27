<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use app\admin\model\Admin;

class Operator extends Common
{
    public function perList()
    {
        if (request()->isPost()) {
            return $this->perSearch();
        }

        $current_admin = Admin::getMyInfo();
        $khRankList = Db::table('crm_client_rank')->select();
        $this->assign('khRankList', $khRankList);
        $productList = $this->getProductList();
        $this->assign('productList', $productList);
      $adminResult = Db::name('admin')
    ->where('group_id', '<>', 1)
    ->where($this->getOrgWhere($current_admin['org']))
    ->field('admin_id,username,team_name') // 添加 team_name
    ->select();
$this->assign('adminResult', $adminResult);
         $teamNames = Db::name('admin')
        ->where('group_id', '<>', 1)
        ->where($this->getOrgWhere($current_admin['org']))
        ->where('team_name', '<>', '') // 排除空团队
        ->distinct(true)
        ->column('team_name');
    
    // 转换为模板需要的格式
    $teamResult = [];
    foreach ($teamNames as $teamName) {
        $teamResult[] = ['team_name' => $teamName];
    }
    $this->assign('teamResult', $teamResult);
        return $this->fetch();
    }

  public function perSearch($params = null)
{
    // 如果没有传入参数，从请求获取
    if ($params === null) {
        $params = Request::param();
    }
    
    $keyword = $params['keyword'] ?? [];
    $model   = model('client');

    // 确保获取分页参数
    $page = $params['page'] ?? input('page', 1);
    $limit = $params['limit'] ?? input('limit', 15); // 恢复默认分页值

    $list = $this->_search($params, $model, function ($query, $p) {
        $keyword = $p['keyword'] ?? [];
        
        // 处理联系方式搜索 - 先查询联系方式获取 leads_id
        $contactLeadsIds = [];
        if (!empty($keyword['contact'])) {
            $con = trim($keyword['contact']);
            $cleaned = preg_replace('/\D+/', '', $con); // 仅保留数字
            
            $contactLeadsIds = Db::table('crm_contacts')
                ->where('is_delete', 0)
                ->where(function ($q) use ($con, $cleaned) {
                    $q->where('contact_value', 'like', '%' . addslashes($con) . '%')
                      ->whereOrRaw("CONCAT(contact_extra, contact_value) LIKE '%" . addslashes($con) . "%'");
                    if ($cleaned && $cleaned !== $con) {
                        $q->whereOr('vdigits', 'like', '%' . $cleaned . '%');
                    }
                })
                ->column('leads_id');
            
            if (empty($contactLeadsIds)) {
                // 如果没有匹配的联系方式，返回空结果
                $query->where('1=0');
                return $query;
            }
        }
        
         $query->alias('c')
              ->join('admin a', 'c.pr_user = a.username', 'LEFT')
              ->field('c.*, a.team_name')
              ->append(['contact'])
              ->hidden(['contacts']);
        
        // 如果有联系方式搜索条件，添加 leads_id 过滤
        if (!empty($contactLeadsIds)) {
            $query->whereIn('c.id', $contactLeadsIds);
        }
        
        if (!empty($keyword['status'])) {
            $query->where('c.status', '=', $keyword['status']);
        }
        if (!empty($keyword['kh_name'])) {
            $query->where('c.kh_name', 'like', '%' . $keyword['kh_name'] . '%');
        }
        if (!empty($keyword['pr_user'])) {
            $query->where('c.pr_user', '=', $keyword['pr_user']);
        }
        if (!empty($keyword['product_name'])) {
            // 产品名称搜索：先查找匹配的产品ID，然后搜索
            $productIds = Db::table('crm_products')
                ->where('product_name', 'like', '%' . $keyword['product_name'] . '%')
                ->column('id');
            if (!empty($productIds)) {
                $query->where(function ($q) use ($keyword, $productIds) {
                    $q->whereIn('c.product_name', $productIds)
                      ->whereOr('c.product_name', 'like', '%' . $keyword['product_name'] . '%');
                });
            } else {
                $query->where('c.product_name', 'like', '%' . $keyword['product_name'] . '%');
            }
        }
        if (!empty($keyword['timebucket'])) {
            $where = $this->getClientimeWhere($keyword['timebucket'], 'c');
            $query->where($where);
        }
        if (!empty($keyword['at_time'])) {
            $where = $this->getClientimeWhere($keyword['at_time'], 'c');
            $query->where($where);
        }
        if (!empty($keyword['team_name'])) {
            $current_admin = Admin::getMyInfo();
            $usernames = Db::name('admin')
                ->where('team_name', $keyword['team_name'])
                ->where($this->getOrgWhere($current_admin['org']))
                ->column('username');
            
            if (!empty($usernames)) {
                $query->whereIn('c.pr_user', $usernames);
            } else {
                // 没有匹配的用户，返回空结果
                $query->where('1=0');
            }
        }
        
        // 根据当前运营人员的 inquiry_id 和 port_id 匹配询盘数据
        $current_admin = Admin::getMyInfo();
        $current_admin_info = Db::table('admin')->where('admin_id', $current_admin['admin_id'])->find();
        
        // 如果配置了 inquiry_id，则按渠道过滤
        if (!empty($current_admin_info['inquiry_id'])) {
            // 匹配渠道ID - 明确指定表别名 c（crm_leads）
            $query->where('c.inquiry_id', '=', $current_admin_info['inquiry_id']);
            
            // 如果同时配置了 port_id，则进一步按端口过滤
            if (!empty($current_admin_info['port_id'])) {
                // port_id 是逗号分隔的多选值，需要检查交集 - 明确指定表别名 c
                $admin_port_ids = array_filter(explode(',', $current_admin_info['port_id']));
                $port_conditions = [];
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_conditions[] = "FIND_IN_SET('{$port_id}', c.port_id) > 0";
                    }
                }
                
                if (!empty($port_conditions)) {
                    $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                    $query->whereRaw($port_where);
                }
                // 如果 port_id 配置了但为空，不添加端口过滤条件，只按渠道过滤
            }
            // 如果只配置了 inquiry_id 没有配置 port_id，只按渠道过滤
        } else {
            // 如果没有配置 inquiry_id，返回空结果（运营人员必须配置渠道）
            $query->where('1=0');
        }
        
        return $query;
    }, $page, $limit);

    // 补充展示所需的派生字段，与 personClientSearch 保持一致
    if (empty($list) || empty($list['data'])) {
    return [
        'code'  => 0,
        'msg'   => '获取成功!',
            'data'  => [],
            'count' => 0,
            'rel'   => 1
        ];
    }
    
    $rows = &$list['data'];
    $leadIds = array_column($rows, 'id');
    
    // 1) 批量查询所属渠道名称和运营端口名称
    $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
    $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');
    
    // 1.5) 批量查询产品名称映射（将产品ID转换为产品名称）
    $productMap = Db::table('crm_products')->column('product_name', 'id');
    
    // 2) 批量查询主/辅电话（crm_contacts：1=主，3=辅）
    $phoneMap = [];
    if (!empty($leadIds)) {
        $contacts = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', 'in', $leadIds)
            ->where('contact_type', 'in', [1, 3])
            ->order('id', 'asc')
            ->field('leads_id, contact_type, contact_value')
            ->select();
        
        foreach ($contacts as $c) {
            $lid = $c['leads_id'];
            if (!isset($phoneMap[$lid])) {
                $phoneMap[$lid] = ['main' => '', 'aux' => ''];
            }
            if ($c['contact_type'] == 1 && $phoneMap[$lid]['main'] === '') {
                $phoneMap[$lid]['main'] = $c['contact_value'];
            } elseif ($c['contact_type'] == 3 && $phoneMap[$lid]['aux'] === '') {
                $phoneMap[$lid]['aux'] = $c['contact_value'];
            }
        }
    }
    
    // 3) 协同人姓名：从 admin 表按 joint_person 映射
    $uidSet = [];
    foreach ($rows as &$row) {
        // 所属渠道名称（如无对应名称则用自身ID）
        $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) 
                                ? $inquiryMap[$row['inquiry_id']] 
                                : (string)($row['inquiry_id'] ?? '');
        
        // 运营端口名称（port_id 可能是逗号分隔的多选值，取第一个）
        $port_name = '';
        if (!empty($row['port_id'])) {
            $port_ids = array_filter(explode(',', $row['port_id']));
            if (!empty($port_ids)) {
                $first_port_id = trim($port_ids[0]);
                if ($first_port_id && isset($portMap[$first_port_id])) {
                    $port_name = $portMap[$first_port_id];
                } else {
                    $port_name = (string)$first_port_id;
                }
            }
        }
        $row['port_name'] = $port_name;
        
        // 产品名称转换（将产品ID转换为产品名称）
        if (!empty($row['product_name'])) {
            // 如果 product_name 是数字，尝试从产品表中查找名称
            if (is_numeric($row['product_name']) && isset($productMap[$row['product_name']])) {
                $row['product_name'] = $productMap[$row['product_name']];
            } elseif (is_numeric($row['product_name'])) {
                // 如果是数字但找不到对应产品，保持原值
                // $row['product_name'] = $row['product_name'];
            }
        }
        
        // 主/辅电话
        $row['main_phone'] = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['main'] : '';
        $row['aux_phone']  = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['aux'] : '';
        
        // joint_person 可能是 JSON 数组或逗号分隔的 ID 字符串
        $idsArr = [];
        if (!empty($row['joint_person'])) {
            $jp = $row['joint_person'];
            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                $tmp = json_decode($jp, true);
                if (is_array($tmp)) $idsArr = $tmp;
            } else {
                $idsArr = preg_split('/[,，\s]+/', $jp, -1, PREG_SPLIT_NO_EMPTY);
            }
        }
        $row['_joint_ids'] = $idsArr;
        foreach ($idsArr as $uid) {
            $uidSet[$uid] = true;
        }
    }
    unset($row);
    
    // 一次性把协同人的 username 查出来
    $adminMap = [];
    if (!empty($uidSet)) {
        $adminMap = Db::table('admin')
            ->where('admin_id', 'in', array_keys($uidSet))
            ->column('username', 'admin_id');
    }
    
    foreach ($rows as &$row) {
        $names = [];
        foreach ($row['_joint_ids'] as $uid) {
            $names[] = isset($adminMap[$uid]) ? $adminMap[$uid] : (string)$uid;
        }
        $row['joint_person_names'] = $names ? implode('、', $names) : '';
        unset($row['_joint_ids']);
    }
    unset($row);
    
    return [
        'code'  => 0,
        'msg'   => '获取成功!',
        'data'  => $rows,
        'count' => $list['total'],
        'rel'   => 1
    ];
}

/**
 * 导出全部客户数据
 */
public function exportAll()
{
    // 1. 直接获取所有请求参数
    $allParams = Request::param();
    
    // 2. 提取 keyword 参数（处理可能的嵌套结构）
    $keyword = [];
    if (isset($allParams['keyword']) && is_array($allParams['keyword'])) {
        $keyword = $allParams['keyword'];
    } else {
        // 处理扁平化参数（如 keyword[kh_rank]=xxx）
        foreach ($allParams as $key => $value) {
            if (strpos($key, 'keyword[') === 0) {
                $field = substr($key, 8, -1); // 提取字段名
                $keyword[$field] = $value;
            }
        }
    }
    
    // 3. 确保时间参数一致性
    if (!empty($keyword['timebucket']) && $keyword['timebucket'] !== '') {
        $keyword['at_time'] = ''; // 清空自定义时间范围
    }
    
    // 4. 创建查询参数
    $params = [
        'keyword' => $keyword,
        'page' => 1,
        'limit' => PHP_INT_MAX
    ];
    
    // 5. 记录调试信息（关键！）
    \think\facade\Log::info('ExportAll received params: ' . json_encode($allParams));
    \think\facade\Log::info('ExportAll processed keyword: ' . json_encode($keyword));
    
    // 6. 直接调用 perSearch，传入参数
    $result = $this->perSearch($params);
    
    // 7. 检查是否有数据
    if (empty($result['data'])) {
        $this->error('没有可导出的数据');
    }
    
    // 8. 记录导出数量
    \think\facade\Log::info('Exporting ' . count($result['data']) . ' records');
    
    // 9. 导出Excel
    $this->exportToExcel($result['data']);
}

/**
 * 导出数据到Excel
 * @param array $data 要导出的数据
 */
private function exportToExcel($data)
{
    // 检查是否安装了必要的扩展
    if (!class_exists('\PhpOffice\PhpSpreadsheet\Spreadsheet')) {
        $this->error('请先安装PhpSpreadsheet库');
    }
    
    // 1. 创建Excel对象
    $spreadsheet = new \PhpOffice\PhpSpreadsheet\Spreadsheet();
    $sheet = $spreadsheet->getActiveSheet();
    
    // 2. 设置标题行
    $headers = [
        '团队名称','客户名称', '产品', '地区', '联系方式', '客户级别', 
        '客户来源', '客户状态', '成交状态', '最新跟进记录', 
        '负责人', '创建时间'
    ];
    
    // 填充标题
    foreach ($headers as $index => $header) {
        $sheet->setCellValueByColumnAndRow($index + 1, 1, $header);
    }
    
     // 3. 填充数据
    $row = 2;
    foreach ($data as $item) {
        $col = 1;
        // ✅ 1. 先填充团队名称 (第一列)
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['team_name'] ?? '');
        
        // ✅ 2. 再填充客户名称 (第二列)
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['kh_name']);
        
        // ✅ 3. 其他字段保持不变
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['product_name']);
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['xs_area']);
        
        // 安全处理 contact 字段
        $contact = is_array($item['contact']) ? 
                   implode(',', $item['contact']) : 
                   (string)($item['contact'] ?? '');
        $sheet->setCellValueByColumnAndRow($col++, $row, $contact);
        
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['kh_rank']);
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['kh_status']);
        
        // 客户状态特殊处理
        $status = '';
        if (isset($item['status']) && $item['status'] == 1) {
            $status = $item['to_kh_time'] ? '公海提取' : '正常';
        } else {
            $status = '在公海';
        }
        $sheet->setCellValueByColumnAndRow($col++, $row, $status);
        
        // 成交状态
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['issuccess'] == 1 ? '已成交' : '未成交');
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['last_up_records']);
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['pr_user']);
        $sheet->setCellValueByColumnAndRow($col++, $row, $item['at_time']);
        
        $row++;
    }
    
    // 4. 设置自动列宽
    foreach (range('A', $sheet->getHighestColumn()) as $col) {
        $sheet->getColumnDimension($col)->setAutoSize(true);
    }
    
    // 5. 设置HTTP头
    header('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    header('Content-Disposition: attachment;filename="客户列表_' . date('Ymd') . '.xlsx"');
    header('Cache-Control: max-age=0');
    
    // 6. 输出Excel
    $writer = new \PhpOffice\PhpSpreadsheet\Writer\Xlsx($spreadsheet);
    $writer->save('php://output');
    exit;
}

    //跟进
    public function dialogue()
    {
        $result = Db::table('crm_leads')->where(['id' => Request::param('id')])->find();
        $result['comment'] = Db::table('crm_comment')->alias('com')->join('admin adm', 'com.user_id = adm.admin_id')->where(['leads_id' => Request::param('id')])->field('com.*,adm.username,adm.avatar')->select();
        foreach ($result['comment'] as $k => $v) {
            $result['comment'][$k]['reply'] = Db::table('crm_reply')->where(['comment_id' => $v['id']])->select();
        }
        $current_admin = Admin::getMyInfo();
        $this->assign('group_id', $current_admin['group_id']);
        $this->assign('curname', $current_admin['username']);
        $this->assign('result', $result);
        return $this->fetch();
    }

    public function order()
    {
        if (request()->isPost()) {
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            // 只在前端没传 timebucket/at_time 的情况下，默认设置 timebucket = month
            if (!isset($params['keyword']['timebucket']) && !isset($params['keyword']['at_time'])) {
                $params['keyword']['timebucket'] = 'month';
            }
            Request::merge($params);
            return $this->personOrderSearch();
        }
        
        // 获取所有渠道列表（启用状态的）
        $inquiryList = Db::name('crm_inquiry')
            ->where('status', 0)
            ->field('inquiry_name')
            ->order('inquiry_name', 'asc')
            ->select();
        $channelList = array_column($inquiryList, 'inquiry_name');
        
        // 获取所有端口列表（启用状态的）
        $portList = Db::name('crm_inquiry_port')
            ->where('status', 0)
            ->field('port_name')
            ->order('port_name', 'asc')
            ->select();
        $portList = array_column($portList, 'port_name');
        
        //查询所有管理员（去除admin，排除 group_id=1 的 admin）
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        $this->assign('adminResult', $adminResult);
        
        //查询所有团队
        $teamList = $this->getTeamList();
        $this->assign('teamList', $teamList);
        
        $this->assign('customer_type', Order::CUSTOMER_TYPE);
        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);
        return $this->fetch();
    }

    public function personOrderSearch()
    {
        $where = [];
        $client_where = [];
        
        // 获取当前登录用户信息
        $current_admin = Admin::getMyInfo();
        
        // 统一的权限判断变量 $isSuper
        $isSuper = (
            (int)session('aid') === 1
            || ($current_admin['username'] ?? '') === 'admin'
            || (int)($current_admin['group_id'] ?? 0) === 12
            || (int)($current_admin['group_id'] ?? 0) === 13
            || (int)($current_admin['group_id'] ?? 0) === 16
        );
        
        $page = input('page') ?? 1;
        $limit = input('limit') ?? config('pageSize');
        $keyword = Request::param('keyword');
        // 过滤掉 null 元素
        if ($keyword) $keyword = array_filter($keyword);

        // if (isset($keyword['status'])) $where[] = ['status', '=', $keyword['status']];
        if (isset($keyword['order_no'])) $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        
        // 时间筛选逻辑：如果 keyword['timebucket'] 有值（today/week/month…），用 buildTimeWhere；如果为空（自定义）且 keyword['at_time'] 有值，用 buildTimeWhere
        $timeCondition = null;
        if (isset($keyword['timebucket']) && $keyword['timebucket'] !== '') {
            $timeCondition = $keyword['timebucket'];
        } elseif (isset($keyword['at_time']) && $keyword['at_time'] !== '') {
            $timeCondition = $keyword['at_time'];
        }
        
        if ($timeCondition) {
            $where[] = $this->buildTimeWhere($timeCondition, 'order_time');
            $timeWhere['at_time'] = $this->buildTimeWhere($timeCondition, 'at_time');
            $timeWhere['to_kh_time'] = $this->buildTimeWhere($timeCondition, 'to_kh_time');
            $client_where[] = function ($query) use ($timeWhere) {
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
        
        // 权限过滤逻辑
        $current_username = $current_admin['username'] ?? '';
        
        if ($isSuper === true) {
            // 超级管理员：不添加任何权限过滤条件，可以查看所有成交订单
            // 但保留用户主动筛选条件（team_name、org、pr_user）
        } else {
            // 普通员工：只能查看自己创建/负责的成交订单
            if ($current_username) {
                $where[] = ['pr_user', '=', $current_username];
                $client_where[] = ['pr_user', '=', $current_username];
            }
        }
        
        // 用户主动筛选：团队名称筛选（仅当用户主动选择时）
        if (isset($keyword['team_name']) && !empty($keyword['team_name'])) {
            $where[] = ['team_name', '=', $keyword['team_name']];
        }
        
        // 用户主动筛选：组织过滤（仅当用户主动选择时）
        $org_where = [];
        if (!empty($keyword['org'])) {
            $org_where[] = $this->getOrgWhere($keyword['org']);
        }
        
        // 如果用户主动筛选了团队名称或组织，需要根据筛选条件进一步过滤业务员列表
        $filter_team_name = $keyword['team_name'] ?? '';
        if ($filter_team_name || !empty($org_where)) {
            $filter_query = Db::table('admin');
            if ($filter_team_name) {
                $filter_query->where('team_name', $filter_team_name);
            }
            if (!empty($org_where)) {
                $filter_query->where($org_where);
            }
            $filtered_usernames = $filter_query->column('username');
            
            // 如果筛选后没有匹配的业务员，返回空结果
            if (empty($filtered_usernames)) {
                $client_where[] = ['pr_user', '=', time()];
                $where[] = ['pr_user', '=', time()];
            } else {
                if ($isSuper) {
                    // 超级管理员：使用筛选后的业务员列表
                    $client_where[] = ['pr_user', 'in', $filtered_usernames];
                    $where[] = ['pr_user', 'in', $filtered_usernames];
                } else {
                    // 普通员工：检查当前用户是否在筛选范围内
                    if (!in_array($current_username, $filtered_usernames)) {
                        // 当前用户不在筛选范围内，返回空结果
                        $client_where[] = ['pr_user', '=', time()];
                        $where[] = ['pr_user', '=', time()];
                    }
                    // 如果当前用户在筛选范围内，保持原有权限过滤（pr_user = 当前登录用户名）
                }
            }
        }
        
        // 询盘渠道查询
        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            //兼容历史数据
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        // 询盘端口查询
        if (isset($keyword['source_port'])) {
            $where[] = ['source_port', '=', $keyword['source_port']];
        }
        
        // 业务员筛选：若传了 pr_user，则追加 ['pr_user','=',$keyword['pr_user']]
        if (isset($keyword['pr_user'])) {
            if ($isSuper) {
                // 超级管理员：可以使用筛选的业务员
                $where[] = ['pr_user', '=', $keyword['pr_user']];
                $client_where[] = ['pr_user', '=', $keyword['pr_user']];
            } else {
                // 普通员工：只能查看自己的订单，如果筛选的不是自己，返回空结果
                if ($keyword['pr_user'] !== $current_username) {
                    $client_where[] = ['pr_user', '=', time()];
                    $where[] = ['pr_user', '=', time()];
                }
                // 如果筛选的是自己，保持原有权限过滤（pr_user = 当前登录用户名）
            }
        }
        
        // 客户查询条件：只查询启用状态的客户
        $client_where[] = ['l.status', '=', 1];
        
        $list = Db::table('crm_client_order')
            ->alias('o')
            ->where($where)
            ->order('o.order_time desc')
            ->paginate([
                'list_rows' => $limit,
                'page' => $page
            ])
            ->toArray();

        // ====== 新增：批量查询产品明细 ======
        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = [];
        if (!empty($orderIds)) {
            // 批量查询订单明细表，关联产品和分类表获取产品名称
            $items = Db::table('crm_order_item')
                ->alias('oi')
                ->leftJoin('crm_products p', 'oi.product_id = p.id')
                ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                ->whereIn('oi.order_id', $orderIds)
                ->order('oi.order_id asc, oi.line_no asc')
                ->field('oi.order_id, oi.product_name, oi.product_id, p.product_name as product_name_from_table')
                ->select();

            // 按 order_id 分组组装产品明细
            foreach ($items as $item) {
                $orderId = $item['order_id'];
                // 优先使用明细表中的 product_name，如果没有则使用关联表的产品名称
                $productName = !empty($item['product_name']) ? $item['product_name'] : ($item['product_name_from_table'] ?? '');
                if (!empty($productName)) {
                    $orderItemsMap[$orderId][] = [
                        'product_name' => $productName
                    ];
                }
            }
        }

        // 将产品明细添加到每条订单数据中
        foreach ($list['data'] as &$order) {
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
        }
        unset($order);
        // ====== 新增代码结束 ======

        // === wechat_receipt_images 解析兼容开始 ===
        // 【重要说明】crm_client_hangye 表已废弃，不再使用任何 hangye 表的回退逻辑
        // 若订单 wechat_receipt_image 为空，直接保持为空，不做任何回退查询
        
        /**
         * 解析图片数据，统一输出格式
         * @param mixed $raw 原始数据（可能是字符串、JSON字符串、数组等）
         * @return array 统一格式：[['full' => '...', 'thumb' => '...'], ...]
         */
        $parseImages = function($raw) {
            // 空值安全处理：null、空字符串、'null'、'[]' 都返回空数组
            if (empty($raw)) {
                return [];
            }
            
            // 处理字符串 'null' 或 '[]'
            if (is_string($raw)) {
                $raw = trim($raw);
                if (empty($raw) || $raw === 'null' || $raw === '[]') {
                    return [];
                }
                
                // 如果以 [ 开头，尝试解析为JSON数组
                if (substr($raw, 0, 1) === '[') {
                    $decoded = json_decode($raw, true);
                    if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                        $raw = $decoded;
                    } else {
                        // JSON解析失败，当作单字符串处理
                        return [['full' => $raw, 'thumb' => $raw]];
                    }
                } else {
                    // 不是JSON数组，当作单字符串处理
                    return [['full' => $raw, 'thumb' => $raw]];
                }
            }

            // 如果是数组
            if (is_array($raw)) {
                $result = [];
                $seenFulls = []; // 用于去重

                foreach ($raw as $item) {
                    if (is_string($item)) {
                        // 数组元素是字符串
                        $full = trim($item);
                        if (!empty($full) && !isset($seenFulls[$full])) {
                            $seenFulls[$full] = true;
                            $result[] = ['full' => $full, 'thumb' => $full];
                        }
                    } elseif (is_array($item) || is_object($item)) {
                        // 数组元素是对象
                        $item = (array)$item;
                        
                        // 优先从 full/path/src/url 取 full
                        $full = '';
                        foreach (['full', 'path', 'src', 'url'] as $key) {
                            if (!empty($item[$key])) {
                                $full = trim($item[$key]);
                                break;
                            }
                        }
                        
                        if (empty($full)) {
                            continue; // 没有找到full，跳过
                        }

                        // 去重
                        if (isset($seenFulls[$full])) {
                            continue;
                        }
                        $seenFulls[$full] = true;

                        // thumb 优先 thumb/thumbnail/small，没有则 thumb=full
                        $thumb = $full;
                        foreach (['thumb', 'thumbnail', 'small'] as $key) {
                            if (!empty($item[$key])) {
                                $thumb = trim($item[$key]);
                                break;
                            }
                        }

                        $result[] = ['full' => $full, 'thumb' => $thumb];
                    }
                }

                return $result;
            }

            return [];
        };

        // 为每条订单添加 wechat_receipt_images 字段（不再从 hangye 表回退）
        foreach ($list['data'] as &$order) {
            $raw = $order['wechat_receipt_image'] ?? '';
            
            // 【修改】直接解析订单自身的 wechat_receipt_image，不再尝试从 hangye 回退
            // 如果订单 wechat_receipt_image 为空，parseImages 会返回空数组 []
            $order['wechat_receipt_images'] = $parseImages($raw);
            
            // 注意：保留原字段 wechat_receipt_image 以兼容旧逻辑
        }
        unset($order);
        // === wechat_receipt_images 解析兼容结束 ===

        //成单率

        $totalInquiries = Db::table('crm_leads')
            ->alias('l')
            ->where($client_where)
            ->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = Db::table('crm_client_order')
            ->alias('o')
            ->where($where)
            ->sum('o.money');
        $totalProfit = Db::table('crm_client_order')
            ->alias('o')
            ->where($where)
            ->sum('o.profit');
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


    //数据分析
    public function main()
    {

        $params  = Request::param();

        //最近跟进动态 - 根据当前运营人员的 inquiry_id 和 port_id 匹配
        $current_admin = Admin::getMyInfo();
        // 缓存 admin 信息，避免重复查询
        static $current_admin_info_cache = null;
        if ($current_admin_info_cache === null) {
            $current_admin_info_cache = Db::table('admin')->where('admin_id', $current_admin['admin_id'])->find();
        }
        $current_admin_info = $current_admin_info_cache;
        $result = [];
        
        if (!empty($current_admin_info['inquiry_id']) && !empty($current_admin_info['port_id'])) {
            // port_id 是逗号分隔的多选值，需要检查交集
            $admin_port_ids = !empty($current_admin_info['port_id']) ? explode(',', $current_admin_info['port_id']) : [];
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                }
            }
            
            $result = [];
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
        $result = Db::table('crm_leads')
            ->alias('l')
            ->join('crm_comment c', 'c.leads_id = l.id')
            ->join('admin a', 'c.user_id = a.admin_id')
            ->field('l.id,a.username,a.avatar,l.kh_name,c.reply_msg,c.create_date')
            ->order('c.create_date desc')
                    ->where('l.inquiry_id', $current_admin_info['inquiry_id'])
                    ->whereRaw($port_where)
            ->limit(10)->select();
            }
        }
        $this->assign('result', $result);

        //管理员
        $strTimeToString = "000111222334455556666667";
        $strWenhou = array('夜深了，', '凌晨了，', '早上好！', '上午好！', '中午好！', '下午好！', '晚上好！', '夜深了，');
        //echo $strWenhou[(int)$strTimeToString[(int)date('G',time())]];
        $this->assign('wenhou', '尊敬的管理员' . $strWenhou[(int)$strTimeToString[(int)date('G', time())]]);

        //跟进数据 - 根据当前运营人员的 inquiry_id 和 port_id 匹配
        $wheretoday = [];
        $wheretoday['status'] = 1;
        $wheretoday['issuccess'] = -1;
        
        if (!empty($current_admin_info['inquiry_id']) && !empty($current_admin_info['port_id'])) {
            $wheretoday['inquiry_id'] = $current_admin_info['inquiry_id'];
            // port_id 是逗号分隔的多选值，需要检查交集
            $admin_port_ids = !empty($current_admin_info['port_id']) ? explode(',', $current_admin_info['port_id']) : [];
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', port_id) > 0";
                }
            }
            
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                $all_count = Db::table('crm_leads')
                    ->where($wheretoday)
                    ->whereRaw($port_where)
                    ->count();
                $today_count = Db::table('crm_leads')
                    ->where($wheretoday)
                    ->whereRaw($port_where)
                    ->whereTime('last_up_time', 'today')
                    ->count();
            } else {
                $all_count = 0;
                $today_count = 0;
            }
        } else {
            // 超级管理员或没有配置 inquiry_id 和 port_id 的情况
            $isSuper = (session('aid') == 1) || ($current_admin_info['group_id'] == 1) || ($current_admin_info['username'] === 'admin');
            if ($isSuper) {
                // 超级管理员：统计全部数据
                $all_count = Db::table('crm_leads')
                    ->where($wheretoday)
                    ->count();
                $today_count = Db::table('crm_leads')
                    ->where($wheretoday)
                    ->whereTime('last_up_time', 'today')
                    ->count();
            } else {
                // 如果没有配置 inquiry_id 和 port_id，返回0
                $all_count = 0;
                $today_count = 0;
            }
        }
        if ($all_count > 0) {
            $genjinlv = $today_count / $all_count * 100;
        } else {
            $genjinlv = 0;
        }

        $this->assign('all_count', $all_count - $today_count);
        $this->assign('today_count', $today_count);
        $this->assign('genjinlv', round($genjinlv, 2));
        if (request()->isPost()) {
            $data = $this->getPanelData($params);
            $this->assign('data', $data);
            return $this->fetch('main_content');
        }
        $params['keyword']['timebucket'] = 'today';
        $data = $this->getPanelData($params);
        $this->assign('data', $data);
        return $this->fetch('op_main');
    }

    public function perPanel()
    {
        $params  = Request::param();
        if (request()->isPost()) {
            $data = $this->getPerPanelData($params);
            $this->assign('data', $data);
            return $this->fetch('per_content');
        }
        $params['keyword']['timebucket'] = 'today';
        $data = $this->getPerPanelData($params);
        $this->assign('data', $data);
        return $this->fetch();
    }

    private function getPanelData($params)
    {
        $data = [
            'yw_data' => [],
            'yy_data' => [],
            'product_data' => [],
        ];
        $keyword  = $params['keyword'] ?? [];
        // 缓存 admin 信息，避免重复查询
        static $admin_info_cache = null;
        if ($admin_info_cache === null) {
            $admin_info_cache = Admin::getMyInfo();
        }
        $current_admin = $admin_info_cache;
        $where = [$this->getOrgWhere($current_admin['org']), ['is_open', '=', 1],];
        // 对于子查询，使用不带别名的条件，直接构建时间条件而不是使用闭包
        $l_where_sub = [['status', '=', 1]];
        // 对于主查询，使用带别名的条件
        $l_where = [['l.status', '=', 1]];
        $o_where = [];
        if (!empty($keyword['timebucket'])) {
            // 子查询：直接使用 buildTimeWhere 构建条件，然后手动组合 OR
            // 注意：buildTimeWhere 返回的格式需要包装在数组中才能用于 where 方法
            $time_where_at = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $time_where_kh = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            $l_where_sub[] = function($query) use ($time_where_at, $time_where_kh) {
                $query->where([$time_where_at])->whereOr([$time_where_kh]);
            };
            $l_where[] = $this->getClientimeWhere($keyword['timebucket'], 'l');
            $o_where[] = $this->buildTimeWhere($keyword['timebucket'], 'order_time');
        }
        if (!empty($keyword['at_time'])) {
            // 子查询：直接使用 buildTimeWhere 构建条件，然后手动组合 OR
            // 注意：buildTimeWhere 返回的格式需要包装在数组中才能用于 where 方法
            $time_where_at = $this->buildTimeWhere($keyword['at_time'], 'at_time');
            $time_where_kh = $this->buildTimeWhere($keyword['at_time'], 'to_kh_time');
            $l_where_sub[] = function($query) use ($time_where_at, $time_where_kh) {
                $query->where([$time_where_at])->whereOr([$time_where_kh]);
            };
            $l_where[] = $this->getClientimeWhere($keyword['at_time'], 'l');
            $o_where[] = $this->buildTimeWhere($keyword['at_time'], 'order_time');
        }

        //业务询盘数据
        $yw_where = array_merge($where, [['group_id', 'in', [$this->ywgid, $this->ywzgid]]]);
        $ywData = $this->getLeadsSubQuery($l_where_sub)->where($yw_where)->group('a.username,a.team_name')->field('a.username,a.team_name,count(l.id) as yw_num')->order('yw_num desc')->order('a.team_name')->order('a.username')->select();
        // 团队汇总：空团队统一归并到“未分组”（不改变统计口径：仍 count(l.id)）
        $ywTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";
        $ywData_total = $this->getLeadsSubQuery($l_where_sub)
            ->where($yw_where)
            ->group($ywTeamExpr)
            ->field($ywTeamExpr . ' as team_name,count(l.id) as yw_num')
            ->order('yw_num desc')
            ->order('team_name')
            ->select();

        //运营数据 - 根据 inquiry_id 和 port_id 匹配运营人员
        // getYyLeadsSubQuery 已经处理了所有必要的条件（组织、is_open、inquiry_id、port_id）
        $yyData = $this->getYyLeadsSubQuery($l_where_sub)
            ->group('a.username,a.team_name,a.inquiry_id,a.port_id')
            ->field('a.username,a.team_name,a.inquiry_id,a.port_id,count(l.id) as yy_num')
            ->order('yy_num', 'desc')
            ->order('a.team_name')
            ->order('a.username')
            ->select();
        
        // 获取渠道名称用于显示 - 批量查询优化
        $inquiry_ids = array_filter(array_unique(array_column($yyData, 'inquiry_id')));
        $inquiry_map = [];
        if (!empty($inquiry_ids)) {
            $inquiry_map = Db::table('crm_inquiry')
                ->where('id', 'in', $inquiry_ids)
                ->column('inquiry_name', 'id');
        }
        foreach ($yyData as &$item) {
            $item['channel'] = isset($inquiry_map[$item['inquiry_id']]) ? $inquiry_map[$item['inquiry_id']] : '';
        }
        unset($item);
        
        // 按团队名称汇总（只统计运营组人员）
        $yyData_total = $this->getYyLeadsSubQuery($l_where_sub)
            ->where('a.team_name', '<>', '')
            ->group('a.team_name')
            ->field('a.team_name,count(l.id) as yy_num')
            ->order('yy_num', 'desc')
            ->order('a.team_name')
            ->select();

        //询盘产品数据 - 根据运营人员的 inquiry_id 和 port_id 匹配（与运营询盘汇总保持一致）
        // 先获取所有运营人员的 inquiry_id 和 port_id
        $yy_admins_for_prod = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('admin_id,inquiry_id,port_id')
            ->select();
        
        $oper_prod = [];
        if (!empty($yy_admins_for_prod)) {
            // 构建运营人员的 inquiry_id 和 port_id 匹配条件
            $yy_conditions = [];
            foreach ($yy_admins_for_prod as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                // 为每个 port_id 构建 FIND_IN_SET 条件
                $port_conditions = [];
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                    }
                }
                
                if (!empty($port_conditions)) {
                    $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                    $yy_conditions[] = "(l.inquiry_id = {$admin_inquiry_id} AND {$port_where})";
                }
            }
            
            if (!empty($yy_conditions)) {
                $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
                // 处理 where 条件，确保 status 字段明确指定为 l.status（因为 crm_products 表也有 status 字段）
                $oper_prod_where = [];
                foreach ($l_where_sub as $condition) {
                    if (is_array($condition) && isset($condition[0])) {
                        $field_name = $condition[0];
                        // 如果字段名是 status，明确指定为 l.status
                        if ($field_name === 'status') {
                            $oper_prod_where[] = ['l.status', $condition[1] ?? '=', $condition[2] ?? 1];
                        } else {
                            $oper_prod_where[] = $condition;
                        }
                    } elseif (is_callable($condition)) {
                        $oper_prod_where[] = $condition;
                    } else {
                        $oper_prod_where[] = $condition;
                    }
                }
                // 通过 JOIN crm_products 表获取产品名称
                // 按产品名称分组，而不是按产品ID分组
                $oper_prod = Db::table('crm_leads')
                    ->alias('l')
                    ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
                    ->where($oper_prod_where)
                    ->where('l.inquiry_id', '<>', '')
                    ->where('l.inquiry_id', '<>', null)
                    ->where('l.port_id', '<>', '')
                    ->where('l.port_id', '<>', null)
                    ->where('l.product_name', '<>', '')
                    ->where('l.product_name', '<>', null)
                    ->whereRaw($yy_where_raw)
                    ->group('IFNULL(p.product_name, l.product_name)')
                    ->field('IFNULL(p.product_name, l.product_name) as product_name,count(l.id) as count')
                    ->order('count', 'desc')
                    ->limit(10)
                    ->select();
            }
        }
        
        //订单产品数据 - 通过来源端口匹配运营人员
        // 先获取所有运营人员的 inquiry_id 和 port_id
        $yy_admins_for_order = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', '=', $this->yygid)
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('admin_id,inquiry_id,port_id')
            ->select();
        
        $order_prod = [];
        if (!empty($yy_admins_for_order)) {
            // 批量获取所有渠道信息
            $inquiry_ids = array_unique(array_column($yy_admins_for_order, 'inquiry_id'));
            $inquiry_map = [];
            if (!empty($inquiry_ids)) {
                $inquiry_map = Db::table('crm_inquiry')
                    ->where('id', 'in', $inquiry_ids)
                    ->column('inquiry_name', 'id');
            }
            
            // 批量获取所有端口信息
            $all_port_ids = [];
            $inquiry_port_map = [];
            foreach ($yy_admins_for_order as $admin) {
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(array_map('trim', explode(',', $admin['port_id']))) : [];
                foreach ($admin_port_ids as $port_id) {
                    $all_port_ids[] = $port_id;
                    if (!isset($inquiry_port_map[$admin['inquiry_id']])) {
                        $inquiry_port_map[$admin['inquiry_id']] = [];
                    }
                }
            }
            $all_port_ids = array_unique($all_port_ids);
            
            if (!empty($all_port_ids)) {
                $port_list = Db::table('crm_inquiry_port')
                    ->where('id', 'in', $all_port_ids)
                    ->field('id,inquiry_id,port_name')
                    ->select();
                foreach ($port_list as $port) {
                    if (!isset($inquiry_port_map[$port['inquiry_id']])) {
                        $inquiry_port_map[$port['inquiry_id']] = [];
                    }
                    $inquiry_port_map[$port['inquiry_id']][$port['id']] = $port['port_name'];
                }
            }
            
            // 构建运营人员的匹配条件
            $yy_conditions = [];
            foreach ($yy_admins_for_order as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(array_map('trim', explode(',', $admin['port_id']))) : [];
                
                if (empty($admin_port_ids)) continue;
                
                // 从缓存中获取端口名称
                $port_names = [];
                if (isset($inquiry_port_map[$admin_inquiry_id])) {
                    foreach ($admin_port_ids as $port_id) {
                        if (isset($inquiry_port_map[$admin_inquiry_id][$port_id]) && !empty($inquiry_port_map[$admin_inquiry_id][$port_id])) {
                            $port_names[] = addslashes($inquiry_port_map[$admin_inquiry_id][$port_id]);
                        }
                    }
                }
                
                if (!empty($port_names)) {
                    // 构建端口名称匹配条件
                    $port_conditions = [];
                    foreach ($port_names as $port_name) {
                        $port_conditions[] = "o.source_port = '{$port_name}'";
                    }
                    if (!empty($port_conditions)) {
                        $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                        // 从缓存中获取渠道名称
                        $inquiry_name = isset($inquiry_map[$admin_inquiry_id]) ? $inquiry_map[$admin_inquiry_id] : '';
                        if (!empty($inquiry_name)) {
                            $inquiry_name_escaped = addslashes($inquiry_name);
                            $yy_conditions[] = "(o.source = '{$inquiry_name_escaped}' AND {$port_where})";
                        }
                    }
                }
            }
            
            if (!empty($yy_conditions)) {
                $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
                // 统计订单表中的产品名称，按相同名称统计销量
                // 优先从主表的 product_name 字段统计，如果为空则从明细表 crm_order_item 获取
                // 先统计主表有 product_name 的订单
                $order_prod_main = Db::table('crm_client_order')
                    ->alias('o')
                    ->where($o_where)
                    ->whereRaw($yy_where_raw)
                    ->where('o.product_name', '<>', '')
                    ->where('o.product_name', '<>', null)
                    ->group('o.product_name')
                    ->field('o.product_name,count(o.id) as count')
                    ->select();
                
                // 统计主表 product_name 为空，但从明细表获取的订单
                // 通过 JOIN 订单明细表，获取产品名称
                $order_prod_detail = Db::table('crm_client_order')
                    ->alias('o')
                    ->join('crm_order_item oi', 'o.id = oi.order_id', 'INNER')
                    ->where($o_where)
                    ->whereRaw($yy_where_raw)
                    ->where(function($query) {
                        $query->where('o.product_name', '')
                            ->whereOr('o.product_name', null);
                    })
                    ->where('oi.product_name', '<>', '')
                    ->where('oi.product_name', '<>', null)
                    ->group('oi.product_name')
                    ->field('oi.product_name,count(oi.id) as count')
                    ->select();
                
                // 合并结果，按产品名称汇总
                $order_prod_map = [];
                foreach ($order_prod_main as $item) {
                    $product_name = trim($item['product_name']);
                    if (!empty($product_name)) {
                        if (!isset($order_prod_map[$product_name])) {
                            $order_prod_map[$product_name] = 0;
                        }
                        $order_prod_map[$product_name] += $item['count'];
                    }
                }
                foreach ($order_prod_detail as $item) {
                    $product_name = trim($item['product_name']);
                    if (!empty($product_name)) {
                        if (!isset($order_prod_map[$product_name])) {
                            $order_prod_map[$product_name] = 0;
                        }
                        $order_prod_map[$product_name] += $item['count'];
                    }
                }
                
                // 转换为数组格式并排序
                $order_prod = [];
                foreach ($order_prod_map as $product_name => $count) {
                    $order_prod[] = [
                        'product_name' => $product_name,
                        'count' => $count
                    ];
                }
                // 按销量降序排序
                usort($order_prod, function($a, $b) {
                    return $b['count'] - $a['count'];
                });
                // 限制前10条
                $order_prod = array_slice($order_prod, 0, 10);
            }
        }

        // 个人询盘统计 - 根据当前运营人员的 inquiry_id 和 port_id 匹配
        // 如果是超级管理员，则统计全部数据
        // 使用缓存的 admin 信息，避免重复查询
        $isSuper = (session('aid') == 1) || ($current_admin['group_id'] == 1) || ($current_admin['username'] === 'admin');
        
        $xp_count = 0;
        $profit = 0;
        
        if ($isSuper) {
            // 超级管理员：统计全部询盘数量
            $time_where_at = $this->buildTimeWhere('month', 'at_time');
            $time_where_kh = $this->buildTimeWhere('month', 'to_kh_time');
            $xp_count = Db::table('crm_leads')
                ->where('status', 1)
                ->where(function($query) use ($time_where_at, $time_where_kh) {
                    $query->where([$time_where_at])->whereOr([$time_where_kh]);
                })
                ->count();
        } elseif (!empty($current_admin_info['inquiry_id']) && !empty($current_admin_info['port_id'])) {
            // 匹配当前运营人员的 inquiry_id 和 port_id
            // port_id 是逗号分隔的多选值，需要检查交集
            $admin_port_ids = !empty($current_admin_info['port_id']) ? explode(',', $current_admin_info['port_id']) : [];
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', port_id) > 0";
                }
            }
            
            $xp_count = 0;
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                // 直接使用 buildTimeWhere 构建时间条件，避免闭包问题
                $time_where_at = $this->buildTimeWhere('month', 'at_time');
                $time_where_kh = $this->buildTimeWhere('month', 'to_kh_time');
                $xp_count = Db::table('crm_leads')
                    ->where('status', 1)
                    ->where(function($query) use ($time_where_at, $time_where_kh) {
                        $query->where([$time_where_at])->whereOr([$time_where_kh]);
                    })
                    ->where('inquiry_id', $current_admin_info['inquiry_id'])
                    ->whereRaw($port_where)
                    ->count();
            }
        }

        // ====== 我的业绩（严格对齐「业绩订单 -> 订单列表」本月利润合计口径）======
        // 口径：crm_client_order，check_status=2，时间=本月（order_time），普通用户：pr_user=本人 OR at_user=本人
        $profit = $this->getPanelMonthProfitByOrderListRule($current_admin_info);

        $data['xp_count'] = $xp_count;
        $data['profit'] = $profit;

        //总询盘统计 - 按渠道和端口统计
        $total_inquiry_stats = $this->getTotalInquiryStats($l_where_sub);

        $data['yw_data']['list'] = $ywData;
        $data['yw_data']['total'] = $ywData_total;
        $data['yy_data']['list'] = $yyData;
        $data['yy_data']['total'] = $yyData_total;
        $data['product_data']['oper_prod'] = $oper_prod;
        $data['product_data']['order_prod'] = $order_prod;
        $data['total_inquiry_stats'] = $total_inquiry_stats;

        $data['org'] = trim($current_admin['org'], $this->org_fgx);

        return $data;
    }

    /**
     * 控制面板「我的业绩」- 复用订单列表统计口径
     * 口径对齐 Order.php -> clientSearch():
     * - 表：crm_client_order
     * - 审核通过：check_status = 2
     * - 时间范围：本月（order_time）
     * - 普通用户：pr_user=本人 OR at_user=本人
     * - 返回：两位小数（字符串）
     */
    private function getPanelMonthProfitByOrderListRule($currentAdmin): string
    {
        $username = $currentAdmin['username'] ?? '';
        $isSuper = (session('aid') == 1) || (($currentAdmin['group_id'] ?? null) == 1) || ($username === 'admin');

        $where = [];
        $where[] = ['check_status', '=', 2];
        $where[] = $this->buildTimeWhere('month', 'order_time');

        $query = Db::table('crm_client_order')->where($where);
        if (!$isSuper && $username !== '') {
            $query->where(function ($q) use ($username) {
                $q->where('pr_user', '=', $username)
                    ->whereOr('at_user', '=', $username);
            });
        }

        $profit = (float)$query->sum('profit');
        return number_format($profit, 2, '.', '');
    }

    private function getPerPanelData($params)
    {
        $data = [
            'yw_data' => [],
            'yy_data' => [],
            'product_data' => [],
        ];
        $keyword  = $params['keyword'] ?? [];
        $current_admin = Admin::getMyInfo();
        $current_admin_info = Db::table('admin')->where('admin_id', $current_admin['admin_id'])->find();
        $where = [$this->getOrgWhere($current_admin['org']), ['is_open', '=', 1]];
        
        // 判断当前用户是业务员还是运营人员
        $is_yw = in_array($current_admin_info['group_id'] ?? 0, [$this->ywgid, $this->ywzgid]);
        $is_yy = ($current_admin_info['group_id'] ?? 0) == $this->yygid;
        
        // 对于子查询，使用不带别名的条件
        $l_where_sub = [['status', '=', 1]];
        // 对于主查询，使用带别名的条件
        $l_where = [['l.status', '=', 1]];
        
        // 根据用户类型添加不同的过滤条件
        if ($is_yw) {
            // 业务员：通过 pr_user 或 oper_user 匹配
            $l_where_sub[] = ['pr_user', '=', $current_admin['username']];
            $l_where[] = ['l.pr_user', '=', $current_admin['username']];
        } elseif ($is_yy && !empty($current_admin_info['inquiry_id']) && !empty($current_admin_info['port_id'])) {
            // 运营人员：通过 inquiry_id 和 port_id 匹配
            $admin_port_ids = !empty($current_admin_info['port_id']) ? array_filter(explode(',', $current_admin_info['port_id'])) : [];
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', port_id) > 0";
                }
            }
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                // 对于子查询，使用闭包处理复杂条件
                $l_where_sub[] = function($query) use ($current_admin_info, $port_where) {
                    $query->where('inquiry_id', $current_admin_info['inquiry_id'])
                        ->whereRaw($port_where);
                };
                // 对于主查询，直接添加条件
                $l_where[] = ['l.inquiry_id', '=', $current_admin_info['inquiry_id']];
                $l_where[] = function($query) use ($port_where) {
                    $query->whereRaw(str_replace('port_id', 'l.port_id', $port_where));
                };
            }
        }
        
        // 处理时间条件 - 对于子查询，直接使用 buildTimeWhere 构建条件，然后手动组合 OR
        if (!empty($keyword['timebucket'])) {
            $time_where_at = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
            $time_where_kh = $this->buildTimeWhere($keyword['timebucket'], 'to_kh_time');
            // 对于子查询，使用闭包组合时间条件
            $l_where_sub[] = function($query) use ($time_where_at, $time_where_kh) {
                $query->where([$time_where_at])->whereOr([$time_where_kh]);
            };
            // 对于主查询，使用 getClientimeWhere 方法
            $l_where[] = $this->getClientimeWhere($keyword['timebucket'], 'l');
        }
        if (!empty($keyword['at_time'])) {
            $time_where_at = $this->buildTimeWhere($keyword['at_time'], 'at_time');
            $time_where_kh = $this->buildTimeWhere($keyword['at_time'], 'to_kh_time');
            // 对于子查询，使用闭包组合时间条件
            $l_where_sub[] = function($query) use ($time_where_at, $time_where_kh) {
                $query->where([$time_where_at])->whereOr([$time_where_kh]);
            };
            // 对于主查询，使用 getClientimeWhere 方法
            $l_where[] = $this->getClientimeWhere($keyword['at_time'], 'l');
        }

        //业务询盘数据 - 只显示业务员的数据
        $yw_where = array_merge($where, [['group_id', 'in', [$this->ywgid, $this->ywzgid]]]);
        $ywData = $this->getLeadsSubQuery($l_where_sub, 'pr_user')->where($yw_where)->group('a.username,a.team_name')->field('a.username,a.team_name,count(l.id) as yw_num')->order('yw_num desc')->order('a.team_name')->order('a.username')->select();
        // 团队汇总：空团队统一归并到“未分组”（不改变统计口径：仍 count(l.id)）
        $ywTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";
        $ywData_total = $this->getLeadsSubQuery($l_where_sub, 'pr_user')
            ->where($yw_where)
            ->group($ywTeamExpr)
            ->field($ywTeamExpr . ' as team_name,count(l.id) as yw_num')
            ->order('yw_num desc')
            ->order('team_name')
            ->select();

        //询盘产品数据 - 根据当前用户的类型匹配
        $oper_prod = [];
        if ($is_yw) {
            // 业务员：通过 pr_user 匹配
            $oper_prod = Db::table('crm_leads')
                ->alias('l')
                ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
                ->where($l_where)
                ->where('l.product_name', '<>', '')
                ->where('l.product_name', '<>', null)
                ->group('IFNULL(p.product_name, l.product_name)')
                ->field('IFNULL(p.product_name, l.product_name) as product_name,count(l.id) as count')
                ->order('count', 'desc')
                ->select();
        } elseif ($is_yy && !empty($current_admin_info['inquiry_id']) && !empty($current_admin_info['port_id'])) {
            // 运营人员：通过 inquiry_id 和 port_id 匹配
            $admin_port_ids = !empty($current_admin_info['port_id']) ? array_filter(explode(',', $current_admin_info['port_id'])) : [];
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                }
            }
            
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                $oper_prod = Db::table('crm_leads')
                    ->alias('l')
                    ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
                    ->where($l_where)
                    ->where('l.inquiry_id', $current_admin_info['inquiry_id'])
                    ->whereRaw($port_where)
                    ->where('l.product_name', '<>', '')
                    ->where('l.product_name', '<>', null)
                    ->group('IFNULL(p.product_name, l.product_name)')
                    ->field('IFNULL(p.product_name, l.product_name) as product_name,count(l.id) as count')
                    ->order('count', 'desc')
                    ->select();
            }
        }
        
        $data['yw_data']['list'] = $ywData;
        $data['yw_data']['total'] = $ywData_total;
        $data['product_data']['oper_prod'] = $oper_prod;
        return $data;
    }

    private function getLeadsSubQuery($where, $field = 'pr_user')
    {
        $current_admin = Admin::getMyInfo();
        $users = Db::table('admin')->where($this->getOrgWhere($current_admin['org']))->column('username');
        
        // 如果用户列表为空，返回空查询
        if (empty($users)) {
            return Db::table('admin')->alias('a')->where('1=0');
        }
        
        // 处理 where 条件，将带表别名的字段转换为不带别名的（因为子查询中没有别名）
        $sub_where = [];
        foreach ($where as $condition) {
            if (is_array($condition) && isset($condition[0])) {
                // 检查是否是嵌套数组格式（如 buildTimeWhere 返回的 [[$field, '>=', $start]]）
                if (is_array($condition[0]) && isset($condition[0][0])) {
                    // 嵌套数组格式
                    $nested_condition = $condition[0];
                    $field_name = $nested_condition[0];
                    if (is_string($field_name) && strpos($field_name, '.') !== false) {
                        $field_name = substr($field_name, strpos($field_name, '.') + 1);
                    }
                    $sub_where[] = [[$field_name, $nested_condition[1] ?? '=', $nested_condition[2] ?? null]];
                } else {
                    // 普通数组格式
                    $field_name = $condition[0];
                    // 如果字段名包含表别名（如 l.status），移除别名
                    if (is_string($field_name) && strpos($field_name, '.') !== false) {
                        $field_name = substr($field_name, strpos($field_name, '.') + 1);
                    }
                    // 处理 buildTimeWhere 返回的格式：[$field, 'between time', [...]]
                    if (count($condition) == 3 && isset($condition[1]) && $condition[1] == 'between time') {
                        $sub_where[] = [$field_name, $condition[1], $condition[2]];
                    } else {
                        $sub_where[] = [$field_name, $condition[1] ?? '=', $condition[2] ?? null];
                    }
                }
            } elseif (is_callable($condition)) {
                // 闭包条件：创建一个包装闭包，确保字段名不包含表别名
                $sub_where[] = function($query) use ($condition) {
                    // 直接调用原闭包，因为 getClientimeWhere 已经处理了别名（传递空字符串时）
                    $condition($query);
                };
            } else {
                $sub_where[] = $condition;
            }
        }
        // 构建子查询，使用 * 选择所有字段以确保包含 id 字段
        $subQuery = Db::table('crm_leads')
            ->where($sub_where)->where($field,'in',$users)
            ->buildSql();
        return Db::table('admin')->alias('a')
            ->leftJoin([$subQuery => 'l'], 'a.username = l.' . $field);
    }

    /**
     * 获取运营人员询盘数据的子查询
     * 根据 crm_leads 的 inquiry_id 和 port_id 匹配 admin 表中的运营人员
     * port_id 是多选字段（逗号分隔），需要检查交集
     */
    private function getYyLeadsSubQuery($where)
    {
        $current_admin = Admin::getMyInfo();
        
        // 处理 where 条件，将带表别名的字段转换为不带别名的（因为子查询中没有别名）
        $sub_where = [];
        foreach ($where as $condition) {
            if (is_array($condition) && isset($condition[0])) {
                // 检查是否是嵌套数组格式（如 buildTimeWhere 返回的 [[$field, '>=', $start]]）
                if (is_array($condition[0]) && isset($condition[0][0])) {
                    // 嵌套数组格式
                    $nested_condition = $condition[0];
                    $field_name = $nested_condition[0];
                    if (is_string($field_name) && strpos($field_name, '.') !== false) {
                        $field_name = substr($field_name, strpos($field_name, '.') + 1);
                    }
                    $sub_where[] = [[$field_name, $nested_condition[1] ?? '=', $nested_condition[2] ?? null]];
                } else {
                    // 普通数组格式
                    $field_name = $condition[0];
                    // 如果字段名包含表别名（如 l.status），移除别名
                    if (is_string($field_name) && strpos($field_name, '.') !== false) {
                        $field_name = substr($field_name, strpos($field_name, '.') + 1);
                    }
                    // 处理 buildTimeWhere 返回的格式：[$field, 'between time', [...]]
                    if (count($condition) == 3 && isset($condition[1]) && $condition[1] == 'between time') {
                        $sub_where[] = [$field_name, $condition[1], $condition[2]];
                    } else {
                        $sub_where[] = [$field_name, $condition[1] ?? '=', $condition[2] ?? null];
                    }
                }
            } elseif (is_callable($condition)) {
                // 闭包条件：创建一个包装闭包，确保字段名不包含表别名
                $sub_where[] = function($query) use ($condition) {
                    // 直接调用原闭包，因为 getClientimeWhere 已经处理了别名（传递空字符串时）
                    $condition($query);
                };
            } else {
                $sub_where[] = $condition;
            }
        }
        
        // 构建子查询：查询所有符合条件的 leads（有 inquiry_id 和 port_id）
        // 使用 * 选择所有字段以确保包含 id 字段
        $subQuery = Db::table('crm_leads')
            ->where($sub_where)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->buildSql();
        
        // 先获取所有运营人员及其 port_id 列表，用于构建 JOIN 条件
        // 只统计用户组是运营的人员（group_id = yygid）
        $yy_admins = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', '=', $this->yygid)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->where('is_open', '=', 1)
            ->field('admin_id,inquiry_id,port_id')
            ->select();
        
        // 构建 JOIN 条件：对于每个运营人员，检查其 port_id 是否与 leads 的 port_id 有交集
        $joinConditions = [];
        foreach ($yy_admins as $admin) {
            $admin_inquiry_id = $admin['inquiry_id'];
            $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
            
            if (empty($admin_port_ids)) continue;
            
            // 为每个 port_id 构建 FIND_IN_SET 条件
            $port_conditions = [];
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                }
            }
            
            if (!empty($port_conditions)) {
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                $joinConditions[] = "(a.admin_id = {$admin['admin_id']} AND l.inquiry_id = {$admin_inquiry_id} AND {$port_where})";
            }
        }
        
        if (empty($joinConditions)) {
            return Db::table('admin')->alias('a')->where('1=0');
        }
        
        $joinCondition = '(' . implode(' OR ', $joinConditions) . ')';
        
        return Db::table('admin')->alias('a')
            ->leftJoin([$subQuery => 'l'], $joinCondition)
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('a.group_id', '=', $this->yygid)
            ->where('a.inquiry_id', '<>', '')
            ->where('a.inquiry_id', '<>', null)
            ->where('a.port_id', '<>', '')
            ->where('a.port_id', '<>', null)
            ->where('a.is_open', '=', 1);
    }

    public function getProdAll()
    {
        $type = Request::param('type');
        if (!in_array($type, ['order', 'oper'])) {
            return ['code' => 400, 'msg' => '参数错误'];
        }
        $current_admin = Admin::getMyInfo();
        $where = [$this->getOrgWhere($current_admin['org']), ['is_open', '=', 1],];
        $l_where = [];
        $o_where = [];
        $time = '';
        if (Request::param('timebucket')) {
            $time = Request::param('timebucket');
        }
        if (Request::param('at_time')) {
            $time = Request::param('at_time');
        }
        if ($time) {
            $l_where[] = $this->buildTimeWhere($time, 'at_time');
            $o_where[] = $this->buildTimeWhere($time, 'order_time');
        }
        if ($type == 'oper') {
            // 询盘产品数据 - 根据运营人员的 inquiry_id 和 port_id 匹配（与运营询盘汇总保持一致）
            $current_admin = Admin::getMyInfo();
            // 先获取所有运营人员的 inquiry_id 和 port_id
            $yy_admins_for_prod = Db::table('admin')
                ->where($this->getOrgWhere($current_admin['org']))
                ->where('is_open', '=', 1)
                ->where('inquiry_id', '<>', '')
                ->where('inquiry_id', '<>', null)
                ->where('port_id', '<>', '')
                ->where('port_id', '<>', null)
                ->field('admin_id,inquiry_id,port_id')
                ->select();
            
            $data = [];
            if (!empty($yy_admins_for_prod)) {
                // 构建运营人员的 inquiry_id 和 port_id 匹配条件
                $yy_conditions = [];
                foreach ($yy_admins_for_prod as $admin) {
                    $admin_inquiry_id = $admin['inquiry_id'];
                    $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                    
                    if (empty($admin_port_ids)) continue;
                    
                    // 为每个 port_id 构建 FIND_IN_SET 条件
                    $port_conditions = [];
                    foreach ($admin_port_ids as $port_id) {
                        $port_id = trim($port_id);
                        if ($port_id) {
                            $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                        }
                    }
                    
                    if (!empty($port_conditions)) {
                        $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                        $yy_conditions[] = "(l.inquiry_id = {$admin_inquiry_id} AND {$port_where})";
                    }
                }
                
                if (!empty($yy_conditions)) {
                    $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
                    // 处理时间条件，转换为不带别名的格式，但 status 字段需要明确指定为 l.status
                    $l_where_sub = [];
                    foreach ($l_where as $condition) {
                        if (is_array($condition) && isset($condition[0])) {
                            $field_name = $condition[0];
                            // 如果字段名是 status，明确指定为 l.status（因为 crm_products 表也有 status 字段）
                            if ($field_name === 'status' || $field_name === 'l.status') {
                                $l_where_sub[] = ['l.status', $condition[1] ?? '=', $condition[2] ?? 1];
        } else {
                                // 如果字段名包含表别名（如 l.at_time），移除别名
                                if (is_string($field_name) && strpos($field_name, '.') !== false) {
                                    $field_name = substr($field_name, strpos($field_name, '.') + 1);
                                }
                                // 处理 buildTimeWhere 返回的格式
                                if (count($condition) == 3 && isset($condition[1]) && $condition[1] == 'between time') {
                                    $l_where_sub[] = [$field_name, $condition[1], $condition[2]];
                                } else {
                                    $l_where_sub[] = [$field_name, $condition[1] ?? '=', $condition[2] ?? null];
                                }
                            }
                        } elseif (is_callable($condition)) {
                            // 闭包条件：直接使用
                            $l_where_sub[] = function($query) use ($condition) {
                                $condition($query);
                            };
                        } else {
                            $l_where_sub[] = $condition;
                        }
                    }
                    // 添加 status 条件，明确指定为 l.status（因为 crm_products 表也有 status 字段）
                    $l_where_sub[] = ['l.status', '=', 1];
                    
                    // 通过 JOIN crm_products 表获取产品名称
                    // 按产品名称分组，而不是按产品ID分组
                    $data = Db::table('crm_leads')
                        ->alias('l')
                        ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
                        ->where($l_where_sub)
                        ->where('l.inquiry_id', '<>', '')
                        ->where('l.inquiry_id', '<>', null)
                        ->where('l.port_id', '<>', '')
                        ->where('l.port_id', '<>', null)
                        ->where('l.product_name', '<>', '')
                        ->where('l.product_name', '<>', null)
                        ->whereRaw($yy_where_raw)
                        ->group('IFNULL(p.product_name, l.product_name)')
                        ->field('IFNULL(p.product_name, l.product_name) as product_name,count(l.id) as count')
                        ->order('count', 'desc')
                        ->select();
                }
            }
        } else {
            //订单产品数据 - 通过来源端口匹配运营人员
            // 先获取所有运营人员的 inquiry_id 和 port_id
            $yy_admins_for_order = Db::table('admin')
                ->where($this->getOrgWhere($current_admin['org']))
                ->where('group_id', '=', $this->yygid)
                ->where('is_open', '=', 1)
                ->where('inquiry_id', '<>', '')
                ->where('inquiry_id', '<>', null)
                ->where('port_id', '<>', '')
                ->where('port_id', '<>', null)
                ->field('admin_id,inquiry_id,port_id')
                ->select();
            
            $data = [];
            if (!empty($yy_admins_for_order)) {
                // 构建运营人员的匹配条件
                // 订单表的 source_port 是端口名称（文字），需要通过 crm_inquiry_port 表转换为端口ID
                // 然后通过端口ID匹配 admin 表中的 port_id（多选值，使用 FIND_IN_SET）
                $yy_conditions = [];
                foreach ($yy_admins_for_order as $admin) {
                    $admin_inquiry_id = $admin['inquiry_id'];
                    $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                    
                    if (empty($admin_port_ids)) continue;
                    
                    // 获取该渠道下所有端口的名称列表
                    $port_names = [];
                    foreach ($admin_port_ids as $port_id) {
                        $port_id = trim($port_id);
                        if ($port_id) {
                            $port_info = Db::table('crm_inquiry_port')
                                ->where('id', $port_id)
                                ->where('inquiry_id', $admin_inquiry_id)
                                ->field('port_name')
                                ->find();
                            if ($port_info && !empty($port_info['port_name'])) {
                                $port_names[] = addslashes($port_info['port_name']); // 转义防止SQL注入
                            }
                        }
                    }
                    
                    if (!empty($port_names)) {
                        // 构建端口名称匹配条件
                        $port_conditions = [];
                        foreach ($port_names as $port_name) {
                            $port_conditions[] = "o.source_port = '{$port_name}'";
                        }
                        if (!empty($port_conditions)) {
                            $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                            // 还需要匹配渠道：通过 source 字段（渠道名称）匹配
                            $inquiry_info = Db::table('crm_inquiry')
                                ->where('id', $admin_inquiry_id)
                                ->field('inquiry_name')
                                ->find();
                            if ($inquiry_info && !empty($inquiry_info['inquiry_name'])) {
                                $inquiry_name = addslashes($inquiry_info['inquiry_name']); // 转义防止SQL注入
                                $yy_conditions[] = "(o.source = '{$inquiry_name}' AND {$port_where})";
                            }
                        }
                    }
                }
                
                if (!empty($yy_conditions)) {
                    $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
                    // 统计订单表中的产品名称，按相同名称统计销量
                    // 优先从主表的 product_name 字段统计，如果为空则从明细表 crm_order_item 获取
                    // 先统计主表有 product_name 的订单
                    $order_prod_main = Db::table('crm_client_order')
                        ->alias('o')
                        ->where($o_where)
                        ->whereRaw($yy_where_raw)
                        ->where('o.product_name', '<>', '')
                        ->where('o.product_name', '<>', null)
                        ->group('o.product_name')
                        ->field('o.product_name,count(o.id) as count')
                        ->select();
                    
                    // 统计主表 product_name 为空，但从明细表获取的订单
                    // 通过 JOIN 订单明细表，获取产品名称
                    $order_prod_detail = Db::table('crm_client_order')
                        ->alias('o')
                        ->join('crm_order_item oi', 'o.id = oi.order_id', 'INNER')
                        ->where($o_where)
                        ->whereRaw($yy_where_raw)
                        ->where(function($query) {
                            $query->where('o.product_name', '')
                                ->whereOr('o.product_name', null);
                        })
                        ->where('oi.product_name', '<>', '')
                        ->where('oi.product_name', '<>', null)
                        ->group('oi.product_name')
                        ->field('oi.product_name,count(oi.id) as count')
                        ->select();
                    
                    // 合并结果，按产品名称汇总
                    $order_prod_map = [];
                    foreach ($order_prod_main as $item) {
                        $product_name = trim($item['product_name']);
                        if (!empty($product_name)) {
                            if (!isset($order_prod_map[$product_name])) {
                                $order_prod_map[$product_name] = 0;
                            }
                            $order_prod_map[$product_name] += $item['count'];
                        }
                    }
                    foreach ($order_prod_detail as $item) {
                        $product_name = trim($item['product_name']);
                        if (!empty($product_name)) {
                            if (!isset($order_prod_map[$product_name])) {
                                $order_prod_map[$product_name] = 0;
                            }
                            $order_prod_map[$product_name] += $item['count'];
                        }
                    }
                    
                    // 转换为数组格式并排序
                    $data = [];
                    foreach ($order_prod_map as $product_name => $count) {
                        $data[] = [
                            'product_name' => $product_name,
                            'count' => $count
                        ];
                    }
                    // 按销量降序排序
                    usort($data, function($a, $b) {
                        return $b['count'] - $a['count'];
                    });
                }
            }
        }
        return  json([
            'code' => 200,
            'msg' => '获取成功!',
            'data' => $data,
        ]);
    }

    public function getDetail()
    {

        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        if (Request::isPost()) {
            // $type = Request::param('type', 'yw-total');
            if ($timebucket || $at_time) {
                $timebucket = $timebucket ? $timebucket : $at_time;
            }
            if (!$timebucket) $timebucket = 'today';

            // if (!in_array($type, ['yw-total', 'yy-total'])) {
            //     return ['code' => 400, 'msg' => '参数错误'];
            // }

            $data = $this->getCrossData($timebucket);
            return json([
                'code' => 200,
                'msg' => '获取成功!',
                'data' => $data
            ]);
        }
        $this->assign('timebucket', $timebucket);
        $this->assign('at_time', $at_time);
        return $this->fetch();
    }

    private function getCrossData($timebucket)
    {
        $current_admin = Admin::getMyInfo();
        $a_where = [$this->getOrgWhere($current_admin['org']), ['is_open', '=', 1], ['group_id', 'in', [$this->ywgid, $this->ywzgid, $this->yygid]]];
        $l_where = [['status', '=', 1]];
        // 时间条件
        if (!empty($timebucket)) {
            $l_where[] = $this->getClientimeWhere($timebucket);
        }
        //时间段内所有客户
        $leads = Db::table('crm_leads')
            ->where($l_where)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->fetchCollection()
            ->select();

        //所有业务
        $yw_where = array_merge($a_where, [['group_id', 'in', [$this->ywgid, $this->ywzgid]]]);
        $yw_admins = Db::table('admin')
            ->where($yw_where)
            ->where('username', '<>', '')
            ->order('team_name,username')
            ->select();

        //所有运营 - 根据 inquiry_id 和 port_id 有值的记录
        $yy_where = array_merge($a_where, [
            ['inquiry_id', '<>', ''],
            ['inquiry_id', '<>', null],
            ['port_id', '<>', ''],
            ['port_id', '<>', null]
        ]);
        $yy_admins = Db::table('admin')
            ->where($yy_where)
            ->where('username', '<>', '')
            ->order('team_name,username')
            ->select();
        
        // 构建运营人员映射表 - 由于 port_id 是逗号分隔的多选值，需要检查交集
        $yy_map = [];
        foreach ($yy_admins as $yy_admin) {
            $admin_inquiry_id = $yy_admin['inquiry_id'];
            $admin_port_ids = !empty($yy_admin['port_id']) ? array_filter(explode(',', $yy_admin['port_id'])) : [];
            
            // 为每个运营人员创建一个键，包含 inquiry_id 和所有 port_id
            foreach ($admin_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $key = $admin_inquiry_id . '_' . $port_id;
                    if (!isset($yy_map[$key])) {
                        $yy_map[$key] = [];
                    }
                    $yy_map[$key][] = $yy_admin['username'];
                }
            }
        }
        
        // 构建交叉数据 - 根据 inquiry_id 和 port_id 匹配运营人员
        $cross = [];
        foreach ($leads as $lead) {
            if (empty($lead['pr_user'])) continue;
            
            // 根据 lead 的 inquiry_id 和 port_id 找到对应的运营人员
            // port_id 可能是逗号分隔的多个值
            $lead_inquiry_id = $lead['inquiry_id'];
            $lead_port_ids = !empty($lead['port_id']) ? array_filter(explode(',', $lead['port_id'])) : [];
            
            foreach ($lead_port_ids as $port_id) {
                $port_id = trim($port_id);
                if ($port_id) {
                    $key = $lead_inquiry_id . '_' . $port_id;
                    if (isset($yy_map[$key])) {
                        // 可能有多个运营人员匹配同一个 port_id
                        foreach ($yy_map[$key] as $yy_username) {
            if (!isset($cross[$lead['pr_user']])) {
                $cross[$lead['pr_user']] = [];
            }
                            if (!isset($cross[$lead['pr_user']][$yy_username])) {
                                $cross[$lead['pr_user']][$yy_username] = 1;
            } else {
                                $cross[$lead['pr_user']][$yy_username]++;
                            }
                        }
                    }
                }
            }
        }

        // 按团队分组业务人员
        $team_colors = ['#db7070','#c6db70','#70db9b','#709bdb','#c670db',  '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#cf1717', '#aacf17', '#17cf60', '#1760cf', '#aa17cf']; // 每个组的颜色
        $color_index = 0;

        // 构建横向表头（所有运营）
        $headers = ['姓名'];
        foreach ($yy_admins as $yy_admin) {
            $headers[] = $yy_admin['username'];
        }
        $headers[] = '合计';
        
        // 转换 yy_admins 为数组格式以便后续使用
        $yy_admins_array = $yy_admins;

        // 按团队处理业务人员
        $teams = [];
        foreach ($yw_admins as $yw_admin) {
            $team_name = $yw_admin['team_name'];
            if (!isset($teams[$team_name])) {
                $teams[$team_name] = [
                    'name' => $team_name,
                    'color' => $team_colors[$color_index % count($team_colors)],
                    'headers' => $headers,
                    'rows' => [],
                    'totals' => array_fill(0, count($headers), 0),
                    'grandTotal' => 0
                ];
                $color_index++;
            }

            // 构建行数据
            $row_data = [$yw_admin['username']];
            $row_total = 0;

            foreach ($yy_admins as $yy_admin) {
                $count = isset($cross[$yw_admin['username']][$yy_admin['username']]) ? $cross[$yw_admin['username']][$yy_admin['username']] : 0;
                $row_data[] = $count;
                $row_total += $count;
            }
            $row_data[] = $row_total; // 小计

            $teams[$team_name]['rows'][] = $row_data;
            $teams[$team_name]['grandTotal'] += $row_total;

            // 更新团队列总计
            foreach ($row_data as $col_index => $value) {
                if ($col_index > 0) { // 跳过姓名列
                    $teams[$team_name]['totals'][$col_index] += $value;
                }
            }
        }

        // 计算总统计
        $grand_totals = array_fill(0, count($headers), 0);
        $grand_grand_total = 0;

        foreach ($teams as $team) {
            foreach ($team['totals'] as $index => $total) {
                $grand_totals[$index] += $total;
            }
            $grand_grand_total += $team['grandTotal'];
        }

        // 构建最终数据结构
        $result = [
            'teams' => array_values($teams),
            'grandTotals' => [
                'headers' => $headers,
                'totals' => $grand_totals,
                'grandTotal' => $grand_grand_total
            ]
        ];
        return $result;
    }

    private function getCrossData_style1($timebucket)
    {
        $current_admin = Admin::getMyInfo();
        $a_where = [$this->getOrgWhere($current_admin['org']), ['is_open', '=', 1], ['group_id', 'in', [$this->ywgid, $this->ywzgid, $this->yygid]]];
        $l_where = [['status', '=', 1]];
        // 时间条件
        if (!empty($timebucket)) {
            $l_where[] = $this->getClientimeWhere($timebucket);
        }
        //时间段内所有客户
        $leads = Db::table('crm_leads')->where($l_where)->fetchCollection()->select();

        //所有业务和运营
        $admins = Db::table('admin')
            ->where($a_where)
            ->where('username', '<>', '')
            ->order('team_name,channel,username')->fetchCollection()
            ->select();

        $cross = [];
        foreach ($leads as $lead) {
            if (!isset($cross[$lead['pr_user']])) {
                $cross[$lead['pr_user']] = [];
            }
            if (!isset($cross[$lead['pr_user']][$lead['oper_user']])) {
                $cross[$lead['pr_user']][$lead['oper_user']] = 1;
            } else {
                $cross[$lead['pr_user']][$lead['oper_user']]++;
            }
        }

        // 数据结构
        $yw_admins = $admins->where('group_id', 'in', [$this->ywgid, $this->ywzgid]);
        $data = [];
        $team_yy_users = [];
        foreach ($yw_admins as $yw_admin) {
            if (!isset($team_yy_users[$yw_admin['team_name']])) {
                $team_yy_users[$yw_admin['team_name']] = [];
            }

            if (isset($cross[$yw_admin['username']])) {
                foreach ($cross[$yw_admin['username']] as $yy_user => $count) {
                    if (!in_array($yy_user, $team_yy_users[$yw_admin['team_name']])) {
                        $team_yy_users[$yw_admin['team_name']][] = $yy_user;
                    }
                }
            }
        }

        // 构建数据结构
        foreach ($yw_admins as $yw_admin) {
            if (!isset($data[$yw_admin['team_name']])) {
                $data[$yw_admin['team_name']] = [
                    'name' => $yw_admin['team_name'],
                    'headers' => ['姓名'], // 第一列空格
                    'rows' => [],
                    'totals' => [],
                    'grandTotal' => 0
                ];
                // 添加该团队有数据的运营人员表头
                foreach ($team_yy_users[$yw_admin['team_name']] as $yy_user) {
                    $data[$yw_admin['team_name']]['headers'][] = $yy_user;
                }
            }

            // 添加业务人员行数据
            $row_data = [$yw_admin['username']]; // 第一列是业务人员名称
            $row_total = 0;

            foreach ($team_yy_users[$yw_admin['team_name']] as $yy_user) {
                $count = isset($cross[$yw_admin['username']][$yy_user]) ? $cross[$yw_admin['username']][$yy_user] : 0;
                $row_data[] = $count;
                $row_total += $count;
            }

            $data[$yw_admin['team_name']]['rows'][] = $row_data;
            $data[$yw_admin['team_name']]['grandTotal'] += $row_total;
        }

        // 计算每个团队的列总计
        foreach ($data as $team_name => &$team) {
            $col_totals = [0]; // 第一列总计为0
            for ($i = 1; $i < count($team['headers']); $i++) {
                $col_total = 0;
                foreach ($team['rows'] as $row) {
                    $col_total += $row[$i];
                }
                $col_totals[] = $col_total;
            }
            $team['totals'] = [$col_totals];
        }

        return array_values($data);
    }

    private function getYyCrossData($where, $l_where)
    {
        $data = [
            'headers' => [],
            'rows' => [],
            'totals' => [],
            'grandTotal' => 0
        ];

        // 获取业务人员列表（横向表头）
        $yw_where = array_merge($where, [['group_id', 'in', [$this->ywgid, $this->ywzgid]]]);
        $yw_users = Db::table('admin')
            ->where($yw_where)
            ->where('username', '<>', '')
            ->field('username,team_name')
            ->order('team_name,username')
            ->select();

        // 获取运营人员列表（纵向表头）
        $yy_where = array_merge($where, [['group_id', '=', $this->yygid]]);
        $yy_users = Db::table('admin')
            ->where($yy_where)
            ->where('username', '<>', '')
            ->field('username,team_name')
            ->order('team_name,username')
            ->select();

        // 构建业务表头
        $yw_headers = [];
        foreach ($yw_users as $yw_user) {
            $yw_headers[] = $yw_user['username'];
        }
        $data['headers'] = $yw_headers;

        // 构建运营数据行
        foreach ($yy_users as $yy_user) {
            $row = [
                'name' => $yy_user['username'],
                'values' => [],
                'total' => 0
            ];

            // 获取该运营人员与各业务人员的交叉数据
            foreach ($yw_users as $yw_user) {
                $count = Db::table('crm_leads')
                    ->where('pr_user', $yw_user['username'])
                    ->where('oper_user', $yy_user['username'])
                    ->where($l_where)
                    ->count();

                $row['values'][] = $count;
                $row['total'] += $count;
            }

            $data['rows'][] = $row;
        }

        // 计算列总计
        $col_totals = array_fill(0, count($yw_headers), 0);
        $grand_total = 0;

        foreach ($data['rows'] as $row) {
            foreach ($row['values'] as $index => $value) {
                $col_totals[$index] += $value;
                $grand_total += $value;
            }
        }

        $data['totals'] = $col_totals;
        $data['grandTotal'] = $grand_total;

        return $data;
    }

    /**
     * 获取总询盘统计
     * 优先统计各个渠道的端口下的询盘数据，如果该渠道的端口为空，则统计该渠道的数据
     */
    private function getTotalInquiryStats($l_where_sub)
    {
        $current_admin = Admin::getMyInfo();
        $stats = [];
        
        // 获取所有渠道（启用状态的）
        $inquiries = Db::table('crm_inquiry')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('status', '=', 0)
            ->field('id, inquiry_name')
            ->order('inquiry_name', 'asc')
            ->select();
        
        if (empty($inquiries)) {
            return $stats;
        }
        
        $inquiry_ids = array_column($inquiries, 'id');
        $inquiry_map = array_column($inquiries, 'inquiry_name', 'id');
        
        // 批量获取所有端口（启用状态的）
        $all_ports = Db::table('crm_inquiry_port')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('inquiry_id', 'in', $inquiry_ids)
            ->where('status', '=', 0)
            ->field('id, inquiry_id, port_name')
            ->order('inquiry_id, port_name', 'asc')
            ->select();
        
        // 按渠道分组端口
        $ports_by_inquiry = [];
        foreach ($all_ports as $port) {
            $ports_by_inquiry[$port['inquiry_id']][] = $port;
        }
        
        // 批量统计询盘数量 - 优化：使用 GROUP BY 和条件聚合
        // 先获取所有有询盘的渠道和端口组合
        $leads_query = Db::table('crm_leads')
            ->where($l_where_sub)
            ->where('inquiry_id', 'in', $inquiry_ids)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->field('inquiry_id, port_id')
            ->select();
        
        // 统计每个渠道-端口组合的询盘数量
        $count_map = [];
        foreach ($leads_query as $lead) {
            $inquiry_id = $lead['inquiry_id'];
            $port_ids = !empty($lead['port_id']) ? explode(',', $lead['port_id']) : [];
            
            if (empty($port_ids)) {
                // 没有端口，统计整个渠道
                $key = "{$inquiry_id}_";
                if (!isset($count_map[$key])) {
                    $count_map[$key] = 0;
                }
                $count_map[$key]++;
            } else {
                // 有端口，统计每个端口
                foreach ($port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $key = "{$inquiry_id}_{$port_id}";
                        if (!isset($count_map[$key])) {
                            $count_map[$key] = 0;
                        }
                        $count_map[$key]++;
                    }
                }
            }
        }
        
        // 构建统计结果
        foreach ($inquiries as $inquiry) {
            $inquiry_id = $inquiry['id'];
            $inquiry_name = $inquiry['inquiry_name'];
            
            if (isset($ports_by_inquiry[$inquiry_id]) && !empty($ports_by_inquiry[$inquiry_id])) {
                // 如果有端口，按端口统计
                foreach ($ports_by_inquiry[$inquiry_id] as $port) {
                    $port_id = $port['id'];
                    $port_name = $port['port_name'];
                    $key = "{$inquiry_id}_{$port_id}";
                    
                    $count = isset($count_map[$key]) ? $count_map[$key] : 0;
                    
                    if ($count > 0) {
                        $stats[] = [
                            'channel_name' => $inquiry_name,
                            'port_name' => $port_name,
                            'display_name' => $inquiry_name . ' - ' . $port_name,
                            'count' => $count
                        ];
                    }
                }
            } else {
                // 如果没有端口，统计整个渠道的询盘数量
                $key = "{$inquiry_id}_";
                $count = isset($count_map[$key]) ? $count_map[$key] : 0;
                
                if ($count > 0) {
                    $stats[] = [
                        'channel_name' => $inquiry_name,
                        'port_name' => '',
                        'display_name' => $inquiry_name,
                        'count' => $count
                    ];
                }
            }
        }
        
        // 按数量降序排序
        usort($stats, function($a, $b) {
            return $b['count'] - $a['count'];
        });
        
        return $stats;
    }

    /**
     * 构建与 Order::clientSearch() 订单列表一致的 crm_client_order 条件（仅时间 + 权限口径，不含关键词）。
     * 说明：历史上业绩表先用「启用 + 业务组」admin 名单再 whereIn(pr_user)，会漏掉列表里仍统计到的订单；
     * 此处对齐列表：profile team_name、org→admin 拉 pr_user 范围、与列表相同的 check_status/order_time。
     */
    private function buildOrderListAlignedOrderWhere(string $timebucket, string $at_time, string $filterPrUser = ''): array
    {
        $where = [];
        $user = Admin::getMyInfo();
        $profileTeamName = trim((string)($user['team_name'] ?? ''));
        if ($profileTeamName !== '') {
            $where[] = ['team_name', '=', $profileTeamName];
        }
        $where[] = ['check_status', '=', 2];

        // 时间：与 buildPerformanceOrderWhere 一致（自定义逗号区间优先，否则 timebucket；弹窗默认本月）
        $effective_timebucket = $timebucket;
        if ($at_time === '' && $effective_timebucket === '') {
            $effective_timebucket = 'month';
        }
        if ($at_time !== '' && strpos($at_time, ',') !== false) {
            $date_parts = explode(',', $at_time);
            if (count($date_parts) === 2) {
                $start_date = trim($date_parts[0]);
                $end_date = trim($date_parts[1]);
                if ($start_date !== '' && $end_date !== '') {
                    $where[] = ['order_time', '>=', $start_date . ' 00:00:00'];
                    $where[] = ['order_time', '<=', $end_date . ' 23:59:59'];
                }
            }
        } elseif ($effective_timebucket !== '') {
            $where[] = $this->buildTimeWhere($effective_timebucket, 'order_time');
        } else {
            $where[] = $this->buildTimeWhere('month', 'order_time');
        }

        // pr_user 范围：复制 Order::clientSearch() 中 admin + org 逻辑（不限制 is_open / group_id）
        $org_where = [];
        if (!empty($user['org'])) {
            $org_where[] = $this->getOrgWhere($user['org']);
        }
        $team_name = $profileTeamName;
        if ($team_name) {
            $usernames = Db::table('admin')->where('team_name', $team_name)->where($org_where)->column('username');
        } else {
            if (!empty($org_where)) {
                $usernames = Db::table('admin')->where($org_where)->column('username');
            }
        }
        if (isset($usernames)) {
            if (!$usernames) {
                $where[] = ['pr_user', '=', time()];
            } else {
                $where[] = ['pr_user', 'in', $usernames];
            }
        }

        if ($filterPrUser !== '') {
            $where[] = ['pr_user', '=', $filterPrUser];
        }

        return $where;
    }

    /**
     * 获取业务人员业绩数据
     * 合计与分组均以「订单列表」同一套 where 为口径；补零成员仅增加展示行，不改变 summary。
     */
    public function getPerformanceData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $filter_username = Request::param('username', '');

        // 需要从业绩表中排除的业务员（按姓名匹配）
        // 后续只需要在这里增删名字即可控制显示范围
        $excludeUsernames = [
            // '张三',
            // '李四',
            '范文清',
            '郭志华',
            '郭志华2',
            '付淑雅'
        ];
        // 清洗排除名单：去空格、去空值、去重
        $excludeUsernames = array_values(array_unique(array_filter(array_map(function ($name) {
            return trim((string)$name);
        }, $excludeUsernames))));
        $excludeMap = array_fill_keys($excludeUsernames, true);

        $where = $this->buildOrderListAlignedOrderWhere($timebucket, $at_time, $filter_username);

        $empty_summary = [
            'total_profit' => number_format(0, 2),
            'total_money'  => number_format(0, 2),
            'profit_rate'  => number_format(0, 2),
        ];

        // 1) summary：与订单列表 totalProfit/totalMoney 同源（对整批 where 求和，不受补零行影响）
        $totals_row = Db::table('crm_client_order')->where($where)
            ->field('SUM(profit) AS total_profit, SUM(money) AS total_money')
            ->find();
        $sum_profit_all = round((float)($totals_row['total_profit'] ?? 0), 2);
        $sum_money_all = round((float)($totals_row['total_money'] ?? 0), 2);
        $sum_rate_all = $sum_money_all > 0 ? round(($sum_profit_all / $sum_money_all) * 100, 2) : 0;

        // 2) 按 pr_user 聚合（空 pr_user 单独成桶，避免丢单）
        // MySQL 5.7: GROUP_CONCAT 的 SEPARATOR 需使用字符串字面量，不能用 CHAR(30)
        $teamNameSeparator = '||#||';
        $order_stats = Db::table('crm_client_order')->where($where)
            ->fieldRaw(
                "IFNULL(NULLIF(TRIM(pr_user),''), '__PR_EMPTY__') AS pr_bucket, "
                . 'COUNT(id) AS order_count, SUM(profit) AS total_profit, SUM(money) AS total_money, '
                . "SUBSTRING_INDEX("
                . "GROUP_CONCAT(NULLIF(TRIM(team_name), '') ORDER BY order_time DESC, id DESC SEPARATOR '{$teamNameSeparator}')"
                . ", '{$teamNameSeparator}', 1"
                . ") AS snap_team_name"
            )
            ->group('pr_bucket')
            ->select();

        $current_admin = Admin::getMyInfo();
        $admin_map = [];
        $adminQuery = Db::table('admin')->field('username,team_name');
        if (!empty($current_admin['org'])) {
            $adminQuery->where($this->getOrgWhere($current_admin['org']));
        }
        foreach ($adminQuery->select() as $ar) {
            if (!empty($ar['username'])) {
                $admin_map[$ar['username']] = $ar;
            }
        }

        // 3) 补零：仅展示用（启用 + 业务组），不参与上面的 SUM
        $business_users_query = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14])
            ->where('is_open', '=', 1)
            ->where('username', '<>', '')
            ->whereNotNull('username');
        if ($filter_username !== '') {
            $business_users_query->where('username', '=', $filter_username);
        }
        $business_users = $business_users_query
            ->field('admin_id,username,team_name')
            ->order('team_name,username')
            ->limit(500)
            ->select();

        $agg_map = [];
        foreach ($order_stats as $stat) {
            $bucket = (string)$stat['pr_bucket'];
            $is_empty_pr = ($bucket === '__PR_EMPTY__');
            $display_username = $is_empty_pr ? '未知业务员' : $bucket;
            $agg_map[$bucket] = [
                'username' => $display_username,
                'pr_bucket' => $bucket,
                'order_count' => (int)($stat['order_count'] ?? 0),
                'total_profit' => round((float)($stat['total_profit'] ?? 0), 2),
                'total_money' => round((float)($stat['total_money'] ?? 0), 2),
                'snap_team_name' => trim((string)($stat['snap_team_name'] ?? '')),
            ];
        }

        foreach ($business_users as $u) {
            $un = trim((string)($u['username'] ?? ''));
            if ($un === '') {
                continue;
            }
            if (!isset($agg_map[$un])) {
                $agg_map[$un] = [
                    'username' => $un,
                    'pr_bucket' => $un,
                    'order_count' => 0,
                    'total_profit' => 0.0,
                    'total_money' => 0.0,
                    'snap_team_name' => '',
                ];
            }
        }

        $result = [];
        foreach ($agg_map as $row) {
            $username = trim((string)($row['username'] ?? ''));
            // 统一在输出前过滤，确保有订单/补零人员都能正确排除
            if ($username !== '' && isset($excludeMap[$username])) {
                continue;
            }

            $bucket = $row['pr_bucket'];
            $total_profit = (float)$row['total_profit'];
            $total_money = (float)$row['total_money'];
            $profit_rate = $total_money > 0 ? round(($total_profit / $total_money) * 100, 2) : 0;

            $snap = trim((string)$row['snap_team_name']);
            $admin_team = '';
            if ($bucket !== '__PR_EMPTY__' && isset($admin_map[$bucket])) {
                $admin_team = trim((string)($admin_map[$bucket]['team_name'] ?? ''));
            }
            if ($snap !== '') {
                $team_display = $snap;
            } elseif ($admin_team !== '') {
                $team_display = $admin_team;
            } else {
                $team_display = '未分组';
            }

            $result[] = [
                'username' => $row['username'],
                'team_name' => $team_display,
                'order_count' => (int)$row['order_count'],
                'profit_raw' => $total_profit,
                'profit' => number_format($total_profit, 2),
                'total_money_raw' => $total_money,
                'total_money' => number_format($total_money, 2),
                'profit_rate_raw' => $profit_rate,
                'profit_rate' => number_format($profit_rate, 2),
            ];
        }

        usort($result, function ($a, $b) {
            return ((float)$b['profit_raw']) <=> ((float)$a['profit_raw']);
        });

        $rank = 1;
        foreach ($result as &$item) {
            $item['rank'] = $rank++;
        }
        unset($item);

        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => $result,
            'summary' => [
                'total_profit' => number_format($sum_profit_all, 2),
                'total_money' => number_format($sum_money_all, 2),
                'profit_rate' => number_format($sum_rate_all, 2),
            ],
        ]);
    }

    /**
     * 临时调试：对比「订单列表口径」与「旧业绩表口径」命中订单差集，用于核对利润差额来源。
     * 调用示例：POST admin/operator/debugPerformanceDiff 参数 timebucket、at_time 与业绩表一致。
     */
    public function debugPerformanceDiff()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        $where_new = $this->buildOrderListAlignedOrderWhere($timebucket, $at_time, '');

        $effective_timebucket = $timebucket;
        if ($at_time === '' && $effective_timebucket === '') {
            $effective_timebucket = 'month';
        }

        $current_admin = Admin::getMyInfo();
        $old_usernames = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14])
            ->where('is_open', '=', 1)
            ->where('username', '<>', '')
            ->whereNotNull('username')
            ->limit(500)
            ->column('username');
        $old_usernames = $old_usernames ? array_values(array_filter($old_usernames)) : [];

        $cols = 'id,order_no,pr_user,team_name,order_time,money,profit,check_status';

        $orders_new = Db::table('crm_client_order')->where($where_new)->field($cols)->select();
        $ids_new = array_column($orders_new ?: [], 'id');

        if ($old_usernames === []) {
            $orders_old = [];
            $ids_old = [];
        } else {
            $orders_old = $this->buildPerformanceOrderQuery($effective_timebucket, $at_time, [], '')
                ->where('pr_user', 'in', $old_usernames)
                ->field($cols)
                ->select();
            $ids_old = array_column($orders_old ?: [], 'id');
        }

        $set_new = array_fill_keys($ids_new, true);
        $set_old = array_fill_keys($ids_old, true);

        $only_new = array_values(array_diff($ids_new, $ids_old));
        $only_old = array_values(array_diff($ids_old, $ids_new));

        $only_new_rows = [];
        $diff_profit = 0.0;
        foreach ($orders_new ?: [] as $o) {
            if (isset($set_old[$o['id']])) {
                continue;
            }
            $only_new_rows[] = $o;
            $diff_profit += (float)($o['profit'] ?? 0);
        }
        $diff_profit = round($diff_profit, 2);

        $sum_new = round((float)Db::table('crm_client_order')->where($where_new)->sum('profit'), 2);
        $sum_old = $old_usernames === []
            ? 0.0
            : round((float)$this->buildPerformanceOrderQuery($effective_timebucket, $at_time, [], '')
                ->where('pr_user', 'in', $old_usernames)
                ->sum('profit'), 2);

        return json([
            'code' => 0,
            'msg' => '调试数据：only_new 为旧业绩表漏掉的订单（其利润合计应接近列表与旧表差额）',
            'criteria' => [
                'new_where_note' => '对齐 Order::clientSearch 权限 + check_status=2 + order_time',
                'old_where_note' => '历史口径：同时间 + pr_user in (启用且业务组 admin)',
            ],
            'counts' => [
                'new_orders' => count($ids_new),
                'old_orders' => count($ids_old),
                'only_in_new' => count($only_new),
                'only_in_old' => count($only_old),
            ],
            'sums' => [
                'profit_new_aligned' => $sum_new,
                'profit_old_legacy' => $sum_old,
                'delta_legacy_minus_new' => round($sum_old - $sum_new, 2),
            ],
            'diff_only_in_new_profit_sum' => $diff_profit,
            'orders_in_new_criteria' => $orders_new,
            'orders_in_old_criteria' => $orders_old,
            'order_ids_only_in_new' => $only_new,
            'order_ids_only_in_old' => $only_old,
            'orders_only_in_new_detail' => $only_new_rows,
        ]);
    }
    
    /**
     * =========================
     * 团队业绩（统一订单口径）
     * =========================
     * 统一口径严格对齐「业绩订单 -> 订单列表」：
     * - 来源表：crm_client_order
     * - 审核状态：check_status = 2
     * - 时间字段：order_time（at_time 自定义范围优先，否则 timebucket buildTimeWhere，否则默认 month）
     * - 金额字段：money
     * - 利润字段：profit
     */
    private function buildPerformanceOrderWhere(string $timebucket = '', string $at_time = '', array $extra = [], string $fieldPrefix = ''): array
    {
        $prefix = $fieldPrefix ? rtrim($fieldPrefix, '.') . '.' : '';
        $where = [];
        $where[] = [$prefix . 'check_status', '=', 2];

        // 自定义时间范围（格式：start_date,end_date）优先
        if ($at_time !== '' && strpos($at_time, ',') !== false) {
            $date_parts = explode(',', $at_time);
            if (count($date_parts) === 2) {
                $start_date = trim($date_parts[0]);
                $end_date   = trim($date_parts[1]);
                if ($start_date !== '' && $end_date !== '') {
                    $where[] = [$prefix . 'order_time', '>=', $start_date . ' 00:00:00'];
                    $where[] = [$prefix . 'order_time', '<=', $end_date . ' 23:59:59'];
                    foreach ($extra as $k => $v) {
                        if (is_int($k) && is_array($v)) {
                            $where[] = $v;
                        } else {
                            $where[] = [$prefix . $k, '=', $v];
                        }
                    }
                    return $where;
                }
            }
        }

        // 否则按 timebucket / 默认本月
        if ($timebucket !== '') {
            $where[] = $this->buildTimeWhere($timebucket, $prefix . 'order_time');
        } else {
            $where[] = $this->buildTimeWhere('month', $prefix . 'order_time');
        }

        foreach ($extra as $k => $v) {
            if (is_int($k) && is_array($v)) {
                $where[] = $v;
            } else {
                $where[] = [$prefix . $k, '=', $v];
            }
        }

        return $where;
    }

    private function applyPerformanceWhereToQuery($query, array $where)
    {
        foreach ($where as $condition) {
            if (is_callable($condition)) {
                $query->where($condition);
                continue;
            }

            if (!is_array($condition)) {
                continue;
            }

            // 兼容 buildTimeWhere 可能返回的嵌套数组格式
            if (isset($condition[0]) && is_array($condition[0]) && isset($condition[0][0])) {
                foreach ($condition as $sub) {
                    if (is_array($sub) && isset($sub[0])) {
                        $query->where($sub[0], $sub[1] ?? '=', $sub[2] ?? null);
                    }
                }
                continue;
            }

            if (isset($condition[0])) {
                $query->where($condition[0], $condition[1] ?? '=', $condition[2] ?? null);
            }
        }

        return $query;
    }

    private function buildPerformanceOrderQuery(string $timebucket = '', string $at_time = '', array $extra = [], string $alias = '')
    {
        $query = $alias === '' ? Db::table('crm_client_order') : Db::table('crm_client_order')->alias($alias);
        $where = $this->buildPerformanceOrderWhere($timebucket, $at_time, $extra, $alias);
        return $this->applyPerformanceWhereToQuery($query, $where);
    }

    /**
     * 获取当前组织内所有业务人员 username 列表（不含 group_id 白名单限制）
     * 与订单列表口径一致：只按 org 过滤，不排除 group_id=17/18 等真实团队成员
     */
    private function getOrgUsernames($org)
    {
        $query = Db::table('admin')
            ->where('username', '<>', '')
            ->whereNotNull('username')
            ->field('username')
            ->limit(2000);
        if (!empty($org)) {
            $query->where($this->getOrgWhere($org));
        }
        $rows = $query->select();
        return $rows ? array_filter(array_column($rows, 'username')) : [];
    }

    /**
     * 获取【团队】业绩数据
     * 统计口径：统一订单口径（crm_client_order，check_status=2，order_time）
     * 团队名称：只使用订单表 crm_client_order.team_name（订单快照团队）
     * 人员范围：组织内全部 username（与订单列表一致，不限制 group_id）
     */
    public function getTeamPerformanceData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');

        $current_admin = Admin::getMyInfo();

        // 组织内全部 username，与订单列表口径一致（不限制 group_id，避免漏掉 17/18 等真实成员）
        $usernames = $this->getOrgUsernames($current_admin['org'] ?? '');
        if (empty($usernames)) {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [],
                'summary' => ['total_profit' => '0.00', 'total_money' => '0.00', 'profit_rate' => '0.00']
            ]);
        }

        $rows = $this->buildPerformanceOrderQuery($timebucket, $at_time, [], '')
            ->where('pr_user', 'in', $usernames)
            ->field('team_name, SUM(profit) as total_profit, SUM(money) as total_money')
            ->group('team_name')
            ->order('total_profit desc')
            ->select();

        $result = [];
        $sum_profit = 0;
        $sum_money  = 0;
        foreach ($rows as $r) {
            $profit = (float)($r['total_profit'] ?: 0);
            $money  = (float)($r['total_money'] ?: 0);
            $sum_profit += $profit;
            $sum_money  += $money;

            $rate = $money > 0 ? round($profit / $money * 100, 2) : 0;
            $result[] = [
                'team_name'   => ($r['team_name'] ?? '') !== '' ? $r['team_name'] : '未分组',
                'profit'      => number_format($profit, 2),
                'total_money' => number_format($money, 2),
                'profit_rate' => number_format($rate, 2),
            ];
        }

        // 已按 total_profit desc 排序，这里只补 rank
        $rank = 1;
        foreach ($result as &$item) {
            $item['rank'] = $rank++;
        }
        unset($item);

        $sum_rate = $sum_money > 0 ? round($sum_profit / $sum_money * 100, 2) : 0;
        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => $result,
            'summary' => [
                'total_profit' => number_format($sum_profit, 2),
                'total_money'  => number_format($sum_money, 2),
                'profit_rate'  => number_format($sum_rate, 2),
            ]
        ]);
    }

    /**
     * 团队成员业绩排行页面
     * 用于渲染团队成员业绩排行的页面模板
     */
    public function teamMemberPerformancePage()
    {
        $team_name = Request::param('team_name', '');
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        $this->assign('team_name', $team_name);
        $this->assign('timebucket', $timebucket);
        $this->assign('at_time', $at_time);

        return $this->fetch('operator/team_member_performance_table');
    }

    /**
     * 获取团队成员业绩数据
     * 返回指定团队下业务员的业绩排行数据
     */
    public function getTeamMemberPerformanceData()
    {
        $team_name_raw = Request::param('team_name', '');
        $team_name = trim((string)$team_name_raw);
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        $current_admin = Admin::getMyInfo();

        // 1) 组织权限内的全部 username（与订单列表一致，不限制 group_id）
        $org_usernames = $this->getOrgUsernames($current_admin['org'] ?? '');

        // 标准化：空/null/空格/"未分组" 都视为未分组
        $is_ungrouped = ($team_name === '' || $team_name === '未分组');

        if (empty($org_usernames)) {
            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => [],
                'summary' => ['total_profit' => '0.00', 'total_money' => '0.00', 'profit_rate' => '0.00']
            ]);
        }

        // 2) 先从 admin 表取该团队的业务员用户名集合（组织权限内）
        $team_usernames_query = Db::name('admin')
            ->whereIn('username', $org_usernames);
        if ($is_ungrouped) {
            $team_usernames_query->where(function ($q) {
                $q->whereNull('team_name')->whereOr('team_name', '=', '');
            });
        } else {
            $team_usernames_query->where('team_name', '=', $team_name);
        }
        $team_usernames = $team_usernames_query->column('username');
        $team_usernames = $team_usernames ? array_values(array_filter($team_usernames)) : [];

        // 该团队没有成员：优雅返回空数据
        if (empty($team_usernames)) {
            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => [],
                'summary' => ['total_profit' => '0.00', 'total_money' => '0.00', 'profit_rate' => '0.00']
            ]);
        }

        // 3) 按成员用户名集合统计订单（不要依赖订单表 team_name，避免历史脏数据/不同步）
        $order_stats = $this->buildPerformanceOrderQuery($timebucket, $at_time, [], '')
            ->whereIn('pr_user', $team_usernames)
            ->field('pr_user, SUM(profit) as total_profit, SUM(money) as total_money')
            ->group('pr_user')
            ->order('total_profit desc')
            ->select();

        $result = [];
        $sum_profit = 0.0;
        $sum_money  = 0.0;
        $hasUser = [];
        foreach ($order_stats as $stat) {
            $u = (string)($stat['pr_user'] ?? '');
            if ($u === '') {
                continue;
            }
            $profit = (float)($stat['total_profit'] ?: 0);
            $money  = (float)($stat['total_money'] ?: 0);
            $sum_profit += $profit;
            $sum_money  += $money;
            $hasUser[$u] = true;
            $rate = $money > 0 ? round($profit / $money * 100, 2) : 0;
            $result[] = [
                'username' => $u,
                'profit' => number_format($profit, 2),
                'total_money' => number_format($money, 2),
                'profit_rate' => number_format($rate, 2),
            ];
        }

        // 4) 补零：团队成员本期无订单也要显示（保证成员列表完整且可点右侧）
        foreach ($team_usernames as $u) {
            $u = (string)$u;
            if ($u === '' || isset($hasUser[$u])) {
                continue;
            }
            $result[] = [
                'username' => $u,
                'profit' => number_format(0, 2),
                'total_money' => number_format(0, 2),
                'profit_rate' => number_format(0, 2),
            ];
        }

        // 按利润降序排序
        usort($result, function($a, $b) {
            $profit_a = floatval(str_replace(',', '', $a['profit']));
            $profit_b = floatval(str_replace(',', '', $b['profit']));
            return $profit_b <=> $profit_a;
        });

        // 添加排名（从1开始）
        $rank = 1;
        foreach ($result as &$item) {
            $item['rank'] = $rank++;
        }
        unset($item);

        $sum_rate = $sum_money > 0 ? round($sum_profit / $sum_money * 100, 2) : 0;
        return json([
            'code' => 200,
            'msg' => 'ok',
            'data' => $result,
            'summary' => [
                'total_profit' => number_format($sum_profit, 2),
                'total_money'  => number_format($sum_money, 2),
                'profit_rate'  => number_format($sum_rate, 2),
            ]
        ]);
    }

    /**
     * 获取个人业绩清单
     * 返回指定业务员的订单明细数据，按成交日期倒序排列
     * 口径与订单列表一致：check_status=2，order_time，pr_user，team_name（订单快照）
     */
    public function getMemberPerformanceDetail()
    {
        $username = Request::param('username', '');
        $team_name = Request::param('team_name', '');
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        if (empty($username)) {
            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => [],
                'summary' => ['total_money' => '0.00', 'total_profit' => '0.00']
            ]);
        }

        $current_admin = Admin::getMyInfo();

        // 组织权限校验：仅允许查询组织内成员
        $org_usernames = $this->getOrgUsernames($current_admin['org'] ?? '');
        if (!empty($org_usernames) && !in_array($username, $org_usernames)) {
            return json([
                'code' => 403,
                'msg' => '无权限查看该成员数据',
                'data' => []
            ]);
        }

        // 1) 查询该业务员订单明细（统一订单口径：crm_client_order，check_status=2，order_time）
        $extra = ['pr_user' => $username];
        $orders_query = $this->buildPerformanceOrderQuery($timebucket, $at_time, $extra, '');
        $orders = $orders_query
            ->field('id,order_time,cname,money,profit')
            ->order('order_time desc,id desc')
            ->limit(1000)
            ->select();

        // 4) 格式化数据并累计合计（使用原始数值）
        $result = [];
        $sum_money  = 0;
        $sum_profit = 0;
        foreach ($orders as $order) {
            $money_val  = (float)($order['money'] ?: 0);
            $profit_val = (float)($order['profit'] ?: 0);
            $sum_money  += $money_val;
            $sum_profit += $profit_val;

            $result[] = [
                'order_time' => $order['order_time'] ?: '',
                'cname'      => $order['cname'] ?: '',
                'money'      => number_format($money_val, 2),
                'profit'     => number_format($profit_val, 2),
            ];
        }

        return json([
            'code' => 200,
            'msg' => 'ok',
            'data' => $result,
            'summary' => [
                'total_money'  => number_format($sum_money, 2),
                'total_profit' => number_format($sum_profit, 2),
            ]
        ]);
    }

    /**
     * 统一解析时间参数：与 buildPerformanceOrderWhere 保持同一规则。
     * - 自定义格式：at_time = "YYYY-MM-DD,YYYY-MM-DD"（优先）
     * - 否则使用 timebucket
     * - 若二者都为空，默认 month
     */
    private function resolveInquirySummaryTimeParams(string $timebucket = '', string $at_time = ''): array
    {
        if ($at_time !== '' && strpos($at_time, ',') !== false) {
            $date_parts = explode(',', $at_time);
            if (count($date_parts) === 2) {
                $start_date = trim($date_parts[0]);
                $end_date   = trim($date_parts[1]);
                if ($start_date !== '' && $end_date !== '') {
                    return [
                        'is_custom' => true,
                        'start_date' => $start_date,
                        'end_date' => $end_date,
                        'timebucket' => '',
                    ];
                }
            }
        }

        return [
            'is_custom' => false,
            'start_date' => '',
            'end_date' => '',
            'timebucket' => $timebucket !== '' ? $timebucket : 'month',
        ];
    }

    /**
     * 业务询盘汇总三连屏：构建与团队业绩表同时间解析规则的 leads 时间条件。
     */
    private function buildInquirySummaryLeadsWhere(string $timebucket = '', string $at_time = ''): array
    {
        $l_where_sub = [['status', '=', 1]];
        $time_params = $this->resolveInquirySummaryTimeParams($timebucket, $at_time);

        if ($time_params['is_custom']) {
            $start_time = $time_params['start_date'] . ' 00:00:00';
            $end_time = $time_params['end_date'] . ' 23:59:59';
            $l_where_sub[] = function ($query) use ($start_time, $end_time) {
                $query->where(function ($q) use ($start_time, $end_time) {
                    $q->where('at_time', '>=', $start_time)
                        ->where('at_time', '<=', $end_time);
                })->whereOr(function ($q) use ($start_time, $end_time) {
                    $q->where('to_kh_time', '>=', $start_time)
                        ->where('to_kh_time', '<=', $end_time);
                });
            };
            return $l_where_sub;
        }

        $effective_timebucket = $time_params['timebucket'];
        $time_where_at = $this->buildTimeWhere($effective_timebucket, 'at_time');
        $time_where_kh = $this->buildTimeWhere($effective_timebucket, 'to_kh_time');
        $l_where_sub[] = function ($query) use ($time_where_at, $time_where_kh) {
            $query->where([$time_where_at])->whereOr([$time_where_kh]);
        };
        return $l_where_sub;
    }

    /**
     * 业务询盘汇总三连屏：复用客户列表真实口径基础查询。
     * 口径来源：application/admin/model/Client.php::buildClientSearchAllBaseQuery()
     */
    private function buildInquirySummaryClientBaseQuery(string $timebucket = '', string $at_time = '')
    {
        $keyword = [];
        $time_params = $this->resolveInquirySummaryTimeParams($timebucket, $at_time);
        if ($time_params['is_custom']) {
            $keyword['timebucket'] = [
                ['at_time', '>=', $time_params['start_date'] . ' 00:00:00'],
                ['at_time', '<=', $time_params['end_date'] . ' 23:59:59'],
            ];
        } else {
            $keyword['timebucket'] = $this->buildTimeWhere($time_params['timebucket'], 'at_time');
        }
        // 关键：必须以“客户列表最终结果集（join+group 去重后）”作为基础集，否则会出现与列表 total 不一致
        $finalIdQuerySql = model('Client')->buildClientSearchListAllFinalIdQuery($keyword)->buildSql();
        return Db::table([$finalIdQuerySql => 'l']);
    }

    // ===========================
    // 新增：团队询盘汇总排除配置（仅影响三连屏）
    // ===========================

    /**
     * 需要从「团队询盘汇总」排除的团队名称（第一屏/联动查询均生效）。
     * 说明：这里只做数组配置，不读库、不改表结构。
     */
    private function getExcludedInquiryTeamNames(): array
    {
        $items = [
            // '测试团队',
            // '自己的团队11',
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
     * 需要从「团队询盘汇总」排除的业务员名称（第二屏/第三屏均生效，并且从第一屏统计口径扣除）。
     */
    private function getExcludedInquiryUsernames(): array
    {
        $items = [
            // '测试账号',
            // '郭志华',
            // '郭志华2',
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
     * 新增：统一应用团队/业务员排除条件（优先 SQL 层过滤）。
     * 注意：这里默认按 a.team_name / a.username 过滤，保持现有 getLeadsSubQuery() 链式别名写法不变。
     */
    private function applyInquirySummaryExcludes($query, array $excludedTeams = [], array $excludedUsers = [], string $normalizedTeamExpr = '')
    {
        if (!empty($excludedTeams)) {
            // 团队排除必须基于“归一化后的团队名”，否则 NULL/空白会漏排或误排
            if ($normalizedTeamExpr !== '') {
                $escaped = array_map(function ($v) {
                    return "'" . addslashes((string)$v) . "'";
                }, $excludedTeams);
                $inSql = implode(',', $escaped);
                $query->whereRaw("({$normalizedTeamExpr}) NOT IN ({$inSql})");
            } else {
                // 兼容旧调用：没有传归一化表达式时，退回按 a.team_name 排除
                $query->where('a.team_name', 'not in', $excludedTeams);
            }
        }
        if (!empty($excludedUsers)) {
            // 业务员排除以 leads.pr_user 为准，避免因 admin 不存在/未启用导致排除失效
            $query->where('l.pr_user', 'not in', $excludedUsers);
        }
        return $query;
    }

    /**
     * 第一屏：团队询盘汇总（复用业务询盘口径）。
     */
    public function getInquiryTeamSummaryData()
    {
        try {
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            // 新增：排除团队/排除业务员（影响统计口径，不只是展示隐藏）
            $excludedTeams = $this->getExcludedInquiryTeamNames();
            $excludedUsers = $this->getExcludedInquiryUsernames();

            // 团队名称归一化：NULL/空字符串/纯空白 => "未分组"，其余 TRIM 后作为团队名
            $normalizedTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";

            // 基础数据集：严格复用客户列表口径（crm_leads + at_time + pr_user 可见范围）
            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
            $baseTotal = (int)(clone $baseQuery)->count();

            \think\facade\Log::info('[InquirySummaryDebug] params=' . json_encode([
                'timebucket' => $timebucket,
                'at_time' => $at_time,
                'excludedTeams' => $excludedTeams,
                'excludedUsers' => $excludedUsers,
            ], JSON_UNESCAPED_UNICODE));
            \think\facade\Log::info('[InquirySummaryDebug] base_total=' . $baseTotal);

            $query = (clone $baseQuery)->leftJoin('admin a', 'l.pr_user = a.username');
            $rawTotal = (int)(clone $query)->count();
            \think\facade\Log::info('[InquirySummaryDebug] raw_total_before_excludes=' . $rawTotal);

            // 分步应用排除，便于定位差异
            $afterExcludeTeamTotal = $rawTotal;
            if (!empty($excludedTeams)) {
                $qTeam = (clone $query);
                $this->applyInquirySummaryExcludes($qTeam, $excludedTeams, [], $normalizedTeamExpr);
                $afterExcludeTeamTotal = (int)$qTeam->count();
            }
            \think\facade\Log::info('[InquirySummaryDebug] after_exclude_team_total=' . $afterExcludeTeamTotal);

            $afterExcludeUserTotal = $afterExcludeTeamTotal;
            if (!empty($excludedUsers)) {
                $qUser = (clone $query);
                $this->applyInquirySummaryExcludes($qUser, $excludedTeams, $excludedUsers, $normalizedTeamExpr);
                $afterExcludeUserTotal = (int)$qUser->count();
            }
            \think\facade\Log::info('[InquirySummaryDebug] after_excludes_total=' . $afterExcludeUserTotal);

            // 先保证基础集一致，再做人为排除
            $this->applyInquirySummaryExcludes($query, $excludedTeams, $excludedUsers, $normalizedTeamExpr);

            $rows = $query
                ->group($normalizedTeamExpr)
                ->field($normalizedTeamExpr . ' as team_name,count(*) as yw_num')
                ->order('yw_num desc')
                ->order('team_name')
                ->select();

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $result[] = [
                    'rank' => $idx + 1,
                    'team_name' => trim((string)($row['team_name'] ?? '')) !== '' ? $row['team_name'] : '未分组',
                    'yw_num' => $count,
                ];
            }
            \think\facade\Log::info('[InquirySummaryDebug] grouped_total=' . $total);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryTeamSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 临时调试接口：对比“客户列表真实总数”与“询盘汇总基础集总数”，并给出 id 差异样本（最多20条）
     * 仅管理员可访问（aid=1 / group_id=1 / username=admin）
     */
    public function debugInquirySummaryCompare()
    {
        $user = Admin::getMyInfo();
        $isAdmin = (int)session('aid') === 1
            || (int)($user['group_id'] ?? 0) === 1
            || (string)($user['username'] ?? '') === 'admin';
        if (!$isAdmin) {
            return json(['code' => 403, 'msg' => 'forbidden', 'data' => []]);
        }

        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        // 与 Client::clientSearchAll() 一致的关键字结构：最终只用 keyword['timebucket'] 承载 at_time 条件
        $keyword = [];
        if ($timebucket !== '') {
            $keyword['timebucket'] = $this->buildTimeWhere($timebucket, 'at_time');
        }
        if ($at_time !== '') {
            $keyword['timebucket'] = $this->buildTimeWhere($at_time, 'at_time');
        }

        $clientList = model('Client')->getClientSearchListAll(1, 1, $keyword);
        $clientTotal = (int)($clientList['total'] ?? 0);

        $inquiryBaseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
        $inquiryBaseTotal = (int)(clone $inquiryBaseQuery)->count();

        // 用“客户列表最终结果集”做基准差异对比（id层面）
        $clientFinalIdSql = model('Client')->buildClientSearchListAllFinalIdQuery($keyword)->field('l.id')->buildSql();
        $inquiryFinalIdSql = (clone $inquiryBaseQuery)->field('l.id')->buildSql();

        $onlyInClient = Db::table([$clientFinalIdSql => 'c'])
            ->leftJoin([$inquiryFinalIdSql => 'i'], 'c.id = i.id')
            ->whereNull('i.id')
            ->limit(20)
            ->column('c.id');

        $onlyInInquiry = Db::table([$inquiryFinalIdSql => 'i'])
            ->leftJoin([$clientFinalIdSql => 'c'], 'i.id = c.id')
            ->whereNull('c.id')
            ->limit(20)
            ->column('i.id');

        return json([
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'params' => ['timebucket' => $timebucket, 'at_time' => $at_time],
                'client_total' => $clientTotal,
                'inquiry_base_total' => $inquiryBaseTotal,
                'only_in_client_ids' => array_values(array_map('intval', (array)$onlyInClient)),
                'only_in_inquiry_ids' => array_values(array_map('intval', (array)$onlyInInquiry)),
            ],
        ]);
    }

    /**
     * 第二屏：团队下个人询盘汇总（复用业务询盘口径）。
     */
    public function getInquiryMemberSummaryData()
    {
        try {
            $team_name = trim((string)Request::param('team_name', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            if ($team_name === '') {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            // 新增：如果团队在排除名单里，禁止联动查出
            $excludedTeams = $this->getExcludedInquiryTeamNames();
            if (!empty($excludedTeams) && in_array($team_name, $excludedTeams, true)) {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            // 新增：排除业务员（第二屏不显示，且不计入统计）
            $excludedUsers = $this->getExcludedInquiryUsernames();

            // 同一套基础数据集（与客户列表一致）
            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
            $query = (clone $baseQuery)
                ->leftJoin('admin a', 'l.pr_user = a.username');

            if ($team_name === '未分组') {
                // 点击“未分组”时：admin 不存在、team_name 空/null/纯空白都纳入
                $query->whereRaw("(a.username IS NULL OR a.team_name IS NULL OR TRIM(a.team_name) = '')");
            } else {
                // 正常团队：TRIM 后匹配，避免前后空格导致查不到
                $query->whereRaw("TRIM(a.team_name) = :team_name", ['team_name' => $team_name]);
            }

            $this->applyInquirySummaryExcludes($query, [], $excludedUsers);

            $rows = $query
                ->group('l.pr_user')
                ->field('l.pr_user as username,count(distinct l.id) as yw_num')
                ->order('yw_num desc')
                ->order('username')
                ->select();

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $result[] = [
                    'rank' => $idx + 1,
                    'username' => (string)($row['username'] ?? ''),
                    'team_name' => $team_name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryMemberSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '成员汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：个人询盘渠道分类汇总（优先 inquiry_id 关联渠道名）。
     */
    public function getInquiryChannelSummaryData()
    {
        try {
            $username = trim((string)Request::param('username', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            if ($username === '') {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            // 新增：如果业务员在排除名单里，禁止通过接口参数绕过前端隐藏继续查看
            $excludedUsers = $this->getExcludedInquiryUsernames();
            if (!empty($excludedUsers) && in_array($username, $excludedUsers, true)) {
                return json([
                    'code' => 403,
                    'msg' => '无权限查看该成员数据',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            // 权限口径：以客户列表可见负责人范围为准
            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
            $rows = (clone $baseQuery)
                ->where('l.pr_user', '=', $username)
                ->group('l.inquiry_id')
                ->field('l.inquiry_id,count(distinct l.id) as yw_num')
                ->order('yw_num desc')
                ->select();

            $inquiry_ids = [];
            foreach ($rows as $row) {
                $iid = (int)($row['inquiry_id'] ?? 0);
                if ($iid > 0) {
                    $inquiry_ids[] = $iid;
                }
            }
            $inquiry_ids = array_values(array_unique($inquiry_ids));
            $inquiry_map = [];
            if (!empty($inquiry_ids)) {
                $inquiry_map = Db::table('crm_inquiry')->where('id', 'in', $inquiry_ids)->column('inquiry_name', 'id');
            }

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $iid = (int)($row['inquiry_id'] ?? 0);
                $channel_name = '未分类';
                if ($iid > 0 && !empty($inquiry_map[$iid])) {
                    $channel_name = $inquiry_map[$iid];
                } elseif ($iid <= 0) {
                    $channel_name = '未分类';
                } else {
                    $channel_name = '其他';
                }
                $result[] = [
                    'rank' => $idx + 1,
                    'channel_name' => $channel_name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryChannelSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '渠道汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    // ===========================
    // 运营询盘汇总表三连屏（operationInquirySummary）
    // ===========================

    /**
     * 解析逗号分隔端口 ID：trim、去重，仅保留正整数（与现有 FIND_IN_SET 用法兼容）。
     */
    private function parseCsvPortIdsForOperatorInquiry($raw): array
    {
        $raw = trim((string)$raw);
        if ($raw === '') {
            return [];
        }
        $parts = preg_split('/\s*,\s*/', $raw);
        $out = [];
        foreach ($parts as $p) {
            $p = trim((string)$p);
            if ($p === '' || !ctype_digit($p)) {
                continue;
            }
            $v = (int)$p;
            if ($v > 0) {
                $out[$v] = $v;
            }
        }
        return array_values($out);
    }

    /**
     * 运营人员列表口径：与 getYyLeadsSubQuery 一致（运营组、启用、组织），可按询盘来源筛选；
     * 未配置 port_id 的账号也返回，便于第二屏展示 0、第三屏空态。
     */
    private function fetchOperationStaffRowsForInquirySummary(int $inquiryIdToken): array
    {
        $current_admin = Admin::getMyInfo();
        $q = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', '=', $this->yygid)
            ->where('is_open', '=', 1);

        if ($inquiryIdToken <= 0) {
            $q->where(function ($sub) {
                $sub->whereNull('inquiry_id')
                    ->whereOr('inquiry_id', '=', '')
                    ->whereOr('inquiry_id', '=', 0);
            });
        } else {
            $q->whereRaw('CAST(inquiry_id AS UNSIGNED) = ?', [$inquiryIdToken]);
        }

        return $q->field('username,port_id,inquiry_id')->order('username')->select();
    }

    /**
     * 按询盘来源 bucket 过滤基础子查询 l（0 表示未知/空来源）。
     */
    private function applyOperatorInquiryLeadsSourceBucket($query, int $inquiryIdToken)
    {
        if ($inquiryIdToken <= 0) {
            $query->whereRaw("(l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), '')) = '')");
        } else {
            $query->where('l.inquiry_id', '=', $inquiryIdToken);
        }
        return $query;
    }

    /**
     * l.port_id 与运营人员端口集合存在交集时的 SQL 片段（无端口时返回永假，计数为 0）。
     */
    private function buildLeadPortIntersectAdminPortsWhere(array $adminPortIds): string
    {
        if (empty($adminPortIds)) {
            return '1=0';
        }
        $parts = [];
        foreach ($adminPortIds as $pid) {
            $pid = (int)$pid;
            if ($pid <= 0) {
                continue;
            }
            $parts[] = "FIND_IN_SET('{$pid}', l.port_id) > 0";
        }
        return empty($parts) ? '1=0' : '(' . implode(' OR ', $parts) . ')';
    }

    /**
     * 校验并返回当前组织下、运营组、负责指定来源的运营账号（第三屏用）。
     */
    private function findOperationStaffForPortSummary(string $username, int $inquiryIdToken): ?array
    {
        $username = trim($username);
        if ($username === '') {
            return null;
        }
        $current_admin = Admin::getMyInfo();
        $row = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('username', '=', $username)
            ->where('group_id', '=', $this->yygid)
            ->where('is_open', '=', 1)
            ->field('username,port_id,inquiry_id')
            ->find();
        if (empty($row)) {
            return null;
        }
        if ($inquiryIdToken <= 0) {
            $ok = ($row['inquiry_id'] === null || $row['inquiry_id'] === '' || (int)$row['inquiry_id'] === 0);
        } else {
            $ok = ((int)$row['inquiry_id'] === $inquiryIdToken) || ((string)(int)$row['inquiry_id'] === (string)$inquiryIdToken);
        }
        return $ok ? $row : null;
    }

    /**
     * 第一屏：按询盘来源汇总（基础集与「团队询盘汇总」相同：客户列表最终 id 集）。
     */
    public function getOperationInquirySummarySourceData()
    {
        try {
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);

            $bucketExpr = 'CASE WHEN l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), \'\')) = \'\' THEN 0 ELSE CAST(l.inquiry_id AS UNSIGNED) END';

            $rows = (clone $baseQuery)
                ->field($bucketExpr . ' AS inquiry_bucket, COUNT(*) AS yw_num')
                ->group([$bucketExpr])
                ->order('yw_num', 'desc')
                ->order('inquiry_bucket', 'asc')
                ->select();

            $buckets = [];
            foreach ($rows as $row) {
                $buckets[] = (int)($row['inquiry_bucket'] ?? 0);
            }
            $buckets = array_values(array_unique(array_filter($buckets, function ($v) {
                return $v > 0;
            })));
            $inquiry_map = [];
            if (!empty($buckets)) {
                $inquiry_map = Db::table('crm_inquiry')->where('id', 'in', $buckets)->column('inquiry_name', 'id');
            }

            $result = [];
            $total = 0;
            $idx = 0;
            foreach ($rows as $row) {
                $bucket = (int)($row['inquiry_bucket'] ?? 0);
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                if ($bucket <= 0) {
                    $name = '未知来源';
                    $iid = 0;
                } else {
                    $name = $inquiry_map[$bucket] ?? ('ID:' . $bucket);
                    $iid = $bucket;
                }
                $idx++;
                $result[] = [
                    'rank' => $idx,
                    'inquiry_id' => $iid,
                    'inquiry_name' => $name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummarySourceData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '询盘来源汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第二屏：指定来源下各运营人员询盘数（线索与本人 port_id 配置有交集才计入；每人 count 为 distinct l.id，不重复膨胀）。
     */
    public function getOperationInquirySummaryStaffData()
    {
        try {
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);

            $staffRows = $this->fetchOperationStaffRowsForInquirySummary($inquiry_id);
            if (empty($staffRows)) {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $result = [];
            $total = 0;
            foreach ($staffRows as $op) {
                $uname = trim((string)($op['username'] ?? ''));
                if ($uname === '') {
                    continue;
                }
                $ports = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
                $portWhere = $this->buildLeadPortIntersectAdminPortsWhere($ports);
                $cnt = (int)(clone $baseQuery)->whereRaw($portWhere)->count('l.id');
                $total += $cnt;
                $result[] = [
                    'username' => $uname,
                    'yw_num' => $cnt,
                ];
            }

            usort($result, function ($a, $b) {
                if ($a['yw_num'] !== $b['yw_num']) {
                    return $b['yw_num'] <=> $a['yw_num'];
                }
                return strcmp($a['username'], $b['username']);
            });
            foreach ($result as $i => &$r) {
                $r['rank'] = $i + 1;
            }
            unset($r);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummaryStaffData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '运营人员汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：指定运营人员名下各端口询盘数（每端口 count distinct l.id；与第二屏同一来源与时间口径）。
     */
    public function getOperationInquirySummaryPortData()
    {
        try {
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $username = trim((string)Request::param('username', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');

            $op = $this->findOperationStaffForPortSummary($username, $inquiry_id);
            if ($op === null) {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $portIds = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
            if (empty($portIds)) {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);

            $port_map = Db::table('crm_inquiry_port')->where('id', 'in', $portIds)->column('port_name', 'id');

            $result = [];
            $total = 0;
            foreach ($portIds as $pid) {
                $w = "FIND_IN_SET('{$pid}', l.port_id) > 0";
                $cnt = (int)(clone $baseQuery)->whereRaw($w)->count('l.id');
                $total += $cnt;
                $result[] = [
                    'port_id' => $pid,
                    'port_name' => $port_map[$pid] ?? ('ID:' . $pid),
                    'yw_num' => $cnt,
                ];
            }

            usort($result, function ($a, $b) {
                if ($a['yw_num'] !== $b['yw_num']) {
                    return $b['yw_num'] <=> $a['yw_num'];
                }
                return strcmp($a['port_name'], $b['port_name']);
            });
            foreach ($result as $i => &$r) {
                $r['rank'] = $i + 1;
            }
            unset($r);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummaryPortData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '运营端口汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

}

