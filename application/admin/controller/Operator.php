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
            $params['keyword']['timebucket'] = 'month';
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
        
        $this->assign('customer_type', Order::CUSTOMER_TYPE);
        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);
        return $this->fetch();
    }

    public function personOrderSearch()
    {
        $where = [];
        $client_where = [];
        
        //判断权限
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
                $query->where([$timeWhere['at_time']])->whereOr([$timeWhere['to_kh_time']]);
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
        $ywData_total = $this->getLeadsSubQuery($l_where_sub)->where('a.team_name', '<>', '')->where($yw_where)->group('a.team_name')->field('a.team_name,count(l.id) as yw_num')->order('yw_num desc')->order('a.team_name')->select();

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
            
            // 超级管理员：统计全部业绩
            $timeWhere = $this->buildTimeWhere('month', 'o.order_time');
            $profit = Db::table('crm_client_order')
                ->alias('o')
                ->where([$timeWhere])
                ->sum('o.profit');
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
            
            // 业绩统计 - 根据订单中的 source 和 source_port 匹配运营人员
            // 订单表的 source 是渠道名称，source_port 是端口名称（文字）
            $timeWhere = $this->buildTimeWhere('month', 'o.order_time');
            
            // 获取该渠道下所有端口的名称列表 - 批量查询优化
            $port_names = [];
            $inquiry_name = '';
            $inquiry_info = Db::table('crm_inquiry')
                ->where('id', $current_admin_info['inquiry_id'])
                ->field('inquiry_name')
                ->find();
            if ($inquiry_info) {
                $inquiry_name = $inquiry_info['inquiry_name'];
            }
            
            $admin_port_ids = !empty($current_admin_info['port_id']) ? array_filter(array_map('trim', explode(',', $current_admin_info['port_id']))) : [];
            if (!empty($admin_port_ids)) {
                $port_list = Db::table('crm_inquiry_port')
                    ->where('id', 'in', $admin_port_ids)
                    ->where('inquiry_id', $current_admin_info['inquiry_id'])
                    ->field('port_name')
                    ->select();
                foreach ($port_list as $port) {
                    if (!empty($port['port_name'])) {
                        $port_names[] = addslashes($port['port_name']);
                    }
                }
            }
            
            $profit = 0;
            if (!empty($inquiry_name) && !empty($port_names)) {
                $port_conditions = [];
                foreach ($port_names as $port_name) {
                    $port_conditions[] = "o.source_port = '{$port_name}'";
                }
                $port_where = '(' . implode(' OR ', $port_conditions) . ')';
                $profit = Db::table('crm_client_order')
                    ->alias('o')
                    ->where([$timeWhere])
                    ->where('o.source', $inquiry_name)
                    ->whereRaw($port_where)
                    ->sum('o.profit');
            } elseif (!empty($inquiry_name)) {
                // 如果没有端口，只按渠道匹配
                $profit = Db::table('crm_client_order')
                    ->alias('o')
                    ->where([$timeWhere])
                    ->where('o.source', $inquiry_name)
                    ->sum('o.profit');
            }
        }
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
        $ywData_total = $this->getLeadsSubQuery($l_where_sub, 'pr_user')->where('a.team_name', '<>', '')->where($yw_where)->group('a.team_name')->field('a.team_name,count(l.id) as yw_num')->order('yw_num desc')->order('a.team_name')->select();

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
     * 获取业务人员业绩数据
     * 返回各业务员的询盘量、成单率、利润、利润率
     * 优化：使用批量查询替代循环查询，大幅提升性能
     */
    public function getPerformanceData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $username = Request::param('username', ''); // 业务员筛选
        
        $current_admin = Admin::getMyInfo();
        // 获取所有业务员（包括没有数据的业务员也要显示）
        // 注意：这里只使用组织权限过滤，不限制 is_open，确保所有业务员都被包含
        // 显式设置无限制查询，确保获取所有数据
        // 业务员包括：业务员(10)、业务主管(11)、产品总监(13)、产品经理(14)
        $business_users_query = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14]) // 10:业务员, 11:业务主管, 13:产品总监, 14:产品经理
            ->where('username', '<>', '')
            ->whereNotNull('username');
        
        // 如果指定了业务员筛选，只查询该业务员
        if (!empty($username)) {
            $business_users_query->where('username', '=', $username);
        }
        
        // 限制最大查询数量，避免查询过多数据导致超时（最多500个业务员）
        $business_users = $business_users_query
            ->field('admin_id,username,team_name')
            ->order('team_name,username')
            ->limit(500) // 限制最大数量，避免超时
            ->select();
        
        if (empty($business_users)) {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => []
            ]);
        }
        
        // 提取所有业务员用户名，过滤空值
        $usernames = array_filter(array_column($business_users, 'username'));
        if (empty($usernames)) {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => []
            ]);
        }
        
        $user_map = [];
        foreach ($business_users as $user) {
            if (!empty($user['username'])) {
                $user_map[$user['username']] = $user;
            }
        }
        
        // 构建订单时间条件（不再查询询盘数量，只查询订单数据）
        $o_where = [];
        
        // 处理自定义时间范围（格式：start_date,end_date）
        $has_custom_time = false;
        if (!empty($at_time) && strpos($at_time, ',') !== false) {
            $date_parts = explode(',', $at_time);
            if (count($date_parts) == 2) {
                $start_date = trim($date_parts[0]);
                $end_date = trim($date_parts[1]);
                if ($start_date && $end_date) {
                    $has_custom_time = true;
                    // 自定义时间范围
                    $o_where[] = ['order_time', '>=', $start_date . ' 00:00:00'];
                    $o_where[] = ['order_time', '<=', $end_date . ' 23:59:59'];
                }
            }
        }
        
        // 使用 buildTimeWhere 方法构建订单时间条件
        if (!$has_custom_time && !empty($timebucket)) {
            $o_where[] = $this->buildTimeWhere($timebucket, 'order_time');
        } elseif (!$has_custom_time && !empty($at_time)) {
            $o_where[] = $this->buildTimeWhere($at_time, 'order_time');
        } elseif (!$has_custom_time && empty($timebucket) && empty($at_time)) {
            // 如果没有指定时间，默认使用本月
            $o_where[] = $this->buildTimeWhere('month', 'order_time');
        }
        
        // 批量查询所有业务员的订单数据（通过 pr_user）
        $order_base_query = Db::table('crm_client_order')
            ->where('pr_user', 'in', $usernames);
        
        // 应用时间条件
        if (!empty($o_where)) {
            foreach ($o_where as $condition) {
                if (is_array($condition)) {
                    if (isset($condition[0]) && is_array($condition[0])) {
                        foreach ($condition as $sub_condition) {
                            if (is_array($sub_condition) && isset($sub_condition[0])) {
                                $order_base_query->where($sub_condition[0], $sub_condition[1] ?? '=', $sub_condition[2] ?? null);
                            }
                        }
                    } else {
                        $order_base_query->where($condition[0], $condition[1] ?? '=', $condition[2] ?? null);
                    }
                } elseif (is_callable($condition)) {
                    $order_base_query->where($condition);
                }
            }
        }
        
        // 批量获取订单统计：订单数、利润、金额
        // 使用聚合查询，一次性获取所有统计数据，避免 N+1 问题
        $order_stats = $order_base_query
            ->field('pr_user, count(id) as order_count, sum(profit) as total_profit, sum(money) as total_money')
            ->group('pr_user')
            ->select();
        
        $order_map = [];
        foreach ($order_stats as $stat) {
            $order_map[$stat['pr_user']] = [
                'order_count' => (int)$stat['order_count'],
                'total_profit' => (float)($stat['total_profit'] ?: 0),
                'total_money' => (float)($stat['total_money'] ?: 0)
            ];
        }
        
        // 3. 对于没有订单的业务员，尝试通过客户名称关联（备用方案）- 优化为批量查询
        $users_without_orders = [];
        foreach ($usernames as $username) {
            if (!isset($order_map[$username]) || $order_map[$username]['order_count'] == 0) {
                $users_without_orders[] = $username;
            }
        }
        
        if (!empty($users_without_orders)) {
            // 批量获取这些业务员的客户名称（不再需要时间条件，因为订单表已经有时间条件）
            $customer_query = Db::table('crm_leads')
                ->where('status', '=', 1)  // 只查询有效询盘
                ->where('pr_user', 'in', $users_without_orders)
                ->where('kh_name', '<>', '')
                ->where('kh_name', '<>', null)
                ->field('pr_user, kh_name')
                ->select();
            
            $customer_map = [];
            $all_customer_names = [];
            foreach ($customer_query as $row) {
                if (!isset($customer_map[$row['pr_user']])) {
                    $customer_map[$row['pr_user']] = [];
                }
                $customer_map[$row['pr_user']][] = $row['kh_name'];
                $all_customer_names[] = $row['kh_name'];
            }
            
            // 批量查询所有相关订单（一次性查询，避免 N+1 问题）
            if (!empty($all_customer_names)) {
                $all_customer_names = array_unique($all_customer_names);
                
                // 如果客户名称太多，限制查询数量以避免超时（最多1000个）
                if (count($all_customer_names) > 1000) {
                    $all_customer_names = array_slice($all_customer_names, 0, 1000);
                }
                
                $backup_order_query = Db::table('crm_client_order')
                    ->where('cname', 'in', $all_customer_names);
                
                // 应用时间条件
                if (!empty($o_where)) {
                    foreach ($o_where as $condition) {
                        if (is_array($condition)) {
                            if (isset($condition[0]) && is_array($condition[0])) {
                                foreach ($condition as $sub_condition) {
                                    if (is_array($sub_condition) && isset($sub_condition[0])) {
                                        $backup_order_query->where($sub_condition[0], $sub_condition[1] ?? '=', $sub_condition[2] ?? null);
                                    }
                                }
                            } else {
                                $backup_order_query->where($condition[0], $condition[1] ?? '=', $condition[2] ?? null);
                            }
                        } elseif (is_callable($condition)) {
                            $backup_order_query->where($condition);
                        }
                    }
                }
                
                // 批量获取所有订单数据，按客户名称分组
                $backup_orders = $backup_order_query
                    ->field('cname, count(id) as order_count, sum(profit) as total_profit, sum(money) as total_money')
                    ->group('cname')
                    ->select();
                
                // 构建客户名称到订单统计的映射
                $order_by_cname = [];
                foreach ($backup_orders as $order_stat) {
                    $order_by_cname[$order_stat['cname']] = [
                        'order_count' => (int)$order_stat['order_count'],
                        'total_profit' => (float)($order_stat['total_profit'] ?: 0),
                        'total_money' => (float)($order_stat['total_money'] ?: 0)
                    ];
                }
                
                // 将订单统计分配回对应的业务员
                foreach ($users_without_orders as $username) {
                    if (isset($customer_map[$username]) && !empty($customer_map[$username])) {
                        $customer_names = $customer_map[$username];
                        $user_order_count = 0;
                        $user_total_profit = 0;
                        $user_total_money = 0;
                        
                        foreach ($customer_names as $cname) {
                            if (isset($order_by_cname[$cname])) {
                                $user_order_count += $order_by_cname[$cname]['order_count'];
                                $user_total_profit += $order_by_cname[$cname]['total_profit'];
                                $user_total_money += $order_by_cname[$cname]['total_money'];
                            }
                        }
                        
                        if ($user_order_count > 0) {
                            $order_map[$username] = [
                                'order_count' => $user_order_count,
                                'total_profit' => $user_total_profit,
                                'total_money' => $user_total_money
                            ];
                        }
                    }
                }
            }
        }
        
        // 4. 组装结果数据 - 确保所有业务员都被包含，即使没有数据也显示
        $result = [];
        foreach ($business_users as $user) {
            $username = $user['username'];
            
            // 跳过用户名为空的记录
            if (empty($username)) {
                continue;
            }
            
            // 获取订单数据（如果没有数据则为0）
            $order_data = isset($order_map[$username]) ? $order_map[$username] : ['order_count' => 0, 'total_profit' => 0, 'total_money' => 0];
            
            $total_profit = $order_data['total_profit'];
            $total_money = $order_data['total_money'];
            
            // 计算利润率
            $profit_rate = $total_money > 0 ? round(($total_profit / $total_money) * 100, 2) : 0;
            
            $result[] = [
                'username' => $username,
                'team_name' => $user['team_name'] ?: '',
                'profit' => number_format($total_profit, 2),
                'profit_rate' => number_format($profit_rate, 2)
            ];
        }
        
        // 按利润降序排序（默认排序）
        usort($result, function($a, $b) {
            $profit_a = floatval(str_replace(',', '', $a['profit']));
            $profit_b = floatval(str_replace(',', '', $b['profit']));
            return $profit_b <=> $profit_a;
        });
        
        // 添加排名（初始按利润排序）
        $rank = 1;
        foreach ($result as &$item) {
            $item['rank'] = $rank++;
        }
        unset($item);
        
        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => $result
        ]);
    }
}
