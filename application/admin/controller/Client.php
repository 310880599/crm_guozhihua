<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use think\facade\Env;
use PhpOffice\PhpSpreadsheet\IOFactory;
use think\facade\Cache;
use app\admin\model\Admin;

class Client extends Common
{
    protected $middleware = [\app\http\middleware\TrimStrings::class];


    const CONTACT_MAP = [
        'phone'         => 1,
        'email'         => 2,
        'whatsapp'      => 3,
        'ali_id'        => 4,
        'wechat'        => 5,
        'facebook'      => 6,
        'twitter'       => 7,
        'linkedin'      => 8,
        'youtube'       => 9,
        'instagram'     => 10,
        'weibo'         => 11,
        'qq'            => 12,
        'trademanager'  => 13,
        'skype'         => 14,
        '传真'           => 15,
        'msn'           => 16,
        'viber'         => 17,
        'pinterest'     => 18,
        'vk'            => 19,
        'line'          => 20,
        'zalo'          => 21,
        'telegram'      => 22,
    ];



    // 添加公共日志
    static function addOperLog($leads_id, $type, $description)
    {
        $description = is_string($description) ? $description : json_encode($description, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        Db::table('crm_operation_log')->insert([
            'user_id' => Session::get('aid'),
            'leads_id' => $leads_id,
            'oper_type' => $type,
            'description' => $description,
            'oper_user' => Session::get('username'),
            'created_at' => date("Y-m-d H:i:s")
        ]);
    }

    //客户联系方式格式化
    public function formatContact($contactList)
    {
        $contactGroup = [];
        foreach ($contactList as $contact) {
            if (isset($contactGroup[$contact['leads_id']][$contact['contact_type']])) {
                $contactGroup[$contact['leads_id']][$contact['contact_type']][] = $contact['contact_extra'] ? $contact['contact_extra'] . '-' . $contact['contact_value'] : $contact['contact_value'];
            } else {
                $contactGroup[$contact['leads_id']][$contact['contact_type']] = [$contact['contact_extra'] ? $contact['contact_extra'] . '-' . $contact['contact_value'] : $contact['contact_value']];
            }
        }
        return $contactGroup;
    }

    public function getContactType($contactGroup)
    {
        $con_map = array_flip(self::CONTACT_MAP);
        $result = [];
        foreach ($con_map as $k => $v) {
            if (isset($contactGroup[$k])) $result[$v] = $contactGroup[$k];
            else $result[$v] = [''];
        }
        // foreach ($contactGroup as $key => $vo) {
        //     $typeName = $con_map[$key] ?? 'unknown';
        //     $result[$typeName] = $vo;
        // }
        return $result;
    }


    public function getPortsByInquiry() {
        if (request()->isAjax()) {
            $inquiryId = Request::param('inquiry_id');
            if (!$inquiryId) {
                // 若未提供渠道ID参数，返回错误信息
                return json(['code' => 500, 'msg' => '缺少渠道ID', 'data' => []]);
            }
            // 查询对应渠道的所有运营端口（仅获取id和port_name字段）
            $ports = Db::table('crm_inquiry_port')
                        ->where(['inquiry_id' => $inquiryId, 'status' => 0])
                        ->field('id, port_name')
                        ->select();
            // 返回JSON数据：code为0表示成功，data中是端口列表
            return json(['code' => 0, 'msg' => '获取成功', 'data' => $ports]);
        }
        // 如有需要，可处理非 AJAX 的请求情形
    }


    //客户列表
    // public function index()
    // {
    //     if (request()->isPost()) {
    //         return $this->clientSearch();
    //         $page = input('page', 1);
    //         $pageSize = input('limit', config('pageSize'));
    //         $adminId = Session::get('aid');
    //         $subordinates = Db::name('admin')->where('parent_id', $adminId)->column('username');

    //         // 基本客户条件
    //         $query = Db::name('crm_leads')->alias('l')->where(['l.status' => 1, 'l.issuccess' => -1]);

    //         // if ($adminId == 1) {
    //         //     // 超级管理员无需额外条件
    //         // } elseif (!empty($subordinates)) {
    //         //     // 主管查看直属下属及自己的客户
    //         //     $usernames = array_merge($subordinates, [Session::get('username')]);
    //         //     $query->whereIn('l.pr_user', $usernames);
    //         // } else {
    //         //     // 普通员工仅查看自己名下的客户
    //         //     $query->where(['l.pr_user' => Session::get('username')]);
    //         // }
    //         $usernames  = [session('username')];
    //         $team_name = session('team_name') ?? '';
    //         if ($adminId == 1) {
    //             $usernames = [];
    //         } else if ($team_name) {
    //             // 主管查看直属下属及自己的客户
    //             $usernames = Db::name('admin')->where('team_name', $team_name)->column('username');
    //         }

    //         // 查询客户数据，并拼接联系方式
    //         $list = $query->where(function ($query) use ($usernames) {
    //             if ($usernames) {
    //                 $query->whereIn('l.pr_user', $usernames);
    //             }
    //         })
    //             ->field([
    //                 'l.*',
    //                 "GROUP_CONCAT(
    //                 DISTINCT CASE c.contact_type
    //                     WHEN 1 THEN '手机号'
    //                     WHEN 3 THEN 'WhatsApp'
    //                 END ORDER BY c.id SEPARATOR '<br>'
    //             ) AS contact_type",
    //                 "GROUP_CONCAT(DISTINCT c.contact_value ORDER BY c.id SEPARATOR '<br>') AS contact_value"
    //             ])
    //             ->leftJoin('crm_contacts c', 'l.id = c.leads_id')
    //             ->group('l.id')
    //             ->order('l.at_time desc')
    //             ->paginate([
    //                 'list_rows' => $pageSize,
    //                 'page' => $page
    //             ])
    //             ->toArray();

    //         return [
    //             'code' => 0,
    //             'msg' => '获取成功!',
    //             'data' => $list['data'],
    //             'count' => $list['total'],
    //             'rel' => 1
    //         ];
    //     }

    //     $khRankList = Db::table('crm_client_rank')->select();
    //     $khStatusList = Db::table('crm_client_status')->select();
    //     $xsSourceList = Db::table('crm_clues_source')->select();

    //     $team_name = session('team_name') ?? '';
    //     $adminResult = Db::name('admin')->where('group_id', '<>', 1)->where(function ($query) use ($team_name) {
    //         if ($team_name) {
    //             $query->where('team_name', $team_name);
    //         }
    //     })->field('admin_id,username')->select();
    //     $this->assign('adminResult', $adminResult);
    //     $this->assign('khRankList', $khRankList);
    //     $this->assign('khStatusList', $khStatusList);
    //     $this->assign('xsSourceList', $xsSourceList);

    //     return $this->fetch();
    // }

    // 客户列表
    public function index()
    {
        if (request()->isPost()) {
            // 统一走搜索方法，保证列表初次加载与查询结果一致
            return $this->clientSearchAll();
        }

        // ----- 以下为原样保留的页面下拉数据 -----
        $khRankList   = Db::table('crm_client_rank')->select();
        $inquiryList = Db::table('crm_inquiry')->select();
        $xsSourceList = Db::table('crm_clues_source')->select();

        $team_name   = session('team_name') ?? '';
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)
            ->where(function ($query) use ($team_name) {
                if ($team_name) $query->where('team_name', $team_name);
            })
            ->field('admin_id,username')->select();

        $this->assign('adminResult', $adminResult);
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);
        $this->assign('xsSourceList', $xsSourceList);
        return $this->fetch();
    }


    // 客户搜索（统一给表格用）
    public function clientSearchAll()
    {
        $page    = input('page/d', 1);
        $limit   = input('limit/d', config('pageSize'));
        $keyword = input('keyword/a', []);  // 获取筛选参数数组，默认为空

        // 处理时间范围筛选参数
        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            // 若提供自定义日期范围，则使用它覆盖 timebucket
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }

        // 调用模型方法获取查询结果列表
        $list = model('Client')->getClientSearchListAll($page, $limit, $keyword);

        // 返回数据给前端表格
        if (empty($list) || empty($list['data'])) {
            return [
                'code'  => 0,
                'msg'   => '获取成功!',
                'data'  => [],
                'count' => 0,
                'rel'   => 1
            ];
        }

        // 映射所属渠道名称和运营端口名称
        $rows = &$list['data'];
        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');
        // 【新增】批量获取产品名称和供应商名称映射
        $productIds = array_unique(array_filter(array_column($rows, 'product_name')));
        $productNameMap = [];
        if (!empty($productIds)) {
            // 查询所涉及的产品信息
            $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
            // 提取所有相关供应商ID并查询对应供应商名称
            $categoryIds = array_unique(array_column($productRows, 'category_id'));
            $categoryNameMap = !empty($categoryIds)
                ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                : [];
            // 生成 产品名称(供应商) 映射表
            foreach ($productRows as $prod) {
                $supplierName = isset($categoryNameMap[$prod['category_id']]) ? $categoryNameMap[$prod['category_id']] : '';
                $productNameMap[$prod['id']] = $prod['product_name'] . ($supplierName ? "({$supplierName})" : '');
            }
        } 
        foreach ($rows as &$row) {
            $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) ? $inquiryMap[$row['inquiry_id']] : (string)$row['inquiry_id'];
            $row['port_name'] = isset($portMap[$row['port_id']])  ? $portMap[$row['port_id']] : (string)$row['port_id'];
            // 【替换】将产品ID替换为“产品名称（供应商）”格式
            $row['product_name'] = isset($productNameMap[$row['product_name']]) 
                                    ? $productNameMap[$row['product_name']] 
                                    : (string)$row['product_name'];
        }
        unset($row);    
        return [
            'code'  => 0,
            'msg'   => '获取成功!',
            'data'  => $list['data'],
            'count' => $list['total'],
            'rel'   => 1
        ];
    }



    //（我的客户）列表
    public function perCliList()
    {
        if (request()->isPost()) {
            $page = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');

            // 基础列表（我的客户）
            $list = Db::table('crm_leads')
                ->where(['status' => 1, 'issuccess' => -1])
                ->where(['pr_user' => Session::get('username')])
                ->order('at_time desc')
                ->paginate(['list_rows' => $pageSize, 'page' => $page])
                ->toArray();

            if (empty($list) || empty($list['data'])) {
                return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
            }

            $rows = &$list['data'];
            $leadIds = array_column($rows, 'id');

            // 询盘来源映射（id -> 中文名），若 kh_status 已是中文则回退自身
            //$statusMap = Db::table('crm_client_status')->column('status_name', 'id');
            // 构建所属渠道和运营端口名称映射表
            $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
            $portMap    = Db::table('crm_inquiry_port')->column('port_name', 'id');

            // 【新增】批量获取产品名称和供应商名称映射
            $productIds = array_column($rows, 'product_name');
            $productIds = array_filter($productIds);
            $productIds = array_unique($productIds);
            $productNameMap = [];
            if (!empty($productIds)) {
                // 查询所涉及的产品信息
                $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
                // 提取所有相关供应商ID并查询对应供应商名称
                $categoryIds = array_unique(array_column($productRows, 'category_id'));
                $categoryNameMap = !empty($categoryIds)
                    ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                    : [];
                // 生成 产品名称(供应商) 映射表
                foreach ($productRows as $prod) {
                    $supplierName = isset($categoryNameMap[$prod['category_id']]) ? $categoryNameMap[$prod['category_id']] : '';
                    $productNameMap[$prod['id']] = $prod['product_name'] . ($supplierName ? "({$supplierName})" : '');
                }
            }

            // 一次性取出所有客户的主/辅电话：1=主电话，3=辅助电话
            $phoneMap = []; // leads_id => ['main'=>'', 'aux'=>'']
            if (!empty($leadIds)) {
                $contacts = Db::table('crm_contacts')
                    ->where('is_delete', 0)
                    ->whereIn('leads_id', $leadIds)
                    ->whereIn('contact_type', [1, 3])
                    ->order('id', 'asc')
                    ->field('leads_id, contact_type, contact_value')
                    ->select();
                foreach ($contacts as $c) {
                    $lid = $c['leads_id'];
                    if (!isset($phoneMap[$lid])) $phoneMap[$lid] = ['main' => '', 'aux' => ''];

                    if ($c['contact_type'] == 1) {
                        if ($phoneMap[$lid]['main'] === '') {
                            $phoneMap[$lid]['main'] = $c['contact_value'];
                        } else {
                            $phoneMap[$lid]['main'] .= ',' . $c['contact_value'];
                        }
                    } elseif ($c['contact_type'] == 3 && $phoneMap[$lid]['aux'] === '') {
                        $phoneMap[$lid]['aux'] = $c['contact_value'];
                    }

                }
            }

            // 收集协同人ID，后续统一查用户名
            $uidSet = [];
            foreach ($rows as &$row) {
                // 询盘来源中文
                //$row['kh_status_name'] = isset($statusMap[$row['kh_status']]) ? $statusMap[$row['kh_status']] : (string)$row['kh_status'];
                
                // 所属渠道名称（如无对应名称则用自身ID）
                $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) 
                                        ? $inquiryMap[$row['inquiry_id']] 
                                        : (string)$row['inquiry_id'];
                // 运营端口名称（如无对应名称则用自身ID）
                $row['port_name'] = isset($portMap[$row['port_id']]) 
                                    ? $portMap[$row['port_id']] 
                                    : (string)$row['port_id'];
                
                // 【替换】将产品ID替换为“产品名称（供应商）”格式
                $row['product_name'] = isset($productNameMap[$row['product_name']]) 
                                    ? $productNameMap[$row['product_name']] 
                                    : (string)$row['product_name'];  

                // 主/辅电话
                $row['main_phone'] = $phoneMap[$row['id']]['main'] ?? '';
                $row['aux_phone']  = $phoneMap[$row['id']]['aux'] ?? '';

                // 协同人ID解析（支持 JSON 数组或逗号分隔）
                $idsArr = [];
                if (!empty($row['joint_person'])) {
                    $jp = $row['joint_person'];
                    if (preg_match('/^\\s*\\[.*\\]\\s*$/', $jp)) {
                        $tmp = json_decode($jp, true);
                        if (is_array($tmp)) $idsArr = $tmp;
                    } else {
                        $idsArr = array_values(array_filter(explode(',', $jp)));
                    }
                }
                $row['_joint_ids'] = $idsArr;
                foreach ($idsArr as $uid) $uidSet[$uid] = true;
            }
            unset($row);

            // 协同人ID -> 用户名
            $adminMap = [];
            if (!empty($uidSet)) {
                $adminMap = Db::table('admin')
                    ->whereIn('admin_id', array_keys($uidSet))
                    ->column('username', 'admin_id');
            }
            foreach ($rows as &$row) {
                $names = [];
                foreach ($row['_joint_ids'] as $uid) {
                    $names[] = $adminMap[$uid] ?? (string)$uid;
                }
                $row['joint_person_names'] = $names ? implode('、', $names) : '';
                unset($row['_joint_ids']);
            }
            unset($row);

            return ['code' => 0, 'msg' => '获取成功!', 'data' => $rows, 'count' => $list['total'], 'rel' => 1];
        }

        // 页面渲染所需下拉数据
        $khRankList = Db::table('crm_client_rank')->select();
        $inquiryList  = Db::table('crm_inquiry')->select();        // 所属渠道下拉数据
        $khStatusList = Db::table('crm_client_status')->select();
        $xsSourceList = Db::table('crm_clues_source')->select();
        $yyList = $this->getYyList();
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);
        $this->assign('xsSourceList', $xsSourceList);  //线索/客户来源


        

        return $this->fetch('personclient/index');
    }





    //（协同人客户）列表
    public function joiCliList()
    {
        if (request()->isPost()) {
            $page     = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');
            $adminId  = Session::get('aid');          // 当前登录用户 admin_id
            $username = Session::get('username');     // 当前登录用户名

            // 查询当前用户作为协同人的客户列表
            $list = Db::table('crm_leads')
                ->where(['status' => 1, 'issuccess' => -1])           // 仅有效客户（未成交）
                ->where('pr_user', '<>', $username)                   // 排除当前用户自己负责的客户
                ->where(function ($query) use ($adminId) {
                    // joint_person 字段包含当前用户ID
                    $query->whereRaw("FIND_IN_SET('{$adminId}', joint_person)");
                })
                ->order('at_time desc')
                ->paginate(['list_rows' => $pageSize, 'page' => $page])
                ->toArray();

            if (empty($list) || empty($list['data'])) {
                // 无数据情况
                return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
            }

            // 提取数据集合
            $rows    = &$list['data'];
            $leadIds = array_column($rows, 'id');

            // 准备映射：询盘来源ID -> 中文名称
            //$statusMap = Db::table('crm_client_status')->column('status_name', 'id');
            // 【替换】不再使用 kh_status/source_port 映射，改为所属渠道/运营端口名称映射  
            $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
            $portMap    = Db::table('crm_inquiry_port')->column('port_name', 'id');

            // 【新增】批量获取产品名称和供应商名称映射
            $productIds = array_column($rows, 'product_name');
            $productIds = array_filter($productIds);
            $productIds = array_unique($productIds);
            $productNameMap = [];
            if (!empty($productIds)) {
                // 查询所涉及的产品信息
                $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
                // 提取所有相关供应商ID并查询对应供应商名称
                $categoryIds = array_unique(array_column($productRows, 'category_id'));
                $categoryNameMap = !empty($categoryIds)
                    ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                    : [];
                // 生成 产品名称(供应商) 映射表
                foreach ($productRows as $prod) {
                    $supplierName = isset($categoryNameMap[$prod['category_id']]) ? $categoryNameMap[$prod['category_id']] : '';
                    $productNameMap[$prod['id']] = $prod['product_name'] . ($supplierName ? "({$supplierName})" : '');
                }
            }


            // 获取所有选中客户的主/辅电话（contact_type: 1=主电话, 3=辅助电话）
            $phoneMap = [];
            if (!empty($leadIds)) {
                $contacts = Db::table('crm_contacts')
                    ->where('is_delete', 0)
                    ->whereIn('leads_id', $leadIds)
                    ->whereIn('contact_type', [1, 3])
                    ->order('id', 'asc')
                    ->field('leads_id, contact_type, contact_value')
                    ->select();
                foreach ($contacts as $c) {
                    $lid = $c['leads_id'];
                    if (!isset($phoneMap[$lid])) {
                        $phoneMap[$lid] = ['main' => '', 'aux' => ''];
                    }

                    if ($c['contact_type'] == 1) {
                        if ($phoneMap[$lid]['main'] === '') {
                            $phoneMap[$lid]['main'] = $c['contact_value'];
                        } else {
                            $phoneMap[$lid]['main'] .= ',' . $c['contact_value'];
                        }
                    } elseif ($c['contact_type'] == 3 && $phoneMap[$lid]['aux'] === '') {
                        $phoneMap[$lid]['aux'] = $c['contact_value'];
                    }

                }
            }

            // 收集协同人ID，稍后批量查询用户名
            $uidSet = [];
            foreach ($rows as &$row) {
                // 填充询盘来源中文名称
                // $row['kh_status_name'] = isset($statusMap[$row['kh_status']])
                //     ? $statusMap[$row['kh_status']]
                //     : (string)$row['kh_status'];
                // **新增**：所属渠道名称（没有则用自身ID）  
                $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) 
                                    ? $inquiryMap[$row['inquiry_id']] 
                                    : (string)$row['inquiry_id'];
                // **新增**：运营端口名称（没有则用自身ID）  
                $row['port_name'] = isset($portMap[$row['port_id']]) 
                                    ? $portMap[$row['port_id']] 
                                    : (string)$row['port_id'];
                
                // 【替换】将产品ID替换为“产品名称（供应商）”格式
                $row['product_name'] = isset($productNameMap[$row['product_name']]) 
                                    ? $productNameMap[$row['product_name']] 
                                    : (string)$row['product_name'];   

                // 填充主/辅电话
                $row['main_phone'] = $phoneMap[$row['id']]['main'] ?? '';
                $row['aux_phone']  = $phoneMap[$row['id']]['aux']  ?? '';

                // 解析 joint_person 字段为协同人ID数组（支持JSON数组或逗号分隔）
                $idsArr = [];
                if (!empty($row['joint_person'])) {
                    $jp = $row['joint_person'];
                    if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                        $tmp = json_decode($jp, true);
                        if (is_array($tmp)) $idsArr = $tmp;
                    } else {
                        $idsArr = array_values(array_filter(explode(',', $jp)));
                    }
                }
                $row['_joint_ids'] = $idsArr;
                foreach ($idsArr as $uid) {
                    $uidSet[$uid] = true;
                }
            }
            unset($row);

            // 批量查询协同人用户名映射 (admin_id -> username)
            $adminMap = [];
            if (!empty($uidSet)) {
                $adminMap = Db::table('admin')
                    ->whereIn('admin_id', array_keys($uidSet))
                    ->column('username', 'admin_id');
            }
            // 填充协同人名称字符串
            foreach ($rows as &$row) {
                $names = [];
                foreach ($row['_joint_ids'] as $uid) {
                    $names[] = $adminMap[$uid] ?? (string)$uid;
                }
                $row['joint_person_names'] = $names ? implode('、', $names) : '';
                unset($row['_joint_ids']);

                // 处理来源端口（source_port）字段，将MD5加密值转换为店铺名称
            }
            unset($row);

            // 返回数据列表
            return [
                'code'  => 0,
                'msg'   => '获取成功!',
                'data'  => $rows,
                'count' => $list['total'],
                'rel'   => 1
            ];
        }

        // 非 POST 请求时，渲染页面所需下拉数据（保持不变）
        $khRankList   = Db::table('crm_client_rank')->select();
        $inquiryList  = Db::table('crm_inquiry')->select();    // **新增：所属渠道列表**  
        $xsSourceList = Db::table('crm_clues_source')->select();
        $yyList       = $this->getYyList();
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);            // **新增：分配所属渠道下拉数据**  
        $this->assign('xsSourceList', $xsSourceList);



        return $this->fetch('jointclient/index');
    }

    // ====== 新增 enrichLeadsRows 开始 ======
    /**
     * 私有方法：对线索/客户行数据进行完整加工
     * 包括：渠道/端口名称映射、产品名称映射、主/辅电话补齐、协同人姓名补齐
     * @param array &$rows 行数据数组（引用传递）
     */
    private function enrichLeadsRows(&$rows)
    {
        // 兼容性与安全：若 $rows 为空或不是数组，直接返回
        if (empty($rows) || !is_array($rows)) {
            return;
        }

        $leadIds = array_column($rows, 'id');

        // A. 渠道/端口名称映射
        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap    = Db::table('crm_inquiry_port')->column('port_name', 'id');

        // B. 产品名称映射（产品ID -> "产品名称(供应商)"）
        $productIds = array_unique(array_filter(array_column($rows, 'product_name')));
        $productNameMap = [];
        if (!empty($productIds)) {
            $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
            $categoryIds = array_unique(array_column($productRows, 'category_id'));
            $categoryNameMap = !empty($categoryIds)
                ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                : [];
            foreach ($productRows as $prod) {
                $supplierName = isset($categoryNameMap[$prod['category_id']]) ? $categoryNameMap[$prod['category_id']] : '';
                $productNameMap[$prod['id']] = $prod['product_name'] . ($supplierName ? "({$supplierName})" : '');
            }
        }

        // C. 批量获取主/辅电话 (contact_type:1 主电话, 3 辅助电话)
        $phoneMap = [];
        if (!empty($leadIds)) {
            $contacts = Db::table('crm_contacts')
                ->where('is_delete', 0)
                ->whereIn('leads_id', $leadIds)
                ->whereIn('contact_type', [1, 3])
                ->order('id asc')
                ->field('leads_id, contact_type, contact_value')
                ->select();
            foreach ($contacts as $c) {
                $lid = $c['leads_id'];
                if (!isset($phoneMap[$lid])) {
                    $phoneMap[$lid] = ['main' => '', 'aux' => ''];
                }
                if ($c['contact_type'] == 1) {
                    // 主电话支持多个，用逗号连接
                    $phoneMap[$lid]['main'] = $phoneMap[$lid]['main'] === '' ? $c['contact_value'] : ($phoneMap[$lid]['main'] . ',' . $c['contact_value']);
                } elseif ($c['contact_type'] == 3 && $phoneMap[$lid]['aux'] === '') {
                    // 辅助电话仅取第一个
                    $phoneMap[$lid]['aux'] = $c['contact_value'];
                }
            }
        }

        // D. 收集协同人ID以批量查询姓名
        $uidSet = [];
        foreach ($rows as &$row) {
            // 所属渠道名称和运营端口名称
            $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) ? $inquiryMap[$row['inquiry_id']] : (string)$row['inquiry_id'];
            $row['port_name']    = isset($portMap[$row['port_id']]) ? $portMap[$row['port_id']] : (string)$row['port_id'];
            
            // 产品名称替换
            if (!empty($row['product_name'])) {
                $row['product_name'] = isset($productNameMap[$row['product_name']]) ? $productNameMap[$row['product_name']] : (string)$row['product_name'];
            }
            
            // 主/辅电话填充
            $lid = $row['id'];
            $row['main_phone'] = $phoneMap[$lid]['main'] ?? '';
            $row['aux_phone']  = $phoneMap[$lid]['aux']  ?? '';
            
            // 协同人（joint_person）解析为ID数组
            $idsArr = [];
            if (!empty($row['joint_person'])) {
                $jp = $row['joint_person'];
                if (preg_match('/^\\s*\\[.*\\]\\s*$/', $jp)) {
                    $tmp = json_decode($jp, true);
                    if (is_array($tmp)) $idsArr = $tmp;
                } else {
                    $idsArr = array_values(array_filter(explode(',', $jp)));
                }
            }
            $row['_joint_ids'] = $idsArr;
            foreach ($idsArr as $uid) {
                $uidSet[$uid] = true;
            }
        }
        unset($row);

        // 协同人ID映射为用户名
        $adminMap = [];
        if (!empty($uidSet)) {
            $adminMap = Db::table('admin')->whereIn('admin_id', array_keys($uidSet))->column('username', 'admin_id');
        }
        foreach ($rows as &$row) {
            $names = [];
            foreach ($row['_joint_ids'] as $uid) {
                $names[] = isset($adminMap[$uid]) ? $adminMap[$uid] : (string)$uid;
            }
            $row['joint_person_names'] = $names ? implode('、', $names) : '';
            unset($row['_joint_ids']); // 清理临时字段
        }
        unset($row);
    }
   // ====== 新增 enrichLeadsRows 结束 ======

   public function successCliList()
    {
        // 仅处理 AJAX 请求
        if (request()->isPost()) {
            // 基础筛选条件：仅已成交客户，状态有效
            $where = ['issuccess' => 1, 'status' => 1];
            // 非超级管理员只能查看自己负责的客户
            if (session('aid') != 1) {
                $where['pr_user'] = Session::get('username');
            }
            // 分页参数
            $page     = input('page/d', 1);
            $pageSize = input('limit/d', config('pageSize'));
            // 查询已成交客户列表
            $list = Db::table('crm_leads')
                ->where($where)
                ->order('at_time desc')
                ->paginate(['list_rows' => $pageSize, 'page' => $page])
                ->toArray();
            // 无数据情况
            if (empty($list) || empty($list['data'])) {
                return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
            }
            // ====== 修改 successCliList 开始 ======
            // 提取结果集并调用数据加工方法
            $rows = &$list['data'];
            $this->enrichLeadsRows($rows);
            // ====== 修改 successCliList 结束 ======
            // 返回数据列表
            return [
                'code'  => 0,
                'msg'   => '获取成功!',
                'data'  => $rows,
                'count' => $list['total'],
                'rel'   => 1
            ];
        }
        // 非 AJAX 请求时，获取下拉选项数据并渲染页面
        $khRankList   = Db::table('crm_client_rank')->select();
        $khStatusList = Db::table('crm_client_status')->select();
        $xsSourceList = Db::table('crm_clues_source')->select();
        $inquiryList  = Db::table('crm_inquiry')->select();            // 所属渠道列表
        $adminResult  = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        // 获取运营人员列表（运营组用户）
        $yyList = $this->getYyList();
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('khStatusList', $khStatusList);
        $this->assign('xsSourceList', $xsSourceList);
        $this->assign('inquiryList', $inquiryList);
        $this->assign('adminResult', $adminResult);
        return $this->fetch('client/chengjiao');
    }



    // public function xlsUpload() {
    //     $xlsFile = request()->file('xlsFile');

    //     if (!$xlsFile) {
    //         return json([
    //             'code' => -1,
    //             'msg'  => '请上传Excel文件（字段名应为xlsFile）',
    //             'data' => []
    //         ]);
    //     }

    //     $uploadPath = Env::get('root_path') . 'public/uploads/';
    //     $info = $xlsFile->move($uploadPath);
    //     if (!$info) {
    //         return json([
    //             'code' => -1,
    //             'msg'  => '文件上传失败：' . $xlsFile->getError(),
    //             'data' => []
    //         ]);
    //     }

    //     $filePath = $uploadPath . $info->getSaveName();
    //     // 使用 PhpSpreadsheet 读取 Excel（支持中文表头）
    //     try {
    //         $reader = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($filePath);
    //         $spreadsheet = $reader->load($filePath);
    //         $sheet = $spreadsheet->getActiveSheet();
    //         $data = $sheet->toArray(null, true, true, true); // 保留键值为 'A','B','C'...
    //     } catch (\Exception $e) {
    //         return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
    //     }

    //     // 第一行为标题行
    //     $headers = array_shift($data);
    //     $insertData = [];

    //     foreach ($data as $row) {
    //         $rowAssoc = [];
    //         foreach ($headers as $key => $title) {
    //             $rowAssoc[$title] = $row[$key] ?? '';
    //         }

    //         // 开始映射字段（你可以根据表头调整）
    //         $insertData[] = [
    //             'kh_name'     => $rowAssoc['客户名称'] ?? '',
    //             'kh_rank'     => $rowAssoc['客户等级'] ?? '',
    //             'pr_gh_type'  => $rowAssoc['客户归属公海'] ?? '',
    //             'kh_status'   => $rowAssoc['客户来源'] ?? '',
    //             'xs_source'   => $rowAssoc['客户国家'] ?? '',
    //             'kh_contact'  => $rowAssoc['联系人'] ?? '',
    //             'kh_hangye'   => $rowAssoc['联系人邮箱'] ?? '',

    //             'remark'      => $rowAssoc['客户备注'] ?? '',
    //             'pr_user'     => Session::get('username'),
    //             'ut_time'     => date("Y-m-d H:i:s"),
    //             'at_time'     => date("Y-m-d H:i:s"),
    //             'at_user'     => Session::get('username'),
    //             'status'      => 1
    //         ];
    //     }

    //     if (empty($insertData)) {
    //         return json(['code' => -1, 'msg' => 'Excel中无有效数据']);
    //     }

    //     // 插入数据库
    //     $success = db('crm_leads')->insertAll($insertData);
    //     if ($success) {
    //         return json(['code' => 0, 'msg' => '成功导入 ' . count($insertData) . ' 条客户数据']);
    //     } else {
    //         return json(['code' => -1, 'msg' => '导入失败，请检查字段映射或数据库结构']);
    //     }
    // }

    // const CONTACT_TYPE_PHONE = 1;
    // const CONTACT_TYPE_EMAIL = 2;

    //   public function xlsUpload() {
    //     $xlsFile = request()->file('xlsFile');

    //     if (!$xlsFile) {
    //         return json([
    //             'code' => -1,
    //             'msg'  => '请上传Excel文件（字段名应为xlsFile）',
    //             'data' => []
    //         ]);
    //     }

    //     $uploadPath = Env::get('root_path') . 'public/uploads/';
    //     $info = $xlsFile->move($uploadPath);
    //     if (!$info) {
    //         return json([
    //             'code' => -1,
    //             'msg'  => '文件上传失败：' . $xlsFile->getError(),
    //             'data' => []
    //         ]);
    //     }

    //     $filePath = $uploadPath . $info->getSaveName();
    //     // 使用 PhpSpreadsheet 读取 Excel（支持中文表头）
    //     try {
    //         $reader = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($filePath);
    //         $spreadsheet = $reader->load($filePath);
    //         $sheet = $spreadsheet->getActiveSheet();
    //         $data = $sheet->toArray(null, true, true, true); // 保留键值为 'A','B','C'...
    //     } catch (\Exception $e) {
    //         return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
    //     }

    //     // 第一行为标题行
    //     $headers = array_shift($data);
    //     $insertData = [];

    //     foreach ($data as $row) {
    //         $rowAssoc = [];
    //         foreach ($headers as $key => $title) {
    //             $rowAssoc[$title] = $row[$key] ?? '';
    //         }

    //         // 开始映射字段（你可以根据表头调整）
    //         $insertData[] = [
    //             'kh_name'     => $rowAssoc['客户名称'] ?? '',
    //             'kh_rank'     => $rowAssoc['客户等级'] ?? '',
    //             'pr_gh_type'  => $rowAssoc['客户归属公海'] ?? '',
    //             'kh_status'   => $rowAssoc['客户来源'] ?? '',
    //             'xs_area'   => $rowAssoc['客户国家'] ?? '',
    //             'kh_contact'  => $rowAssoc['联系人'] ?? '',
    //             'contact_value'   => $rowAssoc['联系人邮箱'] ?? '',
    //             'contact_value'       => $rowAssoc['联系人电话'] ?? '',
    //             'remark'      => $rowAssoc['客户备注'] ?? '',
    //             'pr_user'     => Session::get('username'),
    //             'ut_time'     => date("Y-m-d H:i:s"),
    //             'at_time'     => date("Y-m-d H:i:s"),
    //             'at_user'     => Session::get('username'),
    //             'status'      => 1
    //         ];
    //     }

    //     if (empty($insertData)) {
    //         return json(['code' => -1, 'msg' => 'Excel中无有效数据']);
    //     }

    //     // 插入数据库
    //     $success = db('crm_leads')->insertAll($insertData);
    //     if ($success) {
    //         return json(['code' => 0, 'msg' => '成功导入 ' . count($insertData) . ' 条客户数据']);
    //     } else {
    //         return json(['code' => -1, 'msg' => '导入失败，请检查字段映射或数据库结构']);
    //     }
    // }
    // public function xlsUpload()
    // {
    //     $xlsFile = request()->file('xlsFile');

    //     if (!$xlsFile) {
    //         return json(['code' => -1, 'msg' => '请上传Excel文件']);
    //     }

    //     $uploadPath = Env::get('root_path') . 'public/uploads/';
    //     $info = $xlsFile->move($uploadPath);
    //     if (!$info) {
    //         return json(['code' => -1, 'msg' => '文件上传失败：' . $xlsFile->getError()]);
    //     }

    //     $filePath = $uploadPath . $info->getSaveName();

    //     try {
    //         $reader = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($filePath);
    //         $spreadsheet = $reader->load($filePath);
    //         $sheet = $spreadsheet->getActiveSheet();
    //         $data = $sheet->toArray(null, true, true, true);
    //     } catch (\Exception $e) {
    //         return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
    //     }

    //     // 第一行为标题行
    //     $headers = array_shift($data);

    //     Db::startTrans();
    //     try {
    //         $contactsInsertData = [];
    //         foreach ($data as $row) {
    //             $rowAssoc = [];
    //             foreach ($headers as $key => $title) {
    //                 $rowAssoc[$title] = $row[$key] ?? '';
    //             }

    //             // 主表数据
    //             $leadsRow = [
    //                 'kh_name'     => $rowAssoc['客户名称'] ?? '',
    //                 'kh_rank'     => $rowAssoc['客户等级'] ?? '',
    //                 'pr_gh_type'  => $rowAssoc['客户归属公海'] ?? '',
    //                 'kh_status'   => $rowAssoc['客户来源'] ?? '',
    //                 'xs_area'     => $rowAssoc['客户国家'] ?? '',
    //                 'kh_contact'  => $rowAssoc['联系人'] ?? '',
    //                 'remark'      => $rowAssoc['客户备注'] ?? '',
    //                 'pr_user'     => Session::get('username'),
    //                 'ut_time'     => date("Y-m-d H:i:s"),
    //                 'at_time'     => date("Y-m-d H:i:s"),
    //                 'at_user'     => Session::get('username'),
    //                 'status'      => 1
    //             ];

    //             db('crm_leads')->insert($leadsRow);
    //             $leadsId = Db::name('crm_leads')->getLastInsID();

    //             // 联系方式
    //             $phone = trim($rowAssoc['联系人电话'] ?? '');
    //             $email = trim($rowAssoc['联系人邮箱'] ?? '');
    //             $whatsapp = trim($rowAssoc['联系人WhatsApp'] ?? '');

    //             if (!empty($phone)) {
    //                 db('crm_contacts')->insert([
    //                     'leads_id' => $leadsId,
    //                     'contact_type' => self::CONTACT_MAP['phone'],
    //                     'contact_value' => $phone,
    //                     'created_at' => date("Y-m-d H:i:s")
    //                 ]);
    //             }

    //             if (!empty($email)) {
    //                 db('crm_contacts')->insert([
    //                     'leads_id' => $leadsId,
    //                     'contact_type' => self::CONTACT_MAP['email'],
    //                     'contact_value' => $email,
    //                     'created_at' => date("Y-m-d H:i:s")
    //                 ]);
    //             }

    //             if (!empty($whatsapp)) {
    //                 db('crm_contacts')->insert([
    //                     'leads_id' => $leadsId,
    //                     'contact_type' => self::CONTACT_MAP['whatsapp'],
    //                     'contact_value' => $whatsapp,
    //                     'created_at' => date("Y-m-d H:i:s")
    //                 ]);
    //             }
    //         }

    //         Db::commit();
    //         return json(['code' => 0, 'msg' => '成功导入']);
    //     } catch (\Exception $e) {
    //         Db::rollback();
    //         return json(['code' => -1, 'msg' => '导入失败', 'error' => $e->getMessage()]);
    //     }
    // }



    // public function xlsUpload() {
    //     $xlsFile = request()->file('xlsFile');

    //     if (!$xlsFile) {
    //         return json(['code' => -1, 'msg' => '请上传Excel文件']);
    //     }

    //     $uploadPath = Env::get('root_path') . 'public/uploads/';
    //     $info = $xlsFile->move($uploadPath);

    //     if (!$info) {
    //         return json(['code' => -1, 'msg' => '文件上传失败：' . $xlsFile->getError()]);
    //     }

    //     $filePath = $uploadPath . $info->getSaveName();

    //     try {
    //         $reader = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($filePath);
    //         $spreadsheet = $reader->load($filePath);
    //         $sheet = $spreadsheet->getActiveSheet();
    //         $data = $sheet->toArray(null, true, true, true);
    //     } catch (\Exception $e) {
    //         return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
    //     }

    //     // 取表头
    //     $headers = array_shift($data);
    //     $current_time = date("Y-m-d H:i:s");
    //     $pr_user = Session::get('username');

    //     Db::startTrans();
    //     try {
    //         $insertedCount = 0;
    //         $rowNum = 1;

    //         foreach ($data as $row) {
    //             $rowNum++;
    //             $rowAssoc = [];

    //             foreach ($headers as $key => $title) {
    //                 $rowAssoc[$title] = $row[$key] ?? '';
    //             }

    //             // 插入主表 crm_leads
    //             $leadsRow = [
    //                 'kh_name'      => $rowAssoc['客户名称'] ?? '',
    //                 'kh_rank'      => $rowAssoc['客户等级'] ?? '',
    //                 'xs_area'      => $rowAssoc['地区'] ?? '',
    //                 'kh_contact'   => $rowAssoc['联系人'] ?? '',
    //                 'remark'       => $rowAssoc['客户备注'] ?? '',
    //                 'kh_status'    => $rowAssoc['客户来源'] ?? '',
    //                 'pr_user'      => $pr_user,
    //                 'ut_time'      => $current_time,
    //                 'at_time'      => $current_time,
    //                 'at_user'      => $pr_user,
    //                 'status'       => 1,
    //                 'ispublic'     => 3,
    //                 'pr_user_bef'  => $pr_user,
    //             ];

    //             $leadsId = db('crm_leads')->insertGetId($leadsRow);

    //             // 构建联系方式
    //             $contacts = [
    //                 [
    //                     'leads_id'      => $leadsId,
    //                     'contact_type'  => self::CONTACT_MAP['phone'],
    //                     'contact_value' => trim($rowAssoc['联系人电话'] ?? ''),
    //                     'contact_extra' => trim($rowAssoc['国家号'] ?? ''),
    //                     'created_at'    => $current_time
    //                 ],
    //                 [
    //                     'leads_id'      => $leadsId,
    //                     'contact_type'  => self::CONTACT_MAP['email'],
    //                     'contact_value' => trim($rowAssoc['联系人邮箱'] ?? ''),
    //                     'contact_extra' => '',
    //                     'created_at'    => $current_time
    //                 ],
    //                 [
    //                     'leads_id'      => $leadsId,
    //                     'contact_type'  => self::CONTACT_MAP['whatsapp'],
    //                     'contact_value' => trim($rowAssoc['联系人WhatsApp'] ?? ''),
    //                     'contact_extra' => '',
    //                     'created_at'    => $current_time
    //                 ],
    //             ];

    //             // 过滤掉 contact_value 为空的记录再插入
    //             $validContacts = [];
    //             foreach ($contacts as $contact) {
    //                 if ($contact['contact_value'] !== '') {
    //                     $validContacts[] = $contact;
    //                 }
    //             }

    //             if (!empty($validContacts)) {
    //                 db('crm_contacts')
    //                     ->field(['leads_id', 'contact_type', 'contact_value', 'contact_extra', 'created_at'])
    //                     ->insertAll($validContacts);
    //             }

    //             $insertedCount++;
    //         }

    //         Db::commit();
    //         return json(['code' => 0, 'msg' => '成功导入客户数据：' . $insertedCount . '条']);

    //     } catch (\Exception $e) {
    //         Db::rollback();
    //         return json([
    //             'code' => -1,
    //             'msg' => '导入失败，出错在Excel第 ' . $rowNum . ' 行',
    //             'error' => $e->getMessage()
    //         ]);
    //     }
    // }






    public function xlsUploadOld()
    {
        $xlsFile = request()->file('xlsFile');

        if (!$xlsFile) {
            return json(['code' => -1, 'msg' => '请上传Excel文件']);
        }

        $uploadPath = Env::get('root_path') . 'public/uploads/';
        if (!is_dir($uploadPath)) {
            mkdir($uploadPath, 0755, true);
        }

        $info = $xlsFile->move($uploadPath);
        if (!$info) {
            return json(['code' => -1, 'msg' => '文件上传失败：' . $xlsFile->getError()]);
        }

        $filePath = $uploadPath . $info->getSaveName();

        try {
            $reader = IOFactory::createReaderForFile($filePath);
            $spreadsheet = $reader->load($filePath);
            $sheet = $spreadsheet->getActiveSheet();
            $data = $sheet->toArray(null, true, true, true);
        } catch (\Exception $e) {
            return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
        }

        $headers = array_shift($data); // 表头
        $current_time = date("Y-m-d H:i:s");
        $pr_user = Session::get('username');

        Db::startTrans();
        try {
            $insertedCount = 0;
            $contactsData = [];

            foreach ($data as $row) {
                $rowAssoc = [];
                foreach ($headers as $key => $title) {
                    $rowAssoc[$title] = $row[$key] ?? '';
                }

                // 插入客户主表（crm_leads）
                $leadsRow = [
                    'kh_name'      => $rowAssoc['客户名称'] ?? '',
                    'kh_rank'      => $rowAssoc['客户等级'] ?? '',
                    'xs_area'      => $rowAssoc['地区'] ?? '',
                    'kh_contact'   => $rowAssoc['联系人'] ?? '',
                    'remark'       => $rowAssoc['客户备注'] ?? '',
                    'kh_status'    => $rowAssoc['客户来源'] ?? '',
                    'pr_user'      => $pr_user,
                    'ut_time'      => $current_time,
                    'at_time'      => $current_time,
                    'at_user'      => $pr_user,
                    'status'       => 1,
                    'ispublic'     => 3,
                    'pr_user_bef'  => $pr_user,
                ];

                $leadsId = Db::name('crm_leads')->insertGetId($leadsRow);

                // 构建联系人数据
                $contacts = [
                    [
                        'leads_id'      => $leadsId,
                        'contact_type'  => self::CONTACT_MAP['phone'],
                        'contact_value' => trim($rowAssoc['联系人电话'] ?? ''),
                        'contact_extra' => trim($rowAssoc['国家号'] ?? ''),
                        'created_at'    => $current_time
                    ],
                    [
                        'leads_id'      => $leadsId,
                        'contact_type'  => self::CONTACT_MAP['email'],
                        'contact_value' => trim($rowAssoc['联系人邮箱'] ?? ''),
                        'contact_extra' => '',
                        'created_at'    => $current_time
                    ],
                    [
                        'leads_id'      => $leadsId,
                        'contact_type'  => self::CONTACT_MAP['whatsapp'],
                        'contact_value' => trim($rowAssoc['联系人WhatsApp'] ?? ''),
                        'contact_extra' => '',
                        'created_at'    => $current_time
                    ],
                ];

                foreach ($contacts as $contact) {
                    if (!empty($contact['contact_value'])) {
                        $contactsData[] = $contact;
                    }
                }

                $insertedCount++;
            }

            // 批量插入联系方式表（crm_contacts）
            if (!empty($contactsData)) {
                Db::name('crm_contacts')
                    ->strict(false)
                    ->field(['leads_id', 'contact_type', 'contact_value', 'contact_extra', 'created_at'])
                    ->insertAll($contactsData);
            }

            Db::commit();
            return json(['code' => 0, 'msg' => '成功导入客户数据：' . $insertedCount . ' 条']);
        } catch (\Exception $e) {
            Db::rollback();
            return json([
                'code' => -1,
                'msg'  => '导入失败，第 ' . ($insertedCount + 1) . ' 行出错',
                'error' => $e->getMessage()
            ]);
        }
    }






    // public function xlsUpload()
    // {

    //     $xlsFile = request()->file('xlsFile');

    //     if (!$xlsFile) {
    //         return json(['code' => -1, 'msg' => '请上传Excel文件']);
    //     }

    //     // 配置文件上传规则
    //     $uploadConfig = [
    //         'size' => 1024 * 1024 * 20, // 20MB 文件大小限制
    //         'ext' => 'xlsx,xls', // 只允许上传 Excel 文件
    //     ];

    //     $uploadPath = Env::get('root_path') . 'public/uploads/';
    //     if (!is_dir($uploadPath)) {
    //         if (!mkdir($uploadPath, 0755, true)) {
    //             return json(['code' => -1, 'msg' => '上传目录创建失败，请检查权限']);
    //         }
    //     }
    //     $info = $xlsFile->validate($uploadConfig)->move($uploadPath, $this->generateUniqueFileName($xlsFile));
    //     if (!$info) {
    //         return json(['code' => -1, 'msg' => '文件上传失败：' . $xlsFile->getError()]);
    //     }

    //     $filePath = $uploadPath . $info->getSaveName();

    //     $fileHash = hash_file('sha256', $filePath);

    //     if (Cache::has('excel_import_hash:' . $fileHash)) {
    //         return json(['code' => -1, 'msg' => '该文件已上传过，请不要重复上传']);
    //     }

    //     Cache::set('excel_import_hash:' . $fileHash, true, 172800);

    //     try {
    //         $reader = IOFactory::createReaderForFile($filePath);
    //         $spreadsheet = $reader->load($filePath);
    //         $sheet = $spreadsheet->getActiveSheet();
    //         $data = $sheet->toArray(null, true, true, true);
    //     } catch (\Exception $e) {
    //         return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
    //     }

    //     $headers = array_shift($data); // 表头
    //     $pr_user = Session::get('username');

    //     // 将数据拆分成小块，每块 100 条记录
    //     $chunkSize = 100;
    //     $chunks = array_chunk($data, $chunkSize);

    //     foreach ($chunks as $chunk) {
    //         $jobData = [
    //             'user_id' => Session::get('aid'),
    //             'filePath' => $filePath,
    //             'pr_user' => $pr_user,
    //             'headers' => $headers,
    //             'chunkData' => $chunk
    //         ];

    //         // 将任务推送到队列
    //         queue(\app\admin\job\ExcelImport::class, $jobData, 0, 'excel_import');
    //     }

    //     return json(['code' => 0, 'msg' => '导入任务已提交，请稍后查看结果']);
    // }

    // application/admin/controller/Client.php


    public function xlsUpload()
    {
        // 获取上传的 Excel 文件
        $xlsFile = request()->file('xlsFile');
        if (!$xlsFile) {
            return json(['code' => -1, 'msg' => '请上传Excel文件']);
        }

        // 文件上传配置：大小限制20MB，扩展名xls/xlsx
        $uploadConfig = [
            'size' => 1024 * 1024 * 20,
            'ext'  => 'xlsx,xls'
        ];
        $uploadPath = Env::get('root_path') . 'public/uploads/';
        // 若上传目录不存在则尝试创建
        if (!is_dir($uploadPath)) {
            if (!mkdir($uploadPath, 0755, true)) {
                return json(['code' => -1, 'msg' => '上传目录创建失败，请检查权限']);
            }
        }
        // 保存上传文件并生成唯一文件名:contentReference[oaicite:0]{index=0}:contentReference[oaicite:1]{index=1}
        $info = $xlsFile->validate($uploadConfig)->move($uploadPath, $this->generateUniqueFileName($xlsFile));
        if (!$info) {
            return json(['code' => -1, 'msg' => '文件上传失败：' . $xlsFile->getError()]);
        }
        $filePath = $uploadPath . $info->getSaveName();
        // 计算文件哈希用于判重
        $fileHash = hash_file('sha256', $filePath);
        if (Cache::has('excel_import_hash:' . $fileHash)) {
            return json(['code' => -1, 'msg' => '该文件已上传过，请不要重复上传']);
        }
        Cache::set('excel_import_hash:' . $fileHash, true, 172800); // 标记48小时内重复

        // 读取Excel内容
        try {
            $reader      = IOFactory::createReaderForFile($filePath);
            $spreadsheet = $reader->load($filePath);
            $sheet       = $spreadsheet->getActiveSheet();
            // 转换表格为数组（保留表头行和空行）
            $data = $sheet->toArray(null, true, true, true);
        } catch (\Exception $e) {
            return json(['code' => -1, 'msg' => '读取Excel出错：' . $e->getMessage()]);
        }
        if (empty($data)) {
            return json(['code' => -1, 'msg' => 'Excel文件内容为空']);
        }

        // 提取表头并去除首行
        $headers = array_shift($data);
        $pr_user = Session::get('username');   // 当前导入操作用户
        $userId  = Session::get('aid');

        // 将数据按每100条分组，逐批加入队列处理:contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}
        $chunkSize = 100;
        $chunks = array_chunk($data, $chunkSize);
        foreach ($chunks as $chunk) {
            $jobData = [
                'user_id'   => $userId,
                'pr_user'   => $pr_user,
                'headers'   => $headers,
                'chunkData' => $chunk
            ];
            // 推送异步任务到名为 excel_import 的队列
            queue(\app\admin\job\ExcelImport::class, $jobData, 0, 'excel_import');
        }
        return json(['code' => 0, 'msg' => '导入任务已提交，请稍后查看结果']);
    }



    // 生成唯一文件名
    private function generateUniqueFileName($file)
    {
        $fileInfo = $file->getInfo();
        $originalName = $fileInfo['name'] ?? '';
        // 利用 pathinfo 函数提取扩展名
        $ext = pathinfo($originalName, PATHINFO_EXTENSION);
        return uniqid() . '.' . $ext;
    }



    // 支持多个主电话的校验
    private function checkDataNew(&$contact)
    {
        // 主电话（支持 数组 / JSON字符串 / 逗号或空白分隔）
        $mainPhones = $this->parsePhones(Request::param('more_phones'));  
        // 辅助电话（单个）
        $aux = preg_replace('/\D/', '', (string)Request::param('phone2', ''));

        // 主电话必填（至少一个）
        if (empty($mainPhones)) {
            return [false, '主电话不能为空'];
        }

        // 辅助电话格式校验（可选/11位）
        if ($aux !== '' && !preg_match('/^\d{11}$/', $aux)) {
            return [false, '辅助电话必须为11位数字'];
        }

        // 主/辅不能相同
        if ($aux !== '' && in_array($aux, $mainPhones, true)) {
            return [false, '主电话与辅助电话不能相同'];
        }

        // 组装 contact['phone']，供后续流程使用（主多条+辅单条）
        $contact = [];
        $numbersForContact = $mainPhones;
        if ($aux !== '') {
            $numbersForContact[] = $aux;
        }
        $contact['phone'] = $numbersForContact;

        // 提供给查重的集合
        $require_check = $numbersForContact;

        return [true, $require_check];
    }


    //数据校验
    private function checkData(&$contact)
    {
        $map = [
            'phone' => '手机号码',
            'whatsapp' => 'whatsapp',
        ];
        $request = request();
        $phone_code = $request->param('phone_code');
        foreach (array_keys($map) as $k) {
            $value = $request->param($k);
            if ($value) {
                if (is_array($value)) {
                    foreach ($value as $i => $vv) {
                        if (!empty($vv)) $contact[$k] = $value;
                        else {
                            if ($k == 'phone') {
                                unset($phone_code[$i]);
                            }
                        }
                    }
                } else {
                    $contact[$k] = $value;
                }
            }
        }
        if (empty($contact)) return [false, '请至少填写WhatsApp、阿里id、微信、邮箱或号码中的一个'];
        $require_check = [];
        foreach ($contact as $k => $v) {
            if (is_string($v)) $v = explode(',', $v);
            $duplicates = getDuplicates($v);
            if ($duplicates) {
                return [false, $map[$k] . ':' . implode(',', $duplicates) . ' 重复录入'];
            }
            $require_check = array_merge($require_check, $v);
        }

        if (!empty($contact['phone'])) {
            $contact['phone'] =  array_map(function ($code, $phone) {
                return [$code, $phone];
            },  $phone_code, $contact['phone']);
            foreach ($contact['phone'] as $item) {
                $require_check[] = $item[0] . $item[1];
            }
        }
        return [true, $require_check];
    }

    //当前录入查重
    private function checkDuplicate($data, $require_checke)
    {
        //更新
        $update = false;
        $where = [];
        if (isset($data['id'])) {
            $update = true;
            $where = [['id', '<>', $data['id']]];
        }
        // $find = db('crm_leads')->where($where)->where(function ($query) use ($data) {
        //     // $query->where('kh_name','like','%'.$data['kh_name'].'%')
        //     $query->where('kh_name', $data['kh_name']);
        //     if ($data['kh_contact']) $query->whereOr('kh_contact', $data['kh_contact']);
        // })->find();
        // if ($find)  return [false, $find['kh_name'] . '客户信息已存在,当前所属人' . $find['pr_user']];
        //查询关联表crm_contacts数据表重复
        if ($update) $where = [['leads_id', '<>', $data['id']]];
        //模糊查询
        foreach ($require_checke as $i => $v) {
            //判断是否是手机号或者whatsapp号码
            if (self::validatePhoneNumber($v)) {
                $contactExist = db('crm_contacts')->where($where)->where('is_delete', 0)->where(function ($q) use ($v) {
                    $q->where('vdigits', $v)
                        ->whereOrRaw("CONCAT(contact_extra, vdigits) = '{$v}'");
                })->find();
                if ($contactExist) {
                    $find =  db('crm_leads')->where('id', $contactExist['leads_id'])->find();
                    return [false, $contactExist['contact_value'] . '客户信息已存在,当前所属人' . $find['pr_user']];
                }
                unset($require_checke[$i]);
            }
        }

        //邮箱和其他
        if ($require_checke) {
            $contactExist = db('crm_contacts')->where($where)->where('is_delete', 0)->whereIn('contact_value', $require_checke)->find();
            if ($contactExist) {
                $find =  db('crm_leads')->where('id', $contactExist['leads_id'])->find();
                return [false, $contactExist['contact_value'] . '客户信息已存在,当前所属人' . $find['pr_user']];
            }
        }

        return [true, ''];
    }

    // 多主电话 + 辅号查重，仅在 crm_contacts.contact_value 上检查（忽略 contact_extra）
    private function checkDuplicateNew($data)
    {
        $mainPhones = $this->parsePhones(Request::param('more_phones'));
        $aux        = preg_replace('/\D/', '', (string)Request::param('phone2', ''));
        $phones     = $mainPhones;
        if ($aux !== '') $phones[] = $aux;

        if (empty($phones)) {
            return [true, ''];
        }

        $query = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->whereIn('contact_value', $phones);

        if (!empty($data['id'])) {
            // 编辑场景：排除自身
            $query->where('leads_id', '<>', $data['id']);
        }

        $contactExist = $query->find();
        if ($contactExist) {
            $find = Db::table('crm_leads')->where('id', $contactExist['leads_id'])->find();
            $owner = $find ? $find['pr_user'] : '';
            return [false, $contactExist['contact_value'] . '客户信息已存在，当前所属人' . $owner];
        }

        return [true, ''];
    }

    
    // 解析主电话：支持 数组 / JSON字符串 / 逗号或空白分隔；仅保留11位纯数字并去重
    private function parsePhones($raw): array
    {
        $phones = [];
        if (is_array($raw)) {
            $phones = $raw;
        } else if (is_string($raw)) {
            $str = trim($raw);
            if ($str !== '') {
                // JSON数组
                if ($str[0] === '[') {
                    $tmp = json_decode($str, true);
                    if (is_array($tmp)) {
                        $phones = $tmp;
                    } else {
                        $phones = [$str];
                    }
                } else {
                    // 常见分隔符归一化
                    $str = str_replace(["，", "、", "；", ";", "|", "\r\n", "\n", "\t"], ',', $str);
                    $phones = preg_split('/[\\s,]+/', $str);
                }
            }
        } else if ($raw !== null) {
            $phones = [(string)$raw];
        }

        // 仅保留数字、11位、去空去重
        $phones = array_map(function ($v) {
            return preg_replace('/\\D/', '', (string)$v);
        }, $phones);

        $phones = array_values(array_unique(array_filter($phones, function ($v) {
            return $v !== '' && preg_match('/^\\d{11}$/', $v);
        })));

        return $phones;
    }

    /**
     * 验证国际手机号格式
     * @param string $phone 原始手机号
     * @return bool 是否有效
     */
    static public function validatePhoneNumber(&$phone)
    {
        // 清理特殊字符
        $cleaned = preg_replace('/[^\w@._#]/', '', $phone);
        // $phone = substr($cleaned, -8);
        $phone = $cleaned;
        // 国际手机号正则: 可选+,首位非0,6-14位数字
        return preg_match('/^\+?[1-9]\d{6,14}$/', $cleaned) === 1;
    }


    //数据组装
    private function assemblyData($contact, $leads_id)
    {
        $contactData = [];
        foreach ($contact as $k => $v) {
            $contact_type = self::CONTACT_MAP[$k];
            if (is_string($v)) $v = explode(',', $v);
            foreach ($v as $e => $c_v) {

                $contact_value = $c_v;
                $contact_extra = '';
                if (is_array($c_v)) {
                    $contact_extra = $c_v[0];
                    $contact_value = $c_v[1];
                }
                $temp = [
                    'leads_id' => $leads_id,
                    'contact_type' => $contact_type,
                    'contact_value' => $contact_value,
                    'contact_extra' => $contact_extra,
                    'vdigits' =>  preg_replace('/[^0-9]/', '', $contact_value),
                    'is_delete' => 0,
                    'created_at' => date("Y-m-d H:i:s", time()),
                ];


                $find = Db::table('crm_contacts')->where(['is_delete' => 1, 'contact_value' => $contact_value])->find();
                if ($find) {
                    Db::table('crm_contacts')->where('id', $find['id'])->update($temp);
                } else {
                    $contactData[] = $temp;
                }
            }
        }
        return $contactData;
    }



    //新建客户
    public function add()
    {
        if (request()->isPost()) {
            $this->redisLock();

            // 1) 基础校验（主电话允许多个且至少一个；每个11位；辅号可选且11位；主/辅不能相同）
            $contact = [];
            list($res, $require_check) = $this->checkDataNew($contact);
            if (!$res) {
                $this->redisUnLock();
                return fail($require_check);
            }

            // 2) 客户名称查重（检查 crm_leads 表中是否已存在相同客户名称）
            $khName = Request::param('kh_name');
            $existingLead = Db::table('crm_leads')->where('kh_name', $khName)->find();
            if ($existingLead) {
                $this->redisUnLock();
                return fail('客户名称重复，请更换客户名称');
            }

            // 3) 组装 leads 数据
            $data['kh_name']      = Request::param('kh_name');
            $data['kh_contact']   = Request::param('kh_contact');
            $data['kh_status']    = Request::param('kh_status');
            $data['product_name'] = Request::param('product_name');
            
            // 3.1) 校验产品/供应商有效性（新增时必须选择未删除的产品）
            $productId = (int)$data['product_name'];
            if ($productId > 0) {
                $productInfo = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', $productId)
                    ->field('p.id, p.is_deleted as p_deleted, c.id as c_id, c.is_deleted as c_deleted')
                    ->find();
                if (!$productInfo) {
                    $this->redisUnLock();
                    return fail('所选产品不存在，请重新选择');
                }
                if ($productInfo['p_deleted'] != 0) {
                    $this->redisUnLock();
                    return fail('所选产品已删除，请重新选择');
                }
                if (!$productInfo['c_id'] || $productInfo['c_deleted'] != 0) {
                    $this->redisUnLock();
                    return fail('所选产品的供应商已删除，请重新选择');
                }
            }
            
            $data['oper_user']    = Request::param('oper_user');
            $data['remark']       = Request::param('remark', '');
            $data['inquiry_id'] = Request::param('inquiry_id'); // 单选渠道
            // 多选端口 ID 数组
            $portIds = Request::param('port_id/a');
            // 多选端口拼接成 1,2,3,4
            $data['port_id'] = $portIds ? implode(',', $portIds) : '';


            // 检查 source_port 字段是否存在，如果存在则添加
            try {
                $columns = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
                if (!empty($columns)) {
                    $data['source_port'] = Request::param('source_port', '');
                }
            } catch (\Exception $e) {
                // 忽略查询失败
            }

            // 4) 解析并写入协同人（joint_person），支持 数组 / JSON / 逗号分隔
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
            if (strlen($jpStr) > 30) {
                $this->redisUnLock();
                return fail('协同人过多，超出存储限制（请减少选择或扩大 joint_person 字段长度）');
            }
            $data['joint_person'] = $jpStr;

            // 5) 系统字段
            $data['at_user']     = Session::get('username');
            // 检查 at_user_id 字段是否存在，如果存在则添加
            try {
                $columns_at_user_id = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'at_user_id'");
                if (!empty($columns_at_user_id)) {
                    $data['at_user_id'] = (int)Session::get('aid');
                }
            } catch (\Exception $e) {
                // 忽略查询失败
            }
            $data['pr_user']     = Session::get('username');
            $data['pr_user_bef'] = Session::get('username');
            $data['ut_time']     = date("Y-m-d H:i:s", time());
            $data['at_time']     = date("Y-m-d H:i:s", time());
            $data['status']      = 1;
            $data['ispublic']    = 3;

            // 检查 pr_user_id 和 pr_user_bef_id 字段是否存在，如果存在则添加
            try {
                $columns_pr_user_id = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'pr_user_id'");
                $columns_pr_user_bef_id = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'pr_user_bef_id'");
                if (!empty($columns_pr_user_id)) {
                    $data['pr_user_id'] = (int)Session::get('aid');
                }
                if (!empty($columns_pr_user_bef_id)) {
                    $data['pr_user_bef_id'] = (int)Session::get('aid');
                }
            } catch (\Exception $e) {
                // 忽略查询失败
            }

            // 6) 查重（按 crm_contacts.contact_value 直接查）
            list($res, $msg) = $this->checkDuplicateNew($data);
            if (!$res) {
                $this->redisUnLock();
                return fail($msg);
            }

            // 7) 解析主/辅电话（主电话支持多个；全部保留纯数字）
            $mainPhones = $this->parsePhones(Request::param('more_phones'));   // 支持多个主电话输入
            $auxPhone   = preg_replace('/\D/', '', (string)Request::param('phone2', ''));

            Db::startTrans();
            try {
                // a) 新增主表
                Db::table('crm_leads')->insert($data);
                $id = Db::getLastInsID();
                if (!$id) {
                    throw new \Exception('客户信息插入失败');
                }

                // b) 新增联系方式（主电话=1，可多条；辅助=3，单条）
                $now = date("Y-m-d H:i:s", time());
                $contactsToInsert = [];
                // 循环处理所有主电话，每个作为一条联系记录
                foreach ($mainPhones as $mp) {
                    $contactsToInsert[] = [
                        'leads_id'      => $id,
                        'contact_type'  => 1,                 // 主电话 = 1
                        'contact_extra' => '',
                        'contact_value' => $mp,
                        'vdigits'       => $mp,
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                }
                // 如果存在辅助电话，则一并加入待插入列表
                if ($auxPhone !== '') {
                    $contactsToInsert[] = [
                        'leads_id'      => $id,
                        'contact_type'  => 3,                 // 辅助电话 = 3
                        'contact_extra' => '',
                        'contact_value' => $auxPhone,
                        'vdigits'       => $auxPhone,
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                }
                // 批量插入所有联系方式
                if (!empty($contactsToInsert)) {
                    Db::table('crm_contacts')->insertAll($contactsToInsert);
                }

                // c) 操作日志
                $this->addOperLog(
                    $id,
                    '新增客户',
                    [
                        '运营人员' => $data['oper_user'],
                        '联系方式' => ['主电话' => $mainPhones, '辅助电话' => $auxPhone],
                        '协同人'  => $jpIds
                    ]
                );

                Db::commit();
                $this->redisUnLock();
                return success();
            } catch (\Exception $e) {
                Db::rollback();
                $this->redisUnLock();
                return fail($e->getMessage());
            }
        }

        // GET：渲染新增页面所需下拉数据（使用新的软删除字段 is_deleted）
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $productRows = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if ($currentAdmin['org'] && strpos($currentAdmin['org'], 'admin') === false) {
            $productRows->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        // 添加新的软删除过滤条件：只显示未删除的产品和供应商
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

        // 检查shop_names字段是否存在
        try {
            $columns = Db::query("SHOW COLUMNS FROM `crm_client_status` LIKE 'shop_names'");
            $has_shop_names = !empty($columns);
        } catch (\Exception $e) {
            $has_shop_names = false;
        }

        // 根据字段是否存在决定查询字段
        if ($has_shop_names) {
            $khStatusList = Db::table('crm_client_status')->field('id,status_name,shop_names')->select();
        } else {
            $khStatusList = Db::table('crm_client_status')->select();
        }
        $this->assign('khStatusList', $khStatusList);
        //print_r($khStatusList);

        $yyData = $this->getYyList();
        $operUserList = $yyData['_yyList'];
        $this->assign('operUserList', $operUserList);
        $this->assign('yyList', json_encode($yyData['yyList'], JSON_UNESCAPED_UNICODE));

        // 获取店铺列表，按来源名称分组（保持不变）
        $shopList = [];
        foreach ($khStatusList as $status) {
            $statusName = $status['status_name'];
            $shops = [];

            if ($has_shop_names && isset($status['shop_names']) && !empty(trim($status['shop_names']))) {
                $shop_names = array_filter(array_map('trim', explode(',', $status['shop_names'])));
                foreach ($shop_names as $shop_name) {
                    if (!empty($shop_name)) {
                        $shops[] = [
                            'id' => md5($status['id'] . '_' . $shop_name),
                            'name' => $shop_name
                        ];
                    }
                }
            }

            if (empty($shops)) {
                $commonShops = $this->getShopsByChannel('', $statusName);
                foreach ($commonShops as $shop) {
                    $shops[] = [
                        'id' => $shop['id'],
                        'name' => $shop['name']
                    ];
                }
            }

            if (!empty($shops)) {
                $shopList[$statusName] = $shops;
            }
        }
        $this->assign('shopList', json_encode($shopList, JSON_UNESCAPED_UNICODE));

        //print_r($shopList);

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

        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        $portList    = Db::table('crm_inquiry_port')->where('status', 0)->select();

        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);

        return $this->fetch('client/add');
    }  



    //编辑客户
    public function edit()
    {
        // 保存
        if (request()->isPost()) {
            $this->redisLock();

            $id = (int)\think\facade\Request::param('id', 0);
            if (!$id) {
                $this->redisUnLock();
                return fail('参数错误：缺少ID');
            }

            $kh_name = Request::param('kh_name');

            // 检查除当前记录外是否存在相同客户名称
            $exists = Db::table('crm_leads')
                        ->where('kh_name', $kh_name)
                        ->where('id', '<>', $id)
                        ->find();
            if ($exists) {
                // 若客户名称重复，返回错误信息并中断保存
                return json(['code' => -200, 'msg' => '客户名称重复，请更换客户名称', 'data' => []]);
            }


            // 1) 基础校验（与新增保持一致：主电话必填、11位；辅号可选且11位；两者不能相同）
            $contact = [];
            list($res, $require_check) = $this->checkDataNew($contact);
            if (!$res) {
                $this->redisUnLock();
                return fail($require_check);
            }

            // 2) 获取旧数据的产品ID，用于判断是否改了产品
            $oldRow = Db::table('crm_leads')->where('id', $id)->field('product_name')->find();
            $oldPid = $oldRow ? (int)$oldRow['product_name'] : 0;
            
            // 2) 组装 leads 数据
            $data = [];
            $data['id']           = $id;
            $data['kh_name']      = \think\facade\Request::param('kh_name');
            $data['kh_contact']   = \think\facade\Request::param('kh_contact', '');
            $data['kh_status']    = \think\facade\Request::param('kh_status');      // 询盘来源ID
            $data['product_name'] = \think\facade\Request::param('product_name');   // 产品ID
            $newPid = (int)$data['product_name'];
            
            // 2.1) 校验产品/供应商有效性
            // 如果用户没有改产品ID，允许保存原产品（即使它已软删，用于历史数据保留）
            // 如果用户改了产品ID，则必须选择未软删的产品/供应商
            if ($newPid > 0 && $newPid != $oldPid) {
                // 用户改了产品ID，必须校验新产品/供应商均未删除
                $productInfo = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', $newPid)
                    ->field('p.id, p.is_deleted as p_deleted, c.id as c_id, c.is_deleted as c_deleted')
                    ->find();
                if (!$productInfo) {
                    $this->redisUnLock();
                    return fail('所选产品不存在，请重新选择');
                }
                if ($productInfo['p_deleted'] != 0) {
                    $this->redisUnLock();
                    return fail('所选产品已删除，请重新选择');
                }
                if (!$productInfo['c_id'] || $productInfo['c_deleted'] != 0) {
                    $this->redisUnLock();
                    return fail('所选产品的供应商已删除，请重新选择');
                }
            }
            // 如果 $newPid == $oldPid，允许保存（即使该产品现在 is_deleted=1，用于保留历史）
            
            $data['oper_user']    = \think\facade\Request::param('oper_user');      // 运营人员ID（与你的 add 保持一致）
            $data['remark']       = \think\facade\Request::param('remark', '');
            $data['ut_time']      = date("Y-m-d H:i:s");
            $data['inquiry_id'] = Request::param('inquiry_id'); // 单选渠道
            // 多选端口 ID 数组
            $portIds = Request::param('port_id/a');
            // 多选端口拼接成 1,2,3,4
            $data['port_id'] = $portIds ? implode(',', $portIds) : '';

            
            // 检查 source_port 字段是否存在，如果存在则添加
            try {
                $columns = \think\Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
                if (!empty($columns)) {
                    $data['source_port'] = \think\facade\Request::param('source_port', '');  // 来源端口
                }
            } catch (\Exception $e) {
                // 如果查询失败，忽略该字段
            }

            // 3) 解析并写入协同人（joint_person），支持 数组 / JSON / 逗号分隔
            $jpRaw = \think\facade\Request::param('joint_person');
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
            if (strlen($jpStr) > 30) { // 若 joint_person 仍为 varchar(30)
                $this->redisUnLock();
                return fail('协同人过多，超出存储限制（请减少选择或扩大 joint_person 字段长度）');
            }
            $data['joint_person'] = $jpStr;

            // 4) 查重（按 contact_value 直接查；会自动排除自身 leads_id）
            list($res, $msg) = $this->checkDuplicateNew($data);
            if (!$res) {
                $this->redisUnLock();
                return fail($msg);
            }

            // 5) 获取主/辅电话（保留纯数字）
            // [替换] 统一解析前端 more_phones[] 主电话输入，支持数组或 JSON 字符串
            $mainPhones = $this->parsePhones(Request::param('more_phones'));   // 主电话数组（支持多个）
            $auxPhone  = preg_replace('/\D/', '', (string)\think\facade\Request::param('phone2', ''));


            // ** 新增：检查主电话和辅助电话在其他客户中是否存在重复（全局唯一） **
            // [替换] 构建去重检查列表（展开多个主电话）
            $phonesToCheck = $mainPhones;
            if ($auxPhone !== '') {
                $phonesToCheck[] = $auxPhone;
            }
            $phonesToCheck = array_values(array_unique(array_filter($phonesToCheck, function ($v) {
                return $v !== '';
            })));
            if (!empty($phonesToCheck)) {
                $duplicateContact = \think\Db::table('crm_contacts')
                    ->where('is_delete', 0)
                    ->where('leads_id', '<>', $id)
                    ->whereIn('contact_value', $phonesToCheck)
                    ->find();
                if ($duplicateContact) {
                    $this->redisUnLock();
                    return json([
                        'code' => -200,
                        'msg'  => '电话号码 ' . $duplicateContact['contact_value'] . ' 已存在于其他客户信息中，请更换号码',
                        'data' => []
                    ]);
                }
            }
            // ** 新增逻辑结束 **

            try {
                \think\Db::startTrans();

                // 更新主表
                \think\Db::table('crm_leads')->where('id', $id)->update($data);

                
                // 读取当前有效（is_delete=0）的主/辅号码
                // [替换] 获取当前所有主电话和辅电话
                $existingMainPhones = \think\Db::table('crm_contacts')
                    ->where('leads_id', $id)
                    ->where('contact_type', 1)
                    ->where('is_delete', 0)
                    ->column('contact_value');
                $oldAux = \think\Db::table('crm_contacts')
                    ->where('leads_id', $id)
                    ->where('contact_type', 3)
                    ->where('is_delete', 0)
                    ->value('contact_value');
                $oldAux = $oldAux ?? '';  // 若没有辅号则设为空字符串

                $now = date("Y-m-d H:i:s");
                $contactsToInsert = [];

                // 主电话更新逻辑
                // [替换] 计算需要“硬删除”的主电话和需要新增的主电话
                $toRemove = array_diff($existingMainPhones, $mainPhones);  // 被用户删除的旧主号码
                $toAdd    = array_diff($mainPhones, $existingMainPhones);  // 新增的主号码
                // [新增] 硬删除被移除的主电话（从数据库物理删除）
                if (!empty($toRemove)) {
                    \think\Db::table('crm_contacts')
                        ->where('leads_id', $id)
                        ->where('contact_type', 1)
                        ->whereIn('contact_value', $toRemove)
                        ->delete();
                }
                // [新增] 插入新增的主电话，每个号码一条记录
                foreach ($toAdd as $newPhone) {
                    // 如果存在已软删的相同号码，则复用更新，否则插入新记录
                    $findDeleted = \think\Db::table('crm_contacts')
                        ->where(['is_delete' => 1, 'contact_value' => $newPhone])
                        ->find();
                    if ($findDeleted) {
                        \think\Db::table('crm_contacts')->where('id', $findDeleted['id'])->update([
                            'leads_id'      => $id,
                            'contact_type'  => 1,
                            'contact_extra' => '',
                            'contact_value' => $newPhone,
                            'vdigits'       => $newPhone,
                            'is_delete'     => 0,
                            'created_at'    => $now,
                        ]);
                    } else {
                        $contactsToInsert[] = [
                            'leads_id'      => $id,
                            'contact_type'  => 1,               // 主电话类型
                            'contact_extra' => '',
                            'contact_value' => $newPhone,
                            'vdigits'       => $newPhone,
                            'is_delete'     => 0,
                            'created_at'    => $now,
                        ];
                    }
                }


                // 辅助电话更新逻辑
                // [替换] 处理辅助电话的删除或更新
                if ($auxPhone === '' && $oldAux !== '') {
                    // [新增] 用户删除了原有辅号，直接删除数据库中的辅号记录
                    \think\Db::table('crm_contacts')
                        ->where('leads_id', $id)
                        ->where('contact_type', 3)
                        ->where('is_delete', 0)
                        ->delete();
                } else if ($auxPhone !== '' && $auxPhone !== $oldAux) {
                    // 用户提供了新的辅号（包括原本无辅号的情况）
                    if ($oldAux !== '') {
                        // [新增] 删除旧辅号记录
                        \think\Db::table('crm_contacts')
                            ->where('leads_id', $id)
                            ->where('contact_type', 3)
                            ->where('is_delete', 0)
                            ->delete();
                    }
                    // [新增] 插入新辅号记录（或复用软删记录）
                    $findDeletedAux = \think\Db::table('crm_contacts')
                        ->where(['is_delete' => 1, 'contact_value' => $auxPhone])
                        ->find();
                    if ($findDeletedAux) {
                        \think\Db::table('crm_contacts')->where('id', $findDeletedAux['id'])->update([
                            'leads_id'      => $id,
                            'contact_type'  => 3,
                            'contact_extra' => '',
                            'contact_value' => $auxPhone,
                            'vdigits'       => $auxPhone,
                            'is_delete'     => 0,
                            'created_at'    => $now,
                        ]);
                    } else {
                        $contactsToInsert[] = [
                            'leads_id'      => $id,
                            'contact_type'  => 3,               // 辅助电话类型
                            'contact_extra' => '',
                            'contact_value' => $auxPhone,
                            'vdigits'       => $auxPhone,
                            'is_delete'     => 0,
                            'created_at'    => $now,
                        ];
                    }
                }

                // 批量插入新联系方式记录
                if (!empty($contactsToInsert)) {
                    \think\Db::table('crm_contacts')->insertAll($contactsToInsert);
                }


                // 日志
                self::addOperLog(
                    $id,
                    '编辑客户',
                    [
                        '运营人员' => $data['oper_user'],
                        '联系方式' => ['主电话' => $mainPhones, '辅助电话' => $auxPhone],
                        '协同人'  => $jpIds
                    ]
                );

                \think\Db::commit();
                $this->redisUnLock();
                return success();
            } catch (\Exception $e) {
                \think\Db::rollback();
                $this->redisUnLock();
                return fail($e->getMessage());
            }
        }

        // 编辑页展示
        $id = (int)\think\facade\Request::param('id', 0);
        if (!$id) return $this->fetch('client/edit'); // 防御

        $result = \think\Db::table('crm_leads')->where(['id' => $id])->find();

        // 主/辅电话：1 主、3 辅
        // [替换] 初始化主电话数组，支持多个主号
        $mainPhones = [];
        $auxPhone   = '';
        $contacts = \think\Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', $id)
            ->whereIn('contact_type', [1, 3])
            ->order('id', 'asc')
            ->field('contact_type, contact_value')
            ->select();
        foreach ($contacts as $c) {
            if ($c['contact_type'] == 1) {
                // [替换] 收集所有主电话
                $mainPhones[] = $c['contact_value'];
            }
            if ($c['contact_type'] == 3 && $auxPhone === '') {
                $auxPhone = $c['contact_value'];
            }
        }

        // 产品列表（使用新的软删除字段 is_deleted，并处理已删除产品的回显）
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $productRows = \think\Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            $productRows->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        // 添加新的软删除过滤条件：只显示未删除的产品和供应商
        $productRows->where([
            'p.is_deleted' => 0,
            'c.is_deleted' => 0,
        ]);
        $productRows = $productRows
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        
        // 处理已删除产品的回显：如果当前客户绑定的产品已删除，需要追加到列表中
        $currentPid = (int)$result['product_name'];
        if ($currentPid > 0) {
            // 检查当前产品ID是否已在列表中
            $existsInList = false;
            foreach ($productRows as $row) {
                if ($row['id'] == $currentPid) {
                    $existsInList = true;
                    break;
                }
            }
            
            // 如果不在列表中，说明该产品或供应商已被删除，需要单独查询并追加
            if (!$existsInList) {
                $deletedProduct = \think\Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', $currentPid)
                    ->field('p.id, p.product_name, c.category_name, p.is_deleted as p_deleted, c.is_deleted as c_deleted')
                    ->find();
                
                if ($deletedProduct) {
                    // 标记为已删除，并在文案中添加标识
                    $deletedProduct['category_name'] = ($deletedProduct['category_name'] ?: '无') . '【已删除】';
                    // 将已删除的产品插入到列表开头，确保能回显
                    array_unshift($productRows, $deletedProduct);
                }
            }
        }
        
        $this->assign('productList', $productRows);

        // 检查shop_names字段是否存在
        try {
            $columns = \think\Db::query("SHOW COLUMNS FROM `crm_client_status` LIKE 'shop_names'");
            $has_shop_names = !empty($columns);
        } catch (\Exception $e) {
            $has_shop_names = false;
        }
        
        // 询盘来源
        if ($has_shop_names) {
            $khStatusList = \think\Db::table('crm_client_status')->field('id,status_name,shop_names')->select();
        } else {
            $khStatusList = \think\Db::table('crm_client_status')->select();
        }
        $this->assign('khStatusList', $khStatusList);

        // 运营人员下拉 + 分组（与 add 一致）
        $yyData = $this->getYyList();
        $operUserList = $yyData['_yyList'];   // [{id,name}]
        $this->assign('operUserList', $operUserList);
        $this->assign('yyList', json_encode($yyData['yyList'], JSON_UNESCAPED_UNICODE));

        // 获取店铺列表，按来源名称分组（与 add 一致）
        $shopList = [];
        foreach ($khStatusList as $status) {
            $statusName = $status['status_name'];
            $shops = [];
            
            // 检查是否有shop_names字段
            if ($has_shop_names && isset($status['shop_names']) && !empty(trim($status['shop_names']))) {
                $shop_names = array_filter(array_map('trim', explode(',', $status['shop_names'])));
                foreach ($shop_names as $index => $shop_name) {
                    if (!empty($shop_name)) {
                        $shops[] = [
                            'id' => md5($status['id'] . '_' . $shop_name),
                            'name' => $shop_name
                        ];
                    }
                }
            }
            
            // 如果shop_names为空，尝试从crm_operation_shops表获取
            if (empty($shops)) {
                $commonShops = $this->getShopsByChannel('', $statusName);
                foreach ($commonShops as $shop) {
                    $shops[] = [
                        'id' => $shop['id'],
                        'name' => $shop['name']
                    ];
                }
            }
            
            if (!empty($shops)) {
                $shopList[$statusName] = $shops;
            }
        }
        $this->assign('shopList', json_encode($shopList, JSON_UNESCAPED_UNICODE));

        // 协同人选项
        $teamName = session('team_name') ?: '';
        $adminList = \think\Db::name('admin')
            ->where('group_id', '<>', 1)  // 非超管
            ->whereIn('group_id', [10, 11, 14])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        foreach ($adminList as $admin) {
            $collaboratorData[] = ['name' => $admin['username'], 'value' => $admin['admin_id']];
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        // 协同人已选
        $jointPersonInit = [];
        if (!empty($result['joint_person'])) {
            $tmp = $result['joint_person'];
            if (preg_match('/^\s*\[.*\]\s*$/', $tmp)) {
                $arr = json_decode($tmp, true);
                if (is_array($arr)) $jointPersonInit = $arr;
            } else {
                $jointPersonInit = array_values(array_filter(explode(',', $tmp)));
            }
        }
        $this->assign('jointPersonInit', json_encode($jointPersonInit, JSON_UNESCAPED_UNICODE));

        // 页面回填
        $this->assign('result', $result);
        $this->assign('mainPhones', json_encode($mainPhones, JSON_UNESCAPED_UNICODE));
        $this->assign('auxPhone',  $auxPhone);

        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        $portList    = Db::table('crm_inquiry_port')->where('status', 0)->select();

        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);

        // GET 加载老数据
        $result = Db::table('crm_leads')->where('id', $id)->find();
        $this->assign('result', $result);

        // 端口多选回显
        $selectedPorts = $result ? explode(',', $result['port_id']) : [];
        $this->assign('selectedPorts', $selectedPorts);
        
        return $this->fetch('client/edit');
    }


    //单个删除客户
    public function del()
    {
        $id = Request::post('id');

        if (!$id) {
            return json(['code' => 500, 'msg' => '请选择要删除的客户']);
        }

        $username = Session::get('username');
        $aid = Session::get('aid'); // 获取管理员ID
        
        Db::startTrans();
        try {
            // 查询客户信息
            $clientQuery = Db::name('crm_leads')->where('id', $id);
            
            // 如果不是超级管理员，需要验证权限
            if ($aid != 1) {
                $clientQuery->where(function ($query) use ($username) {
                    $query->where('pr_user', $username)
                        ->whereOr('pr_user_bef', $username);
                });
            }
            
            $client = $clientQuery->find();

            if (!$client) {
                throw new \Exception('客户不存在或无权限删除');
            }

            // 删除客户的联系方式
            Db::name('crm_contacts')->where('leads_id', $id)->delete();
            
            // 删除客户主记录
            Db::name('crm_leads')->where('id', $id)->delete();

            Db::commit();
            
            //写入操作日志
            $this->addOperLog(
                $id,
                '删除客户',
                "$username 删除客户ID:" . $id . ', 客户名称:' . ($client['kh_name'] ?? '') . ($aid == 1 ? ' [超级管理员操作]' : '')
            );
            
            return json(['code' => 0, 'msg' => '删除成功']);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => $e->getMessage()]);
        }
    }

    //批量删除客户
    public function delBatch()
    {
        $ids = Request::post('ids');

        if (!$ids || !is_array($ids)) {
            return json(['code' => 500, 'msg' => '请选择要删除的客户']);
        }

        $username = Session::get('username');
        $aid = Session::get('aid'); // 获取管理员ID
        
        Db::startTrans();
        try {
            // 查询客户信息
            $clientQuery = model('client')->with('contacts')->where('id', 'in', $ids);
            
            // 如果不是超级管理员，需要验证权限
            if ($aid != 1) {
                $clientQuery->where(function ($query) use ($username) {
                    $query->where('pr_user', $username)
                        ->whereOr('pr_user_bef', $username);
                });
            }
            
            $clients = $clientQuery->select();
            
            if ($clients->isEmpty()) {
                throw new \Exception('无权限删除选中客户或客户不存在');
            }
            
            // 删除主表记录和关联数据
            foreach ($ids as $id) {
                Db::name('crm_contacts')->where('leads_id', $id)->delete();
            }

            Db::name('crm_leads')->where('id', 'in', $ids)->delete();

            Db::commit();
            
            //写入操作日志
            $this->addOperLog(
                null,
                '批量删除客户',
                "$username 批量删除客户:" . implode(',', $ids) . ', 共' . count($ids) . '条' . ($aid == 1 ? ' [超级管理员操作]' : '')
            );
            
            return json(['code' => 0, 'msg' => '批量删除成功，共删除' . count($ids) . '条记录']);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => $e->getMessage()]);
        }
    }

    //单个删除成交客户
    public function delSuccessClient()
    {
        $id = Request::post('id');

        if (!$id) {
            return json(['code' => 500, 'msg' => '请选择要删除的客户']);
        }

        $username = Session::get('username');
        $aid = Session::get('aid');
        
        Db::startTrans();
        try {
            // 查询客户信息（必须是成交客户）
            $clientQuery = Db::name('crm_leads')->where('id', $id)->where('issuccess', 1);
            
            // 如果不是超级管理员，需要验证权限
            if ($aid != 1) {
                $clientQuery->where(function ($query) use ($username) {
                    $query->where('pr_user', $username)
                        ->whereOr('pr_user_bef', $username);
                });
            }
            
            $client = $clientQuery->find();

            if (!$client) {
                throw new \Exception('成交客户不存在或无权限删除');
            }

            // 删除客户的联系方式
            Db::name('crm_contacts')->where('leads_id', $id)->delete();
            
            // 删除客户主记录
            Db::name('crm_leads')->where('id', $id)->delete();

            Db::commit();
            
            //写入操作日志
            $this->addOperLog(
                $id,
                '删除成交客户',
                "$username 删除成交客户ID:" . $id . ', 客户名称:' . ($client['kh_name'] ?? '') . ($aid == 1 ? ' [超级管理员操作]' : '')
            );
            
            return json(['code' => 0, 'msg' => '删除成功']);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => $e->getMessage()]);
        }
    }

    //批量删除成交客户
    public function delSuccessClientBatch()
    {
        $ids = Request::post('ids');

        if (!$ids || !is_array($ids)) {
            return json(['code' => 500, 'msg' => '请选择要删除的客户']);
        }

        $username = Session::get('username');
        $aid = Session::get('aid');
        
        Db::startTrans();
        try {
            // 查询客户信息（必须是成交客户）
            $clientQuery = model('client')->with('contacts')->where('id', 'in', $ids)->where('issuccess', 1);
            
            // 如果不是超级管理员，需要验证权限
            if ($aid != 1) {
                $clientQuery->where(function ($query) use ($username) {
                    $query->where('pr_user', $username)
                        ->whereOr('pr_user_bef', $username);
                });
            }
            
            $clients = $clientQuery->select();
            
            if ($clients->isEmpty()) {
                throw new \Exception('无权限删除选中的成交客户或客户不存在');
            }
            
            // 删除主表记录和关联数据
            foreach ($ids as $id) {
                Db::name('crm_contacts')->where('leads_id', $id)->delete();
            }

            Db::name('crm_leads')->where('id', 'in', $ids)->delete();

            Db::commit();
            
            //写入操作日志
            $this->addOperLog(
                null,
                '批量删除成交客户',
                "$username 批量删除成交客户:" . implode(',', $ids) . ', 共' . count($ids) . '条' . ($aid == 1 ? ' [超级管理员操作]' : '')
            );
            
            return json(['code' => 0, 'msg' => '批量删除成功，共删除' . count($ids) . '条记录']);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => $e->getMessage()]);
        }
    }

    //客户级别
    public function rankList()
    {
        if (request()->isPost()) {
            $page = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');
            $list = db('crm_client_rank')
                ->paginate(array('list_rows' => $pageSize, 'page' => $page))
                ->toArray();
            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        return $this->fetch();
    }

    //添加客户级别
    public function rankAdd()
    {
        if (request()->isPost()) {
            $data['rank_name'] = Request::param('rank_name');
            $data['add_time'] = time();
            $result = Db::table('crm_client_rank')->insert($data);
            if ($result) {
                $msg = ['code' => 0, 'msg' => '添加成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '添加失败！', 'data' => []];
                return json($msg);
            }
        }
        return $this->fetch('client/rank_list_add');
    }
    //编辑客户级别
    public function rankEdit()
    {
        if (Request::isAjax()) {
            $data  = Request::param();
            // 获取原状态
            $oldstatus = Db::table('crm_client_rank')->where(['id' => $data['id']])->find();
            $oldstatusname = $oldstatus['rank_name'];
            $ischange = false;
            if ($oldstatusname == $data['rank_name']) {
                $msg = ['code' => 500, 'msg' => '没有变化无需修改', 'data' => []];
                return json($msg);
            } else {
                $ischange = true;
            }

            $result = Db::table('crm_client_rank')->where(['id' => $data['id']])->update($data);
            if ($result) {
                // 状态修改后 客户编辑的原来状态都必须修改
                if ($ischange) {
                    // 所有的客户状态全部膝盖
                    $result2 = Db::table('crm_leads')->where(['kh_rank' => $oldstatusname])->update(['kh_rank' => $data['rank_name']]);
                }
                $msg = ['code' => 0, 'msg' => '编辑成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '编辑失败！', 'data' => []];
                return json($msg);
            }
        }

        $result = Db::table('crm_client_rank')->where(['id' => Request::param('id')])->find();
        $this->assign('result', $result);
        return $this->fetch('client/rank_list_edit');
    }
    //删除客户级别
    public function rankDel()
    {
        $id = Request::param('id');
        // 获取原状态
        $oldstatus = Db::table('crm_client_rank')->where(['id' => $data['id']])->find();
        $oldstatusname = $oldstatus['rank_name'];

        $result = Db::table('crm_client_rank')->where('id', $id)->delete();
        if ($result) {
            // 所有的客户状态全部膝盖
            $result2 = Db::table('crm_leads')->where(['kh_rank' => $oldstatusname])->update(['kh_rank' => '']);
            $msg = ['code' => 0, 'msg' => '删除成功！', 'data' => []];
            return json($msg);
        } else {
            $msg = ['code' => 500, 'msg' => '删除失败！', 'data' => []];
            return json($msg);
        }
    }


    //客户状态
    public function statusList()
    {
        if (request()->isPost()) {
            $page = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');
            $list = db('crm_client_status')
                ->where('is_active', '=', 1)
                ->order('id desc')
                ->paginate(array('list_rows' => $pageSize, 'page' => $page))
                ->toArray();
            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        return $this->fetch();
    }
    //添加客户状态
    public function statusAdd()
    {
        if (request()->isPost()) {
            $current_admin = Admin::getMyInfo();
            $data['status_name'] = Request::param('status_name');
            $data['submit_person'] = $current_admin['username'];
            $data['is_active'] = 1;
            $data['add_time'] = time();
            $data['edit_time'] = time();
            $data['delete_time'] = null;
            $result = Db::table('crm_client_status')->insert($data);
            if ($result) {
                cache('sourceList', null);
                $msg = ['code' => 0, 'msg' => '添加成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '添加失败！', 'data' => []];
                return json($msg);
            }
        }
        return $this->fetch('client/status_list_add');
    }
    //编辑客户状态
    public function statusEdit()
    {
        if (Request::isAjax()) {
            $data  = Request::param();
            // 获取原状态
            $oldstatus = Db::table('crm_client_status')->where(['id' => $data['id']])->find();
            $oldstatusname = $oldstatus['status_name'];
            $newstatusname = $data['status_name'];
            $ischange = false;
            if ($oldstatusname == $newstatusname) {
                $msg = ['code' => 500, 'msg' => '状态没有变化无需修改', 'data' => []];
                return json($msg);
            } else {
                $ischange = true;
            }
            $result = Db::table('crm_client_status')->where(['id' => $data['id']])->update($data);
            if ($result) {
                // 状态修改后 客户编辑的原来状态都必须修改
                if ($ischange) {
                    // 所有的客户状态全部膝盖
                    $result2 = Db::table('crm_leads')->where(['kh_status' => $oldstatusname])->update(['kh_status' => $newstatusname]);
                }
                cache('sourceList', null);
                $msg = ['code' => 0, 'msg' => '编辑成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '编辑失败！', 'data' => []];
                return json($msg);
            }
        }


        $result = Db::table('crm_client_status')->where(['id' => Request::param('id')])->find();
        $this->assign('result', $result);
        return $this->fetch('client/status_list_edit');
    }
    //删除客户状态
    public function statusDel()
    {
        $id = Request::param('id');
        // 获取原状态
        $oldstatus = Db::table('crm_client_status')->where(['id' => $data['id']])->find();
        $oldstatusname = $oldstatus['status_name'];
        // $ischange = false;
        // if ($oldstatusname == $data['status_name']) {
        //     $msg = ['code' => 500,'msg'=>'状态没有变化无需修改','data'=>[]];
        //     return json($msg);
        // }else{
        //     $ischange = true;
        // }
        $result = Db::table('crm_client_status')->where('id', $id)->delete();
        if ($result) {
            // 所有的客户状态全部膝盖
            $result2 = Db::table('crm_leads')->where(['kh_status' => $oldstatusname])->update(['kh_status' => '']);
            cache('sourceList', null);
            $msg = ['code' => 0, 'msg' => '删除成功！', 'data' => []];
            return json($msg);
        } else {
            $msg = ['code' => 500, 'msg' => '删除失败！', 'data' => []];
            return json($msg);
        }
    }


    //移入公海
    public function toMoveGh()
    {
        //1，获取提交的线索ID 【1,2,3,4,】
        $ids = Request::param('ids');
        $this->assign('ids', $ids);
        if (Request::isAjax()) {
            $pr_gh_type = Request::param('pr_gh_type');
            $idsArr = explode(",", $ids);


            $count = 0;
            foreach ($idsArr as $key => $value) {
                // $data['pr_user_bef'] = Db::table('crm_leads')->where(['id'=>$value])->field('pr_user')->find();
                // $data['pr_user'] = $username;
                // $data['id'] = $value;
                // $insertAll = Db::name('crm_leads')->update($data);
                $data['pr_gh_type'] = $pr_gh_type;
                $data['to_gh_time'] = date("Y-m-d H:i:s", time());
                $data['status'] = 2; //0-线索，1客户，2公海
                $data['id']  = $value;
                $result = Db::table('crm_leads')->where(['id' => $value])->update($data);
                if ($result) {
                    $count++;
                }
                // 添加日志记录
                $this->addOperLog(
                    $value,
                    '移入公海',
                    "移入 [{$pr_gh_type}] 公海池"
                );
            }
            if ($count > 0) {
                $msg = ['code' => 0, 'msg' => $count . '个客户移入公海成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '转入公海失败！', 'data' => []];
                return json($msg);
            }
            // $data['pr_gh_type'] = Request::param('pr_gh_type');
            // $data['to_gh_time'] = date("Y-m-d H:i:s",time());
            // $data['status'] = 2;//0-线索，1客户，2公海
            // $data['id']  = Request::param('id');
            // $result = Db::table('crm_leads')->where(['id'=>$data['id']])->update($data);
            // if ($result){
            //     $msg = ['code' => 0,'msg'=>'移入公海成功！','data'=>[]];
            //     return json($msg);
            // }else{
            //     $msg = ['code' => 500,'msg'=>'抱歉，移入公海失败！','data'=>[]];
            //     return json($msg);
            // }
        }


        $libTypeList = Db::table('crm_liberum_type')->select();

        $this->assign('libTypeList', $libTypeList);

        // $result = Db::table('crm_leads') ->where(['id' => Request::param('id')])->find();
        // $this -> assign('result',$result);
        return $this->fetch('client/move_gh');
    }
    //客户搜索
    public function clientSearch()
    {
        $page = input('page') ? input('page') : 1;
        $limit = input('limit') ? input('limit') : config('pageSize');
        $keyword = Request::param('keyword');
        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }
        $list = model('client')->getClientSearchList($page, $limit, $keyword);
        return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }

    public function personClientSearch()
    {
        // TP5.1 建议使用 input() 获取参数
        $page    = input('page/d', 1);
        $limit   = input('limit/d', config('pageSize'));
        $keyword = input('keyword/a', []); // 强制为数组

        // 处理时间范围筛选（与原有 buildTimeWhere 兼容）
        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }

        // 【新增-跟进筛选-参数归一】最新跟进时间筛选参数处理
        $followFilter = isset($keyword['follow_filter']) ? trim($keyword['follow_filter']) : '';
        $followDays   = isset($keyword['follow_days']) ? intval($keyword['follow_days']) : 0;

        // 只有当 follow_filter 非空 且 follow_days > 0 时才计算边界并写入 keyword
        if (!empty($followFilter) && $followDays > 0) {
            $boundaryTime = date('Y-m-d H:i:s', time() - $followDays * 86400);
            $keyword['__follow_filter']   = $followFilter;
            $keyword['__follow_boundary'] = $boundaryTime;
            // 移除原始字段避免其他旧逻辑误判
            unset($keyword['follow_filter'], $keyword['follow_days']);
        }

        // 取列表（保留你原来的模型查询逻辑）
        $list = model('client')->getPersonClientSearchList($page, $limit, $keyword);

        if (empty($list) || empty($list['data'])) {
            return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
        }

        // ===== 补充展示所需的派生字段 =====
        $rows    = &$list['data'];
        $leadIds = array_column($rows, 'id');

        // 1) 客户来源(ID->名称) 映射：若表不存在则优雅降级为原值
        $statusMap = [];
        try {
            $hasStatusTable = Db::query("SHOW TABLES LIKE 'crm_client_status'");
            if (!empty($hasStatusTable)) {
                $statusMap = Db::table('crm_client_status')->column('status_name', 'id');
            }
        } catch (\Exception $e) {
            $statusMap = [];
        }
        
        // 2) 产品名称映射表（product_name ID -> product_name 文字）
        $productMap = Db::table('crm_products')->column('product_name', 'id');
        
        // 3) 所属渠道和运营端口名称映射表
        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');

        // 4) 批量查询主/辅电话（crm_contacts：1=主，3=辅；按 leads_id 汇总）
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

        // 5) 协同人姓名：从 admin 表按 joint_person 映射（若表/ID不存在则回退为原ID）
        $uidSet = [];
        foreach ($rows as &$row) {
            // 客户来源中文名（若映射不到则用原值）
            $row['kh_status_name'] = isset($statusMap[$row['kh_status']]) ? $statusMap[$row['kh_status']] : (string)$row['kh_status'];
            
            // 所属渠道名称（如无对应名称则用自身ID）
            $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) 
                                    ? $inquiryMap[$row['inquiry_id']] 
                                    : (string)$row['inquiry_id'];
            // 运营端口名称（如无对应名称则用自身ID）
            $row['port_name'] = isset($portMap[$row['port_id']]) 
                                ? $portMap[$row['port_id']] 
                                : (string)$row['port_id'];
            
            // 产品名称（将ID转换为文字名称）
            if (!empty($row['product_name'])) {
                $row['product_name'] = isset($productMap[$row['product_name']]) 
                                      ? $productMap[$row['product_name']] 
                                      : (string)$row['product_name'];
            }

            // 主/辅电话
            $row['main_phone'] = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['main'] : '';
            $row['aux_phone']  = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['aux'] : '';

            // joint_person 可能是 JSON 数组或逗号分隔的 ID 字符串（crm_leads 有该字段）:contentReference[oaicite:3]{index=3}
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

        // 一次性把协同人的 username 查出来（若 admin 表不存在则跳过）
        $adminMap = [];
        try {
            if (!empty($uidSet) && Db::query("SHOW TABLES LIKE 'admin'")) {
                $adminMap = Db::table('admin')
                    ->where('admin_id', 'in', array_keys($uidSet))
                    ->column('username', 'admin_id');
            }
        } catch (\Exception $e) {
            $adminMap = [];
        }

        foreach ($rows as &$row) {
            $names = [];
            foreach ($row['_joint_ids'] as $uid) {
                $names[] = isset($adminMap[$uid]) ? $adminMap[$uid] : (string)$uid;
            }
            $row['joint_person_names'] = $names ? implode('、', $names) : '';
            unset($row['_joint_ids']);

            // 处理来源端口（source_port）字段，将MD5加密值转换为店铺名称
            $row['source_port_name'] = '';
            try {
                $columns = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
                if (!empty($columns) && !empty($row['source_port'])) {
                    $sourcePortId = $row['source_port'];
                    
                    // 尝试从 crm_operation_shops 表查找店铺名称
                    $shopInfo = Db::table('crm_operation_shops')
                        ->where('id', $sourcePortId)
                        ->where('is_active', 1)
                        ->field('shop_name')
                        ->find();
                    
                    if ($shopInfo) {
                        $row['source_port_name'] = $shopInfo['shop_name'];
                    } else {
                        // 如果表中找不到，尝试从 crm_client_status 的 shop_names 字段查找
                        // source_port 可能是 md5(status_id + '_' + shop_name) 格式，需要反向查找
                        $statusId = $row['kh_status'];
                        if (!empty($statusId)) {
                            $statusInfo = Db::table('crm_client_status')
                                ->where('id', $statusId)
                                ->field('id, shop_names')
                                ->find();
                            
                            if ($statusInfo && !empty($statusInfo['shop_names'])) {
                                $shop_names = array_filter(array_map('trim', explode(',', $statusInfo['shop_names'])));
                                foreach ($shop_names as $shop_name) {
                                    $expectedId = md5($statusInfo['id'] . '_' . $shop_name);
                                    if ($expectedId === $sourcePortId) {
                                        $row['source_port_name'] = $shop_name;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // 如果还是找不到，显示ID
                        if (empty($row['source_port_name'])) {
                            $row['source_port_name'] = $sourcePortId;
                        }
                    }
                }
            } catch (\Exception $e) {
                // 忽略错误
            }
        }
        unset($row);

        return ['code' => 0, 'msg' => '获取成功!', 'data' => $rows, 'count' => $list['total'], 'rel' => 1];
    }




    // 在 application/admin/controller/Client.php 内新增以下方法
    public function jointPersonClientSearch()
    {
        $page    = input('page/d', 1);
        $limit   = input('limit/d', config('pageSize'));
        $keyword = input('keyword/a', []);

        // 时间范围：沿用你项目里的 buildTimeWhere
        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }

        // 取列表
        $list = model('client')->getJointClientSearchList($page, $limit, $keyword);
        if (empty($list) || empty($list['data'])) {
            return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
        }

        // ===== 结果集二次加工：来源名/主辅电话/协同人名 =====
        $rows    = &$list['data'];
        $leadIds = array_column($rows, 'id');

        // 1) 询盘来源(ID->名称) 映射（表不存在则跳过）
        $statusMap = [];
        try {
            $hasStatusTable = Db::query("SHOW TABLES LIKE 'crm_client_status'");
            if (!empty($hasStatusTable)) {
                $statusMap = Db::table('crm_client_status')->column('status_name', 'id');
            }
        } catch (\Exception $e) {
            $statusMap = [];
        }
        
        // 2) 产品名称映射表（product_name ID -> product_name 文字）
        $productMap = Db::table('crm_products')->column('product_name', 'id');
        
        // 3) 所属渠道和运营端口名称映射表
        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');

        // 4) 批量取主/辅电话（1=主、3=辅）
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

        // 5) 协同人显示名
        $uidSet = [];
        foreach ($rows as &$row) {
            // 来源中文名（映射不到则回退为原值）
            $row['kh_status_name'] = isset($statusMap[$row['kh_status']]) ? $statusMap[$row['kh_status']] : (string)$row['kh_status'];
            
            // 所属渠道名称（如无对应名称则用自身ID）
            $row['inquiry_name'] = isset($inquiryMap[$row['inquiry_id']]) 
                                    ? $inquiryMap[$row['inquiry_id']] 
                                    : (string)$row['inquiry_id'];
            // 运营端口名称（如无对应名称则用自身ID）
            $row['port_name'] = isset($portMap[$row['port_id']]) 
                                ? $portMap[$row['port_id']] 
                                : (string)$row['port_id'];
            
            // 产品名称（将ID转换为文字名称）
            if (!empty($row['product_name'])) {
                $row['product_name'] = isset($productMap[$row['product_name']]) 
                                      ? $productMap[$row['product_name']] 
                                      : (string)$row['product_name'];
            }
            
            // 主/辅电话
            $row['main_phone'] = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['main'] : '';
            $row['aux_phone']  = isset($phoneMap[$row['id']]) ? $phoneMap[$row['id']]['aux']  : '';

            // 解析 joint_person（JSON 数组或逗号分隔）
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
            foreach ($idsArr as $uid) $uidSet[$uid] = true;
        }
        unset($row);

        // 批量查协同人用户名（admin_id -> username）
        $adminMap = [];
        try {
            if (!empty($uidSet) && Db::query("SHOW TABLES LIKE 'admin'")) {
                $adminMap = Db::table('admin')
                    ->where('admin_id', 'in', array_keys($uidSet))
                    ->column('username', 'admin_id');
            }
        } catch (\Exception $e) {
            $adminMap = [];
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

        return ['code' => 0, 'msg' => '获取成功!', 'data' => $rows, 'count' => $list['total'], 'rel' => 1];
    }




    //（我的客户）搜索
    // ====== 修改 chengjiaoClientSearch 开始 ======
    public function chengjiaoClientSearch()
    {
        $page = input('page') ? input('page') : 1;
        $limit = input('limit') ? input('limit') : config('pageSize');
        $keyword = Request::param('keyword');
        $list = model('client')->getChengjiaoClientSearchList($page, $limit, $keyword);
        // 对查询结果进行数据加工，确保返回字段与 successCliList() 一致
        if (!empty($list) && !empty($list['data']) && is_array($list['data'])) {
            $this->enrichLeadsRows($list['data']);
        }
        return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }
    // ====== 修改 chengjiaoClientSearch 结束 ======

    //评论
    public function comment()
    {

        $data['leads_id'] = Request::param('leads_id');
        $data['user_id'] = Session::get('aid');
        $data['reply_msg'] = Request::param('reply_msg');
        $data['create_date'] = time();

        //更新跟进记录
        $genjin['last_up_records'] = $data['reply_msg'];
        $genjin['last_up_time'] = date("Y-m-d H:i:s", $data['create_date']);
        $genjin['ut_time'] = date("Y-m-d H:i:s", time());

        Db::table('crm_leads')->where(['id' => $data['leads_id']])->update($genjin);

        $result = Db::table('crm_comment')->insert($data);
        $data['create_date'] = date("Y年m月d日 H:i", $data['create_date']);

        if ($result) {
            return json(['code' => 0, 'msg' => '评论成功！', 'data' => $data]);
        } else {
            return json(['code' => 1, 'msg' => '评论失败！']);
        }
    }

    // 获取客户详情和评论记录
    public function getClientDetailAndComments()
    {
        $clientId = Request::param('id');
        if (!$clientId) {
            return json(['code' => 1, 'msg' => '缺少客户ID参数']);
        }

        // 获取客户信息
        $client = Db::table('crm_leads')->where(['id' => $clientId])->find();
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在']);
        }

        // 获取主/辅电话（1=主电话，3=辅助电话）
        $mainPhone = '';
        $auxPhone = '';
        $contacts = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', $clientId)
            ->whereIn('contact_type', [1, 3])
            ->order('id', 'asc')
            ->field('contact_type, contact_value')
            ->select();
        foreach ($contacts as $c) {
            if ($c['contact_type'] == 1 && $mainPhone === '') {
                $mainPhone = $c['contact_value'];
            } elseif ($c['contact_type'] == 3 && $auxPhone === '') {
                $auxPhone = $c['contact_value'];
            }
        }
        $client['main_phone'] = $mainPhone;
        $client['aux_phone'] = $auxPhone;

        // 获取询盘来源中文名称（如果 kh_status 是 ID）
        $khStatusValue = $client['kh_status'];
        $statusName = $khStatusValue;
        if (is_numeric($khStatusValue)) {
            $statusInfo = Db::table('crm_client_status')->where('id', $khStatusValue)->find();
            if ($statusInfo) {
                $statusName = $statusInfo['status_name'];
            }
        }
        $client['kh_status_name'] = $statusName;

        // 获取客户级别名称（如果 kh_rank 是 ID）
        $khRankValue = $client['kh_rank'];
        $rankName = $khRankValue;
        if (is_numeric($khRankValue)) {
            $rankInfo = Db::table('crm_client_rank')->where('id', $khRankValue)->find();
            if ($rankInfo) {
                $rankName = $rankInfo['rank_name'];
            }
        }
        $client['kh_rank_name'] = $rankName;

        // 获取询盘来源名称（如果 inquiry_id 是 ID）  // 新增代码
        $inquiryValue = $client['inquiry_id'];
        $inquiryName = $inquiryValue;
        if (is_numeric($inquiryValue)) {
            $inquiryInfo = Db::table('crm_inquiry')->where('id', $inquiryValue)->find();
            if ($inquiryInfo) {
                $inquiryName = $inquiryInfo['inquiry_name'];
            }
        }
        $client['inquiry_name'] = $inquiryName;  // 新增代码

        // 获取运营端口名称（如果 port_id 是 ID）    // 新增代码
        $portValue = $client['port_id'];
        $portName = $portValue;
        if (is_numeric($portValue)) {
            $portInfo = Db::table('crm_inquiry_port')->where('id', $portValue)->find();
            if ($portInfo) {
                $portName = $portInfo['port_name'];
            }
        }
        $client['port_name'] = $portName;  // 新增代码

        // ✅新增：获取产品名称（如果 product_name 是 ID）
        $productValue = $client['product_name'];
        $productName = $productValue;
        if (is_numeric($productValue) && !empty($productValue)) {
            $productInfo = Db::table('crm_products')->where('id', $productValue)->find();
            if ($productInfo) {
                $productName = $productInfo['product_name'];
            }
        }
        $client['product_name'] = $productName;  // 转换为产品名称

        // 获取来源端口名称（如果 source_port 字段存在）
        // $client['source_port_name'] = '';
        // try {
        //     $columns = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'source_port'");
        //     if (!empty($columns) && !empty($client['source_port'])) {
        //         $sourcePortId = $client['source_port'];
                
        //         // 尝试从 crm_operation_shops 表查找店铺名称
        //         $shopInfo = Db::table('crm_operation_shops')
        //             ->where('id', $sourcePortId)
        //             ->where('is_active', 1)
        //             ->field('shop_name')
        //             ->find();
                
        //         if ($shopInfo) {
        //             $client['source_port_name'] = $shopInfo['shop_name'];
        //         } else {
        //             // 如果表中找不到，尝试从 crm_client_status 的 shop_names 字段查找
        //             // source_port 可能是 md5(id + shop_name) 格式，需要反向查找
        //             if (!empty($statusName)) {
        //                 $statusInfo = Db::table('crm_client_status')
        //                     ->where('status_name', $statusName)
        //                     ->field('id, shop_names')
        //                     ->find();
                        
        //                 if ($statusInfo && !empty($statusInfo['shop_names'])) {
        //                     $shop_names = array_filter(array_map('trim', explode(',', $statusInfo['shop_names'])));
        //                     foreach ($shop_names as $shop_name) {
        //                         $expectedId = md5($statusInfo['id'] . '_' . $shop_name);
        //                         if ($expectedId === $sourcePortId) {
        //                             $client['source_port_name'] = $shop_name;
        //                             break;
        //                         }
        //                     }
        //                 }
        //             }
                    
        //             // 如果还是找不到，显示ID
        //             if (empty($client['source_port_name'])) {
        //                 $client['source_port_name'] = $sourcePortId;
        //             }
        //         }
        //     }
        // } catch (\Exception $e) {
        //     // 忽略错误
        // }

        // 解析协同人信息（joint_person）
        $jointPersonIds = [];
        $jointPersonNames = [];
        if (!empty($client['joint_person'])) {
            $jp = $client['joint_person'];
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
            
            // 查询协同人用户名
            if (!empty($jointPersonIds)) {
                $adminMap = Db::table('admin')
                    ->whereIn('admin_id', $jointPersonIds)
                    ->column('username', 'admin_id');
                foreach ($jointPersonIds as $uid) {
                    $jointPersonNames[] = $adminMap[$uid] ?? (string)$uid;
                }
            }
        }
        $client['joint_person_ids'] = $jointPersonIds;
        $client['joint_person_names'] = implode('、', $jointPersonNames);

        // 获取客户的评论记录
        $comments = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where(['leads_id' => $clientId])
            ->field('com.*, adm.username, adm.avatar')
            ->order('com.create_date desc')
            ->select();

        // 格式化时间
        foreach ($comments as &$comment) {
            $comment['create_date'] = date("Y年m月d日 H:i", $comment['create_date']);
        }

        return json([
            'code' => 0,
            'data' => [
                'client' => $client,
                'comments' => $comments
            ],
            'msg' => '获取成功'
        ]);
    }

    // ✅新增：全部跟进 - 分页获取客户跟进记录
    public function getClientCommentsPage()
    {
        $leadsId = Request::param('leads_id');
        $page = input('page/d', 1);
        $limit = input('limit/d', 10);
        $keyword = input('keyword/s', '');
        $startDate = input('start_date/s', '');
        $endDate = input('end_date/s', '');

        // 校验客户ID
        if (empty($leadsId)) {
            return json(['code' => 1, 'msg' => '缺少客户ID', 'count' => 0, 'data' => []]);
        }

        // 校验客户是否存在（可选：检查权限，只能查看自己负责的客户）
        $client = Db::table('crm_leads')->where(['id' => $leadsId])->find();
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在', 'count' => 0, 'data' => []]);
        }

        // 权限检查：如果是个人客户列表，只能查看自己负责的客户
        $currentUsername = session('username');
        if ($client['pr_user'] !== $currentUsername) {
            // 可以扩展为检查协同人权限等
            // 这里先简单处理：如果不是负责人，检查是否是协同人
            $isJointPerson = false;
            if (!empty($client['joint_person'])) {
                $currentAdminId = session('aid');
                $jp = $client['joint_person'];
                if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                    $tmp = json_decode($jp, true);
                    if (is_array($tmp) && in_array($currentAdminId, $tmp)) {
                        $isJointPerson = true;
                    }
                } else {
                    $idsArr = preg_split('/[,，\s]+/', $jp, -1, PREG_SPLIT_NO_EMPTY);
                    if (in_array($currentAdminId, $idsArr)) {
                        $isJointPerson = true;
                    }
                }
            }
            // 如果不是负责人也不是协同人，且不是超级管理员，则拒绝访问
            if (!$isJointPerson && session('aid') != 1) {
                return json(['code' => 1, 'msg' => '无权查看该客户的跟进记录', 'count' => 0, 'data' => []]);
            }
        }

        // 构建查询条件
        $where = [['com.leads_id', '=', $leadsId]];

        // 关键字搜索（跟进内容）
        if (!empty($keyword)) {
            $where[] = ['com.reply_msg', 'like', '%' . $keyword . '%'];
        }

        // 时间范围筛选
        if (!empty($startDate)) {
            // 开始日期：取当天的 00:00:00
            $startTimestamp = strtotime($startDate . ' 00:00:00');
            if ($startTimestamp !== false) {
                $where[] = ['com.create_date', '>=', $startTimestamp];
            }
        }
        if (!empty($endDate)) {
            // 结束日期：取当天的 23:59:59
            $endTimestamp = strtotime($endDate . ' 23:59:59');
            if ($endTimestamp !== false) {
                $where[] = ['com.create_date', '<=', $endTimestamp];
            }
        }

        // 查询总数
        $count = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where($where)
            ->count();

        // 分页查询
        $list = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where($where)
            ->field('com.*, adm.username, adm.avatar')
            ->order('com.create_date desc')
            ->page($page, $limit)
            ->select();

        // 格式化时间：Y-m-d H:i（列表用紧凑格式）
        foreach ($list as &$item) {
            $item['create_date'] = date('Y-m-d H:i', $item['create_date']);
        }
        unset($item);

        // 返回 Layui 标准分页格式
        return json([
            'code' => 0,
            'msg' => '获取成功',
            'count' => $count,
            'data' => $list
        ]);
    }

    //回复
    public function reply()
    {

        $data['comment_id'] = Request::param('cid');
        $data['from_user_id'] = Session::get('user.id');
        $data['to_user_id'] = Request::param('to_uid');
        $data['reply_msg'] = Request::param('reply_msg');
        $data['create_date'] = time();

        $result = Db::table('crm_reply')->insert($data);
        $data['create_date'] = date("Y年m月d日 H:i", $data['create_date']);
        if ($result) {
            return json(['code' => 0, 'msg' => '回复成功！', 'data' => $data]);
        } else {

            return json(['code' => 1, 'msg' => '回复失败！']);
        }
    }


    //客户转移，变更负责人
    public function alterPrUser()
    {
        //1，获取提交的线索ID 【1,2,3,4,】
        $ids = Request::param('ids');
        $this->assign('ids', $ids);


        //查询所有管理员（去除admin）
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        $this->assign('adminResult', $adminResult);

        if (Request::isAjax()) {
            $username = Request::param('username');
            $idsArr = explode(",", $ids);


            $count = 0;
            foreach ($idsArr as $key => $value) {
                $data['pr_user_bef'] = Db::table('crm_leads')->where(['id' => $value])->field('pr_user')->find();
                $data['pr_user'] = $username;
                $data['id'] = $value;
                $insertAll = Db::name('crm_leads')->update($data);
                if ($insertAll) {
                    $count++;
                }
            }




            if ($count > 0) {
                $msg = ['code' => 0, 'msg' => '转移' . $count . '个客户成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '转移失败！', 'data' => []];
                return json($msg);
            }
        }

        return $this->fetch('client/alter_pr_user');
    }


    //客户转移，变更负责人(个人)
    public function alterPrUserPri()
    {
        //1，获取提交的线索ID 【1,2,3,4,】
        $ids = Request::param('ids');
        $this->assign('ids', $ids);
        //客户信息
        $clientList = Db::name('crm_leads')->where('id', 'in', $ids)->field('id,kh_name')->select();
        $clientName = implode(',', array_column($clientList, 'kh_name'));
        $this->assign('client_name', $clientName);

        //查询所有管理员（去除admin）
        $adminResult = Db::name('admin')->where('group_id', '<>', 1)->field('admin_id,username')->select();
        $this->assign('adminResult', $adminResult);

        if (Request::isAjax()) {
            $username = Request::param('username');
            // 查询新负责人的 admin_id
            $newPrUserId = Db::name('admin')->where('username', $username)->value('admin_id');
            if (empty($newPrUserId)) {
                return json(['code' => 500, 'msg' => '未找到该负责人账号，请重新选择', 'data' => []]);
            }
            
            $idsArr = explode(",", $ids);
            $count = 0;
            foreach ($idsArr as $key => $value) {
                // 一次性读出旧负责人信息
                $old = Db::name('crm_leads')->where('id', $value)->field('pr_user,pr_user_id')->find();
                
                // 组装更新数据
                $data = [];
                $data['id'] = $value;
                $data['pr_user_bef'] = $old['pr_user'] ?? '';
                $data['pr_user_bef_id'] = $old['pr_user_id'] ?? 0;
                $data['pr_user'] = $username;
                $data['pr_user_id'] = (int)$newPrUserId;
                
                $insertAll = Db::name('crm_leads')->update($data);
                if ($insertAll) {
                    $count++;
                }
                // 添加日志记录
                $this->addOperLog(
                    $value,
                    '转移负责人',
                    "从 [{$data['pr_user_bef']}] 转移给 [{$username}]"
                );
            }

            if ($count > 0) {
                $msg = ['code' => 0, 'msg' => '转移' . $count . '个客户成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '转移失败！', 'data' => []];
                return json($msg);
            }
        }

        return $this->fetch('personclient/alter_pr_user');
    }

    //客户行业
    public function hangyeList()
    {
        if (request()->isPost()) {
            $page = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');
            $list = db('crm_client_hangye')
                ->paginate(array('list_rows' => $pageSize, 'page' => $page))
                ->toArray();
            return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }
        return $this->fetch();
    }
    //添加客户级别
    public function hangyeAdd()
    {
        if (request()->isPost()) {
            $data['hy_name'] = Request::param('hy_name');
            $data['add_time'] = time();
            $result = Db::table('crm_client_hangye')->insert($data);
            if ($result) {
                $msg = ['code' => 0, 'msg' => '添加成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '添加失败！', 'data' => []];
                return json($msg);
            }
        }
        return $this->fetch('client/hangye_list_add');
    }
    //编辑客户级别
    public function hangyeEdit()
    {
        if (Request::isAjax()) {
            $data  = Request::param();
            // 获取原状态
            $oldstatus = Db::table('crm_client_hangye')->where(['id' => $data['id']])->find();
            $oldstatusname = $oldstatus['hy_name'];
            $ischange = false;
            if ($oldstatusname == $data['hy_name']) {
                $msg = ['code' => 500, 'msg' => '没有变化无需修改', 'data' => []];
                return json($msg);
            } else {
                $ischange = true;
            }

            $result = Db::table('crm_client_hangye')->where(['id' => $data['id']])->update($data);
            if ($result) {
                // 状态修改后 客户编辑的原来状态都必须修改
                if ($ischange) {
                    // 所有的客户状态全部膝盖
                    $result2 = Db::table('crm_leads')->where(['kh_hangye' => $oldstatusname])->update(['kh_hangye' => $data['hy_name']]);
                }
                $msg = ['code' => 0, 'msg' => '编辑成功！', 'data' => []];
                return json($msg);
            } else {
                $msg = ['code' => 500, 'msg' => '编辑失败！', 'data' => []];
                return json($msg);
            }
        }

        $result = Db::table('crm_client_hangye')->where(['id' => Request::param('id')])->find();
        $this->assign('result', $result);
        return $this->fetch('client/hangye_list_edit');
    }
    //删除客户级别
    public function hangyeDel()
    {
        $id = Request::param('id');
        // 获取原状态
        $oldstatus = Db::table('crm_client_hangye')->where(['id' => $data['id']])->find();
        $oldstatusname = $oldstatus['hy_name'];

        $result = Db::table('crm_client_hangye')->where('id', $id)->delete();
        if ($result) {
            // 所有的客户状态全部膝盖
            $result2 = Db::table('crm_leads')->where(['kh_hangye' => $oldstatusname])->update(['kh_hangye' => '']);
            $msg = ['code' => 0, 'msg' => '删除成功！', 'data' => []];
            return json($msg);
        } else {
            $msg = ['code' => 500, 'msg' => '删除失败！', 'data' => []];
            return json($msg);
        }
    }

    //新增各国区号
    public function getCountries()
    {
        $countries = cache('countries');
        //清除缓存
        // cache('countries',null);
        if ($countries) {
            return $countries;
        }
        $list = Db::table('countries')->field('phone_code,english_name,chinese_name')->select();
        foreach ($list as $key => $value) {
            $_key = $value['english_name'] . '(' . $value['chinese_name'] . ')';
            $countries[$_key] =  $value['phone_code'];
        }
        //cache('countries', $countries);
        return $countries;
    }

    //客户成交
    public function chengjiao()
    {
        $ids = Request::param('ids');
        if (is_string($ids)) $ids = explode(',', $ids);
        $count = 0;
        foreach ($ids as $key => $value) {
            $data['issuccess'] = 1;
            $data['id'] = $value;
            $insertAll = Db::name('crm_leads')->update($data);
            if ($insertAll) {
                $count++;
            }
        }
        if ($count > 0) {
            $msg = ['code' => 0, 'msg' => '成交' . $count . '个客户成功！', 'data' => []];
            return json($msg);
        } else {
            $msg = ['code' => 500, 'msg' => '成交失败！', 'data' => []];
            return json($msg);
        }
    }



    //冲突查询
    // public function conflictOld()
    // {
    //     $keyword = Request::param('keyword');
    //     $keyword = trim(preg_replace('/[+\-\s]/', '', $keyword));
    //     if (Request::isAjax()) {
    //         if (empty($keyword)) return success();

    //         $query = Db::name('crm_leads')
    //             ->alias('l')
    //             ->leftJoin('crm_contacts c', 'l.id = c.leads_id AND c.is_delete = 0')
    //             ->field('l.kh_name,l.xs_area,l.kh_rank,l.kh_status,l.at_user,l.at_time,l.pr_gh_type,l.pr_user')
    //             ->group('l.id');
    //         $query->where(function ($q) use ($keyword) {
    //             $q->where('l.kh_name', 'like', "%{$keyword}%")
    //                 ->whereOr(function ($q2) use ($keyword) {
    //                     $q2->where('c.contact_value', $keyword)
    //                         ->whereOrRaw("CONCAT(c.contact_extra, c.contact_value) = '{$keyword}'");
    //                 });
    //         });

    //         $page = Request::param('page/d', 1);
    //         $pageSize = Request::param('limit/d', 10);
    //         $list = $query->paginate($pageSize, false, ['page' => $page])->items();
    //         return success($list);
    //     }
    //     $this->assign('keyword', $keyword);
    //     return $this->fetch('client/conflict');
    // }

    // //冲突查询
    // public function conflict()
    // {
    //     $_keyword = Request::param('keyword');
    //     $keyword = trim(preg_replace('/[+\-\s]/', '', $_keyword));
    //     if (Request::isAjax()) {
    //         if (empty($keyword)) return success();
    //         $leadsQuery = Db::name('crm_leads')
    //             ->alias('l')
    //             ->field('l.id, l.kh_name, l.xs_area, l.kh_rank, l.kh_status, l.at_user, l.at_time,l.pr_gh_type,l.pr_user')
    //             ->field('NULL as contact_type, NULL as contact_value')
    //             ->where('l.kh_name', 'like', "%{$keyword}%");

    //         $contactsQuery = Db::name('crm_contacts')
    //             ->alias('c')
    //             ->leftJoin('crm_leads l', 'l.id = c.leads_id')
    //             ->where('c.is_delete', 0)
    //             ->where(function ($q) use ($keyword,$_keyword) {
    //                 $q->where('c.contact_value','like', $keyword)
    //                     ->whereOrRaw("CONCAT(c.contact_extra, c.contact_value) like '%{$keyword}%'");
    //                 if($_keyword != $keyword)$q->whereOr('c.contact_value','like', "%{$_keyword}%");
    //             })
    //             ->field('l.id, l.kh_name, l.xs_area, l.kh_rank, l.kh_status, l.at_user, l.at_time,l.pr_gh_type,l.pr_user,c.contact_type,c.contact_value');

    //         $query = Db::query("({$leadsQuery->buildSql()}) UNION ({$contactsQuery->buildSql()})");

    //          // 去除重复记录
    //         $uniqueIds = [];
    //         $list = [];
    //         foreach ($query as $item) {
    //             if (!in_array($item['id'], $uniqueIds)) {
    //                 $uniqueIds[] = $item['id'];
    //                 $list[] = $item;
    //             }
    //         }

    //         // 保存总记录数
    //         $total = count($list);

    //         // 分页处理
    //         $page = Request::param('page/d', 1);
    //         $pageSize = Request::param('limit/d', 10);
    //         $offset = ($page - 1) * $pageSize;
    //         $paginatedList = array_slice($list, $offset, $pageSize);

    //         // 添加重复类型信息
    //         foreach ($paginatedList as &$item) {
    //             if (isset($item['contact_type'])) {
    //                 // 修改contact_type判断逻辑，使用数字类型匹配
    //                 switch ((int)$item['contact_type']) {
    //                     case 3:
    //                         $item['repeat_info'] = 'WhatsApp：' . $item['contact_value'];
    //                         break;
    //                     case 1:
    //                         $item['repeat_info'] = '电话：' . $item['contact_value'];
    //                         break;
    //                     case 2:
    //                         $item['repeat_info'] = '邮箱：' . $item['contact_value'];
    //                         break;
    //                     case 4:
    //                         $item['repeat_info'] = '阿里ID：' . $item['contact_value'];
    //                         break;
    //                     case 5:
    //                         $item['repeat_info'] = '微信：' . $item['contact_value'];
    //                         break;
    //                     default:
    //                         $item['repeat_info'] = '未知类型(' . $item['contact_type'] . ')：' . $item['contact_value'];
    //                 }
    //             } else {
    //                 $item['repeat_info'] = '客户名称重复';
    //             }
    //         }
    //         unset($item);

    //         return json([
    //             'code' => 0,
    //             'msg' => '',
    //             'count' => $total,
    //             'data' => $paginatedList
    //         ]);
    //     }
    //     $this->assign('keyword', $_keyword);
    //     return $this->fetch('client/conflict');
    // }


    public function conflict()
    {
        // 获取并清理关键词（去除空格和特殊字符）
        $_keyword = Request::param('keyword');
        $keyword = trim(preg_replace('/[+\-\s]/', '', $_keyword));
        if (Request::isAjax()) {
            if (empty($keyword)) {
                // 关键词为空，直接返回空结果
                return json(['code' => 0, 'msg' => '', 'data' => []]);
            }
            // 生成唯一任务ID，用于关联查重结果
            $taskId = uniqid('', true);  // 例如：生成类似 5f2e5c7fbd98e 的唯一ID
            // 准备任务数据，包含任务ID和原始关键词
            $jobData = [
                'id'      => $taskId,
                'keyword' => $_keyword  // 保留原始关键词，后端队列处理时再做trim处理
            ];
            // 将任务数据推送到Redis队列
            $redis = new \Redis();
            $redis->connect('127.0.0.1', 26739);
            // 若Redis设置了密码，可使用 
            $redis->auth('csE88ifakDGC8PfH');
            $redis->rPush('waimao_conflict_queue', json_encode($jobData));  // 推送任务到外贸专用队列
            // 返回任务已创建的响应，携带任务ID
            return json([
                'code'    => 0,
                'msg'     => '查重任务已提交',
                'task_id' => $taskId   // 前端据此轮询结果
            ]);
        }
        // 非Ajax请求，渲染页面（保留原有逻辑）
        $this->assign('keyword', $_keyword);
        return $this->fetch('client/conflict');
    }


    public function getConflictResult()
    {
        $taskId = Request::param('task_id');
        if (empty($taskId)) {
            return json(['code' => 500, 'msg' => '缺少任务ID', 'data' => []]);
        }

        $redis = new \Redis();
        $redis->connect('127.0.0.1', 26739);
        $redis->auth('csE88ifakDGC8PfH'); // 如有密码请取消注释

        $statusKey = 'waimao_conflict_status:' . $taskId;
        $resultKey = 'waimao_conflict_result:' . $taskId;

        $status = $redis->get($statusKey);

        if ($status === 'done') {
            $resultData = $redis->get($resultKey);
            $resultList = json_decode($resultData, true);
            // 注意：不在这里删除 Redis key，依靠 expire 自动过期（5分钟）
            return json([
                'code'  => 0,
                'msg'   => '获取成功',
                'data'  => $resultList ?: [],
                'count' => count($resultList ?: [])
            ]);
        } elseif ($status === 'processing') {
            return json(['code' => 202, 'msg' => '查重处理中，请稍后...', 'data' => []]);
        } else {
            return json(['code' => 404, 'msg' => '查重失败，请再次尝试搜索', 'data' => []]);
        }
    }


    // 下载导入模板（CSV格式）
    public function tpl()
    {
        // 文件名包含当前日期时间
        $filename = '客户导入模板_' . date('Ymd_His') . '.csv';
        // 工具函数：输出CSV一行，确保字段中包含的引号正确转义
        $csvLine = function(array $cols) {
            $safe = array_map(function($v) {
                $v = str_replace('"', '""', (string)$v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        // 表头字段（客户名称、电话、辅助电话...原来修改时间）
        $header = ['客户名称', '电话', '辅助电话', '产品名称', '所属渠道', '运营端口', '协同人', '负责人', '其他信息', '原来创建时间', '原来修改时间'];
        // 示例数据（可根据需要调整或留空）
        $examples = [
            ['示例客户A', '13800138000', '13900139000', '产品X', '渠道一', '端口A', '5,7', 'wangwu', '备注信息', '2020-01-01 10:00:00', '2020-06-01 08:00:00'],
            ['示例客户B', '13700137000', '', '产品Y', '渠道二', '', '', '当前登录人', '', '2021-05-05 09:30:00', '']
        ];

        // 清除输出缓冲，设置CSV文件下载头
        if (function_exists('ob_get_level')) {
            while (ob_get_level() > 0) {
                ob_end_clean();
            }
        }
        header('Content-Type: text/csv; charset=UTF-8');
        header('Content-Disposition: attachment; filename="' . $filename . '"');
        header('Pragma: no-cache');
        header('Expires: 0');
        // 输出UTF-8 BOM，避免中文乱码
        echo "\xEF\xBB\xBF";
        // 输出表头
        echo $csvLine($header);
        // 输出示例行
        foreach ($examples as $row) {
            echo $csvLine($row);
        }
        exit;
    }


    // 检查最近导入是否有失败记录（供前端提示用）
    public function checkImportFail()
    {
        $adminId   = Session::get('aid');
        $username  = Session::get('username');
        // 查询一小时内当前用户的导入失败日志记录
        $hasFail = Db::table('crm_operation_log')
                    ->where('user_id', $adminId)
                    ->where('oper_type', '数据导入')
                    ->where('description', 'like', '%导入失败%')
                    ->whereTime('created_at', '>=', date('Y-m-d H:i:s', time()-3600))
                    ->find();
        return json(['code' => 0, 'fail' => $hasFail ? 1 : 0]);
    }

    // 获取导入失败日志列表（返回HTML片段）
    public function importLog()
    {
        $adminId = Session::get('aid');
        // 查询当前用户的所有“数据导入”失败日志记录（按时间倒序，最多最近100条）
        $logQuery = Db::table('crm_operation_log')
                    ->where('oper_type', '数据导入')
                    ->where('description', 'like', '%导入失败%')
                    ->order('id', 'desc')
                    ->limit(100);
        // 非超级管理员只查看自己的导入日志
        if ($adminId != 1) {
            $logQuery->where('user_id', $adminId);
        }
        $logList = $logQuery->select();

        // 生成HTML表格
        $html = '<table class="layui-table"><thead><tr><th>时间</th><th>失败详情</th></tr></thead><tbody>';
        foreach ($logList as $log) {
            $time = $log['created_at'];
            $desc = $log['description'];
            // 尝试解析JSON描述
            $detailText = '';
            $descData = json_decode($desc, true);
            if (is_array($descData)) {
                // 使用 message 和 error_message 字段
                $detailText = $descData['message'] ?? '';
                if (!empty($descData['error_message'])) {
                    $detailText .= ' - ' . $descData['error_message'];
                }
                // 附加原始数据内容（如存在）
                if (!empty($descData['task_data'])) {
                    $orig = is_array($descData['task_data']) ? implode(' | ', $descData['task_data']) : $descData['task_data'];
                    $detailText .= ' | 原始数据：' . htmlspecialchars($orig);
                }
            } else {
                // 非JSON则直接输出文本
                $detailText = htmlspecialchars($desc);
            }
            $html .= "<tr><td>{$time}</td><td>{$detailText}</td></tr>";
        }
        $html .= '</tbody></table>';
        // 输出HTML片段
        echo $html;
        exit;
    }

    /**
     * 获取表格列宽
     * 用于页面初始化时读取当前用户已保存的列宽
     */
    /**
     * 获取表格列宽
     * 从数据库读取当前用户的列宽配置
     * 路由：Client/getColWidths
     * 请求：GET（AJAX）
     * 入参：page_key（字符串）、table_id（字符串）
     * 返回：json {code:0, data:{field1:120, field2:180...}}
     * 
     * 验收方式：
     * 1. 登录 A 用户，拖拽"产品名称"列宽到 300，刷新页面：列宽仍为 300
     * 2. 换 B 用户登录：列宽不受影响（仍是默认或 B 自己的）
     * 3. 数据表 crm_table_colwidth 能看到对应记录更新 updated_at
     */
    public function getColWidths()
    {
        if (!request()->isAjax()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = Session::get('aid');
        $pageKey = Request::param('page_key', '');
        $tableId = Request::param('table_id', '');

        // 强制要求：admin_id 为空返回 401
        if (empty($adminId)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        // 校验 page_key/table_id
        if (empty($pageKey) || empty($tableId)) {
            return json(['code' => 0, 'msg' => '获取成功', 'data' => []]);
        }

        try {
            $list = Db::table('crm_table_colwidth')
                ->where('admin_id', $adminId)
                ->where('page_key', $pageKey)
                ->where('table_id', $tableId)
                ->field('field, width')
                ->select();

            $widths = [];
            if (!empty($list)) {
                foreach ($list as $item) {
                    $widths[$item['field']] = (int)$item['width'];
                }
            }

            return json(['code' => 0, 'msg' => '获取成功', 'data' => $widths]);
        } catch (\Exception $e) {
            // 如果表不存在或其他错误，返回空数据，不报错
            return json(['code' => 0, 'msg' => '获取成功', 'data' => []]);
        }
    }

    /**
     * 保存表格列宽
     * 用于前端拖拽列宽后保存到数据库
     * 路由：Client/saveColWidths
     * 请求：POST（AJAX）
     * 入参：page_key, table_id, widths（JSON 字符串或 array，兼容两种）
     * 返回：json {code:0, msg:'保存成功'}
     * 
     * 强制要求：
     * - widths decode 失败/为空，返回 code=400
     * - width 只允许正整数（<=2000 上限保护）
     * - 使用批量 INSERT ... ON DUPLICATE KEY UPDATE
     */
    public function saveColWidths()
    {
        if (!request()->isPost()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = Session::get('aid');
        $pageKey = Request::param('page_key', '');
        $tableId = Request::param('table_id', '');
        $widthsParam = Request::param('widths', '');

        if (empty($adminId) || empty($pageKey) || empty($tableId)) {
            return json(['code' => 500, 'msg' => '参数不完整', 'data' => []]);
        }

        // 兼容 widths 是 JSON 字符串或 array 两种情况
        $widths = null;
        if (is_array($widthsParam)) {
            $widths = $widthsParam;
        } else if (!empty($widthsParam)) {
            $widths = json_decode($widthsParam, true);
        }

        // 强制要求：widths decode 失败/为空，返回 code=400（不要静默成功）
        if (!is_array($widths) || empty($widths)) {
            return json(['code' => 400, 'msg' => '列宽数据格式错误或为空', 'data' => []]);
        }

        try {
            $currentTime = time();
            $values = [];
            $params = [];

            // 准备批量插入数据
            foreach ($widths as $field => $width) {
                if (empty($field) || !is_string($field)) {
                    continue;
                }

                // 验证字段名格式：只允许字母、数字、下划线（防止 SQL 注入）
                if (!preg_match('/^[a-zA-Z0-9_]+$/', $field)) {
                    continue;
                }

                $width = (int)$width;
                
                // 强制要求：width 只允许正整数（<=2000 上限保护）
                if ($width <= 0 || $width > 2000) {
                    continue;
                }
                
                $values[] = "(?, ?, ?, ?, ?, ?)";
                $params[] = $adminId;
                $params[] = $pageKey;
                $params[] = $tableId;
                $params[] = $field; // 已验证格式，直接使用
                $params[] = $width;
                $params[] = $currentTime;
            }

            if (empty($values)) {
                return json(['code' => 400, 'msg' => '没有有效的列宽数据', 'data' => []]);
            }

            // 使用批量 INSERT ... ON DUPLICATE KEY UPDATE（一条 SQL）
            $sql = "INSERT INTO `crm_table_colwidth` (`admin_id`, `page_key`, `table_id`, `field`, `width`, `updated_at`) 
                    VALUES " . implode(', ', $values) . "
                    ON DUPLICATE KEY UPDATE `width` = VALUES(`width`), `updated_at` = VALUES(`updated_at`)";
            
            Db::execute($sql, $params);

            return json(['code' => 0, 'msg' => '保存成功', 'data' => ['count' => count($values)]]);
        } catch (\Exception $e) {
            return json(['code' => 500, 'msg' => '保存失败：' . $e->getMessage(), 'data' => []]);
        }
    }


}
