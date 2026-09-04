<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use think\facade\Env;
use think\facade\Log;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;
use think\facade\Cache;
use app\admin\model\Admin;
use app\admin\service\CheckOrderService;
use app\admin\service\ClientConfigService;
use app\admin\service\ClientDetailService;
use app\admin\service\ClientFollowService;
use app\admin\service\ClientOrderService;
use app\admin\service\ClientOwnerCandidateService;
use app\admin\service\ClientOwnerHistoryService;
use app\admin\service\ClientRowMarkService;
use app\admin\service\ClientStatusService;
use app\admin\service\OrderService;
use app\admin\service\PositionTitleService;
use app\admin\service\SuccessClientOrderService;
use app\admin\service\SuccessClientApplyService;

class Client extends Common
{
    protected $middleware = [\app\http\middleware\TrimStrings::class];
    /** @var array<string, array<string, bool>> */
    private $tableColumnsCache = [];

    /**
     * 客户详情“成交订单”合并展示时，单个数据源的最大拉取条数
     * （用于与 crm_client_history_order 合并排序后再统一分页，避免遗漏数据）
     */
    private const MERGE_ORDER_FETCH_LIMIT = 100000;

    /**
     * 订单编辑流程中的客户编辑放行能力：
     * - 超级管理员 aid=1
     * - admin 用户
     * - group_id in [13, 15]
     */
    private function canEditAnyClientForOrder()
    {
        $adminInfo = Admin::getMyInfo();
        return OrderService::canManageAllOrders($adminInfo);
    }

    /**
     * 原有客户编辑权限：本人负责人或协同人
     */
    private function canEditClientByOwnership(array $clientRow)
    {
        $currentUsername = trim((string)Session::get('username'));
        $currentAid = (string)((int)Session::get('aid'));

        if ($currentUsername !== '' && trim((string)($clientRow['pr_user'] ?? '')) === $currentUsername) {
            return true;
        }

        $jp = (string)($clientRow['joint_person'] ?? '');
        if ($jp === '') {
            return false;
        }

        $jpIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
            $tmp = json_decode($jp, true);
            if (is_array($tmp)) {
                $jpIds = $tmp;
            }
        } else {
            $jpIds = array_values(array_filter(explode(',', $jp)));
        }

        return in_array($currentAid, array_map('strval', $jpIds), true);
    }

    /**
     * 成交客户申请权限：超级管理员或客户负责人/协同人
     */
    private function canApplySuccessClient(array $clientRow)
    {
        if ((int)Session::get('aid') === 1) {
            return true;
        }

        return $this->canEditClientByOwnership($clientRow);
    }

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
        $logId = Db::table('crm_operation_log')->insertGetId([
            'user_id' => Session::get('aid'),
            'leads_id' => $leads_id,
            'oper_type' => $type,
            'description' => $description,
            'oper_user' => Session::get('username'),
            'created_at' => date("Y-m-d H:i:s")
        ]);
        return $logId;
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
        $khRankList   = $this->getClientRankOptions();
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
        $this->assign('followOpenId', (int)Request::param('follow_open_id', 0));
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
            $row['main_phone_position_titles'] = $row['main_phone_position_titles'] ?? '';
            $row['aux_phone_position_titles'] = $row['aux_phone_position_titles'] ?? '';
            $auxParts = array_values(array_filter(explode(',', $row['aux_phone_position_titles'])));
            $row['aux_phone_position_titles'] = $auxParts[0] ?? '';
        }
        unset($row);    
        $this->appendKhRankDisplayForRows($rows);
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
            $keyword = input('keyword/a', []);

            // 基础列表（我的客户）
            $list = model('Client')->getMyClientList($page, $pageSize, Session::get('username'), $keyword);

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

            // 一次性取出所有客户的主/辅电话及职位身份：1=主电话，3=辅助电话
            $phoneMap = []; // leads_id => ['main'=>'', 'aux'=>'', 'main_position_titles'=>'', 'aux_position_titles'=>'']
            if (!empty($leadIds)) {
                $contacts = Db::table('crm_contacts')
                    ->alias('c')
                    ->leftJoin('crm_position_title pt', 'pt.id = c.position_title_id AND pt.is_deleted = 0')
                    ->where('c.is_delete', 0)
                    ->whereIn('c.leads_id', $leadIds)
                    ->whereIn('c.contact_type', [1, 3])
                    ->order('c.id', 'asc')
                    ->field('c.leads_id, c.contact_type, c.contact_value, c.position_title_id, c.position_title, pt.title_name as pt_title_name')
                    ->select();
                foreach ($contacts as $c) {
                    $lid = $c['leads_id'];
                    if (!isset($phoneMap[$lid])) {
                        $phoneMap[$lid] = [
                            'main' => '',
                            'aux' => '',
                            'main_position_titles' => '',
                            'aux_position_titles' => ''
                        ];
                    }

                    $positionTitleName = trim((string)($c['pt_title_name'] ?? ''));
                    if ($positionTitleName === '') {
                        $positionTitleName = trim((string)($c['position_title'] ?? ''));
                    }
                    if ($positionTitleName === '') {
                        $positionTitleName = '未填写';
                    }
                    $phoneAndTitle = (string)$c['contact_value'] . '-' . $positionTitleName;

                    if ($c['contact_type'] == 1) {
                        if ($phoneMap[$lid]['main'] === '') {
                            $phoneMap[$lid]['main'] = $c['contact_value'];
                        } else {
                            $phoneMap[$lid]['main'] .= ',' . $c['contact_value'];
                        }
                        if ($phoneMap[$lid]['main_position_titles'] === '') {
                            $phoneMap[$lid]['main_position_titles'] = $phoneAndTitle;
                        } else {
                            $phoneMap[$lid]['main_position_titles'] .= ',' . $phoneAndTitle;
                        }
                    } elseif ($c['contact_type'] == 3 && $phoneMap[$lid]['aux'] === '') {
                        $phoneMap[$lid]['aux'] = $c['contact_value'];
                        $phoneMap[$lid]['aux_position_titles'] = $phoneAndTitle;
                    }

                }
            }

            // 收集协同人ID，后续统一查用户名
            $uidSet = [];
            foreach ($rows as &$row) {
                if (!array_key_exists('next_up_time', $row)) {
                    $row['next_up_time'] = '';
                }
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
                $row['main_phone_position_titles'] = $phoneMap[$row['id']]['main_position_titles'] ?? '';
                $row['aux_phone_position_titles'] = $phoneMap[$row['id']]['aux_position_titles'] ?? '';

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

            $this->appendKhRankDisplayForRows($rows);
            return ['code' => 0, 'msg' => '获取成功!', 'data' => $rows, 'count' => $list['total'], 'rel' => 1];
        }

        // 页面渲染所需下拉数据
        $khRankList = $this->getClientRankOptions();
        $inquiryList  = Db::table('crm_inquiry')->select();        // 所属渠道下拉数据
        $khStatusList = Db::table('crm_client_status')->select();
        $xsSourceList = Db::table('crm_clues_source')->select();
        $currentAdmin = \app\admin\model\Admin::getMyInfo();
        $productListQuery = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($currentAdmin['org']) && strpos($currentAdmin['org'], 'admin') === false) {
            $productListQuery->where($this->getOrgWhere($currentAdmin['org'], 'p'));
        }
        $productList = $productListQuery
            ->where([
                'p.is_deleted' => 0,
                'c.is_deleted' => 0,
            ])
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();
        $yyList = $this->getYyList();
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);
        $this->assign('xsSourceList', $xsSourceList);  //线索/客户来源
        $this->assign('productList', $productList);


        

        return $this->fetch('personclient/index');
    }

    /**
     * （检查客户）统一计算当前登录人可见的业务员用户名列表
     * 返回经过 trim / 去空 / 去重 处理后的 username 数组
     */
    private function getCheckClientAllowedUsernames(): array
    {
        $currentAdminId  = (int) Session::get('aid');
        $currentUsername = trim((string) Session::get('username'));
        $currentGroupId  = (int) Session::get('group_id');
        $currentTeamName = trim((string) (Session::get('team_name') ?? ''));

        if (!$currentAdminId && $currentUsername === '') {
            return [];
        }

        // 如 session 信息不完整，则从 admin 表补全
        if (!$currentGroupId || $currentTeamName === '' || $currentUsername === '') {
            if ($currentAdminId) {
                $currentAdmin = Db::name('admin')
                    ->where('admin_id', $currentAdminId)
                    ->field('admin_id,username,group_id,team_name')
                    ->find();
                if ($currentAdmin) {
                    $currentUsername = trim((string) $currentAdmin['username']);
                    $currentGroupId  = (int) $currentAdmin['group_id'];
                    $currentTeamName = trim((string) $currentAdmin['team_name']);
                }
            }
        }

        // 角色相关常量
        $specialAdminIds     = [1, 395, 350, 375, 387,391, 405];
        $allVisibleGroupIds  = [10, 11, 14, 17, 18 ,19, 21, 22]; 
        $teamVisibleGroupIds = [17, 18];
        $selfVisibleGroupIds = [10, 11, 14 ,19, 21, 22]; 

        $allowed = [];

        // 特殊 admin：可看指定 group_id 的所有人
        if (in_array($currentAdminId, $specialAdminIds, true)) {
            $allowed = Db::name('admin')
                ->where('group_id', 'in', $allVisibleGroupIds)
                ->where('username', '<>', '')
                ->column('username');

        // 团队角色：可看本 team_name 下所有人
        } elseif (in_array($currentGroupId, $teamVisibleGroupIds, true) && $currentTeamName !== '') {
            $allowed = Db::name('admin')
                ->where('team_name', $currentTeamName)
                ->where('username', '<>', '')
                ->column('username');

        // 自己可见角色：只看自己
        } elseif (in_array($currentGroupId, $selfVisibleGroupIds, true) && $currentUsername !== '') {
            $allowed = [$currentUsername];

        // 兜底：只看自己
        } elseif ($currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        // 统一清洗：trim / 去空 / 去重
        $allowed = array_values(array_unique(array_filter(array_map('trim', (array) $allowed))));

        // 兜底：若前面逻辑异常导致结果为空，但当前用户名有效，则只返回自己
        if (empty($allowed) && $currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        return $allowed;
    }

    /**
     * 兼容旧方法名，内部统一调用 getCheckClientAllowedUsernames
     */
    private function getCheckClientVisibleUsernames(): array
    {
        return $this->getCheckClientAllowedUsernames();
    }

    /**
     * 检查客户页超级管理员识别
     * - 与历史习惯兼容：admin_id=1 或 group_id=1 均视为超级管理员
     */
    private function isCheckClientSuperAdmin(): bool
    {
        $adminId = (int) Session::get('aid');
        $groupId = (int) Session::get('group_id');
        $specialAdminIds = [395, 350, 375, 387,391, 405];

        return (
            $adminId === 1
            || $groupId === 1
            || in_array($adminId, $specialAdminIds, true)
        );
    }

    //（检查客户）
    public function checkClient()
    {
        if (request()->isPost()) {
            // 统一走 checkClientSearch，确保“首次加载”与“查询”口径完全一致
            return $this->checkClientSearch();
        }

        // 页面渲染所需下拉数据
        $khRankList  = $this->getClientRankOptions();
        $inquiryList = Db::table('crm_inquiry')->select();        // 所属渠道下拉数据
        $khStatusList = Db::table('crm_client_status')->select();
        $xsSourceList = Db::table('crm_clues_source')->select();

        // 统一使用“检查客户业务员可见名单”生成业务员下拉
        $allowedUsernames = $this->getCheckClientAllowedUsernames();

        $adminResult = [];
        if ($this->isCheckClientSuperAdmin()) {
            $adminResult = Db::name('admin')
                ->field('admin_id,username')
                ->where('group_id', '<>', 1)
                ->where('username', '<>', '')
                ->order('admin_id', 'asc')
                ->select();
        } elseif (!empty($allowedUsernames)) {
            $adminResult = Db::name('admin')
                ->field('admin_id,username')
                ->where('username', 'in', $allowedUsernames)
                ->order('admin_id', 'asc')
                ->select();
        }

        $yyList = $this->getYyList();
        $this->assign('adminResult', $adminResult);
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);
        $this->assign('xsSourceList', $xsSourceList);  //线索/客户来源


        

        return $this->fetch('checkclient/index');
    }


    //检查订单（调度：View / Service；时间条件与组织条件沿用 Common）
    public function checkOrder()
    {
        if (request()->isPost()) {
            $params = Request::param();
            if (!isset($params['keyword'])) {
                $params['keyword'] = [];
            }
            if (!isset($params['keyword']['timebucket']) && !isset($params['keyword']['at_time'])) {
                $params['keyword']['timebucket'] = 'month';
            }
            Request::merge($params);
            return $this->checkOrderSearch();
        }

        $svc = new CheckOrderService();
        $assign = $svc->getPageAssignData(
            $this->getCheckClientAllowedUsernames(),
            function () {
                return $this->getTeamList();
            }
        );
        $this->assign($assign);
        $this->assign('customer_type', Order::CUSTOMER_TYPE);
        return $this->fetch();
    }

    /**
     * 检查订单列表数据（供 POST JSON；业务见 CheckOrderService）
     */
    public function checkOrderSearch()
    {
        $keyword = Request::param('keyword', []);
        $svc = new CheckOrderService();
        return json($svc->search(
            is_array($keyword) ? $keyword : [],
            input('page') ?? 1,
            input('limit') ?? config('pageSize'),
            $this->getCheckClientAllowedUsernames(),
            trim((string) Session::get('username')),
            function ($timeCondition, $field) {
                return $this->buildTimeWhere($timeCondition, $field);
            },
            function ($org, $alias = '') {
                return $this->getOrgWhere($org, $alias);
            }
        ));
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

            $this->appendKhRankDisplayForRows($rows);

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
        $khRankList   = $this->getClientRankOptions();
        $inquiryList  = Db::table('crm_inquiry')->select();    // **新增：所属渠道列表**  
        $xsSourceList = Db::table('crm_clues_source')->select();
        $yyList       = $this->getYyList();
        $this->assign('_yyList', json_encode($yyList['_yyList']));
        $this->assign('khRankList', $khRankList);
        $this->assign('inquiryList', $inquiryList);            // **新增：分配所属渠道下拉数据**  
        $this->assign('xsSourceList', $xsSourceList);



        return $this->fetch('jointclient/index');
    }

    /**
     * 获取未删除的客户级别下拉数据（按 sort、id 升序）
     * 用途：添加客户、各页面高级查询、编辑客户的正常候选项
     */
    private function getClientRankOptions()
    {
        return Db::table('crm_client_rank')
            ->where('is_deleted', 0)
            ->field('id,rank_name,rank_code,sort')
            ->order('sort asc,id asc')
            ->select();
    }

    /**
     * 编辑客户专用：获取客户级别下拉列表
     * 如果当前客户绑定的是已删除级别，额外补入该条并标注"（已删除）"
     * @param mixed $currentKhRank 当前客户的 kh_rank 值
     * @return array 每项含 id, rank_name, rank_name_display
     */
    private function getClientRankOptionsForEdit($currentKhRank)
    {
        $activeList = Db::table('crm_client_rank')
            ->where('is_deleted', 0)
            ->field('id,rank_name,rank_code,sort,is_deleted')
            ->order('sort asc,id asc')
            ->select();

        $result = [];
        foreach ($activeList as $row) {
            $row['rank_name_display'] = $row['rank_name'];
            $result[] = $row;
        }

        $raw = trim((string)$currentKhRank);
        if ($raw !== '' && preg_match('/^\d+$/', $raw)) {
            $rankId = (int)$raw;
            $alreadyInList = false;
            foreach ($result as $r) {
                if ((int)$r['id'] === $rankId) {
                    $alreadyInList = true;
                    break;
                }
            }
            if (!$alreadyInList && $rankId > 0) {
                $deletedRow = Db::table('crm_client_rank')
                    ->where('id', $rankId)
                    ->where('is_deleted', 1)
                    ->field('id,rank_name,rank_code,sort,is_deleted')
                    ->find();
                if ($deletedRow) {
                    $deletedRow['rank_name_display'] = $deletedRow['rank_name'] . '（已删除）';
                    $result[] = $deletedRow;
                }
            }
        }

        return $result;
    }

    /**
     * 将 kh_rank（可能是ID/旧名称/空）标准化为可回显的 rank_id
     * 查询全量（含已删除），确保历史数据也能正确回显
     * @param mixed $rawKhRank
     * @param array $rankList
     * @return string
     */
    private function normalizeKhRankToId($rawKhRank, array $rankList = [])
    {
        $raw = trim((string)$rawKhRank);
        if ($raw === '') {
            return '';
        }

        if (empty($rankList)) {
            $rankList = Db::table('crm_client_rank')
                ->field('id,rank_name')
                ->select();
        }

        $idMap = [];
        $nameMap = [];
        foreach ($rankList as $rankRow) {
            $id = (string)($rankRow['id'] ?? '');
            $name = trim((string)($rankRow['rank_name'] ?? ''));
            if ($id !== '') {
                $idMap[$id] = $id;
            }
            if ($name !== '') {
                $nameMap[$name] = $id;
            }
        }

        if (preg_match('/^\d+$/', $raw) && isset($idMap[$raw])) {
            return $idMap[$raw];
        }

        return $nameMap[$raw] ?? '';
    }

    /**
     * 保存时校验并规范 kh_rank（兼容软删除）
     * @param mixed  $rawKhRank    前端提交的 kh_rank
     * @param string $oldKhRank    编辑时数据库中的原始 kh_rank（新增时传空字符串）
     * @return array [bool, string, string, string] [是否通过, 错误信息, 入库值, 展示名]
     */
    private function validateKhRankForSave($rawKhRank, $oldKhRank = '')
    {
        $raw = trim((string)$rawKhRank);
        if ($raw === '') {
            return [true, '', '', ''];
        }

        if (!preg_match('/^\d+$/', $raw)) {
            return [false, '客户级别参数不合法，请重新选择', '', ''];
        }

        $rankId = (int)$raw;
        if ($rankId <= 0) {
            return [false, '客户级别参数不合法，请重新选择', '', ''];
        }

        $rankRow = Db::table('crm_client_rank')
            ->where('id', $rankId)
            ->field('id,rank_name,is_deleted')
            ->find();
        if (empty($rankRow)) {
            return [false, '客户级别不存在或已失效，请重新选择', '', ''];
        }

        if ((int)$rankRow['is_deleted'] === 1) {
            $oldRaw = trim((string)$oldKhRank);
            if ($oldRaw === (string)$rankId) {
                return [true, '', (string)$rankId, trim((string)$rankRow['rank_name'])];
            }
            return [false, '客户级别不存在或已失效，请重新选择', '', ''];
        }

        return [true, '', (string)$rankId, trim((string)$rankRow['rank_name'])];
    }

    /**
     * 解析客户级别展示名称（兼容新ID与旧名称）
     * @param mixed $rawKhRank
     * @param array $rankNameMap id => rank_name
     * @return string
     */
    private function resolveKhRankDisplayName($rawKhRank, array $rankNameMap = [])
    {
        $raw = trim((string)$rawKhRank);
        if ($raw === '') {
            return '';
        }

        if (preg_match('/^\d+$/', $raw)) {
            if (isset($rankNameMap[$raw]) && trim((string)$rankNameMap[$raw]) !== '') {
                return trim((string)$rankNameMap[$raw]);
            }
            return $raw;
        }
        return $raw;
    }

    /**
     * 批量补齐客户级别展示字段（并覆盖 kh_rank 显示值）
     * @param array $rows
     * @return void
     */
    private function appendKhRankDisplayForRows(&$rows)
    {
        if (empty($rows) || !is_array($rows)) {
            return;
        }

        $rankMap = Db::table('crm_client_rank')->column('rank_name', 'id');
        $rankNameMap = [];
        foreach ($rankMap as $id => $name) {
            $rankNameMap[(string)$id] = trim((string)$name);
        }

        foreach ($rows as &$row) {
            $rankName = $this->resolveKhRankDisplayName($row['kh_rank'] ?? '', $rankNameMap);
            $row['kh_rank_text'] = $rankName;
            $row['kh_rank_name'] = $rankName;
            $row['kh_rank'] = $rankName;
        }
        unset($row);
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

        // E. 客户级别展示兼容：新ID映射名称，旧名称原样显示
        $this->appendKhRankDisplayForRows($rows);

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

    /**
     * 批量补充成交客户关联订单汇总字段
     *
     * @param array $rows
     * @return void
     */
    private function appendSuccessClientOrderSummary(array &$rows): void
    {
        if (empty($rows)) {
            return;
        }

        $leadIds = array_values(array_unique(array_filter(array_map('intval', array_column($rows, 'id')))));
        if (empty($leadIds)) {
            return;
        }

        $summaryMap = (new SuccessClientOrderService())->getOrderSummaryByLeadIds($leadIds);
        foreach ($rows as &$row) {
            $leadId = (int)($row['id'] ?? 0);
            $summary = $summaryMap[$leadId] ?? [
                'order_count' => 0,
                'order_amount_total' => 0,
                'profit_total' => 0,
                'order_summary_text' => '0单 / ¥0 / 利润¥0',
            ];
            $row['order_count'] = (int)($summary['order_count'] ?? 0);
            $row['order_amount_total'] = (float)($summary['order_amount_total'] ?? 0);
            $row['profit_total'] = (float)($summary['profit_total'] ?? 0);
            $row['order_summary_text'] = (string)($summary['order_summary_text'] ?? '0单 / ¥0 / 利润¥0');
        }
        unset($row);
    }

    /**
     * 跟进筛选参数归一（follow_filter + follow_days -> __follow_*）
     */
    private function normalizeFollowFilterKeyword(array $keyword): array
    {
        $followFilter = isset($keyword['follow_filter']) ? trim((string)$keyword['follow_filter']) : '';
        $followDays = isset($keyword['follow_days']) ? (int)$keyword['follow_days'] : 0;

        if ($followFilter !== '' && $followDays > 0) {
            $keyword['__follow_filter'] = $followFilter;
            $keyword['__follow_boundary'] = date('Y-m-d H:i:s', time() - $followDays * 86400);
        }

        unset($keyword['follow_filter'], $keyword['follow_days']);
        return $keyword;
    }

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
            $this->appendSuccessClientOrderSummary($rows);
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
        $khRankList   = $this->getClientRankOptions();
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
        // pr_user_id：负责人生命周期初始化（createInitialOwnerStage）强制要求 owner.user_id > 0，
        // 与 Client::add() 保持一致，直接取当前登录人的 admin_id
        $pr_user_id = (int)Session::get('aid');

        Db::startTrans();
        try {
            $insertedCount = 0;
            $contactsData = [];
            $ownerHistoryService = new ClientOwnerHistoryService();

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
                    'pr_user_id'   => $pr_user_id,
                    'ut_time'      => $current_time,
                    'at_time'      => $current_time,
                    'at_user'      => $pr_user,
                    'status'       => 1,
                    'ispublic'     => 3,
                    'pr_user_bef'  => $pr_user,
                ];

                $leadsId = Db::name('crm_leads')->insertGetId($leadsRow);
                if (!$leadsId) {
                    throw new \Exception('客户主表数据插入失败');
                }

                // 负责人生命周期初始化：必须使用最终写入 crm_leads 的负责人信息（pr_user/pr_user_id）。
                // 失败将抛出异常，交由外层 catch 触发整批事务 rollback，避免出现
                // crm_leads 存在但 crm_client_owner_history 不存在的情况。
                $ownerHistoryService->createInitialOwnerStage(
                    $leadsId,
                    [
                        'user_id'   => $pr_user_id,
                        'user_name' => trim((string)$pr_user),
                    ],
                    [
                        'admin_id' => $pr_user_id,
                        'username' => trim((string)$pr_user),
                    ],
                    0,
                    $current_time,
                    'excel_import',
                    'Excel导入初始化负责人生命周期'
                );

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
        list($ok, $errMsg, $mainContacts, $auxContact, $mainPhones, $aux) = $this->parsePhoneWithPositionTitles();
        if (!$ok) {
            return [false, $errMsg];
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

    // 解析主/辅电话与职位身份，确保电话与职位身份一一对应
    private function parsePhoneWithPositionTitles(): array
    {
        $phonesRaw = Request::param('more_phones');
        $titleIdsRaw = Request::param('more_position_title_ids');
        if ($titleIdsRaw === null) {
            $titleIdsRaw = Request::param('more_phone_title_ids');
        }
        $titlesRaw = Request::param('more_phone_titles');
        if ($titlesRaw === null) {
            $titlesRaw = Request::param('more_position_titles');
        }

        $phones = $this->parseTextArray($phonesRaw);
        $titleIds = $this->parseTextArray($titleIdsRaw);
        $titles = $this->parseTextArray($titlesRaw);
        $size = max(count($phones), count($titleIds), count($titles));

        $mainContacts = [];
        $mainPhones = [];
        for ($i = 0; $i < $size; $i++) {
            $phone = preg_replace('/\D/', '', (string)($phones[$i] ?? ''));
            $titleIdRaw = $titleIds[$i] ?? '';
            $legacyTitle = trim((string)($titles[$i] ?? ''));

            // 空行过滤
            if ($phone === '') {
                continue;
            }
            if (!preg_match('/^\d{11}$/', $phone)) {
                return [false, '号码应为11位纯数字', [], null, [], ''];
            }

            list($titleOk, $titleErrMsg, $titlePayload) = $this->resolvePositionTitleSelection($titleIdRaw, $legacyTitle);
            if (!$titleOk) {
                return [false, $titleErrMsg, [], null, [], ''];
            }

            // 第1行职位身份可空；第2行开始必填（兼容旧文本）
            if ($i > 0 && empty($titlePayload['position_title_id']) && $titlePayload['position_title'] === '') {
                return [false, '第' . ($i + 1) . '个主电话请填写职位身份', [], null, [], ''];
            }
            if (in_array($phone, $mainPhones, true)) {
                continue;
            }
            $mainPhones[] = $phone;
            $mainContacts[] = [
                'contact_type' => 1,
                'contact_value' => $phone,
                'position_title' => $titlePayload['position_title'],
                'position_title_id' => $titlePayload['position_title_id'],
            ];
        }

        if (empty($mainContacts)) {
            return [false, '主电话不能为空', [], null, [], ''];
        }

        $aux = preg_replace('/\D/', '', (string)Request::param('phone2', ''));
        $auxTitleIdRaw = Request::param('phone2_position_title_id', '');
        $auxLegacyTitle = trim((string)Request::param('phone2_position_title', ''));
        $auxContact = null;
        if ($aux !== '') {
            if (!preg_match('/^\d{11}$/', $aux)) {
                return [false, '辅助电话必须为11位数字', [], null, [], ''];
            }
            if (in_array($aux, $mainPhones, true)) {
                return [false, '主电话与辅助电话不能相同', [], null, [], ''];
            }

            list($auxTitleOk, $auxTitleErrMsg, $auxTitlePayload) = $this->resolvePositionTitleSelection($auxTitleIdRaw, $auxLegacyTitle);
            if (!$auxTitleOk) {
                return [false, $auxTitleErrMsg, [], null, [], ''];
            }
            if (empty($auxTitlePayload['position_title_id']) && $auxTitlePayload['position_title'] === '') {
                return [false, '填写辅助电话时，职位身份必填', [], null, [], ''];
            }
            $auxContact = [
                'contact_type' => 3,
                'contact_value' => $aux,
                'position_title' => $auxTitlePayload['position_title'],
                'position_title_id' => $auxTitlePayload['position_title_id'],
            ];
        }

        return [true, '', $mainContacts, $auxContact, $mainPhones, $aux];
    }

    // 解析纯文本数组：支持数组 / JSON字符串 / 常见分隔符字符串
    private function parseTextArray($raw): array
    {
        if (is_array($raw)) {
            return $raw;
        }
        if (is_string($raw)) {
            $str = trim($raw);
            if ($str === '') {
                return [];
            }
            if ($str[0] === '[') {
                $tmp = json_decode($str, true);
                return is_array($tmp) ? $tmp : [$str];
            }
            $str = str_replace(["，", "、", "；", ";", "|", "\r\n", "\n", "\t"], ',', $str);
            return preg_split('/[\\s,]+/', $str);
        }
        if ($raw !== null) {
            return [(string)$raw];
        }
        return [];
    }

    private function getPositionTitleService(): PositionTitleService
    {
        return new PositionTitleService();
    }

    private function getClientDetailService()
    {
        return new ClientDetailService();
    }

    /**
     * 根据职位身份ID（新字段）或职位文本（旧字段）解析标准值
     *
     * @param mixed $titleIdRaw
     * @param string $legacyTitle
     * @return array{0:bool,1:string,2:array{position_title_id:?int,position_title:string}}
     */
    private function resolvePositionTitleSelection($titleIdRaw, string $legacyTitle = ''): array
    {
        $titleId = (int)preg_replace('/\D/', '', (string)$titleIdRaw);
        $legacyTitle = trim($legacyTitle);
        $service = $this->getPositionTitleService();

        if ($titleId > 0) {
            list($ok, $msg, $row) = $service->validatePositionTitle($titleId);
            if (!$ok) {
                return [false, $msg, ['position_title_id' => null, 'position_title' => '']];
            }
            return [true, '', [
                'position_title_id' => (int)$row['id'],
                'position_title' => (string)$row['name'],
            ]];
        }

        if ($legacyTitle !== '') {
            $matched = $service->findByName($legacyTitle);
            if ($matched) {
                return [true, '', [
                    'position_title_id' => (int)$matched['id'],
                    'position_title' => (string)$matched['name'],
                ]];
            }
            return [true, '', [
                'position_title_id' => null,
                'position_title' => $legacyTitle,
            ]];
        }

        return [true, '', ['position_title_id' => null, 'position_title' => '']];
    }

    /**
     * 提取联系方式里的职位身份字段（兼容不同库结构）
     *
     * @param array $contact
     * @return array{position_title_id:?int,position_title:string}
     */
    private function extractContactPositionTitlePayload(array $contact): array
    {
        $titleId = isset($contact['position_title_id']) ? (int)$contact['position_title_id'] : 0;
        $title = trim((string)($contact['position_title'] ?? ''));
        if ($titleId > 0) {
            return ['position_title_id' => $titleId, 'position_title' => $title];
        }
        if ($title !== '') {
            list($ok, $msg, $resolved) = $this->resolvePositionTitleSelection('', $title);
            if ($ok) {
                return $resolved;
            }
        }
        return ['position_title_id' => null, 'position_title' => $title];
    }

    /**
     * 判断表字段是否存在，避免 Unknown column
     */
    private function tableHasColumn(string $table, string $column): bool
    {
        if (!isset($this->tableColumnsCache[$table])) {
            $this->tableColumnsCache[$table] = [];
            try {
                $rows = Db::query("SHOW COLUMNS FROM `{$table}`");
                foreach ($rows as $row) {
                    if (!empty($row['Field'])) {
                        $this->tableColumnsCache[$table][$row['Field']] = true;
                    }
                }
            } catch (\Throwable $e) {
                $this->tableColumnsCache[$table] = [];
            }
        }
        return !empty($this->tableColumnsCache[$table][$column]);
    }

    /**
     * 按表结构追加职位身份字段
     */
    private function appendPositionTitleFieldsBySchema(string $table, array &$row, array $titlePayload): void
    {
        if ($this->tableHasColumn($table, 'position_title_id')) {
            $row['position_title_id'] = !empty($titlePayload['position_title_id']) ? (int)$titlePayload['position_title_id'] : null;
        }
        if ($this->tableHasColumn($table, 'position_title')) {
            $row['position_title'] = (string)($titlePayload['position_title'] ?? '');
        }
    }

    // 解析运营端口：支持数组 / JSON字符串 / 逗号分隔；仅保留正整数并去重
    private function normalizePortIds($raw): array
    {
        $portIds = [];
        if (is_array($raw)) {
            $portIds = $raw;
        } else if (is_string($raw)) {
            $str = trim($raw);
            if ($str !== '') {
                if ($str[0] === '[') {
                    $tmp = json_decode($str, true);
                    if (is_array($tmp)) {
                        $portIds = $tmp;
                    } else {
                        $portIds = [$str];
                    }
                } else {
                    $portIds = explode(',', $str);
                }
            }
        } else if ($raw !== null) {
            $portIds = [(string)$raw];
        }

        $portIds = array_map(function ($v) {
            return preg_replace('/\D/', '', (string)$v);
        }, $portIds);

        return array_values(array_unique(array_filter($portIds, function ($v) {
            return $v !== '';
        })));
    }

    // 校验所属渠道 + 运营端口（必填 + 合法组合）
    private function validateInquiryAndPortData($inquiryId, $portIds): array
    {
        $inquiryId = (int)preg_replace('/\D/', '', (string)$inquiryId);
        if ($inquiryId <= 0) {
            return [false, '请选择所属渠道', 0, []];
        }

        $portIds = $this->normalizePortIds($portIds);
        if (empty($portIds)) {
            return [false, '请选择运营端口', $inquiryId, []];
        }

        $inquiryExists = Db::table('crm_inquiry')->where('id', $inquiryId)->find();
        if (!$inquiryExists) {
            return [false, '请选择所属渠道', $inquiryId, []];
        }

        $validCount = Db::table('crm_inquiry_port')
            ->where('inquiry_id', $inquiryId)
            ->whereIn('id', $portIds)
            ->count();
        if ((int)$validCount !== count($portIds)) {
            return [false, '运营端口数据无效，请重新选择', $inquiryId, []];
        }

        return [true, '', $inquiryId, $portIds];
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
            list($rankOk, $rankErrMsg, $khRankStore, $khRankName) = $this->validateKhRankForSave(Request::param('kh_rank', ''));
            if (!$rankOk) {
                $this->redisUnLock();
                return fail($rankErrMsg);
            }
            $data['kh_rank'] = $khRankStore;
            $inquiryId = Request::param('inquiry_id');
            $portIdsRaw = Request::param('port_id/a');
            if ($portIdsRaw === null) {
                $portIdsRaw = Request::param('port_id');
            }
            list($validInquiryPort, $inquiryPortMsg, $cleanInquiryId, $cleanPortIds) = $this->validateInquiryAndPortData($inquiryId, $portIdsRaw);
            if (!$validInquiryPort) {
                $this->redisUnLock();
                return fail($inquiryPortMsg);
            }
            $data['inquiry_id'] = $cleanInquiryId;
            $data['port_id'] = implode(',', $cleanPortIds);


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

            // 7) 解析主/辅电话及职位身份（严格一一对应）
            list($ok, $errMsg, $mainContacts, $auxContact, $mainPhones, $auxPhone) = $this->parsePhoneWithPositionTitles();
            if (!$ok) {
                $this->redisUnLock();
                return fail($errMsg);
            }

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
                // 兼容字段：首个主电话的职位身份同步到 crm_leads（仅字段存在时）
                $firstMainTitle = !empty($mainContacts) ? $this->extractContactPositionTitlePayload($mainContacts[0]) : ['position_title_id' => null, 'position_title' => ''];
                $leadsTitlePatch = [];
                $this->appendPositionTitleFieldsBySchema('crm_leads', $leadsTitlePatch, $firstMainTitle);
                if (!empty($leadsTitlePatch)) {
                    Db::table('crm_leads')->where('id', $id)->update($leadsTitlePatch);
                }
                // 循环处理所有主电话，每个作为一条联系记录
                foreach ($mainContacts as $item) {
                    $titlePayload = $this->extractContactPositionTitlePayload($item);
                    $contactRow = [
                        'leads_id'      => $id,
                        'contact_type'  => $item['contact_type'],
                        'contact_extra' => '',
                        'contact_value' => $item['contact_value'],
                        'vdigits'       => $item['contact_value'],
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                    $this->appendPositionTitleFieldsBySchema('crm_contacts', $contactRow, $titlePayload);
                    $contactsToInsert[] = $contactRow;
                }
                // 如果存在辅助电话，则一并加入待插入列表
                if ($auxContact) {
                    $auxTitlePayload = $this->extractContactPositionTitlePayload($auxContact);
                    $auxRow = [
                        'leads_id'      => $id,
                        'contact_type'  => $auxContact['contact_type'],
                        'contact_extra' => '',
                        'contact_value' => $auxContact['contact_value'],
                        'vdigits'       => $auxContact['contact_value'],
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                    $this->appendPositionTitleFieldsBySchema('crm_contacts', $auxRow, $auxTitlePayload);
                    $contactsToInsert[] = $auxRow;
                }
                // 批量插入所有联系方式
                if (!empty($contactsToInsert)) {
                    Db::table('crm_contacts')->insertAll($contactsToInsert);
                }

                // c) 操作日志
                $logId = $this->addOperLog(
                    $id,
                    '新增客户',
                    [
                        '运营人员' => $data['oper_user'],
                        '联系方式' => ['主电话' => $mainPhones, '辅助电话' => $auxPhone],
                        '客户级别' => $khRankName,
                        '协同人'  => $jpIds
                    ]
                );
                if ((int)$logId <= 0) {
                    throw new \Exception('新增客户操作日志写入失败');
                }

                // d) 负责人生命周期：为新客户创建第一条负责人任职阶段（source_type=create）
                //    owner/时间必须完全来自本次落库的 $data 快照，不重新查询、不重新猜测
                $owner = [
                    'user_id'   => (int)($data['pr_user_id'] ?? 0),
                    'user_name' => trim((string)($data['pr_user'] ?? '')),
                ];
                $operatorInfo = [
                    'admin_id' => (int)Session::get('aid'),
                    'username' => trim((string)Session::get('username')),
                ];
                $ownerHistoryService = new ClientOwnerHistoryService();
                $ownerHistoryService->createInitialOwnerStage(
                    $id,
                    $owner,
                    $operatorInfo,
                    (int)$logId,
                    $data['at_time'],
                    'create',
                    '新增客户创建负责人初始阶段'
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
            ->whereIn('group_id', [10, 11, 14,17,18, 19, 21, 22])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        foreach ($adminList as $admin) {
            $collaboratorData[] = ['name' => $admin['username'], 'value' => $admin['admin_id']];
        }
        $this->assign('collaboratorList', json_encode($collaboratorData, JSON_UNESCAPED_UNICODE));

        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        $portList    = Db::table('crm_inquiry_port')->where('status', 0)->select();
        $clientRankList = $this->getClientRankOptions();
        $positionTitleList = $this->getPositionTitleService()->getActivePositionTitleList();

        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);
        $this->assign('clientRankList', $clientRankList);
        $this->assign('positionTitleList', $positionTitleList);
        $this->assign('positionTitleListJson', json_encode($positionTitleList, JSON_UNESCAPED_UNICODE));

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

            $clientForPermission = Db::table('crm_leads')
                ->where('id', $id)
                ->field('id,pr_user,joint_person')
                ->find();
            if (!$clientForPermission) {
                $this->redisUnLock();
                return fail('客户不存在');
            }

            $canEditAnyClient = $this->canEditAnyClientForOrder();
            if (!$canEditAnyClient && !$this->canEditClientByOwnership($clientForPermission)) {
                $this->redisUnLock();
                return fail('您无此操作权限');
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
            $oldKhRankRow = Db::table('crm_leads')->where('id', $id)->field('kh_rank')->find();
            $oldKhRank = $oldKhRankRow ? trim((string)$oldKhRankRow['kh_rank']) : '';
            list($rankOk, $rankErrMsg, $khRankStore, $khRankName) = $this->validateKhRankForSave(Request::param('kh_rank', ''), $oldKhRank);
            if (!$rankOk) {
                $this->redisUnLock();
                return fail($rankErrMsg);
            }
            $data['kh_rank'] = $khRankStore;
            $inquiryId = Request::param('inquiry_id');
            $portIdsRaw = Request::param('port_id/a');
            if ($portIdsRaw === null) {
                $portIdsRaw = Request::param('port_id');
            }
            list($validInquiryPort, $inquiryPortMsg, $cleanInquiryId, $cleanPortIds) = $this->validateInquiryAndPortData($inquiryId, $portIdsRaw);
            if (!$validInquiryPort) {
                $this->redisUnLock();
                return fail($inquiryPortMsg);
            }
            $data['inquiry_id'] = $cleanInquiryId;
            $data['port_id'] = implode(',', $cleanPortIds);

            
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

            // 5) 解析主/辅电话及职位身份（严格一一对应）
            list($ok, $errMsg, $mainContacts, $auxContact, $mainPhones, $auxPhone) = $this->parsePhoneWithPositionTitles();
            if (!$ok) {
                $this->redisUnLock();
                return fail($errMsg);
            }


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
                // 兼容字段：首个主电话职位身份同步到 crm_leads（字段存在才写）
                $firstMainTitle = !empty($mainContacts) ? $this->extractContactPositionTitlePayload($mainContacts[0]) : ['position_title_id' => null, 'position_title' => ''];
                $leadsTitlePatch = [];
                $this->appendPositionTitleFieldsBySchema('crm_leads', $leadsTitlePatch, $firstMainTitle);
                if (!empty($leadsTitlePatch)) {
                    \think\Db::table('crm_leads')->where('id', $id)->update($leadsTitlePatch);
                }

                
                $now = date("Y-m-d H:i:s");
                $contactsToInsert = [];

                // 编辑按要求：删除旧联系方式（主电话/辅助电话）后重插
                \think\Db::table('crm_contacts')
                    ->where('leads_id', $id)
                    ->whereIn('contact_type', [1, 3])
                    ->delete();

                foreach ($mainContacts as $item) {
                    $titlePayload = $this->extractContactPositionTitlePayload($item);
                    $contactRow = [
                        'leads_id'      => $id,
                        'contact_type'  => $item['contact_type'],
                        'contact_extra' => '',
                        'contact_value' => $item['contact_value'],
                        'vdigits'       => $item['contact_value'],
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                    $this->appendPositionTitleFieldsBySchema('crm_contacts', $contactRow, $titlePayload);
                    $contactsToInsert[] = $contactRow;
                }
                if ($auxContact) {
                    $auxTitlePayload = $this->extractContactPositionTitlePayload($auxContact);
                    $auxRow = [
                        'leads_id'      => $id,
                        'contact_type'  => $auxContact['contact_type'],
                        'contact_extra' => '',
                        'contact_value' => $auxContact['contact_value'],
                        'vdigits'       => $auxContact['contact_value'],
                        'is_delete'     => 0,
                        'created_at'    => $now,
                    ];
                    $this->appendPositionTitleFieldsBySchema('crm_contacts', $auxRow, $auxTitlePayload);
                    $contactsToInsert[] = $auxRow;
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
                        '客户级别' => $khRankName,
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
        if (!$result) {
            return $this->error('客户不存在');
        }

        $clientForPermission = [
            'id' => (int)($result['id'] ?? 0),
            'pr_user' => (string)($result['pr_user'] ?? ''),
            'joint_person' => (string)($result['joint_person'] ?? ''),
        ];
        $canEditAnyClient = $this->canEditAnyClientForOrder();
        Log::write(
            'Client/edit permission debug: aid=' . session('aid')
            . ', gid=' . session('gid')
            . ', canEditAny=' . ($canEditAnyClient ? '1' : '0')
            . ', id=' . $id,
            'info'
        );
        if (!$canEditAnyClient && !$this->canEditClientByOwnership($clientForPermission)) {
            return $this->error('您无此操作权限');
        }

        // 主/辅电话：1 主、3 辅
        // [替换] 初始化主电话数组，支持多个主号
        $mainPhoneList = [];
        $auxPhone   = '';
        $auxPhonePositionTitle = '';
        $auxPhonePositionTitleId = '';
        $contactFields = ['contact_type', 'contact_value'];
        if ($this->tableHasColumn('crm_contacts', 'position_title')) {
            $contactFields[] = 'position_title';
        }
        if ($this->tableHasColumn('crm_contacts', 'position_title_id')) {
            $contactFields[] = 'position_title_id';
        }
        $contacts = \think\Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', $id)
            ->whereIn('contact_type', [1, 3])
            ->order('id', 'asc')
            ->field(implode(',', $contactFields))
            ->select();
        foreach ($contacts as $c) {
            $titlePayload = $this->extractContactPositionTitlePayload($c);
            if ($c['contact_type'] == 1) {
                $mainPhoneList[] = [
                    'phone' => (string)$c['contact_value'],
                    'position_title' => (string)$titlePayload['position_title'],
                    'position_title_id' => $titlePayload['position_title_id'],
                ];
            }
            if ($c['contact_type'] == 3 && $auxPhone === '') {
                $auxPhone = $c['contact_value'];
                $auxPhonePositionTitle = (string)$titlePayload['position_title'];
                $auxPhonePositionTitleId = $titlePayload['position_title_id'] ?: '';
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
            ->whereIn('group_id', [10, 11, 14,17,18, 19, 21, 22])
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
        $this->assign('mainPhoneList', json_encode($mainPhoneList, JSON_UNESCAPED_UNICODE));
        $this->assign('auxPhone',  $auxPhone);
        $this->assign('auxPhonePositionTitle',  $auxPhonePositionTitle);
        $this->assign('auxPhonePositionTitleId',  $auxPhonePositionTitleId);
        $positionTitleList = $this->getPositionTitleService()->getActivePositionTitleList();
        $this->assign('positionTitleList', $positionTitleList);
        $this->assign('positionTitleListJson', json_encode($positionTitleList, JSON_UNESCAPED_UNICODE));

        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        $portList    = Db::table('crm_inquiry_port')->where('status', 0)->select();

        $this->assign('channelList', $channelList);
        $this->assign('portList', $portList);

        // GET 加载老数据
        $result = Db::table('crm_leads')->where('id', $id)->find();
        $this->assign('result', $result);
        $clientRankList = $this->getClientRankOptionsForEdit($result['kh_rank'] ?? '');
        $khRankValue = $this->normalizeKhRankToId($result['kh_rank'] ?? '', $clientRankList);
        $this->assign('clientRankList', $clientRankList);
        $this->assign('khRankValue', $khRankValue);

        // 端口多选回显
        $selectedPorts = $result ? explode(',', $result['port_id']) : [];
        $this->assign('selectedPorts', $selectedPorts);
        
        return $this->fetch('client/edit');
    }

    // 客户详情（只读）
    public function details()
    {
        if (request()->isPost()) {
            return $this->error('请求方式错误');
        }

        $id = (int)\think\facade\Request::param('id', 0);
        if ($id <= 0) {
            return $this->error('参数错误');
        }

        $client = model('Client')->getClientDetailById($id);
        if (!$client) {
            return $this->error('客户不存在或已删除');
        }

        $clientForPermission = [
            'id' => (int)($client['id'] ?? 0),
            'pr_user' => (string)($client['pr_user'] ?? ''),
            'joint_person' => (string)($client['joint_person'] ?? ''),
        ];
        $canEditAnyClient = $this->canEditAnyClientForOrder();
        if (!$canEditAnyClient && !$this->canEditClientByOwnership($clientForPermission)) {
            return $this->error('您无此操作权限');
        }

        $viewData = $this->getClientDetailService()->buildDetailViewData($client);
        foreach ($viewData as $key => $value) {
            $this->assign($key, $value);
        }

        return $this->fetch('client/details');
    }

    /**
     * 客户详情页：历史审核通过订单列表
     */
    public function historyApprovedOrders()
    {
        $clientId = (int)Request::param('client_id/d', 0);
        $page = (int)Request::param('page/d', 1);
        $limit = (int)Request::param('limit/d', 10);
        $excludeOrderId = (int)Request::param('exclude_order_id/d', 0);

        if ($clientId <= 0) {
            return json(['code' => 500, 'msg' => '缺少客户ID', 'count' => 0, 'data' => []]);
        }

        $client = model('Client')->getClientDetailById($clientId);
        if (!$client) {
            return json(['code' => 500, 'msg' => '客户不存在或已删除', 'count' => 0, 'data' => []]);
        }

        // 与客户详情页保持一致的权限口径：超级权限或客户负责人/协同人
        $clientForPermission = [
            'id' => (int)($client['id'] ?? 0),
            'pr_user' => (string)($client['pr_user'] ?? ''),
            'joint_person' => (string)($client['joint_person'] ?? ''),
        ];
        $canEditAnyClient = $this->canEditAnyClientForOrder();
        if (!$canEditAnyClient && !$this->canEditClientByOwnership($clientForPermission)) {
            return json(['code' => 500, 'msg' => '您无此操作权限', 'count' => 0, 'data' => []]);
        }

        $service = new ClientOrderService();

        // 原有逻辑保持不变：crm_client_order 成交订单（此处取全量用于与历史订单合并后统一分页，
        // 不修改 getHistoryApprovedOrders 方法本身，不影响其独立调用场景）
        $normalResult = $service->getHistoryApprovedOrders($clientId, 1, self::MERGE_ORDER_FETCH_LIMIT, $excludeOrderId);
        $normalRows = [];
        foreach (($normalResult['data'] ?? []) as $row) {
            $row['order_type'] = 'normal';
            $normalRows[] = $row;
        }

        // 新增逻辑：crm_client_history_order 历史订单
        $historyResult = $service->getHistoryClientHistoryOrders($clientId);
        $historyRows = $historyResult['data'] ?? [];

        // 合并两个数据来源，按成交时间倒序排列后再统一分页返回给前端
        $mergedRows = array_merge($normalRows, $historyRows);
        usort($mergedRows, function ($a, $b) {
            $timeA = strtotime((string)($a['order_time'] ?? '')) ?: 0;
            $timeB = strtotime((string)($b['order_time'] ?? '')) ?: 0;
            if ($timeA === $timeB) {
                return ((int)($b['id'] ?? 0)) <=> ((int)($a['id'] ?? 0));
            }
            return $timeB <=> $timeA;
        });

        $totalCount = count($mergedRows);

        // 合计统计：基于合并后的全部订单（正式订单 + 历史订单），而非当前分页数据
        $totalMoney = 0;
        $totalProfit = 0;
        foreach ($mergedRows as $row) {
            $totalMoney += (float)($row['money'] ?? 0);
            $totalProfit += (float)($row['profit'] ?? 0);
        }
        $totalMoney = round($totalMoney, 2);
        $totalProfit = round($totalProfit, 2);

        $offset = ($page - 1) * $limit;
        $pagedRows = array_slice($mergedRows, max(0, $offset), $limit);

        return json([
            'code' => 0,
            'msg' => '',
            'count' => $totalCount,
            'data' => $pagedRows,
            'totalMoney' => $totalMoney,
            'totalProfit' => $totalProfit,
        ]);
    }


    //单个删除客户
    public function del()
    {
        $id = Request::post('id', '');
        $ids = Request::post('ids', []);

        if (is_string($ids)) {
            $ids = explode(',', $ids);
        }

        $idsArr = [];
        if (!empty($ids)) {
            $idsArr = is_array($ids) ? $ids : [$ids];
        } elseif ($id !== '' && $id !== null) {
            $idsArr = [$id];
        }

        $idsArr = array_values(array_unique(array_filter(array_map('intval', $idsArr))));

        if (empty($idsArr)) {
            return json(['code' => 500, 'msg' => '请选择要删除的客户', 'data' => []]);
        }

        $username = Session::get('username');
        $aid = Session::get('aid'); // 获取管理员ID
        
        Db::startTrans();
        try {
            // 查询客户信息
            $clientQuery = Db::name('crm_leads')->where('id', 'in', $idsArr);
            
            // 如果不是超级管理员，需要验证权限
            if ($aid != 1) {
                $clientQuery->where(function ($query) use ($username) {
                    $query->where('pr_user', $username)
                        ->whereOr('pr_user_bef', $username);
                });
            }
            
            $clients = $clientQuery->field('id,kh_name')->select();

            if (empty($clients)) {
                throw new \Exception(count($idsArr) > 1 ? '无权限删除选中客户或客户不存在' : '客户不存在或无权限删除');
            }

            $allowedIds = [];
            $clientMap = [];
            foreach ($clients as $client) {
                $cid = (int)($client['id'] ?? 0);
                if ($cid <= 0) {
                    continue;
                }
                $allowedIds[] = $cid;
                $clientMap[$cid] = $client;
            }

            if (empty($allowedIds)) {
                throw new \Exception(count($idsArr) > 1 ? '无权限删除选中客户或客户不存在' : '客户不存在或无权限删除');
            }

            // 删除客户的联系方式
            Db::name('crm_contacts')->where('leads_id', 'in', $allowedIds)->delete();
            
            // 删除客户主记录
            $deletedCount = Db::name('crm_leads')->where('id', 'in', $allowedIds)->delete();

            Db::commit();
            
            //写入操作日志
            if (count($allowedIds) === 1) {
                $onlyId = $allowedIds[0];
                $this->addOperLog(
                    $onlyId,
                    '删除客户',
                    "$username 删除客户ID:" . $onlyId . ', 客户名称:' . ($clientMap[$onlyId]['kh_name'] ?? '') . ($aid == 1 ? ' [超级管理员操作]' : '')
                );
            } else {
                $this->addOperLog(
                    null,
                    '批量删除客户',
                    "$username 批量删除客户:" . implode(',', $allowedIds) . ', 共' . count($allowedIds) . '条' . ($aid == 1 ? ' [超级管理员操作]' : '')
                );
            }

            $requestCount = count($idsArr);
            $successCount = (int)$deletedCount;
            $failCount = max(0, $requestCount - $successCount);

            if ($successCount <= 0) {
                return json(['code' => 500, 'msg' => '删除失败', 'data' => []]);
            }

            if ($failCount > 0) {
                return json([
                    'code' => 0,
                    'msg' => '成功删除' . $successCount . '个客户，' . $failCount . '个客户不存在或无权限删除',
                    'data' => [
                        'success_count' => $successCount,
                        'fail_count' => $failCount
                    ]
                ]);
            }
            
            return json([
                'code' => 0,
                'msg' => (count($idsArr) > 1 ? '成功删除' . $successCount . '个客户' : '删除成功'),
                'data' => [
                    'success_count' => $successCount,
                    'fail_count' => 0
                ]
            ]);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => $e->getMessage(), 'data' => []]);
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
            // 获取原级别
            $oldstatus = Db::table('crm_client_rank')->where(['id' => $data['id']])->find();
            $oldstatusname = $oldstatus['rank_name'] ?? '';
            if ($oldstatusname == ($data['rank_name'] ?? '')) {
                $msg = ['code' => 500, 'msg' => '没有变化无需修改', 'data' => []];
                return json($msg);
            }

            $result = Db::table('crm_client_rank')->where(['id' => $data['id']])->update($data);
            if ($result) {
                // 兼容改造后 crm_leads.kh_rank 统一存级别ID，不再随名称变更批量更新
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
    //删除客户级别（软删除）
    public function rankDel()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return json(['code' => 500, 'msg' => '参数错误', 'data' => []]);
        }

        $row = Db::table('crm_client_rank')->where('id', $id)->find();
        if (empty($row)) {
            return json(['code' => 500, 'msg' => '记录不存在', 'data' => []]);
        }
        if ((int)($row['is_deleted'] ?? 0) === 1) {
            return json(['code' => 500, 'msg' => '该客户级别已处于删除状态，无需重复操作', 'data' => []]);
        }

        $result = Db::table('crm_client_rank')->where('id', $id)->update([
            'is_deleted'   => 1,
            'deleted_time' => date('Y-m-d H:i:s'),
            'deleted_by'   => session('aid'),
            'update_time'  => time(),
        ]);
        if ($result) {
            return json(['code' => 0, 'msg' => '删除成功！', 'data' => []]);
        } else {
            return json(['code' => 500, 'msg' => '删除失败！', 'data' => []]);
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
        $idsParam = Request::param('ids', '');
        if (is_array($idsParam)) {
            $idsParam = implode(',', $idsParam);
        }

        $ids = [];
        foreach (explode(',', (string)$idsParam) as $idValue) {
            $id = (int)trim((string)$idValue);
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }
        $ids = array_values($ids);

        if (empty($ids)) {
            $msg = ['code' => 500, 'msg' => '请选择客户', 'data' => []];
            return Request::isAjax() ? json($msg) : $this->error('请选择客户');
        }

        $idsStr = implode(',', $ids);
        $this->assign('ids', $idsStr);

        if (Request::isAjax()) {
            $prGhTypeId = (int)Request::param('pr_gh_type', 0);
            $manualReason = trim((string)Request::param('reason', ''));
            if ($prGhTypeId <= 0) {
                return json(['code' => 500, 'msg' => '请选择公海类型', 'data' => []]);
            }

            $liberumType = Db::table('crm_liberum_type')
                ->where('id', $prGhTypeId)
                ->where('is_deleted', 0)
                ->field('id,type_name')
                ->find();
            if (empty($liberumType)) {
                return json(['code' => 500, 'msg' => '请选择有效的公海类型。', 'data' => []]);
            }

            $leadRows = Db::table('crm_leads')
                ->whereIn('id', $ids)
                ->field('id,kh_name,issuccess,status,pr_user,pr_user_id,pr_user_bef,pr_user_bef_id,pr_gh_type')
                ->select();
            if (is_object($leadRows) && method_exists($leadRows, 'toArray')) {
                $leadRows = $leadRows->toArray();
            } elseif (!is_array($leadRows)) {
                $leadRows = [];
            }
            if (empty($leadRows) || count($leadRows) !== count($ids)) {
                return json(['code' => 500, 'msg' => '客户不存在或已被处理', 'data' => []]);
            }
            $leadMap = [];
            foreach ($leadRows as $leadRow) {
                $leadMap[(int)$leadRow['id']] = $leadRow;
            }
            if (count($leadMap) !== count($ids)) {
                return json(['code' => 500, 'msg' => '客户不存在或已被处理', 'data' => []]);
            }
            foreach ($leadMap as $leadRow) {
                if ((int)($leadRow['status'] ?? 0) !== 1) {
                    return json(['code' => 500, 'msg' => '选中的客户中包含非客户状态数据，禁止移入公海', 'data' => []]);
                }
            }
            foreach ($leadMap as $leadRow) {
                if ((int)($leadRow['issuccess'] ?? 0) === 1) {
                    $errorMsg = count($ids) > 1
                        ? '选中的客户中包含已成交客户，禁止移入公海'
                        : '这个客户是已成交客户，禁止移入公海';
                    return json(['code' => 500, 'msg' => $errorMsg, 'data' => []]);
                }
            }

            $now = date('Y-m-d H:i:s');
            $count = 0;
            Db::startTrans();
            try {
                foreach ($ids as $leadId) {
                    if (empty($leadMap[$leadId])) {
                        throw new \RuntimeException('客户不存在或已被处理');
                    }
                    $leadRow = $leadMap[$leadId];
                    $updateData = [
                        'status' => 2,
                        'to_gh_time' => $now,
                        'pr_gh_type' => $prGhTypeId,
                    ];
                    if (trim((string)($leadRow['pr_user_bef'] ?? '')) === '') {
                        $updateData['pr_user_bef'] = (string)($leadRow['pr_user'] ?? '');
                    }
                    if ((int)($leadRow['pr_user_bef_id'] ?? 0) <= 0 && (int)($leadRow['pr_user_id'] ?? 0) > 0) {
                        $updateData['pr_user_bef_id'] = (int)$leadRow['pr_user_id'];
                    }

                    $result = Db::table('crm_leads')
                        ->where('id', $leadId)
                        ->where('status', 1)
                        ->where('issuccess', -1)
                        ->update($updateData);
                    if ($result !== 1) {
                        throw new \RuntimeException('选中的客户状态发生变化，请刷新后重试');
                    }

                    $inLogSaved = $this->recordManualLiberumInLog(
                        $leadRow,
                        $prGhTypeId,
                        (string)($liberumType['type_name'] ?? ''),
                        $manualReason,
                        $now
                    );
                    if (!$inLogSaved) {
                        throw new \RuntimeException('流入公海记录写入失败，请重试');
                    }

                    $count++;

                    $this->addOperLog(
                        $leadId,
                        '移入公海',
                        "移入 [{$liberumType['type_name']}] 公海池"
                    );
                }

                if ($count !== count($ids)) {
                    throw new \RuntimeException('转入公海失败！');
                }
                Db::commit();
                return json(['code' => 0, 'msg' => $count . '个客户移入公海成功！', 'data' => []]);
            } catch (\Throwable $e) {
                Db::rollback();
                Log::error('toMoveGh failed: ' . $e->getMessage());
                return json(['code' => 500, 'msg' => $e->getMessage() ?: '转入公海失败！', 'data' => []]);
            }
        }

        $leadList = Db::table('crm_leads')
            ->whereIn('id', $ids)
            ->field('id,kh_name')
            ->select();
        if (is_object($leadList) && method_exists($leadList, 'toArray')) {
            $leadList = $leadList->toArray();
        } elseif (!is_array($leadList)) {
            $leadList = [];
        }
        $leadNameMap = [];
        foreach ($leadList as $leadRow) {
            $leadNameMap[(int)$leadRow['id']] = (string)($leadRow['kh_name'] ?? '');
        }

        $contactRows = Db::table('crm_contacts')
            ->whereIn('leads_id', $ids)
            ->where('is_delete', 0)
            ->whereIn('contact_type', [1, 3])
            ->field('leads_id,contact_type,contact_value')
            ->order('id asc')
            ->select();
        if (is_object($contactRows) && method_exists($contactRows, 'toArray')) {
            $contactRows = $contactRows->toArray();
        } elseif (!is_array($contactRows)) {
            $contactRows = [];
        }

        $contactMap = [];
        foreach ($contactRows as $contactRow) {
            $leadId = (int)($contactRow['leads_id'] ?? 0);
            if ($leadId <= 0) {
                continue;
            }
            if (!isset($contactMap[$leadId])) {
                $contactMap[$leadId] = ['main_phone' => '', 'aux_phone' => ''];
            }
            $contactValue = trim((string)($contactRow['contact_value'] ?? ''));
            if ($contactValue === '') {
                continue;
            }
            if ((int)$contactRow['contact_type'] === 1 && $contactMap[$leadId]['main_phone'] === '') {
                $contactMap[$leadId]['main_phone'] = $contactValue;
            }
            if ((int)$contactRow['contact_type'] === 3 && $contactMap[$leadId]['aux_phone'] === '') {
                $contactMap[$leadId]['aux_phone'] = $contactValue;
            }
        }

        $clientPhoneList = [];
        foreach ($ids as $leadId) {
            $mainPhone = isset($contactMap[$leadId]) ? (string)$contactMap[$leadId]['main_phone'] : '';
            $auxPhone = isset($contactMap[$leadId]) ? (string)$contactMap[$leadId]['aux_phone'] : '';
            if ($mainPhone !== '') {
                $displayPhone = $mainPhone;
            } elseif ($auxPhone !== '') {
                $displayPhone = $auxPhone;
            } else {
                $displayPhone = '未找到电话（客户ID：' . $leadId . '）';
            }
            $clientPhoneList[] = [
                'id' => $leadId,
                'kh_name' => isset($leadNameMap[$leadId]) ? $leadNameMap[$leadId] : '',
                'main_phone' => $mainPhone,
                'aux_phone' => $auxPhone,
                'display_phone' => $displayPhone,
            ];
        }
        $this->assign('clientPhoneList', $clientPhoneList);

        $libTypeList = Db::table('crm_liberum_type')
            ->where('is_deleted', 0)
            ->field('id,type_name')
            ->order('id asc')
            ->select();
        $this->assign('libTypeList', $libTypeList);

        return $this->fetch('client/move_gh');
    }

    /**
     * 记录客户管理手动移入公海日志（crm_liberum_in_log）
     */
    private function recordManualLiberumInLog(array $clientBefore, int $ghTypeId, string $ghTypeName = '', string $reason = '', string $inTime = ''): bool
    {
        $leadId = (int)($clientBefore['id'] ?? 0);
        if ($leadId <= 0) {
            return false;
        }
        if ((int)($clientBefore['status'] ?? 0) !== 1) {
            return false;
        }

        $nowTime = $inTime !== '' ? $inTime : date('Y-m-d H:i:s');
        $timestamp = time();
        $reasonText = trim($reason) !== '' ? trim($reason) : '手动移入公海';
        $operatorId = (int)Session::get('aid');
        $operatorName = (string)Session::get('username');
        $liberumTypeValue = $ghTypeId > 0 ? $ghTypeId : (int)($clientBefore['pr_gh_type'] ?? 0);
        if ($liberumTypeValue <= 0) {
            $latestLiberumType = (int)Db::table('crm_leads')
                ->where('id', $leadId)
                ->value('pr_gh_type');
            if ($latestLiberumType > 0) {
                $liberumTypeValue = $latestLiberumType;
            }
        }

        $inLogData = [];
        if ($this->tableHasColumn('crm_liberum_in_log', 'leads_id')) {
            $inLogData['leads_id'] = $leadId;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'kh_name')) {
            $inLogData['kh_name'] = (string)($clientBefore['kh_name'] ?? '');
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'before_pr_user')) {
            $inLogData['before_pr_user'] = (string)($clientBefore['pr_user'] ?? '');
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'before_status')) {
            $inLogData['before_status'] = 1;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'after_status')) {
            $inLogData['after_status'] = 2;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'liberum_type')) {
            $inLogData['liberum_type'] = $liberumTypeValue > 0 ? (int)$liberumTypeValue : 0;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'reason')) {
            $inLogData['reason'] = $reasonText;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'in_time')) {
            $inLogData['in_time'] = $nowTime;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'operator_id')) {
            $inLogData['operator_id'] = $operatorId;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'operator_name')) {
            $inLogData['operator_name'] = $operatorName;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'source_type')) {
            $inLogData['source_type'] = 'manual';
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'remark')) {
            $inLogData['remark'] = '客户管理手动移入公海';
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'is_recovered')) {
            $inLogData['is_recovered'] = 0;
        }
        if ($this->tableHasColumn('crm_liberum_in_log', 'create_time')) {
            $inLogData['create_time'] = $timestamp;
        }

        if (empty($inLogData)) {
            return false;
        }

        $insertRows = Db::table('crm_liberum_in_log')->insert($inLogData);
        return (int)$insertRows === 1;
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
        if (!empty($list) && !empty($list['data']) && is_array($list['data'])) {
            $this->enrichLeadsRows($list['data']);
        }
        return $result = ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }

    public function personClientSearch()
    {
        // TP5.1 建议使用 input() 获取参数
        $page    = input('page/d', 1);
        $limit   = input('limit/d', config('pageSize'));
        $keyword = input('keyword/a', []); // 强制为数组
        $keyword['kh_rank'] = isset($keyword['kh_rank']) ? trim((string)$keyword['kh_rank']) : '';

        // 处理时间范围筛选（与原有 buildTimeWhere 兼容）
        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }

        $keyword = $this->normalizeFollowFilterKeyword($keyword);

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

        $this->appendKhRankDisplayForRows($rows);
        return ['code' => 0, 'msg' => '获取成功!', 'data' => $rows, 'count' => $list['total'], 'rel' => 1];
    }

    //（检查客户）搜索
    public function checkClientSearch()
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

        $keyword = $this->normalizeFollowFilterKeyword($keyword);

        // 获取当前登录人可见的负责人列表（支持团队可见）
        $visibleUsers = $this->getCheckClientVisibleUsernames();

        // 取列表（保留你原来的模型查询逻辑）
        $currentAdmin = [
            'admin_id' => (int)Session::get('aid'),
            'group_id' => (int)Session::get('group_id'),
            'username' => (string)Session::get('username'),
            'is_super_admin' => $this->isCheckClientSuperAdmin() ? 1 : 0,
        ];
        $list = model('client')->getCheckClientSearchList($page, $limit, $keyword, $visibleUsers, $currentAdmin);

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

        $this->appendKhRankDisplayForRows($rows);
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

        $this->appendKhRankDisplayForRows($rows);
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
        if (empty($list) || empty($list['data'])) {
            return ['code' => 0, 'msg' => '获取成功!', 'data' => [], 'count' => 0, 'rel' => 1];
        }
        $this->enrichLeadsRows($list['data']);
        $this->appendSuccessClientOrderSummary($list['data']);
        return ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }
    // ====== 修改 chengjiaoClientSearch 结束 ======

    /**
     * 成交客户 - 关联订单明细
     */
    public function getSuccessClientOrders()
    {
        $id = (int)Request::param('id/d', 0);
        $page = (int)Request::param('page/d', 1);
        $limit = (int)Request::param('limit/d', 10);

        if ($id <= 0) {
            return json(['code' => 500, 'msg' => '缺少成交客户ID', 'count' => 0, 'data' => []]);
        }

        $client = model('client')->getSuccessClientById($id);
        if (empty($client) || (int)($client['issuccess'] ?? 0) !== 1) {
            return json(['code' => 500, 'msg' => '成交客户不存在', 'count' => 0, 'data' => []]);
        }

        $aid = (int)Session::get('aid');
        $username = trim((string)Session::get('username'));
        if ($aid !== 1) {
            $canView = false;
            if ($username !== '') {
                $canView = ((string)($client['pr_user'] ?? '') === $username) || ((string)($client['pr_user_bef'] ?? '') === $username);
            }
            if (!$canView) {
                return json(['code' => 500, 'msg' => '无权限查看该成交客户订单', 'count' => 0, 'data' => []]);
            }
        }

        $service = new SuccessClientOrderService();
        $result = $service->getOrderDetailsByLeadId($id, $page, $limit);

        return json([
            'code' => 0,
            'msg' => '获取成功',
            'count' => (int)($result['count'] ?? 0),
            'data' => $result['data'] ?? [],
        ]);
    }

    //兼容旧地址：转发到正式保存接口
    public function comment()
    {
        return $this->saveClientFollow();
    }

    // 保存客户跟进（供右侧新增跟进表单调用）
    public function saveClientFollow()
    {
        $leadsId = (int) Request::param('leads_id', 0);
        $content = trim((string) Request::param('content', ''));
        if ($content === '') {
            // 兼容历史前端字段 reply_msg
            $content = trim((string) Request::param('reply_msg', ''));
        }
        $nextUpTime = trim((string) Request::param('next_up_time', ''));

        if ($leadsId <= 0) {
            return json(['code' => 1, 'msg' => '缺少客户ID', 'data' => []]);
        }
        if ($content === '') {
            return json(['code' => 1, 'msg' => '请输入跟进内容', 'data' => []]);
        }

        $operatorInfo = [
            'admin_id' => (int) Session::get('aid'),
            'username' => trim((string) Session::get('username')),
        ];
        if ($operatorInfo['admin_id'] <= 0 || $operatorInfo['username'] === '') {
            return json(['code' => 1, 'msg' => '登录状态已失效，请重新登录', 'data' => []]);
        }

        $service = new ClientFollowService();
        $result = $service->saveFollow($leadsId, $content, $nextUpTime, $operatorInfo);

        $code = (int)($result['code'] ?? 1);
        $msg = (string)($result['msg'] ?? '保存失败');
        $data = is_array($result['data'] ?? null) ? $result['data'] : [];

        if ($code === 0) {
            $data['leads_id'] = isset($data['leads_id']) ? (int)$data['leads_id'] : $leadsId;
            $data['content'] = isset($data['reply_msg']) ? (string)$data['reply_msg'] : $content;
            $data['latest_follow_summary'] = $data['content'];
            $data['next_up_time'] = $service->formatNextUpTimeForDisplay($data['next_up_time'] ?? $nextUpTime);
        }

        return json([
            'code' => $code,
            'msg' => $msg,
            'data' => $data,
        ]);
    }

    /**
     * 批量快速添加跟进：我的客户列表（layui table）
     */
    public function quickFollowList()
    {
        $page = input('page/d', 1);
        $limit = input('limit/d', config('pageSize'));
        $keyword = input('keyword/a', []);

        if (!empty($keyword['timebucket'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['timebucket'], 'at_time');
        }
        if (!empty($keyword['at_time'])) {
            $keyword['timebucket'] = $this->buildTimeWhere($keyword['at_time'], 'at_time');
        }

        $keyword = is_array($keyword) ? $keyword : [];
        $keyword['kh_name'] = isset($keyword['kh_name']) ? trim((string)$keyword['kh_name']) : '';
        $keyword['phone'] = isset($keyword['phone']) ? trim((string)$keyword['phone']) : '';
        $keyword['product_name'] = isset($keyword['product_name']) ? trim((string)$keyword['product_name']) : '';
        $keyword['kh_rank'] = isset($keyword['kh_rank']) ? trim((string)$keyword['kh_rank']) : '';
        $keyword['inquiry_id'] = isset($keyword['inquiry_id']) ? trim((string)$keyword['inquiry_id']) : '';
        $keyword['port_id'] = isset($keyword['port_id']) ? trim((string)$keyword['port_id']) : '';
        // V2：颜色筛选，仅在拿到当前页数据+颜色标记后做内存过滤，不参与数据库查询，不影响原查询逻辑
        $colorFilter = isset($keyword['color_filter']) ? trim((string)$keyword['color_filter']) : '';
        unset($keyword['color_filter']);
        $keyword = $this->normalizeFollowFilterKeyword($keyword);

        $username = trim((string)Session::get('username'));
        if ($username === '') {
            return json(['code' => 1, 'msg' => '登录状态已失效，请重新登录', 'data' => [], 'count' => 0]);
        }

        $list = model('Client')->getQuickFollowList($page, $limit, $username, $keyword);
        if (empty($list) || empty($list['data'])) {
            return json(['code' => 0, 'msg' => '获取成功', 'data' => [], 'count' => 0]);
        }

        // 行颜色标记：仅查询当前登录员工自己的标记，颜色互不影响
        $adminId = (int)Session::get('aid');
        $leadsIds = array_column((array)$list['data'], 'id');
        $rowMarkService = new ClientRowMarkService();
        $markDetailMap = $rowMarkService->getMarksMapDetail($leadsIds, $adminId, 1);
        $markMap = [];
        $remarkMap = [];
        foreach ($markDetailMap as $lid => $detail) {
            $markMap[$lid] = (string)($detail['bg_color'] ?? '');
            $remarkMap[$lid] = (string)($detail['remark'] ?? '');
        }

        $service = new ClientFollowService();
        $rows = $service->buildQuickFollowListRows((array)$list['data'], $markMap, $remarkMap);

        // V2：颜色筛选（根据 admin_id + bg_color 在当前页数据上过滤，不改变数据库分页查询）
        if ($colorFilter !== '') {
            $rows = $rowMarkService->filterRowsByColor($rows, $colorFilter);
        }

        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => $rows,
            'count' => (int)($list['total'] ?? 0),
        ]);
    }

    /**
     * 批量快速添加跟进：保存行颜色标记（仅属于当前登录员工）
     */
    public function saveClientRowMark()
    {
        $leadsId = (int)Request::param('leads_id', 0);
        $bgColor = trim((string)Request::param('bg_color', ''));
        $markType = (int)Request::param('mark_type', 1);
        $remark = trim((string)Request::param('remark', ''));

        $adminId = (int)Session::get('aid');
        if ($adminId <= 0) {
            return json(['code' => 1, 'msg' => '登录状态已失效，请重新登录']);
        }

        $result = (new ClientRowMarkService())->saveMark($leadsId, $adminId, $bgColor, $markType, $remark);

        return json([
            'code' => (int)($result['code'] ?? 1),
            'msg' => (string)($result['msg'] ?? '保存失败'),
        ]);
    }

    /**
     * 批量快速添加跟进：批量保存行颜色标记（仅属于当前登录员工，admin_id 不信任前端）
     */
    public function batchSaveClientRowMark()
    {
        $leadsIds = Request::param('leads_ids/a', []);
        $bgColor = trim((string)Request::param('bg_color', ''));
        $remark = trim((string)Request::param('remark', ''));
        $markType = (int)Request::param('mark_type', 1);

        $adminId = (int)Session::get('aid');
        if ($adminId <= 0) {
            return json(['code' => 1, 'msg' => '登录状态已失效，请重新登录']);
        }

        $result = (new ClientRowMarkService())->batchSaveMark($leadsIds, $adminId, $bgColor, $remark, $markType);

        return json([
            'code' => (int)($result['code'] ?? 1),
            'msg' => (string)($result['msg'] ?? '保存失败'),
            'data' => $result['data'] ?? [],
        ]);
    }

    /**
     * 批量快速添加跟进：单行保存
     */
    public function quickSaveFollow()
    {
        $leadsId = (int)Request::param('leads_id', Request::param('leads_id/d', 0));
        $content = trim((string)Request::param('content', ''));
        $nextUpTime = trim((string)Request::param('next_up_time', ''));

        if ($leadsId <= 0) {
            return json(['code' => 1, 'msg' => '缺少客户ID', 'data' => []]);
        }
        if ($content === '') {
            return json(['code' => 1, 'msg' => '请输入跟进内容', 'data' => []]);
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => trim((string)Session::get('username')),
        ];

        $service = new ClientFollowService();
        $result = $service->saveFollow($leadsId, $content, $nextUpTime, $operatorInfo);
        return json($result);
    }

    // 获取客户详情和评论记录
    public function getClientDetailAndComments()
    {
        $clientId = Request::param('id');
        if (!$clientId) {
            return json(['code' => 1, 'msg' => '缺少客户ID参数']);
        }

        // 获取客户信息
        $client = model('Client')->getFollowClientDetailById($clientId);
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在']);
        }
        $followService = new ClientFollowService();
        $client = $followService->buildFollowClientDetailData($client, $clientId);
        $comments = Db::table('crm_comment')
            ->alias('c')
            ->leftJoin('admin a', 'a.admin_id = c.user_id')
            ->where([
                'c.leads_id' => (int)$clientId,
                'c.is_deleted' => 0
            ])
            ->field('c.id,c.leads_id,c.user_id,c.reply_msg,c.create_date,a.username as username')
            ->order('c.create_date desc')
            ->select();
        foreach ($comments as &$comment) {
            $ts = (int)($comment['create_date'] ?? 0);
            if ($ts > 0) {
                $comment['create_date'] = date('Y-m-d H:i:s', $ts);
            } else {
                $comment['create_date'] = '';
            }
            $comment['username'] = trim((string)($comment['username'] ?? ''));
        }
        unset($comment);

        return json([
            'code' => 0,
            'data' => [
                'client' => $client,
                'comments' => $comments
            ],
            'msg' => '获取成功'
        ]);
    }

    /**
     * 只读：客户负责人生命周期历史（全部客户列表入口）
     * 权限与全部客户列表可见范围一致，不写入负责人历史。
     */
    public function getOwnerHistory()
    {
        $id = (int)Request::param('id');
        if ($id <= 0) {
            return json(['code' => 1, 'msg' => '客户ID无效']);
        }

        $client = model('Client')->buildClientSearchAllBaseQuery([])
            ->where('l.id', $id)
            ->field('l.id,l.kh_name,l.pr_user,l.pr_user_id')
            ->find();
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在或无权查看']);
        }

        $result = (new ClientOwnerHistoryService())->getHistoryByLeadsId($id);

        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => [
                'client' => [
                    'id' => (int)($client['id'] ?? 0),
                    'kh_name' => (string)($client['kh_name'] ?? ''),
                    'current_owner' => (string)($client['pr_user'] ?? ''),
                    'current_owner_id' => (int)($client['pr_user_id'] ?? 0),
                ],
                'history' => $result['history'] ?? [],
                'anomaly' => [
                    'open_stage_count' => (int)($result['open_stage_count'] ?? 0),
                    'message' => (string)($result['anomaly_message'] ?? ''),
                ],
            ],
        ]);
    }

    /**
     * 删除跟进记录（逻辑删除）
     */
    public function deleteFollowComment()
    {
        if (!request()->isPost()) {
            return json(['code' => 1, 'msg' => '请求方式错误']);
        }

        $commentId = (int)Request::param('comment_id', 0);
        if ($commentId <= 0) {
            return json(['code' => 1, 'msg' => '缺少跟进记录ID']);
        }

        $operatorInfo = [
            'admin_id' => Session::get('aid'),
            'username' => Session::get('username'),
        ];
        if ((int)$operatorInfo['admin_id'] <= 0) {
            return json(['code' => 1, 'msg' => '登录状态已失效，请重新登录']);
        }

        $service = new ClientFollowService();
        $result = $service->deleteFollowComment($commentId, $operatorInfo);

        return json($result);
    }

    /**
     * 跟进弹窗 - 获取客户级别下拉候选项（AJAX）
     * 复用 getClientRankOptionsForEdit，兼容已删除级别
     */
    public function getFollowRankOptions()
    {
        $clientId = Request::param('id');
        if (!$clientId) {
            return json(['code' => 1, 'msg' => '缺少客户ID']);
        }

        $client = Db::table('crm_leads')->where('id', $clientId)->field('id,kh_rank')->find();
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在']);
        }

        $currentKhRank = trim((string)($client['kh_rank'] ?? ''));
        $normalizedId = $this->normalizeKhRankToId($currentKhRank);
        $options = $this->getClientRankOptionsForEdit($normalizedId);

        return json(['code' => 0, 'data' => $options]);
    }

    /**
     * 跟进弹窗 - 就地修改客户级别（AJAX）
     * 职责单一：仅修改 kh_rank，不涉及跟进记录
     */
    public function updateClientRankInFollow()
    {
        if (!request()->isPost()) {
            return fail('请求方式不正确');
        }

        $clientId = Request::param('id');
        $rawKhRank = Request::param('kh_rank', '');

        if (!$clientId) {
            return fail('缺少客户ID');
        }
        if (trim((string)$rawKhRank) === '') {
            return fail('请选择客户级别');
        }

        $client = Db::table('crm_leads')->where('id', $clientId)->find();
        if (!$client) {
            return fail('客户不存在');
        }

        $currentUsername = trim((string)Session::get('username'));
        $currentAid = (int)Session::get('aid');

        if ($currentAid !== 1) {
            $isOwner = (trim((string)$client['pr_user']) === $currentUsername);
            $isJointPerson = false;
            if (!empty($client['joint_person'])) {
                $jp = $client['joint_person'];
                $jpIds = [];
                if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                    $tmp = json_decode($jp, true);
                    if (is_array($tmp)) $jpIds = $tmp;
                } else {
                    $jpIds = array_values(array_filter(explode(',', $jp)));
                }
                $isJointPerson = in_array((string)$currentAid, array_map('strval', $jpIds));
            }
            if (!$isOwner && !$isJointPerson) {
                return fail('您没有权限修改该客户的级别');
            }
        }

        $oldKhRank = trim((string)($client['kh_rank'] ?? ''));

        list($rankOk, $rankErrMsg, $khRankStore, $khRankName) = $this->validateKhRankForSave($rawKhRank, $oldKhRank);
        if (!$rankOk) {
            return fail($rankErrMsg);
        }

        $oldRankDisplayName = '';
        if ($oldKhRank !== '') {
            $rankMap = Db::table('crm_client_rank')->column('rank_name', 'id');
            $rankNameMap = [];
            foreach ($rankMap as $rId => $rName) {
                $rankNameMap[(string)$rId] = trim((string)$rName);
            }
            $oldRankDisplayName = $this->resolveKhRankDisplayName($oldKhRank, $rankNameMap);
        }

        Db::table('crm_leads')->where('id', $clientId)->update([
            'kh_rank' => $khRankStore,
            'ut_time' => date('Y-m-d H:i:s'),
        ]);

        $oldDisplay = $oldRankDisplayName ?: '空';
        $newDisplay = $khRankName ?: '空';
        self::addOperLog($clientId, '编辑', "客户级别：{$oldDisplay} -> {$newDisplay}");

        return success([
            'kh_rank'      => $khRankStore,
            'kh_rank_name' => $khRankName,
        ], '客户级别修改成功');
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

        // 校验客户是否存在（crm_leads 能查到客户才允许继续）
        $client = Db::table('crm_leads')->where(['id' => $leadsId])->find();
        if (!$client) {
            return json(['code' => 1, 'msg' => '客户不存在', 'count' => 0, 'data' => []]);
        }

        // 构建查询条件
        $where = [['com.leads_id', '=', $leadsId], ['com.is_deleted', '=', 0]];

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


    //客户转移，变更负责人（客户列表页批量）
    /**
     * 转移二次确认用主电话映射：crm_contacts contact_type=1 且 is_delete=0
     *
     * @param int[] $leadIds
     * @return array<int,string>
     */
    private function buildAlterTransferMainPhoneMap(array $leadIds)
    {
        $result = [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return $result;
        }

        try {
            $query = Db::table('crm_contacts')
                ->field('leads_id,contact_value')
                ->whereIn('leads_id', $leadIds)
                ->where('contact_type', 1)
                ->order('id', 'asc');
            if ($this->tableHasColumn('crm_contacts', 'is_delete')) {
                $query->where('is_delete', 0);
            }
            $rows = $query->select();
        } catch (\Throwable $e) {
            Log::error('[Client.buildAlterTransferMainPhoneMap] query failed', [
                'error' => $e->getMessage(),
            ]);
            return $result;
        }

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        }
        if (!is_array($rows)) {
            $rows = [];
        }

        foreach ($rows as $row) {
            $leadId = (int)($row['leads_id'] ?? 0);
            if ($leadId <= 0 || isset($result[$leadId])) {
                continue;
            }
            $result[$leadId] = trim((string)($row['contact_value'] ?? ''));
        }

        return $result;
    }

    /**
     * 规范化转移弹窗二次确认 client_list：id / kh_name / main_phone / pr_user
     *
     * @param int[] $idsArr 请求顺序
     * @param array $rows 已查到的客户行（可含 main_phone）
     * @return array<int,array{id:int,kh_name:string,main_phone:string,pr_user:string}>
     */
    private function buildAlterTransferClientList(array $idsArr, array $rows = [])
    {
        $byId = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $id = (int)($row['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $byId[$id] = $row;
        }

        $needPhoneIds = [];
        foreach ($idsArr as $id) {
            $id = (int)$id;
            if ($id <= 0 || !isset($byId[$id])) {
                continue;
            }
            if (!array_key_exists('main_phone', $byId[$id])) {
                $needPhoneIds[] = $id;
            }
        }

        $phoneMap = [];
        if (!empty($needPhoneIds)) {
            $phoneMap = $this->buildAlterTransferMainPhoneMap($needPhoneIds);
        }

        $list = [];
        foreach ($idsArr as $id) {
            $id = (int)$id;
            if ($id <= 0 || !isset($byId[$id])) {
                continue;
            }
            $row = $byId[$id];
            if (array_key_exists('main_phone', $row)) {
                $mainPhone = trim((string)$row['main_phone']);
            } else {
                $mainPhone = (string)($phoneMap[$id] ?? '');
            }
            $list[] = [
                'id' => $id,
                'kh_name' => trim((string)($row['kh_name'] ?? '')),
                'main_phone' => $mainPhone,
                'pr_user' => trim((string)($row['pr_user'] ?? '')),
            ];
        }

        return $list;
    }

    /**
     * 向模板注入 client_list 及安全 JSON（二次确认用）
     *
     * @param array $clientList
     * @return void
     */
    private function assignAlterTransferClientList(array $clientList)
    {
        $this->assign('client_list', $clientList);
        $this->assign(
            'client_list_json',
            json_encode(
                $clientList,
                JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_AMP | JSON_HEX_QUOT
            )
        );
    }

    public function alterPrUser()
    {
        // ids 严格清洗：仅保留正整数，去重并保持顺序
        $idsRawInput = Request::param('ids', '');
        if (is_array($idsRawInput)) {
            $idsRawInput = implode(',', $idsRawInput);
        }
        $idsParam = trim((string)$idsRawInput);
        $idParts = preg_split('/[,\s，]+/', $idsParam, -1, PREG_SPLIT_NO_EMPTY);
        $idsArr = [];
        $seenIds = [];
        foreach ($idParts as $part) {
            $id = (int)$part;
            if ($id > 0 && !isset($seenIds[$id])) {
                $seenIds[$id] = 1;
                $idsArr[] = $id;
            }
        }
        $ids = implode(',', $idsArr);

        Log::info('[Client.alterPrUser] ids', [
            'ids_raw' => $idsParam,
            'ids_clean' => $idsArr,
            'is_ajax' => Request::isAjax() ? 1 : 0
        ]);

        // 仅展示可合法成为客户负责人的销售相关账号
        $ownerCandidateService = new ClientOwnerCandidateService();
        $adminResult = $ownerCandidateService->getTransferableOwnerCandidates();
        $this->assign('adminResult', $adminResult);

        $batchLimit = 500;
        $scopeDeniedMsg = '所选客户中存在不存在或无权操作的客户，请刷新列表后重新选择';
        $batchLimitMsg = '一次最多转移500个客户，请分批操作';
        $scopeError = '';
        if (!empty($idsArr)) {
            if (count($idsArr) > $batchLimit) {
                $scopeError = $batchLimitMsg;
            } else {
                $allowedIds = model('Client')->buildClientSearchAllBaseQuery([])
                    ->where('l.id', 'in', $idsArr)
                    ->column('l.id');
                $allowedMap = [];
                foreach ((array)$allowedIds as $allowedId) {
                    $allowedMap[(int)$allowedId] = true;
                }
                foreach ($idsArr as $requestId) {
                    if (!isset($allowedMap[$requestId])) {
                        $scopeError = $scopeDeniedMsg;
                        break;
                    }
                }
            }
        }

        if (Request::isAjax()) {
            $username = trim((string)Request::param('username'));

            if (empty($idsArr)) {
                return json(['code' => 500, 'msg' => '参数错误或未选择客户', 'data' => []]);
            }
            if ($scopeError !== '') {
                return json(['code' => 500, 'msg' => $scopeError, 'data' => []]);
            }

            $ownerValidate = $ownerCandidateService->validateTransferTargetOwner($username);
            if (empty($ownerValidate['ok'])) {
                return json([
                    'code' => (int)($ownerValidate['code'] ?? 500),
                    'msg' => (string)($ownerValidate['msg'] ?? '该账号当前不能作为客户负责人，请重新选择'),
                    'data' => [],
                ]);
            }
            $username = (string)$ownerValidate['username'];
            $newPrUserId = (int)$ownerValidate['admin_id'];

            $ownerHistoryService = new ClientOwnerHistoryService();
            $operatorInfo = [
                'admin_id' => (int)Session::get('aid'),
                'username' => (string)Session::get('username'),
            ];

            Db::startTrans();
            try {
                $successCount = 0;
                $failCount = 0;
                $skipCount = 0;
                foreach ($idsArr as $value) {
                    $old = Db::name('crm_leads')
                        ->where('id', $value)
                        ->where('status', 1)
                        ->field('id,kh_name,pr_user,pr_user_id')
                        ->lock(true)
                        ->find();
                    if (!$old) {
                        throw new \RuntimeException('客户状态已变化，请刷新列表后重新选择');
                    }

                    $oldOwner = [
                        'user_id' => isset($old['pr_user_id']) ? (int)$old['pr_user_id'] : 0,
                        'user_name' => isset($old['pr_user']) ? (string)$old['pr_user'] : '',
                    ];
                    $newOwner = [
                        'user_id' => (int)$newPrUserId,
                        'user_name' => $username,
                    ];

                    // 同负责人转给自己：整个客户跳过，不修改 crm_leads / operation_log / owner_history
                    if ($ownerHistoryService->isSameOwner($oldOwner, $newOwner)) {
                        $skipCount++;
                        continue;
                    }

                    // 同一个客户的本次转移统一使用同一个变更时间
                    $changeTime = date('Y-m-d H:i:s');

                    $updateData = [
                        'pr_user_bef' => $oldOwner['user_name'],
                        'pr_user_bef_id' => $oldOwner['user_id'],
                        'pr_user' => $username,
                        'pr_user_id' => (int)$newPrUserId
                    ];
                    $res = Db::name('crm_leads')->where('id', $value)->where('status', 1)->update($updateData);
                    if ($res === false || (int)$res <= 0) {
                        throw new \RuntimeException('负责人转移更新失败');
                    }

                    $khName = isset($old['kh_name']) ? $old['kh_name'] : ('ID:' . $value);
                    $oldPrUser = $oldOwner['user_name'];
                    $logId = $this->addOperLog(
                        $value,
                        '转移负责人',
                        "客户[{$khName}] 从 [{$oldPrUser}] 转移给 [{$username}]"
                    );
                    if ((int)$logId <= 0) {
                        throw new \RuntimeException('负责人转移操作日志写入失败');
                    }

                    // 负责人历史：校验当前阶段一致 -> 关闭旧阶段 -> 新增新阶段。
                    // 该调用一旦抛异常，说明历史未初始化或数据不一致，必须让整批事务回滚，
                    // 因此不在此处 catch，直接向外层传播。
                    $ownerHistoryService->changeOwner(
                        (int)$value,
                        $oldOwner,
                        $newOwner,
                        'manual_transfer',
                        $operatorInfo,
                        (int)$logId,
                        $changeTime,
                        ''
                    );
                    $successCount++;
                }

                Db::commit();
                if ($successCount === 0 && $skipCount === 0) {
                    return json(['code' => 500, 'msg' => '转移失败，未找到有效客户或更新异常', 'data' => []]);
                }
                if ($successCount === 0 && $failCount === 0) {
                    // 全部跳过：均已经是该负责人
                    return json(['code' => 0, 'msg' => "所选 {$skipCount} 个客户已经是该负责人，无需转移", 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
                }
                if ($failCount === 0 && $skipCount === 0) {
                    // 全部成功（保持原有兼容文案）
                    return json(['code' => 0, 'msg' => '转移' . $successCount . '个客户成功！', 'data' => []]);
                }
                $msgParts = ["成功转移 {$successCount} 个客户"];
                if ($skipCount > 0) {
                    $msgParts[] = "跳过 {$skipCount} 个（已是该负责人）";
                }
                return json(['code' => 0, 'msg' => implode('，', $msgParts), 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
            } catch (\Exception $e) {
                Db::rollback();
                Log::error('[Client.alterPrUser] transfer exception', [
                    'ids_clean' => $idsArr,
                    'username' => $username,
                    'error' => $e->getMessage()
                ]);
                return json(['code' => 500, 'msg' => '转移失败！', 'data' => []]);
            }
        }

        // GET：未通过范围/数量校验时不展示客户名称，也不把 ids 交给弹窗提交
        if ($scopeError !== '') {
            $this->assign('ids', '');
            $this->assign('client_name', $scopeError);
            $this->assignAlterTransferClientList([]);
            return $this->fetch('client/alter_pr_user');
        }

        $this->assign('ids', $ids);
        $clientName = '';
        $clientList = [];
        if (!empty($idsArr)) {
            $rawList = model('Client')->buildClientSearchAllBaseQuery([])
                ->where('l.id', 'in', $idsArr)
                ->field('l.id,l.kh_name,l.pr_user')
                ->select();
            if (is_object($rawList) && method_exists($rawList, 'toArray')) {
                $rawList = $rawList->toArray();
            }
            if (!is_array($rawList)) {
                $rawList = [];
            }
            $clientList = $this->buildAlterTransferClientList($idsArr, $rawList);
            $clientCount = count($clientList);
            if ($clientCount > 1) {
                $clientName = '已选择' . $clientCount . '个客户';
            } elseif ($clientCount === 1) {
                $singleName = trim((string)$clientList[0]['kh_name']);
                $clientName = $singleName === '' ? '未命名' : $singleName;
            }
        }
        if ($clientName === '') {
            $clientName = '未找到客户';
        }
        $this->assign('client_name', $clientName);
        $this->assignAlterTransferClientList($clientList);
        return $this->fetch('client/alter_pr_user');
    }


    //客户转移，变更负责人(个人)
    public function alterPrUserPri()
    {
        // ids 严格清洗：兼容字符串/数组，支持英文逗号/中文逗号/空格分隔，仅保留正整数，去重并保持顺序
        $idsRawInput = Request::param('ids', '');
        if (is_array($idsRawInput)) {
            $idsRawInput = implode(',', $idsRawInput);
        }
        $idsParam = trim((string)$idsRawInput);
        $idParts = preg_split('/[,\s，]+/', $idsParam, -1, PREG_SPLIT_NO_EMPTY);
        $idsArr = [];
        $seenIds = [];
        foreach ($idParts as $part) {
            $id = (int)$part;
            if ($id > 0 && !isset($seenIds[$id])) {
                $seenIds[$id] = 1;
                $idsArr[] = $id;
            }
        }
        $ids = implode(',', $idsArr);

        // 仅展示可合法成为客户负责人的销售相关账号
        $ownerCandidateService = new ClientOwnerCandidateService();
        $adminResult = $ownerCandidateService->getTransferableOwnerCandidates();
        $this->assign('adminResult', $adminResult);

        $batchLimit = 500;
        $scopeDeniedMsg = '所选客户中存在无权操作或状态异常的客户，请刷新列表后重新选择';
        $batchLimitMsg = '批量转移最多支持500个客户';
        $currentUsername = trim((string)Session::get('username'));
        $scopeError = '';
        if (!empty($idsArr)) {
            if (count($idsArr) > $batchLimit) {
                $scopeError = $batchLimitMsg;
            } else {
                $allowedIds = Db::name('crm_leads')
                    ->where('id', 'in', $idsArr)
                    ->where('pr_user', $currentUsername)
                    ->where('status', 1)
                    ->where('issuccess', -1)
                    ->column('id');
                $allowedMap = [];
                foreach ((array)$allowedIds as $allowedId) {
                    $allowedMap[(int)$allowedId] = true;
                }
                foreach ($idsArr as $requestId) {
                    if (!isset($allowedMap[$requestId])) {
                        $scopeError = $scopeDeniedMsg;
                        break;
                    }
                }
            }
        }

        if (Request::isAjax()) {
            if (empty($idsArr)) {
                return json(['code' => 500, 'msg' => '参数错误或未选择客户', 'data' => []]);
            }
            if ($scopeError !== '') {
                return json(['code' => 500, 'msg' => $scopeError, 'data' => []]);
            }

            $username = trim((string)Request::param('username'));
            $ownerValidate = $ownerCandidateService->validateTransferTargetOwner($username);
            if (empty($ownerValidate['ok'])) {
                return json([
                    'code' => (int)($ownerValidate['code'] ?? 500),
                    'msg' => (string)($ownerValidate['msg'] ?? '该账号当前不能作为客户负责人，请重新选择'),
                    'data' => [],
                ]);
            }
            $username = (string)$ownerValidate['username'];
            $newPrUserId = (int)$ownerValidate['admin_id'];

            $ownerHistoryService = new ClientOwnerHistoryService();
            $operatorInfo = [
                'admin_id' => (int)Session::get('aid'),
                'username' => $currentUsername,
            ];

            Db::startTrans();
            try {
                $successCount = 0;
                $failCount = 0;
                $skipCount = 0;
                foreach ($idsArr as $value) {
                    // 加行锁读取旧负责人，避免并发转移导致的快照不一致
                    $old = Db::name('crm_leads')
                        ->where('id', $value)
                        ->where('pr_user', $currentUsername)
                        ->where('status', 1)
                        ->where('issuccess', -1)
                        ->lock(true)
                        ->field('id,kh_name,pr_user,pr_user_id')
                        ->find();
                    if (!$old) {
                        throw new \RuntimeException('客户状态已变化，请刷新列表后重新选择');
                    }

                    $oldOwner = [
                        'user_id' => (int)($old['pr_user_id'] ?? 0),
                        'user_name' => trim((string)($old['pr_user'] ?? '')),
                    ];
                    $newOwner = [
                        'user_id' => (int)$newPrUserId,
                        'user_name' => $username,
                    ];

                    // 同负责人转给自己：整个客户跳过，不修改 crm_leads / operation_log / owner_history
                    if ($ownerHistoryService->isSameOwner($oldOwner, $newOwner)) {
                        $skipCount++;
                        continue;
                    }

                    // 同一个客户的本次转移统一使用同一个变更时间
                    $changeTime = date('Y-m-d H:i:s');

                    $updateData = [
                        'pr_user_bef' => $oldOwner['user_name'],
                        'pr_user_bef_id' => $oldOwner['user_id'],
                        'pr_user' => $username,
                        'pr_user_id' => (int)$newPrUserId,
                    ];
                    $res = Db::name('crm_leads')
                        ->where('id', $value)
                        ->where('pr_user', $currentUsername)
                        ->where('status', 1)
                        ->where('issuccess', -1)
                        ->update($updateData);
                    if ($res === false || (int)$res <= 0) {
                        throw new \RuntimeException('负责人转移更新失败');
                    }

                    $khName = isset($old['kh_name']) ? $old['kh_name'] : ('ID:' . $value);
                    $oldPrUser = $oldOwner['user_name'];
                    $logId = $this->addOperLog(
                        $value,
                        '转移负责人',
                        "客户[{$khName}] 从 [{$oldPrUser}] 转移给 [{$username}]"
                    );
                    if ((int)$logId <= 0) {
                        throw new \RuntimeException('负责人转移操作日志写入失败');
                    }

                    // 负责人历史：校验当前阶段一致 -> 关闭旧阶段 -> 新增新阶段。
                    // 该调用一旦抛异常，说明历史未初始化或数据不一致，必须让整批事务回滚，
                    // 因此不在此处 catch，直接向外层传播。
                    $ownerHistoryService->changeOwner(
                        (int)$value,
                        $oldOwner,
                        $newOwner,
                        'manual_transfer',
                        $operatorInfo,
                        (int)$logId,
                        $changeTime,
                        ''
                    );

                    $successCount++;
                }

                Db::commit();
                // 不变量：idsArr 非空，且每个客户恰好计入 success/fail/skip 三者之一
                if ($successCount === 0 && $skipCount === 0) {
                    // 全部失败：未找到有效客户或更新异常（保持原有兼容文案）
                    return json(['code' => 500, 'msg' => '转移失败，未找到有效客户或更新异常', 'data' => []]);
                }
                if ($successCount === 0 && $failCount === 0) {
                    // 全部跳过：均已经是该负责人
                    return json(['code' => 0, 'msg' => "所选 {$skipCount} 个客户已经是该负责人，无需转移", 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
                }
                if ($failCount === 0 && $skipCount === 0) {
                    // 全部成功（保持原有兼容文案）
                    return json(['code' => 0, 'msg' => '转移' . $successCount . '个客户成功！', 'data' => []]);
                }
                // 部分成功 + 跳过混合
                $msgParts = ["成功转移 {$successCount} 个客户"];
                if ($skipCount > 0) {
                    $msgParts[] = "跳过 {$skipCount} 个（已是该负责人）";
                }
                return json(['code' => 0, 'msg' => implode('，', $msgParts), 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
            } catch (\Exception $e) {
                Db::rollback();
                Log::error('[Client.alterPrUserPri] 负责人转移失败', [
                    'ids' => $idsArr,
                    'new_owner_id' => (int)$newPrUserId,
                    'new_owner_name' => $username,
                    'operator_id' => $operatorInfo['admin_id'],
                    'operator_name' => $operatorInfo['username'],
                    'error' => $e->getMessage(),
                ]);
                return json(['code' => 500, 'msg' => '转移失败！', 'data' => []]);
            }
        }

        // GET：未通过权限/数量校验时不展示客户名称，也不把 ids 交给弹窗提交
        if ($scopeError !== '') {
            $this->assign('ids', '');
            $this->assign('client_name', $scopeError);
            $this->assignAlterTransferClientList([]);
            return $this->fetch('personclient/alter_pr_user');
        }

        $this->assign('ids', $ids);
        $clientName = '';
        $clientList = [];
        if (!empty($idsArr)) {
            $rawList = Db::name('crm_leads')
                ->where('id', 'in', $idsArr)
                ->where('pr_user', $currentUsername)
                ->where('status', 1)
                ->where('issuccess', -1)
                ->field('id,kh_name,pr_user')
                ->select();
            if (is_object($rawList) && method_exists($rawList, 'toArray')) {
                $rawList = $rawList->toArray();
            }
            if (!is_array($rawList)) {
                $rawList = [];
            }
            $clientList = $this->buildAlterTransferClientList($idsArr, $rawList);
            $clientCount = count($clientList);
            if ($clientCount > 1) {
                $clientName = '已选择' . $clientCount . '个客户';
            } elseif ($clientCount === 1) {
                $singleName = trim((string)$clientList[0]['kh_name']);
                $clientName = $singleName === '' ? '未命名' : $singleName;
            } else {
                $clientName = '';
            }
        }
        if ($clientName === '') {
            $clientName = '未找到客户';
        }
        $this->assign('client_name', $clientName);
        $this->assignAlterTransferClientList($clientList);

        return $this->fetch('personclient/alter_pr_user');
    }

    /**
     * 检查客户页：批量/单个转移负责人
     * 转移字段、日志、生命周期对齐 alterPrUser；可见范围对齐 getCheckClientSearchList。
     */
    public function alterPrUserCheck()
    {
        $idsRawInput = Request::param('ids', '');
        if (is_array($idsRawInput)) {
            $idsRawInput = implode(',', $idsRawInput);
        }
        $idsParam = trim((string)$idsRawInput);
        $idParts = preg_split('/[,\s，]+/', $idsParam, -1, PREG_SPLIT_NO_EMPTY);
        $idsArr = [];
        $seenIds = [];
        foreach ($idParts as $part) {
            $id = (int)$part;
            if ($id > 0 && !isset($seenIds[$id])) {
                $seenIds[$id] = 1;
                $idsArr[] = $id;
            }
        }
        $ids = implode(',', $idsArr);

        Log::info('[Client.alterPrUserCheck] ids', [
            'ids_raw' => $idsParam,
            'ids_clean' => $idsArr,
            'is_ajax' => Request::isAjax() ? 1 : 0
        ]);

        $ownerCandidateService = new ClientOwnerCandidateService();
        $adminResult = $ownerCandidateService->getTransferableOwnerCandidates();
        $this->assign('adminResult', $adminResult);
        $this->assign('transfer_submit_url', url('Client/alterPrUserCheck'));

        $batchLimit = 500;
        $scopeDeniedMsg = '所选客户中存在不存在或无权操作的客户，请刷新列表后重新选择';
        $batchLimitMsg = '批量转移最多支持500个客户';
        $scopeError = '';
        $scopedRows = [];

        $visibleUsers = $this->getCheckClientVisibleUsernames();
        $currentAdmin = [
            'admin_id' => (int)Session::get('aid'),
            'group_id' => (int)Session::get('group_id'),
            'username' => (string)Session::get('username'),
            'is_super_admin' => $this->isCheckClientSuperAdmin() ? 1 : 0,
        ];

        if (!empty($idsArr)) {
            if (count($idsArr) > $batchLimit) {
                $scopeError = $batchLimitMsg;
            } else {
                $list = model('client')->getCheckClientSearchList(
                    1,
                    count($idsArr),
                    ['__id_in' => $idsArr],
                    $visibleUsers,
                    $currentAdmin
                );
                $scopedRows = (!empty($list) && !empty($list['data'])) ? $list['data'] : [];
                $allowedMap = [];
                foreach ((array)$scopedRows as $row) {
                    $allowedMap[(int)$row['id']] = true;
                }
                foreach ($idsArr as $requestId) {
                    if (!isset($allowedMap[$requestId])) {
                        $scopeError = $scopeDeniedMsg;
                        break;
                    }
                }
            }
        }

        if (Request::isAjax()) {
            $username = trim((string)Request::param('username'));

            if (empty($idsArr)) {
                return json(['code' => 500, 'msg' => '参数错误或未选择客户', 'data' => []]);
            }
            if ($scopeError !== '') {
                return json(['code' => 500, 'msg' => $scopeError, 'data' => []]);
            }

            $ownerValidate = $ownerCandidateService->validateTransferTargetOwner($username);
            if (empty($ownerValidate['ok'])) {
                return json([
                    'code' => (int)($ownerValidate['code'] ?? 500),
                    'msg' => (string)($ownerValidate['msg'] ?? '该账号当前不能作为客户负责人，请重新选择'),
                    'data' => [],
                ]);
            }
            $username = (string)$ownerValidate['username'];
            $newPrUserId = (int)$ownerValidate['admin_id'];

            $ownerHistoryService = new ClientOwnerHistoryService();
            $operatorInfo = [
                'admin_id' => (int)Session::get('aid'),
                'username' => (string)Session::get('username'),
            ];

            Db::startTrans();
            try {
                $successCount = 0;
                $failCount = 0;
                $skipCount = 0;
                foreach ($idsArr as $value) {
                    $old = Db::name('crm_leads')
                        ->where('id', $value)
                        ->where('status', 1)
                        ->field('id,kh_name,pr_user,pr_user_id')
                        ->lock(true)
                        ->find();
                    if (!$old) {
                        throw new \RuntimeException('客户状态已变化，请刷新列表后重新选择');
                    }

                    $oldOwner = [
                        'user_id' => isset($old['pr_user_id']) ? (int)$old['pr_user_id'] : 0,
                        'user_name' => isset($old['pr_user']) ? (string)$old['pr_user'] : '',
                    ];
                    $newOwner = [
                        'user_id' => (int)$newPrUserId,
                        'user_name' => $username,
                    ];

                    if ($ownerHistoryService->isSameOwner($oldOwner, $newOwner)) {
                        $skipCount++;
                        continue;
                    }

                    $changeTime = date('Y-m-d H:i:s');

                    $updateData = [
                        'pr_user_bef' => $oldOwner['user_name'],
                        'pr_user_bef_id' => $oldOwner['user_id'],
                        'pr_user' => $username,
                        'pr_user_id' => (int)$newPrUserId
                    ];
                    $res = Db::name('crm_leads')->where('id', $value)->where('status', 1)->update($updateData);
                    if ($res === false || (int)$res <= 0) {
                        throw new \RuntimeException('负责人转移更新失败');
                    }

                    $khName = isset($old['kh_name']) ? $old['kh_name'] : ('ID:' . $value);
                    $oldPrUser = $oldOwner['user_name'];
                    $logId = $this->addOperLog(
                        $value,
                        '转移负责人',
                        "客户[{$khName}] 从 [{$oldPrUser}] 转移给 [{$username}]"
                    );
                    if ((int)$logId <= 0) {
                        throw new \RuntimeException('负责人转移操作日志写入失败');
                    }

                    $ownerHistoryService->changeOwner(
                        (int)$value,
                        $oldOwner,
                        $newOwner,
                        'manual_transfer',
                        $operatorInfo,
                        (int)$logId,
                        $changeTime,
                        ''
                    );
                    $successCount++;
                }

                Db::commit();
                if ($successCount === 0 && $skipCount === 0) {
                    return json(['code' => 500, 'msg' => '转移失败，未找到有效客户或更新异常', 'data' => []]);
                }
                if ($successCount === 0 && $failCount === 0) {
                    return json(['code' => 0, 'msg' => "所选 {$skipCount} 个客户已经是该负责人，无需转移", 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
                }
                if ($failCount === 0 && $skipCount === 0) {
                    return json(['code' => 0, 'msg' => '转移' . $successCount . '个客户成功！', 'data' => []]);
                }
                $msgParts = ["成功转移 {$successCount} 个客户"];
                if ($skipCount > 0) {
                    $msgParts[] = "跳过 {$skipCount} 个（已是该负责人）";
                }
                return json(['code' => 0, 'msg' => implode('，', $msgParts), 'data' => ['success_count' => $successCount, 'skip_count' => $skipCount, 'fail_count' => $failCount]]);
            } catch (\Exception $e) {
                Db::rollback();
                Log::error('[Client.alterPrUserCheck] transfer exception', [
                    'ids_clean' => $idsArr,
                    'username' => $username,
                    'error' => $e->getMessage()
                ]);
                return json(['code' => 500, 'msg' => '转移失败！', 'data' => []]);
            }
        }

        if ($scopeError !== '') {
            $this->assign('ids', '');
            $this->assign('client_name', $scopeError);
            $this->assignAlterTransferClientList([]);
            return $this->fetch('client/alter_pr_user');
        }

        $this->assign('ids', $ids);
        $clientName = '';
        $clientList = [];
        if (!empty($idsArr) && !empty($scopedRows)) {
            // scopedRows 已含 pr_user / main_phone（getCheckClientSearchList）
            $clientList = $this->buildAlterTransferClientList($idsArr, $scopedRows);
            $clientCount = count($clientList);
            if ($clientCount > 1) {
                $clientName = '已选择' . $clientCount . '个客户';
            } elseif ($clientCount === 1) {
                $singleName = trim((string)$clientList[0]['kh_name']);
                $clientName = $singleName === '' ? '未命名' : $singleName;
            }
        }
        if ($clientName === '') {
            $clientName = '未找到客户';
        }
        $this->assign('client_name', $clientName);
        $this->assignAlterTransferClientList($clientList);
        return $this->fetch('client/alter_pr_user');
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

    /**
     * 提交成交客户申请（不直接改 issuccess，待审核通过后生效）
     */
    public function applySuccessClient()
    {
        if (!Request::isPost()) {
            return json(['code' => 1, 'msg' => '请求方式错误']);
        }

        $leadsId = input('leads_id/d', 0);
        $proofImage = trim((string)input('proof_image', ''));
        $applyRemark = trim((string)input('apply_remark', ''));

        if ($leadsId <= 0) {
            return json(['code' => 1, 'msg' => '客户ID不能为空']);
        }
        if ($proofImage === '') {
            return json(['code' => 1, 'msg' => '请上传成交凭证']);
        }

        $lead = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$lead) {
            return json(['code' => 1, 'msg' => '客户不存在']);
        }
        if ((int)($lead['status'] ?? 0) !== 1) {
            return json(['code' => 1, 'msg' => '客户状态无效，无法提交成交申请']);
        }
        if ((int)($lead['issuccess'] ?? 0) !== -1) {
            return json(['code' => 1, 'msg' => '仅未成交客户可提交成交申请']);
        }
        if (!$this->canApplySuccessClient($lead)) {
            return json(['code' => 1, 'msg' => '仅客户负责人、协同人或超级管理员可提交申请']);
        }

        $service = new SuccessClientApplyService();
        $result = $service->submitApply(
            $leadsId,
            $proofImage,
            $applyRemark,
            $lead,
            (int)Session::get('aid'),
            (string)Session::get('username')
        );

        return json($result);
    }

    //客户成交（保留原接口，前端列表已改为走 applySuccessClient）
    public function chengjiao()
    {
        if (!Request::isPost()) {
            return json([
                'code' => 1,
                'msg' => '请求方式错误'
            ]);
        }

        $ids = Request::param('ids', []);
        if (is_string($ids)) {
            $ids = explode(',', $ids);
        }

        $service = new ClientStatusService();
        $result = $service->batchClientSuccess((array)$ids);
        if ((int)($result['code'] ?? 1) === 0) {
            return json([
                'code' => 0,
                'msg' => $result['msg'] ?? '操作成功'
            ]);
        }

        return json([
            'code' => 1,
            'msg' => $result['msg'] ?? '操作失败'
        ]);
    }

    /**
     * 批量客户未成交：仅允许 POST
     */
    public function batchClientUnSuccess()
    {
        if (!Request::isPost()) {
            return json([
                'code' => 1,
                'msg' => '请求方式错误'
            ]);
        }

        $ids = Request::param('ids', []);
        if (is_string($ids)) {
            $ids = explode(',', $ids);
        }
        if (empty($ids)) {
            return json([
                'code' => 1,
                'msg' => '请选择客户'
            ]);
        }

        $service = new ClientStatusService();
        $result = $service->batchClientUnSuccess((array)$ids);
        if ((int)($result['code'] ?? 1) === 0) {
            return json([
                'code' => 0,
                'msg' => $result['msg'] ?? '操作成功'
            ]);
        }

        return json([
            'code' => 1,
            'msg' => $result['msg'] ?? '操作失败'
        ]);
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


    // 下载导入模板（XLSX格式）
    public function tpl()
    {
        // 文件名包含当前日期时间
        $filename = '客户导入模板_' . date('Ymd_His') . '.xlsx';

        // 表头字段（客户名称、电话、辅助电话...原来修改时间）
        $header = ['客户名称', '电话', '辅助电话', '产品名称', '所属渠道', '运营端口', '协同人', '负责人', '其他信息', '原来创建时间', '原来修改时间'];
        // 示例数据：负责人填系统用户名(username)；空则导入时默认使用当前导入账号
        $examples = [
            ['测试导入客户A', '13800138001', '13900139001', '除雪设备', '竞价', '竞价卡1-13837103643', '311', 'admin', 'Excel导入指定负责人测试', '2026-08-21 10:00:00', '2026-08-21 10:00:00'],
            ['测试导入客户B', '13800138002', '', '除雪设备', '竞价', '竞价卡1-13837103643', '311', '', 'Excel导入默认负责人测试', '2026-08-21 10:00:00', ''],
        ];

        $spreadsheet = new Spreadsheet();
        $sheet = $spreadsheet->getActiveSheet();

        $col = 1;
        foreach ($header as $title) {
            $sheet->setCellValueByColumnAndRow($col, 1, (string)$title);
            $col++;
        }

        $rowNum = 2;
        foreach ($examples as $row) {
            $col = 1;
            foreach ($row as $value) {
                $sheet->setCellValueByColumnAndRow($col, $rowNum, (string)$value);
                $col++;
            }
            $rowNum++;
        }

        // 清除输出缓冲，设置XLSX文件下载头
        if (function_exists('ob_get_level')) {
            while (ob_get_level() > 0) {
                ob_end_clean();
            }
        }
        $encodedFilename = rawurlencode($filename);
        header('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        header("Content-Disposition: attachment; filename=\"{$filename}\"; filename*=UTF-8''{$encodedFilename}");
        header('Pragma: no-cache');
        header('Expires: 0');

        $writer = new Xlsx($spreadsheet);
        $writer->save('php://output');
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

    /**
     * 读取批量跟进表格的显示/隐藏配置和字段顺序配置
     * 路由：Client/getQuickFollowTableConfig
     */
    public function getQuickFollowTableConfig()
    {
        if (!request()->isAjax()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = (int)Session::get('aid');
        $pageKey = Request::param('page_key', 'personclient_quick_follow');
        $tableKey = Request::param('table_key', 'quick-follow-table');

        if ($adminId <= 0) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        $hideConfig = [];
        $columnOrder = [];

        try {
            $configRow = Db::table('crm_table_col_config')
                ->where('uid', $adminId)
                ->where('page_key', $pageKey)
                ->find();

            if ($configRow && !empty($configRow['config_json'])) {
                $tmp = json_decode($configRow['config_json'], true);
                if (is_array($tmp)) {
                    $hideConfig = $tmp;
                }
            }

            $orderRow = Db::table('crm_table_column_order')
                ->where('admin_id', $adminId)
                ->where('page_key', $pageKey)
                ->where('table_key', $tableKey)
                ->find();

            if ($orderRow && !empty($orderRow['column_order'])) {
                $tmp = json_decode($orderRow['column_order'], true);
                if (is_array($tmp)) {
                    $columnOrder = array_values($tmp);
                }
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [
                    'hide_config' => $hideConfig,
                    'column_order' => $columnOrder,
                ]
            ]);
        } catch (\Exception $e) {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [
                    'hide_config' => [],
                    'column_order' => [],
                ]
            ]);
        }
    }

    /**
     * 保存批量跟进表格的显示/隐藏配置和字段顺序配置
     * 路由：Client/saveQuickFollowTableConfig
     */
    public function saveQuickFollowTableConfig()
    {
        if (!request()->isPost()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = (int)Session::get('aid');
        $pageKey = Request::param('page_key', 'personclient_quick_follow');
        $tableKey = Request::param('table_key', 'quick-follow-table');
        $hideConfigParam = Request::param('hide_config', '');
        $columnOrderParam = Request::param('column_order', '');

        if ($adminId <= 0) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        if ($pageKey === '' || $tableKey === '') {
            return json(['code' => 500, 'msg' => '参数不完整', 'data' => []]);
        }

        $hideConfig = [];
        if (is_array($hideConfigParam)) {
            $hideConfig = $hideConfigParam;
        } elseif ($hideConfigParam !== '') {
            $tmp = json_decode($hideConfigParam, true);
            if (is_array($tmp)) {
                $hideConfig = $tmp;
            }
        }

        $columnOrder = [];
        if (is_array($columnOrderParam)) {
            $columnOrder = $columnOrderParam;
        } elseif ($columnOrderParam !== '') {
            $tmp = json_decode($columnOrderParam, true);
            if (is_array($tmp)) {
                $columnOrder = $tmp;
            }
        }

        $safeHideConfig = [];
        foreach ($hideConfig as $field => $hidden) {
            $field = trim((string)$field);
            if ($field !== '' && preg_match('/^[a-zA-Z0-9_]+$/', $field)) {
                $safeHideConfig[$field] = ((int)$hidden) ? 1 : 0;
            }
        }

        $safeColumnOrder = [];
        foreach ($columnOrder as $field) {
            $field = trim((string)$field);
            if ($field !== '' && preg_match('/^[a-zA-Z0-9_]+$/', $field)) {
                $safeColumnOrder[] = $field;
            }
        }
        $safeColumnOrder = array_values(array_unique($safeColumnOrder));

        try {
            $now = time();

            $existsConfig = Db::table('crm_table_col_config')
                ->where('uid', $adminId)
                ->where('page_key', $pageKey)
                ->find();

            if ($existsConfig) {
                Db::table('crm_table_col_config')
                    ->where('id', $existsConfig['id'])
                    ->update([
                        'config_json' => json_encode($safeHideConfig, JSON_UNESCAPED_UNICODE),
                        'ut_time' => $now,
                    ]);
            } else {
                Db::table('crm_table_col_config')->insert([
                    'uid' => $adminId,
                    'page_key' => $pageKey,
                    'config_json' => json_encode($safeHideConfig, JSON_UNESCAPED_UNICODE),
                    'create_time' => $now,
                    'ut_time' => $now,
                ]);
            }

            $existsOrder = Db::table('crm_table_column_order')
                ->where('admin_id', $adminId)
                ->where('page_key', $pageKey)
                ->where('table_key', $tableKey)
                ->find();

            if ($existsOrder) {
                Db::table('crm_table_column_order')
                    ->where('id', $existsOrder['id'])
                    ->update([
                        'column_order' => json_encode($safeColumnOrder, JSON_UNESCAPED_UNICODE),
                        'update_time' => $now,
                    ]);
            } else {
                Db::table('crm_table_column_order')->insert([
                    'admin_id' => $adminId,
                    'page_key' => $pageKey,
                    'table_key' => $tableKey,
                    'column_order' => json_encode($safeColumnOrder, JSON_UNESCAPED_UNICODE),
                    'create_time' => $now,
                    'update_time' => $now,
                ]);
            }

            return json(['code' => 0, 'msg' => '保存成功', 'data' => []]);
        } catch (\Exception $e) {
            return json(['code' => 500, 'msg' => '保存失败：' . $e->getMessage(), 'data' => []]);
        }
    }

    /**
     * 客户管理配置
     * GET: 页面（读取/回显配置）
     * POST: 保存配置（复用 Client/config 权限节点）
     */
    public function config()
    {
        $service = new ClientConfigService();

        if (request()->isPost()) {
            $rawIds = input('post.allowed_owner_user_ids/a', null);
            if (!is_array($rawIds)) {
                $rawIds = input('post.allowed_owner_user_ids', '');
            }

            $excludeDisabled = input('post.exclude_disabled_users', '1');

            $result = $service->saveConfig([
                'allowed_owner_user_ids' => $rawIds,
                'exclude_disabled_users' => $excludeDisabled,
            ]);

            return json($result);
        }

        $pageData = $service->getConfigPageData();
        $this->assign('pageData', $pageData);
        $this->assign('xmSelectDataJson', json_encode($pageData['xm_select_data'] ?? [], JSON_UNESCAPED_UNICODE));
        $this->assign('selectedOwnerIdsJson', json_encode($pageData['selected_owner_ids'] ?? [], JSON_UNESCAPED_UNICODE));
        $this->assign('excludeDisabledUsers', (int)($pageData['exclude_disabled_users'] ?? 1));
        $this->assign('hasConfig', !empty($pageData['has_config']) ? 1 : 0);
        $this->assign('isSuggestedDefault', !empty($pageData['is_suggested_default']) ? 1 : 0);
        $this->assign('updateUser', (string)($pageData['update_user'] ?? ''));
        $this->assign('updateTime', (string)($pageData['update_time'] ?? ''));

        return $this->fetch('client/config');
    }

}
