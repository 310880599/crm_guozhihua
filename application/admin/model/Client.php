<?php

namespace app\admin\model;

use app\admin\controller\Client as ControllerClient;
use think\Model;
use think\Db;
use app\admin\model\Contacts;
use think\facade\Log;


class Client extends Model
{
    protected $table = 'crm_leads';

    /**
     * 客户级别筛选兼容条件：
     * - 新数据：kh_rank 存 crm_client_rank.id
     * - 旧数据：kh_rank 存 crm_client_rank.rank_name
     *
     * @param mixed  $selectedRankId 前端提交的客户级别ID
     * @param string $field           查询字段（支持别名，如 l.kh_rank）
     * @return array
     */
    private function buildKhRankCompatWhere($selectedRankId, $field = 'kh_rank')
    {
        $rankId = trim((string)$selectedRankId);
        if ($rankId === '') {
            return [];
        }

        $rankIdInt = (int)$rankId;
        if ($rankIdInt <= 0) {
            // 非法ID直接构造必不成立条件，避免误查全表
            return [[$field, '=', -1]];
        }

        $values = [(string)$rankIdInt];
        $rankRow = Db::table('crm_client_rank')
            ->where('id', $rankIdInt)
            ->field('id,rank_name')
            ->find();
        if (!empty($rankRow) && isset($rankRow['rank_name'])) {
            $rankName = trim((string)$rankRow['rank_name']);
            if ($rankName !== '') {
                $values[] = $rankName;
            }
        }

        $values = array_values(array_unique(array_filter($values, function ($v) {
            return trim((string)$v) !== '';
        })));

        if (count($values) === 1) {
            return [[$field, '=', $values[0]]];
        }
        return [[$field, 'in', $values]];
    }


    public function contacts()
    {
        return $this->hasMany(Contacts::class, 'leads_id', 'id')->where('is_delete', 0)->field('leads_id,contact_type,contact_extra,contact_value,vdigits');
    }

    public function getContactAttr($value)
    {
        $contactMap = array_flip(ControllerClient::CONTACT_MAP);
        $v_foramt = [];
        foreach ($this->contacts as $v) {
            $contactType = $contactMap[$v['contact_type']] ?? '';
            $contactValue = $contactType . ':' . $v['contact_extra'] . $v['contact_value'];
            $v_foramt[] = $contactValue;
        }
        return $v_foramt;
    }

    /**
     * 我的客户列表（仅负责人为当前登录用户）
     * 保持原有分页/排序/状态口径，明确保留 next_up_time 字段。
     */
    public function getMyClientList($page, $pageSize, $username)
    {
        return Db::table('crm_leads')
            ->where(['status' => 1, 'issuccess' => -1])
            ->where(['pr_user' => $username])
            ->field('*')
            ->order('at_time desc')
            ->paginate(['list_rows' => $pageSize, 'page' => $page])
            ->toArray();
    }

    /**
     * 跟进弹窗客户详情
     * 通过模型层统一查询，保证返回 next_up_time。
     */
    public function getFollowClientDetailById($clientId)
    {
        return Db::table('crm_leads')
            ->where(['id' => (int)$clientId])
            ->field('*')
            ->find();
    }

    /**
     * 客户详情页数据
     */
    public function getClientDetailById($clientId)
    {
        return Db::table('crm_leads')
            ->where('id', (int)$clientId)
            ->where('status', 1)
            ->field('*')
            ->find();
    }

    /**
     * 成交客户基础信息（用于关联订单权限校验）
     */
    public function getSuccessClientById($clientId)
    {
        return Db::table('crm_leads')
            ->where('id', (int)$clientId)
            ->where('issuccess', 1)
            ->where('status', 1)
            ->field('id,kh_name,issuccess,status,pr_user,pr_user_bef')
            ->find();
    }

    /**
     * 按指定状态获取可批量变更的客户基础信息
     */
    public function getBatchStatusClients(array $ids, int $fromIsSuccess, int $status = 1): array
    {
        if (empty($ids)) {
            return [];
        }

        $rows = Db::table('crm_leads')
            ->where('id', 'in', $ids)
            ->where('status', $status)
            ->where('issuccess', $fromIsSuccess)
            ->field('id,kh_name,issuccess,status')
            ->select();

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            return $rows->toArray();
        }
        return is_array($rows) ? $rows : [];
    }

    /**
     * 批量更新客户成交状态（仅更新有效客户且当前状态匹配的数据）
     */
    public function batchUpdateIsSuccess(array $ids, int $fromIsSuccess, int $toIsSuccess, int $utTime, int $status = 1)
    {
        if (empty($ids)) {
            return 0;
        }

        return Db::table('crm_leads')
            ->where('id', 'in', $ids)
            ->where('status', $status)
            ->where('issuccess', $fromIsSuccess)
            ->update([
                'issuccess' => $toIsSuccess,
                'ut_time' => $utTime
            ]);
    }

    //查询
    public function getClientSearchList($page, $limit, $keyword)
    {

        $mapAtTime = []; //添加时间
        $mapKhRank = []; //客户级别
        $mapKhStatus = []; //客户状态
        $mapPhone = []; //手机号模糊查询
        $mapKhName = []; //客户名称
        $mapXsSource = []; //线索/客户来源
        $mapPrUser = []; //业务员/负责人

        if (!empty($keyword['timebucket'])) {
            $mapAtTime[] = $keyword['timebucket'];
        }
        $selectedKhRank = isset($keyword['kh_rank']) ? trim((string)$keyword['kh_rank']) : '';
        if ($selectedKhRank !== '') {
            $mapKhRank = $this->buildKhRankCompatWhere($selectedKhRank, 'kh_rank');
        }

        if ($keyword['kh_status'] != '') {

            $mapKhStatus =  ['kh_status' => $keyword['kh_status']];
        }

        if ($keyword['phone'] != '') {
            $mapPhone = $this->getContactSearch($keyword['phone']);
        }

        if ($keyword['kh_name'] != '') {
            $mapKhName = [['kh_name', 'like', '%' . $keyword['kh_name'] . '%']];
        }

        if ($keyword['xs_source'] != '') {

            $mapXsSource =  ['xs_source' => $keyword['xs_source']];
        }

        if ($keyword['pr_user'] != '') {
            $mapPrUser = [['pr_user', 'like', '%' . $keyword['pr_user'] . '%']];
        }
        $current_admin = Admin::getMyInfo();
        $team_name = $current_admin['team_name'] ?? '';
        $a_where = [];
        if (strpos($current_admin['org'], 'admin') === false) {
            $a_where = [(new ControllerClient())->getorgWhere($current_admin['org'])];
        }
        $usernames  = [$current_admin['username']];
        if ($current_admin['group_id'] == 1) {
            $usernames = [];
            if ($a_where) {
                $usernames = Db::name('admin')->where($a_where)->column('username');
            }
        } else if ($team_name) {
            // 主管查看直属下属及自己的客户
            $usernames = Db::name('admin')->where('team_name', $team_name)->where($a_where)->column('username');
        }

        $result  = Db::table('crm_leads')
            ->where(function ($query) use ($usernames) {
                if ($usernames) {
                    $query->whereIn('pr_user', $usernames);
                }
            })
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapKhStatus)
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapPrUser)
            ->where($mapAtTime)
            ->where(['status' => 1, 'issuccess' => -1])
            ->order('at_time desc')
            ->paginate(array('list_rows' => $limit, 'page' => $page))
            ->toArray();

        //数据集判断方式
        //if($result->isEmpty()){return null;}
        if ($result['total'] == 0) {
            return null;
        } else {
            return $result;
        }
    }


    public function getContactSearch($phone)
    {
        $phone = trim((string)$phone);
        if ($phone === '') {
            return []; // 不加条件
        }

        // 用户可能输入含空格/短横线/国家码，取数字部分做模糊
        $phoneKeyword = preg_replace('/\D+/', '', $phone);

        $rows = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('contact_type', 'in', [1, 3]) // 1主 3辅:contentReference[oaicite:4]{index=4}
            ->where('contact_value', 'like', "%{$phoneKeyword}%")
            ->field('leads_id')
            ->select();

        $leadsIds = array_column($rows, 'leads_id');

        if (empty($leadsIds)) {
            // 返回一个必不成立条件，避免 SQL 报错
            return [['id', '=', -1]];
        }
        return [['id', 'in', $leadsIds]];
    }


    // 在 application/admin/model/Client.php 内新增以下方法
    public function getJointClientSearchList($page, $limit, $keyword)
    {
        $mapAtTime   = [];
        $mapKhRank   = [];
        $mapKhStatus = [];
        $mapPhone    = [];
        $mapKhName   = [];
        $mapXsSource = [];
        $where       = [];
        $mapInquiry = [];
        $mapPort    = [];

        // 时间范围（控制器已转成 between 等 where 数组）
        if (!empty($keyword['timebucket'])) {
            $mapAtTime[] = $keyword['timebucket'];
        }
        if ($keyword['kh_rank'] !== '' && $keyword['kh_rank'] !== null) {
            $mapKhRank = $this->buildKhRankCompatWhere($keyword['kh_rank'], 'kh_rank');
        }
        if ($keyword['kh_status'] !== '' && $keyword['kh_status'] !== null) {
            $mapKhStatus = ['kh_status' => $keyword['kh_status']];
        }
        if (!empty($keyword['phone'])) {
            // 复用你已有的按电话查 leads_id 的逻辑（需在本模型中存在 getContactSearch 方法，TP5.1 版）
            $mapPhone = $this->getContactSearch($keyword['phone']);
        }
        if (!empty($keyword['oper_user'])) {
            $where[] = ['oper_user', 'like', '%' . $keyword['oper_user'] . '%'];
        }
        if (!empty($keyword['kh_name'])) {
            $mapKhName = [['kh_name', 'like', '%' . $keyword['kh_name'] . '%']];
        }
        if ($keyword['xs_source'] !== '' && $keyword['xs_source'] !== null) {
            $mapXsSource = ['xs_source' => $keyword['xs_source']];
        }

        $mapSourcePort = []; // 来源端口
        if (!empty($keyword['source_port'])) {
            $mapSourcePort = ['source_port' => $keyword['source_port']];
        }
        if ($keyword['inquiry_id'] !== '' && $keyword['inquiry_id'] !== null) {
            $mapInquiry = ['inquiry_id' => $keyword['inquiry_id']];
        }
        if ($keyword['port_id'] !== '' && $keyword['port_id'] !== null) {
            $mapPort = ['port_id' => $keyword['port_id']];
        }
                // 当前登录用户，只取"我作为协同人"的客户，且不是我负责
        $currentUsername = session('username');
        $currentAdminId  = session('aid');

        $result = Db::table('crm_leads')
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapInquiry)      // **新增：按所属渠道筛选**  
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapPort)         // **新增：按运营端口筛选**  
            ->where($mapAtTime)
            ->where($where)
            ->where(['status' => 1, 'issuccess' => -1])                  // 仅有效客户且未成交
            ->where('pr_user', '<>', $currentUsername)                    // 负责人不是我
            ->where(function ($query) use ($currentAdminId) {
                $query->whereRaw("FIND_IN_SET('{$currentAdminId}', joint_person)");
            })
            ->order('at_time desc')
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();

        return ($result['total'] == 0) ? null : $result;
    }



    //个人查询
    public function getPersonClientSearchList($page, $limit, $keyword)
    {


        $mapAtTime = []; //添加时间
        $mapKhRank = []; //客户级别
        $mapKhStatus = []; //客户状态
        $mapPhone = []; //手机号模糊查询
        $mapKhName = []; //客户名称
        $mapXsSource = []; //线索/客户来源
        $where = [];
        $mapInquiry = [];
        $mapPort    = [];


        if (!empty($keyword['timebucket'])) {
            $mapAtTime[] = $keyword['timebucket'];
        }

        $selectedKhRank = isset($keyword['kh_rank']) ? trim((string)$keyword['kh_rank']) : '';
        if ($selectedKhRank !== '') {
            $mapKhRank = $this->buildKhRankCompatWhere($selectedKhRank, 'kh_rank');
        }

        if ($keyword['kh_status'] != '') {

            $mapKhStatus =  ['kh_status' => $keyword['kh_status']];
        }

        if ($keyword['inquiry_id'] != '') {
            $mapInquiry = ['inquiry_id' => $keyword['inquiry_id']];
        }

        if ($keyword['phone'] != '') {
            $mapPhone = $this->getContactSearch($keyword['phone']);
        }

        if (!empty($keyword['oper_user'])) {
            $where[] = ['oper_user', 'like', '%' . $keyword['oper_user'] . '%'];
        }

        if ($keyword['kh_name'] != '') {
            $mapKhName = [['kh_name', 'like', '%' . $keyword['kh_name'] . '%']];
        }

        if ($keyword['xs_source'] != '') {

            $mapXsSource =  ['xs_source' => $keyword['xs_source']];
        }

        if ($keyword['port_id'] != '') {
            $mapPort = ['port_id' => $keyword['port_id']];
        }

        $mapSourcePort = []; // 来源端口
        if (!empty($keyword['source_port'])) {
            $mapSourcePort = ['source_port' => $keyword['source_port']];
        }

        // 【新增-跟进筛选】最新跟进时间筛选条件
        $mapFollow = [];
        $followNoFlag = false; // recent_no_follow 标记
        $followBoundary = ''; // 边界时间（用于 recent_no_follow）

        // 【新增-跟进筛选】处理最新跟进时间筛选
        if (!empty($keyword['__follow_filter']) && !empty($keyword['__follow_boundary'])) {
            $ff = $keyword['__follow_filter'];
            $bd = $keyword['__follow_boundary'];

            if ($ff === 'recent_follow') {
                // 最近有跟进：last_up_time >= 边界
                $mapFollow = [['last_up_time', '>=', $bd]];
            } elseif ($ff === 'recent_no_follow') {
                // 最近无跟进（反选）：last_up_time IS NULL OR last_up_time < 边界
                // 需要用闭包实现 OR 条件，此处设置标记
                $followNoFlag = true;
                $followBoundary = $bd;
            }
        }

        $result  = Db::table('crm_leads')
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapInquiry)     // 使用所属渠道筛选
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapPort)        // 使用运营端口筛选
            ->where($mapAtTime)
            ->where($mapFollow)      // 【新增-跟进筛选】最新跟进时间筛选（recent_follow）
            ->where($where)
            ->where(['status' => 1, 'issuccess' => -1]) //0 线索，1客户，2公海
            ->where(['pr_user' => session('username')]) //负责人
            ->where(function($q) use ($followNoFlag, $followBoundary) {
                // 【新增-跟进反选】最近无跟进：last_up_time IS NULL OR last_up_time < 边界
                if ($followNoFlag) {
                    $q->whereNull('last_up_time')
                      ->whereOr('last_up_time', '<', $followBoundary);
                }
            })
            ->order('at_time desc')
            ->paginate(array('list_rows' => $limit, 'page' => $page))
            ->toArray();

        //数据集判断方式
        //if($result->isEmpty()){return null;}
        if ($result['total'] == 0) {
            return null;
        } else {
            return $result;
        }
    }

    //检查客户
    public function getCheckClientSearchList($page, $limit, $keyword, array $visibleUsers = [], array $currentAdmin = [])
    {
        $page  = max(1, (int)$page);
        $limit = max(1, (int)$limit);

        $adminId       = isset($currentAdmin['admin_id']) ? (int)$currentAdmin['admin_id'] : (int)session('aid');
        $adminGroupId  = isset($currentAdmin['group_id']) ? (int)$currentAdmin['group_id'] : (int)session('group_id');
        $currentUser   = isset($currentAdmin['username']) ? trim((string)$currentAdmin['username']) : trim((string)session('username'));
        $isSuperAdmin  = ($adminId === 1 || $adminGroupId === 1 || !empty($currentAdmin['is_super_admin']));

        // “检查客户”使用业务员精确筛选；先从基础 keyword 移除，避免被客户列表基础查询中的 like 影响
        $selectedPrUser = isset($keyword['pr_user']) ? trim((string)$keyword['pr_user']) : '';
        $baseKeyword = (array)$keyword;
        $baseKeyword['pr_user'] = '';

        // 基础口径对齐“客户列表”查询链路
        $query = $this->buildClientSearchAllBaseQuery($baseKeyword, $isSuperAdmin);

        // 检查客户扩展筛选：运营人员
        if (!empty($keyword['oper_user'])) {
            $query->where('l.oper_user', 'like', '%' . trim((string)$keyword['oper_user']) . '%');
        }

        // 检查客户扩展筛选：跟进类型 + 天数
        $followNoFlag = false;
        $followBoundary = '';
        if (!empty($keyword['__follow_filter']) && !empty($keyword['__follow_boundary'])) {
            $ff = (string)$keyword['__follow_filter'];
            $bd = (string)$keyword['__follow_boundary'];
            if ($ff === 'recent_follow') {
                $query->where('l.last_up_time', '>=', $bd);
            } elseif ($ff === 'recent_no_follow') {
                $followNoFlag = true;
                $followBoundary = $bd;
            }
        }

        if ($followNoFlag) {
            $query->where(function ($q) use ($followBoundary) {
                $q->whereNull('l.last_up_time')
                  ->whereOr('l.last_up_time', '<', $followBoundary);
            });
        }

        // 检查客户权限：仅非超级管理员叠加负责人可见范围
        $visibleUsers = array_values(array_unique(array_filter(array_map('trim', (array)$visibleUsers))));
        if (!$isSuperAdmin) {
            if (empty($visibleUsers) && $currentUser !== '') {
                $visibleUsers = [$currentUser];
            }
            if (!empty($visibleUsers) || $currentUser !== '') {
                $query->where(function ($q) use ($visibleUsers, $currentUser) {
                    // 负责人
                    if (!empty($visibleUsers)) {
                        $q->whereIn('l.pr_user', $visibleUsers);
                    }

                    // 协同人（支持 joint_person JSON 或逗号）
                    if ($currentUser !== '') {
                        $adminId = Db::name('admin')
                            ->where('username', $currentUser)
                            ->value('admin_id');

                        if ($adminId) {
                            $adminId = (int)$adminId;
                            $q->whereOrRaw("FIND_IN_SET('{$adminId}', l.joint_person)")
                              ->whereOrRaw("FIND_IN_SET('{$adminId}', REPLACE(REPLACE(REPLACE(l.joint_person, '[', ''), ']', ''), '\"', ''))");
                        }
                    }
                });
            } else {
                return null;
            }
        }

        // 业务员筛选：精确匹配，并校验非超级管理员的权限范围
        if ($selectedPrUser !== '') {
            if (!$isSuperAdmin && (empty($visibleUsers) || !in_array($selectedPrUser, $visibleUsers, true))) {
                // 不返回 null，避免分页异常
                $query->where('l.id', -1);
            }
            $query->where('l.pr_user', '=', $selectedPrUser);
        }

        $total = (int)(clone $query)->distinct(true)->count('l.id');
        if ($total === 0) {
            return null;
        }

        $rows = (clone $query)
            ->field('l.*')
            ->order('l.at_time desc')
            ->page($page, $limit)
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        // 兼容检查客户页面展示字段：主/辅电话
        $leadIds = array_values(array_unique(array_filter(array_column($rows, 'id'))));
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
                $lid = (int)$c['leads_id'];
                if (!isset($phoneMap[$lid])) {
                    $phoneMap[$lid] = ['main' => '', 'aux' => ''];
                }
                if ((int)$c['contact_type'] === 1 && $phoneMap[$lid]['main'] === '') {
                    $phoneMap[$lid]['main'] = (string)$c['contact_value'];
                } elseif ((int)$c['contact_type'] === 3 && $phoneMap[$lid]['aux'] === '') {
                    $phoneMap[$lid]['aux'] = (string)$c['contact_value'];
                }
            }
        }
        foreach ($rows as &$row) {
            $lid = (int)$row['id'];
            $row['main_phone'] = isset($phoneMap[$lid]) ? $phoneMap[$lid]['main'] : '';
            $row['aux_phone']  = isset($phoneMap[$lid]) ? $phoneMap[$lid]['aux'] : '';
        }
        unset($row);

        return [
            'data'  => $rows,
            'total' => $total,
        ];
    }


    //成交客户查询
    public function getChengjiaoClientSearchList($page, $limit, $keyword)
    {


        $mapAtTime = []; //添加时间
        $mapKhRank = []; //客户级别
        $mapKhStatus = []; //客户状态
        $mapPhone = []; //手机号模糊查询
        $mapKhName = []; //客户名称
        $mapXsSource = []; //线索/客户来源
        $mapPrUser = []; //业务员/负责人

        if ($keyword['pr_user'] != '') {
            $mapPrUser['pr_user'] = $keyword['pr_user'];
            //$mapPrUser = [['pr_user','like','%'.$keyword['pr_user'].'%']];
        } else {
            if (session('aid') == 1) {
            } else {
                $mapPrUser['pr_user'] = session('username');
            }
        }
        if ($keyword['at_time'] != '') {
            $at = $keyword['at_time']; //日期
            $end_at = date('Y-m-d', strtotime("$at+1day"));
            $mapAtTime = [['at_time', 'between time', [strtotime($at), strtotime($end_at)]]];
        }

        if ($keyword['kh_rank'] != '') {
            $mapKhRank = $this->buildKhRankCompatWhere($keyword['kh_rank'], 'kh_rank');
        }

        if ($keyword['kh_status'] != '') {

            $mapKhStatus =  ['kh_status' => $keyword['kh_status']];
        }

        if ($keyword['phone'] != '') {
            $mapPhone = $this->getContactSearch($keyword['phone']);
        }

        if ($keyword['kh_name'] != '') {
            $mapKhName = [['kh_name', 'like', '%' . $keyword['kh_name'] . '%']];
        }

        if ($keyword['xs_source'] != '') {

            $mapXsSource =  ['xs_source' => $keyword['xs_source']];
        }



        $result  = Db::table('crm_leads')
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapKhStatus)
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapAtTime)
            ->where($mapPrUser)
            ->where(['status' => 1, 'issuccess' => 1]) //0 线索，1客户，2公海
            // ->where(['pr_user' => session('username')]) //负责人
            ->whereTime('at_time', $keyword['timebucket'] ? $keyword['timebucket'] : null)
            ->order('at_time desc')
            ->paginate(array('list_rows' => $limit, 'page' => $page))
            ->toArray();

        //数据集判断方式
        //if($result->isEmpty()){return null;}
        if ($result['total'] == 0) {
            return null;
        } else {
            return $result;
        }
    }


    /**
     * 客户列表（全部）基础查询：
     * - 与 getClientSearchListAll() 使用完全一致的筛选/权限/时间口径
     * - 不负责分页
     * - 不负责展示字段格式化
     */
    public function buildClientSearchAllBaseQuery(array $keyword = [], bool $skipOrgUserLimit = false)
    {
        $keyword = array_merge([
            'timebucket' => '',
            'kh_rank' => '',
            'kh_status' => '',
            'phone' => '',
            'kh_name' => '',
            'xs_source' => '',
            'pr_user' => '',
            'inquiry_id' => '',
            'port_id' => '',
        ], $keyword);

        $mapAtTime   = []; // 添加时间
        $mapKhRank   = []; // 客户级别
        $mapKhStatus = []; // 客户状态
        $mapPhone    = []; // 手机号模糊 -> leads_id 集合
        $mapKhName   = []; // 客户名称
        $mapXsSource = []; // 客户来源
        $mapPrUser   = []; // 负责人
        $mapInquiry  = []; // 所属渠道
        $mapPort     = []; // 运营端口

        if (!empty($keyword['timebucket'])) $mapAtTime[] = $keyword['timebucket'];
        if ($keyword['kh_rank']   !== '' && $keyword['kh_rank']   !== null) $mapKhRank   = $this->buildKhRankCompatWhere($keyword['kh_rank'], 'l.kh_rank');
        if ($keyword['kh_status'] !== '' && $keyword['kh_status'] !== null) $mapKhStatus = ['kh_status' => $keyword['kh_status']];
        if (!empty($keyword['phone']))  $mapPhone = $this->getContactSearchAll($keyword['phone'], 'l');
        if (!empty($keyword['kh_name'])) $mapKhName = [['kh_name', 'like', '%' . $keyword['kh_name'] . '%']];
        if ($keyword['xs_source'] !== '' && $keyword['xs_source'] !== null) $mapXsSource = ['xs_source' => $keyword['xs_source']];
        if (!empty($keyword['pr_user'])) $mapPrUser = [['pr_user', 'like', '%' . $keyword['pr_user'] . '%']];
        if ($keyword['inquiry_id'] !== '' && $keyword['inquiry_id'] !== null) $mapInquiry = ['l.inquiry_id' => $keyword['inquiry_id']];
        if ($keyword['port_id'] !== '' && $keyword['port_id'] !== null) $mapPort = ['l.port_id' => $keyword['port_id']];

        $current_admin = Admin::getMyInfo();
        $team_name = $current_admin['team_name'] ?? '';
        $a_where = [];
        if (strpos($current_admin['org'], 'admin') === false) {
            $a_where = [(new ControllerClient())->getorgWhere($current_admin['org'])];
        }
        $usernames  = [$current_admin['username']];
        if ($current_admin['group_id'] == 1) {
            $usernames = [];
            if ($a_where) {
                $usernames = Db::name('admin')->where($a_where)->column('username');
            }
        } elseif ($team_name) {
            $usernames = Db::name('admin')->where('team_name', $team_name)->where($a_where)->column('username');
        }

        $query = Db::table('crm_leads')->alias('l')
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapKhStatus)
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapInquiry)
            ->where($mapPort)
            ->where($mapAtTime)
            ->where($mapPrUser);

        if (!$skipOrgUserLimit) {
            $query->where(function ($subQuery) use ($usernames) {
                if ($usernames) {
                    $subQuery->whereIn('l.pr_user', $usernames);
                }
            });
        }

        return $query;
    }

    /**
     * 客户列表（全部）最终结果集（用于对齐其它统计口径）：
     * - 与 getClientSearchListAll() 使用同一套 joins + group 去重逻辑
     * - 只选择必要字段（id/pr_user/inquiry_id/port_id），避免额外开销
     * - 不分页、不排序（排序不影响结果集）
     */
    public function buildClientSearchListAllFinalIdQuery(array $keyword = [])
    {
        return $this->buildClientSearchAllBaseQuery($keyword)
            ->leftJoin('crm_contacts c', "c.leads_id = l.id AND c.is_delete = 0 AND c.contact_type IN (1,3)")
            ->field([
                'l.id',
                'l.pr_user',
                'l.inquiry_id',
                'l.port_id',
            ])
            ->group('l.id');
    }

    //客户列表查询所有
    // 查询（客户列表页用）
    public function getClientSearchListAll($page, $limit, $keyword)
    {
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);

        // 1) 仅按 leads 主表筛选统计，避免 contacts 一对多放大 total
        $baseQuery = $this->buildClientSearchAllBaseQuery((array)$keyword);
        $total = (int)(clone $baseQuery)->distinct(true)->count('l.id');
        if ($total === 0) {
            return null;
        }

        // 2) 分页只查 leads 当前页
        $rows = (clone $baseQuery)
            ->field('l.*')
            ->order('l.at_time desc')
            ->page($page, $limit)
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        // 3) 批量查询当前页 contacts（主/辅电话 + 职位身份）
        $leadIds = array_values(array_unique(array_filter(array_column($rows, 'id'))));
        $phoneMap = [];
        if (!empty($leadIds)) {
            $contacts = Db::table('crm_contacts')
                ->alias('c')
                ->leftJoin('crm_position_title pt', 'pt.id = c.position_title_id AND pt.is_deleted = 0')
                ->where('c.is_delete', 0)
                ->whereIn('c.leads_id', $leadIds)
                ->whereIn('c.contact_type', [1, 3])
                ->order('c.id asc')
                ->field('c.leads_id,c.contact_type,c.contact_value,c.position_title_id,c.position_title,pt.title_name as pt_title_name')
                ->select();
            if (is_object($contacts) && method_exists($contacts, 'toArray')) {
                $contacts = $contacts->toArray();
            }

            foreach ($contacts as $c) {
                $lid = (int)$c['leads_id'];
                if (!isset($phoneMap[$lid])) {
                    $phoneMap[$lid] = [
                        'main_phones' => [],
                        'aux_phones' => [],
                        'main_titles' => [],
                        'aux_title_first' => '',
                    ];
                }

                $contactValue = trim((string)$c['contact_value']);
                if ($contactValue === '') {
                    continue;
                }

                // 职位身份：优先 title_name，其次 position_title，最后“未填写”
                $positionTitleName = trim((string)($c['pt_title_name'] ?? ''));
                if ($positionTitleName === '') {
                    $positionTitleName = trim((string)($c['position_title'] ?? ''));
                }
                if ($positionTitleName === '') {
                    $positionTitleName = '未填写';
                }
                $phoneAndTitle = $contactValue . '-' . $positionTitleName;

                if ((int)$c['contact_type'] === 1) {
                    if (!in_array($contactValue, $phoneMap[$lid]['main_phones'], true)) {
                        $phoneMap[$lid]['main_phones'][] = $contactValue;
                    }
                    if (!in_array($phoneAndTitle, $phoneMap[$lid]['main_titles'], true)) {
                        $phoneMap[$lid]['main_titles'][] = $phoneAndTitle;
                    }
                } elseif ((int)$c['contact_type'] === 3) {
                    if (!in_array($contactValue, $phoneMap[$lid]['aux_phones'], true)) {
                        $phoneMap[$lid]['aux_phones'][] = $contactValue;
                    }
                    // 辅助电话职位身份只保留第一条
                    if ($phoneMap[$lid]['aux_title_first'] === '') {
                        $phoneMap[$lid]['aux_title_first'] = $phoneAndTitle;
                    }
                }
            }
        }

        // 4) 合并回当前页 rows，保持返回字段结构不变
        foreach ($rows as &$row) {
            $lid = (int)$row['id'];
            $mainPhones = isset($phoneMap[$lid]['main_phones']) ? $phoneMap[$lid]['main_phones'] : [];
            $auxPhones = isset($phoneMap[$lid]['aux_phones']) ? $phoneMap[$lid]['aux_phones'] : [];
            $mainTitles = isset($phoneMap[$lid]['main_titles']) ? $phoneMap[$lid]['main_titles'] : [];
            $auxTitleFirst = isset($phoneMap[$lid]['aux_title_first']) ? $phoneMap[$lid]['aux_title_first'] : '';

            $row['main_phone'] = !empty($mainPhones) ? implode(',', $mainPhones) : '';
            $row['aux_phone'] = !empty($auxPhones) ? implode('<br>', $auxPhones) : '';
            $row['main_phone_position_titles'] = !empty($mainTitles) ? implode(',', $mainTitles) : '';
            $row['aux_phone_position_titles'] = $auxTitleFirst;
        }
        unset($row);

        return [
            'data' => $rows,
            'total' => $total,
        ];
    }



    /**
     * 号码模糊 -> leads_id 条件
     * @param string $phone   输入号码（可含空格/符号/国家码）
     * @param string $alias   可选主表别名（如 'l'），用于避免 id 歧义
     * @return array  形如  [['l.id','in',[...]]] 或 [['id','in',[...]]]
     */
    public function getContactSearchAll($phone, $alias = '')
    {
        $phone = trim((string)$phone);
        if ($phone === '') return [];

        // 仅取数字做模糊
        $phoneKeyword = preg_replace('/\D+/', '', $phone);

        $rows = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('contact_type', 'in', [1, 3]) // 只在主/辅电话里匹配
            ->where('contact_value', 'like', "%{$phoneKeyword}%")
            ->field('leads_id')
            ->select();

        $leadsIds = array_column($rows, 'leads_id');
        if (empty($leadsIds)) {
            return [[$alias ? $alias . '.id' : 'id', '=', -1]]; // 返回一个必不成立条件
        }
        return [[$alias ? $alias . '.id' : 'id', 'in', $leadsIds]];
    }



}
