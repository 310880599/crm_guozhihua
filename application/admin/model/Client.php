<?php

namespace app\admin\model;

use app\admin\controller\Client as ControllerClient;
use think\Model;
use think\Db;
use app\admin\model\Contacts;


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
    public function getCheckClientSearchList($page, $limit, $keyword, array $visibleUsers = [])
    {
        $mapAtTime   = []; // 添加时间
        $mapKhRank   = []; // 客户级别
        $mapKhStatus = []; // 客户状态
        $mapPhone    = []; // 手机号模糊查询
        $mapKhName   = []; // 客户名称
        $mapXsSource = []; // 线索/客户来源
        $mapPrUser   = []; // 业务员/负责人（精确匹配）
        $where       = [];
        $mapInquiry  = [];
        $mapPort     = [];

        if (!empty($keyword['timebucket'])) {
            $mapAtTime[] = $keyword['timebucket'];
        }

        if ($keyword['kh_rank'] != '') {
            $mapKhRank = $this->buildKhRankCompatWhere($keyword['kh_rank'], 'kh_rank');
        }

        if ($keyword['kh_status'] != '') {
            $mapKhStatus = ['kh_status' => $keyword['kh_status']];
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
            $mapXsSource = ['xs_source' => $keyword['xs_source']];
        }

        if ($keyword['port_id'] != '') {
            $mapPort = ['port_id' => $keyword['port_id']];
        }

        $mapSourcePort = []; // 来源端口
        if (!empty($keyword['source_port'])) {
            $mapSourcePort = ['source_port' => $keyword['source_port']];
        }

        // 【新增-跟进筛选】最新跟进时间筛选条件
        $mapFollow      = [];
        $followNoFlag   = false; // recent_no_follow 标记
        $followBoundary = '';    // 边界时间（用于 recent_no_follow）

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
                $followNoFlag   = true;
                $followBoundary = $bd;
            }
        }

        // 统一清洗可见用户名列表，并做兜底（当前登录人）
        $visibleUsers = array_values(array_unique(array_filter(array_map('trim', $visibleUsers))));
        $currentUsername = trim((string) session('username'));
        if (empty($visibleUsers) && $currentUsername !== '') {
            $visibleUsers = [$currentUsername];
        }

        // 业务员精确筛选 + 权限校验
        $selectedPrUser = '';
        if (isset($keyword['pr_user'])) {
            $selectedPrUser = trim((string) $keyword['pr_user']);
        }
        if ($selectedPrUser !== '') {
            // 若前端传入的业务员不在可见范围内，直接返回空结果，防止越权
            if (empty($visibleUsers) || !in_array($selectedPrUser, $visibleUsers, true)) {
                return null;
            }
            // 精确匹配指定业务员
            $mapPrUser = ['pr_user' => $selectedPrUser];
        }

        $result  = Db::table('crm_leads')
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapInquiry)     // 使用所属渠道筛选
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapPort)        // 使用运营端口筛选
            ->where($mapPrUser)      // 精确业务员筛选（若有）
            ->where($mapAtTime)
            ->where($mapFollow)      // 【新增-跟进筛选】最新跟进时间筛选（recent_follow）
            ->where($where)
            ->where(['status' => 1]) //0 线索，1客户，2公海；不再限制成交状态，已成交+未成交都可见
            // 负责人：团队可见 / 个人可见（整体可见范围限制始终保留）
            ->where(function ($q) use ($visibleUsers, $currentUsername) {
                if (!empty($visibleUsers)) {
                    $q->whereIn('pr_user', $visibleUsers);
                } elseif ($currentUsername !== '') {
                    // 极端情况兜底：只看自己
                    $q->where('pr_user', $currentUsername);
                }
            })
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
    public function buildClientSearchAllBaseQuery(array $keyword = [])
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

        return Db::table('crm_leads')->alias('l')
            ->where(function ($query) use ($usernames) {
                if ($usernames) $query->whereIn('l.pr_user', $usernames);
            })
            ->where($mapPhone)
            ->where($mapKhName)
            ->where($mapKhStatus)
            ->where($mapKhRank)
            ->where($mapXsSource)
            ->where($mapInquiry)
            ->where($mapPort)
            ->where($mapAtTime)
            ->where($mapPrUser);
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
        $result = $this->buildClientSearchAllBaseQuery((array)$keyword)
            ->leftJoin('crm_contacts c', "c.leads_id = l.id AND c.is_delete = 0 AND c.contact_type IN (1,3)")
            ->leftJoin('crm_position_title pt', 'pt.id = c.position_title_id AND pt.is_deleted = 0')
            ->field([
                'l.*',
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 1, c.contact_value, NULL) ORDER BY c.id SEPARATOR ',') AS main_phone",
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 3, c.contact_value, NULL) ORDER BY c.id SEPARATOR '<br>') AS aux_phone",
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 1, CONCAT(c.contact_value, '-', CASE
                    WHEN c.position_title_id IS NOT NULL AND c.position_title_id <> 0 AND pt.title_name IS NOT NULL AND pt.title_name <> '' THEN pt.title_name
                    WHEN (c.position_title_id IS NULL OR c.position_title_id = 0) AND c.position_title IS NOT NULL AND c.position_title <> '' THEN c.position_title
                    ELSE '未填写'
                END), NULL) ORDER BY c.id SEPARATOR ',') AS main_phone_position_titles",
                "GROUP_CONCAT(DISTINCT IF(c.contact_type = 3, CONCAT(c.contact_value, '-', CASE
                    WHEN c.position_title_id IS NOT NULL AND c.position_title_id <> 0 AND pt.title_name IS NOT NULL AND pt.title_name <> '' THEN pt.title_name
                    WHEN (c.position_title_id IS NULL OR c.position_title_id = 0) AND c.position_title IS NOT NULL AND c.position_title <> '' THEN c.position_title
                    ELSE '未填写'
                END), NULL) ORDER BY c.id SEPARATOR ',') AS aux_phone_position_titles",
            ])
            ->group('l.id')
            ->order('l.at_time desc')
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();

        return ($result['total'] == 0) ? null : $result;
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
