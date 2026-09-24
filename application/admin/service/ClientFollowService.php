<?php

namespace app\admin\service;

use app\admin\model\ClientFollow;
use think\Db;
use Throwable;

class ClientFollowService
{
    const NEXT_UP_TIME_DEFAULT_CLOCK = '09:00:00';

    /** 跟进身份快照：客户负责人 */
    const FOLLOW_ROLE_OWNER = 'owner';
    /** 跟进身份快照：客户协同人 */
    const FOLLOW_ROLE_COLLABORATOR = 'collaborator';
    /** 跟进身份快照：仅超管权限写入 */
    const FOLLOW_ROLE_ADMIN = 'admin';

    /** @var bool|null */
    private $followRoleColumnExists = null;

    /**
     * 保存客户跟进记录，并同步更新 crm_leads 跟进字段
     *
     * @param int|string $leadsId
     * @param string $content
     * @param string|null $nextUpTime
     * @param array $operatorInfo ['admin_id' => int, 'username' => string]
     * @return array
     */
    public function saveFollow($leadsId, $content, $nextUpTime, $operatorInfo = [])
    {
        $leadsId = (int)$leadsId;
        $content = trim((string)$content);
        $nextUpTime = trim((string)$nextUpTime);
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));

        if ($leadsId <= 0) {
            return $this->fail('缺少客户ID');
        }
        if ($operatorId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }
        if ($content === '') {
            return $this->fail('请输入跟进内容');
        }

        // 不信任前端传入的 follow_role；身份仅由后端计算
        if (!$this->hasFollowRoleColumn()) {
            return $this->fail('跟进身份字段未就绪，请先执行数据库变更后再保存跟进');
        }

        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$client) {
            return $this->fail('客户不存在');
        }

        if (!$this->canOperateClient($client, $operatorId, $operatorName)) {
            return $this->fail('您没有权限操作该客户');
        }

        $followRole = $this->resolveFollowRole($client, $operatorId, $operatorName);
        if ($followRole === '') {
            return $this->fail('您没有权限操作该客户');
        }

        list($ok, $nextUpValue, $nextUpErrMsg) = $this->normalizeNextUpTimeValue($nextUpTime);
        if (!$ok) {
            return $this->fail($nextUpErrMsg);
        }

        $nowTimestamp = time();
        $nowDateTime = date('Y-m-d H:i:s', $nowTimestamp);

        Db::startTrans();
        try {
            // 提交时重新读取客户状态，避免弹窗期间客户已转移仍写入旧身份
            $freshClient = Db::table('crm_leads')
                ->where('id', $leadsId)
                ->lock(true)
                ->find();
            if (!$freshClient) {
                throw new \RuntimeException('客户不存在');
            }
            if (!$this->canOperateClient($freshClient, $operatorId, $operatorName)) {
                throw new \RuntimeException('您没有权限操作该客户');
            }
            $followRole = $this->resolveFollowRole($freshClient, $operatorId, $operatorName);
            if ($followRole === '') {
                throw new \RuntimeException('您没有权限操作该客户');
            }

            Db::table('crm_comment')->insert([
                'leads_id' => $leadsId,
                'user_id' => $operatorId,
                'follow_role' => $followRole,
                'reply_msg' => $content,
                'create_date' => $nowTimestamp,
            ]);

            Db::table('crm_leads')->where('id', $leadsId)->update([
                'last_up_records' => $content,
                'last_up_time' => $nowDateTime,
                'next_up_time' => $nextUpValue,
                'ut_time' => $nowDateTime,
            ]);

            Db::commit();

            return [
                'code' => 0,
                'msg' => '保存成功',
                'data' => [
                    'leads_id' => $leadsId,
                    'reply_msg' => $content,
                    'create_date' => date('Y年m月d日 H:i', $nowTimestamp),
                    'next_up_time' => $this->formatNextUpTimeForDisplay($nextUpValue),
                    'follow_role' => $followRole,
                    'follow_role_text' => $this->getFollowRoleText($followRole),
                ],
            ];
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail('保存跟进失败：' . $e->getMessage());
        }
    }

    /**
     * 当前用户是否有权限操作客户（admin_id=1 超管放行）
     * 供颜色标记等场景复用，规则与跟进保存一致
     */
    public function canOperateClient(array $client, $operatorId, $operatorName)
    {
        return $this->resolveFollowRole($client, $operatorId, $operatorName) !== '';
    }

    /**
     * 计算跟进写入身份（不信任前端）
     * 优先级：owner > collaborator > admin
     *
     * @return string owner|collaborator|admin|''（空表示无写权限）
     */
    public function resolveFollowRole(array $client, $operatorId, $operatorName)
    {
        $operatorId = (int)$operatorId;
        $operatorName = trim((string)$operatorName);
        if ($operatorId <= 0) {
            return '';
        }

        $ownerName = trim((string)($client['pr_user'] ?? ''));
        if ($ownerName !== '' && $operatorName !== '' && $ownerName === $operatorName) {
            return self::FOLLOW_ROLE_OWNER;
        }

        if ($this->isJointPerson($client, $operatorId)) {
            return self::FOLLOW_ROLE_COLLABORATOR;
        }

        if ($operatorId === 1) {
            return self::FOLLOW_ROLE_ADMIN;
        }

        return '';
    }

    /**
     * 身份快照展示文案
     */
    public function getFollowRoleText($followRole)
    {
        $role = trim((string)$followRole);
        if ($role === self::FOLLOW_ROLE_OWNER) {
            return '客户负责人';
        }
        if ($role === self::FOLLOW_ROLE_COLLABORATOR) {
            return '客户协同人';
        }
        if ($role === self::FOLLOW_ROLE_ADMIN) {
            return '管理员';
        }
        if ($role === '') {
            return '历史身份待确认';
        }
        return '未知身份';
    }

    /**
     * 客户跟进只读权限（不扩大写权限）
     * 覆盖：负责人 / 协同人 / admin_id=1 / 全订单管理权限 / 全部客户列表可见 / 检查客户可见 / 检查订单可见
     */
    public function canReadClientFollow(array $client, $operatorId, $operatorName, array $adminContext = [])
    {
        $operatorId = (int)$operatorId;
        $operatorName = trim((string)$operatorName);
        if ($operatorId <= 0) {
            return false;
        }

        if ($this->canOperateClient($client, $operatorId, $operatorName)) {
            return true;
        }

        $adminInfo = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($adminContext['group_id'] ?? 0),
        ];
        if (OrderService::canManageAllOrders($adminInfo)) {
            return true;
        }

        $clientId = (int)($client['id'] ?? 0);
        if ($clientId <= 0) {
            return false;
        }

        $visibleId = model('Client')->buildClientSearchAllBaseQuery([])
            ->where('l.id', $clientId)
            ->value('l.id');
        if (!empty($visibleId)) {
            return true;
        }

        $visibleUsers = $this->resolveCheckClientVisibleUsernames($adminContext);
        $currentAdmin = [
            'admin_id' => $operatorId,
            'group_id' => (int)($adminContext['group_id'] ?? 0),
            'username' => $operatorName,
            'is_super_admin' => $this->isCheckClientFullViewer($adminContext) ? 1 : 0,
        ];
        $built = model('Client')->buildCheckClientQuery(
            ['__id_in' => [$clientId]],
            $visibleUsers,
            $currentAdmin
        );
        if ($built !== null && !empty($built['query'])) {
            $checkVisibleId = $built['query']->value('l.id');
            if (!empty($checkVisibleId)) {
                return true;
            }
        }

        $bridgeContext = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($adminContext['group_id'] ?? 0),
            'team_name' => (string)($adminContext['team_name'] ?? ''),
        ];
        return $this->canReadByVisibleCheckOrder($client, $bridgeContext);
    }

    /**
     * 检查订单只读桥接：当前用户对某客户对应的检查订单拥有查看权限时，允许只读跟进
     * 不扩大写权限；可见用户名复用 CheckOrderService::getAllowedUsernames，不复制特殊账号名单
     */
    private function canReadByVisibleCheckOrder(array $client, array $adminContext)
    {
        $clientId = (int)($client['id'] ?? 0);
        if ($clientId <= 0) {
            return false;
        }

        $checkOrderService = new CheckOrderService();
        $allowedUsernames = $checkOrderService->getAllowedUsernames($adminContext);
        $allowedUsernames = array_values(array_unique(array_filter(array_map('trim', (array)$allowedUsernames))));
        if (empty($allowedUsernames)) {
            return false;
        }

        $contactRows = Db::table('crm_contacts')
            ->where('leads_id', $clientId)
            ->where('is_delete', 0)
            ->column('contact_value');
        if (empty($contactRows)) {
            return false;
        }

        $uniqueRaw = [];
        $uniqueNormalized = [];
        $seen = [];
        foreach ($contactRows as $raw) {
            $raw = trim((string)$raw);
            if ($raw === '') {
                continue;
            }
            $norm = OrderService::normalizeContact($raw);
            $seenKey = $norm !== '' ? $norm : $raw;
            if (isset($seen[$seenKey])) {
                continue;
            }
            $seen[$seenKey] = true;

            $match = OrderService::resolveUniqueLeadsIdByContact($raw);
            if (empty($match['ok']) || (int)$match['leads_id'] !== $clientId) {
                continue;
            }
            $uniqueRaw[] = $raw;
            if ($norm !== '') {
                $uniqueNormalized[] = $norm;
            }
        }

        $uniqueRaw = array_values(array_unique(array_filter($uniqueRaw)));
        $uniqueNormalized = array_values(array_unique(array_filter($uniqueNormalized)));
        if (empty($uniqueRaw) && empty($uniqueNormalized)) {
            return false;
        }

        $orderId = Db::table('crm_client_order')
            ->where('check_status', 2)
            ->where('pr_user', 'in', $allowedUsernames)
            ->where(function ($query) use ($uniqueRaw, $uniqueNormalized) {
                if (!empty($uniqueRaw)) {
                    $query->whereIn('contact', $uniqueRaw);
                }
                foreach ($uniqueNormalized as $norm) {
                    $query->whereOrRaw(
                        "REPLACE(REPLACE(REPLACE(IFNULL(contact,''), '+', ''), '-', ''), ' ', '') = '"
                        . addslashes($norm) . "'"
                    );
                }
            })
            ->value('id');

        return !empty($orderId);
    }

    /**
     * 为评论行附加跟进身份展示字段（增量，不覆盖原字段）
     */
    public function appendFollowRoleDisplay(array $comment)
    {
        $role = '';
        if (array_key_exists('follow_role', $comment)) {
            $role = trim((string)$comment['follow_role']);
        }
        $comment['follow_role'] = $role;
        $comment['follow_role_text'] = $this->getFollowRoleText($role);
        return $comment;
    }

    /**
     * 校验单个客户是否可操作（重新查库，不信任前端）
     *
     * @return array{ok:bool,msg:string,client?:array}
     */
    public function assertCanOperateClientId($leadsId, $operatorId, $operatorName): array
    {
        $leadsId = (int)$leadsId;
        $operatorId = (int)$operatorId;
        $operatorName = trim((string)$operatorName);

        if ($leadsId <= 0) {
            return ['ok' => false, 'msg' => '缺少客户ID'];
        }
        if ($operatorId <= 0) {
            return ['ok' => false, 'msg' => '登录状态已失效，请重新登录'];
        }

        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$client) {
            return ['ok' => false, 'msg' => '客户不存在'];
        }

        if (!$this->canOperateClient($client, $operatorId, $operatorName)) {
            return ['ok' => false, 'msg' => '您没有权限操作该客户'];
        }

        return ['ok' => true, 'msg' => '', 'client' => $client];
    }

    /**
     * 批量预校验客户操作权限：整批原子，任一失败则整批拒绝
     *
     * @param array $leadsIds
     * @param int|string $operatorId
     * @param string $operatorName
     * @return array{ok:bool,msg:string,leads_ids?:int[]}
     */
    public function assertCanOperateAllClientIds(array $leadsIds, $operatorId, $operatorName): array
    {
        $operatorId = (int)$operatorId;
        $operatorName = trim((string)$operatorName);

        if ($operatorId <= 0) {
            return ['ok' => false, 'msg' => '登录状态已失效，请重新登录'];
        }

        $leadsIds = array_values(array_unique(array_filter(array_map('intval', $leadsIds), function ($v) {
            return $v > 0;
        })));
        if (empty($leadsIds)) {
            return ['ok' => false, 'msg' => '请先选择客户'];
        }

        $clients = Db::table('crm_leads')
            ->whereIn('id', $leadsIds)
            ->field('id,pr_user,joint_person')
            ->select();
        $clientMap = [];
        foreach ($clients as $client) {
            $clientMap[(int)$client['id']] = $client;
        }

        foreach ($leadsIds as $lid) {
            if (!isset($clientMap[$lid])) {
                return ['ok' => false, 'msg' => '客户不存在（ID:' . $lid . '）'];
            }
            if (!$this->canOperateClient($clientMap[$lid], $operatorId, $operatorName)) {
                return ['ok' => false, 'msg' => '您没有权限操作客户（ID:' . $lid . '），整批已拒绝'];
            }
        }

        return ['ok' => true, 'msg' => '', 'leads_ids' => $leadsIds];
    }

    /**
     * 判断是否协同人：兼容 JSON 数组和逗号分隔两种存储格式
     */
    private function isJointPerson(array $client, $operatorId)
    {
        $jointRaw = trim((string)($client['joint_person'] ?? ''));
        if ($jointRaw === '') {
            return false;
        }

        $jointIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jointRaw)) {
            $decoded = json_decode($jointRaw, true);
            if (is_array($decoded)) {
                $jointIds = $decoded;
            }
        } else {
            $jointIds = array_values(array_filter(explode(',', $jointRaw)));
        }

        return in_array((string)$operatorId, array_map('strval', $jointIds), true);
    }

    /**
     * 标准化 next_up_time：
     * - 有值：校验并格式化为 Y-m-d H:i:s
     * - 无值：按字段可空性写入 NULL 或空字符串
     */
    private function normalizeNextUpTimeValue($nextUpTime)
    {
        if ($nextUpTime !== '') {
            if (preg_match('/^(\d{4}-\d{2}-\d{2})$/', $nextUpTime, $dateOnlyMatch)) {
                return [true, $dateOnlyMatch[1] . ' ' . self::NEXT_UP_TIME_DEFAULT_CLOCK, ''];
            }
            if (preg_match('/^(\d{4}-\d{2}-\d{2})\s+00:00:00$/', $nextUpTime, $midnightMatch)) {
                return [true, $midnightMatch[1] . ' ' . self::NEXT_UP_TIME_DEFAULT_CLOCK, ''];
            }

            $timestamp = strtotime($nextUpTime);
            if ($timestamp === false) {
                return [false, null, '下次跟进时间格式不正确'];
            }

            $normalized = date('Y-m-d H:i:s', $timestamp);
            if (substr($normalized, 11, 8) === '00:00:00') {
                $normalized = substr($normalized, 0, 10) . ' ' . self::NEXT_UP_TIME_DEFAULT_CLOCK;
            }
            return [true, $normalized, ''];
        }

        return [true, $this->emptyNextUpTimeValue(), ''];
    }

    /**
     * next_up_time 置空值的兼容处理
     */
    private function emptyNextUpTimeValue()
    {
        try {
            $column = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'next_up_time'");
            if (!empty($column) && isset($column[0]['Null']) && strtoupper((string)$column[0]['Null']) === 'YES') {
                return null;
            }
        } catch (Throwable $e) {
            // 忽略字段探测异常，默认回退为空字符串
        }

        return '';
    }

    /**
     * next_up_time 展示值格式化（仅用于返回前端展示）。
     */
    public function formatNextUpTimeForDisplay($nextUpTime)
    {
        if ($nextUpTime === null) {
            return '';
        }

        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null' || strtolower($raw) === 'undefined') {
            return '';
        }

        $timestamp = strtotime($raw);
        if ($timestamp === false) {
            return $raw;
        }

        return date('Y-m-d H:i:s', $timestamp);
    }

    /**
     * 跟进弹窗客户信息组装（控制器调度，service 统一组装展示字段）。
     *
     * @param array $operatorInfo ['admin_id'=>int,'username'=>string] 可选，传入时附加当前操作人身份
     */
    public function buildFollowClientDetailData(array $client, $clientId, array $operatorInfo = [])
    {
        if (!array_key_exists('next_up_time', $client)) {
            $client['next_up_time'] = '';
        }
        $client['next_up_time'] = $this->formatNextUpTimeForDisplay($client['next_up_time']);

        // 主/辅电话（1=主，3=辅）
        $mainPhone = '';
        $auxPhone = '';
        $contacts = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', (int)$clientId)
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

        // 客户状态展示名
        $khStatusValue = $client['kh_status'] ?? '';
        $statusName = $khStatusValue;
        if (is_numeric($khStatusValue)) {
            $statusInfo = Db::table('crm_client_status')->where('id', $khStatusValue)->find();
            if ($statusInfo) {
                $statusName = $statusInfo['status_name'];
            }
        }
        $client['kh_status_name'] = $statusName;

        // 客户级别展示名（兼容新老数据）
        $rankMap = Db::table('crm_client_rank')->column('rank_code', 'id');
        $rankNameMap = [];
        foreach ($rankMap as $rankId => $rankName) {
            $rankNameMap[(string)$rankId] = trim((string)$rankName);
        }
        $rawKhRank = trim((string)($client['kh_rank'] ?? ''));
        if ($rawKhRank === '') {
            $client['kh_rank_name'] = '';
        } elseif (preg_match('/^\d+$/', $rawKhRank)) {
            $client['kh_rank_name'] = isset($rankNameMap[$rawKhRank]) && trim((string)$rankNameMap[$rawKhRank]) !== ''
                ? trim((string)$rankNameMap[$rawKhRank])
                : $rawKhRank;
        } else {
            $client['kh_rank_name'] = $rawKhRank;
        }

        // 所属渠道名称
        $inquiryValue = $client['inquiry_id'] ?? '';
        $inquiryName = $inquiryValue;
        if (is_numeric($inquiryValue)) {
            $inquiryInfo = Db::table('crm_inquiry')->where('id', $inquiryValue)->find();
            if ($inquiryInfo) {
                $inquiryName = $inquiryInfo['inquiry_name'];
            }
        }
        $client['inquiry_name'] = $inquiryName;

        // 运营端口名称
        $portValue = $client['port_id'] ?? '';
        $portName = $portValue;
        if (is_numeric($portValue)) {
            $portInfo = Db::table('crm_inquiry_port')->where('id', $portValue)->find();
            if ($portInfo) {
                $portName = $portInfo['port_name'];
            }
        }
        $client['port_name'] = $portName;

        // 产品名称
        $productValue = $client['product_name'] ?? '';
        $productName = $productValue;
        if (is_numeric($productValue) && !empty($productValue)) {
            $productInfo = Db::table('crm_products')->where('id', $productValue)->find();
            if ($productInfo) {
                $productName = $productInfo['product_name'];
            }
        }
        $client['product_name'] = $productName;

        // 协同人
        $jointPersonIds = [];
        $jointPersonNames = [];
        if (!empty($client['joint_person'])) {
            $jp = $client['joint_person'];
            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                $tmp = json_decode($jp, true);
                if (is_array($tmp)) {
                    $jointPersonIds = $tmp;
                }
            } else {
                $jointPersonIds = array_values(array_filter(explode(',', $jp)));
            }

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

        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));
        if ($operatorId > 0) {
            $currentRole = $this->resolveFollowRole($client, $operatorId, $operatorName);
            $client['current_operator'] = $operatorName;
            $client['current_role'] = $currentRole;
            $client['current_role_text'] = $currentRole !== ''
                ? $this->getFollowRoleText($currentRole)
                : '';
            $client['can_write_follow'] = $currentRole !== '';
        }

        return $client;
    }

    /**
     * 获取并格式化跟进历史（统一时间格式）。
     */
    public function getFollowComments($clientId)
    {
        $comments = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where(['leads_id' => (int)$clientId, 'is_deleted' => 0])
            ->field('com.*, adm.username, adm.avatar')
            ->order('com.create_date desc')
            ->select();

        foreach ($comments as &$comment) {
            $comment['create_date'] = date('Y年m月d日 H:i', $comment['create_date']);
            $comment = $this->appendFollowRoleDisplay($comment);
        }
        unset($comment);

        return $comments;
    }

    /**
     * 批量快速跟进列表行组装（字段口径与“我的客户”保持一致）
     *
     * @param array $rows
     * @param array $markMap 行颜色标记：[leads_id => bg_color]，无标记则该行 bg_color 返回空字符串
     * @param array $remarkMap 行标记备注：[leads_id => remark]，无备注则该行 remark 返回空字符串（V2 新增，不影响原有 markMap 用法）
     */
    public function buildQuickFollowListRows(array $rows, array $markMap = [], array $remarkMap = [])
    {
        if (empty($rows)) {
            return [];
        }

        $leadIds = array_values(array_filter(array_map('intval', array_column($rows, 'id'))));
        if (empty($leadIds)) {
            return [];
        }

        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');
        $rankMap = Db::table('crm_client_rank')->column('rank_code', 'id');

        $productIds = array_values(array_unique(array_filter(array_map('intval', array_column($rows, 'product_name')))));
        $productNameMap = [];
        if (!empty($productIds)) {
            $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
            $categoryIds = array_values(array_unique(array_filter(array_map('intval', array_column($productRows, 'category_id')))));
            $categoryNameMap = !empty($categoryIds)
                ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                : [];
            foreach ($productRows as $prod) {
                $prodId = (int)($prod['id'] ?? 0);
                if ($prodId <= 0) {
                    continue;
                }
                $supplierName = isset($categoryNameMap[$prod['category_id']]) ? $categoryNameMap[$prod['category_id']] : '';
                $productNameMap[$prodId] = $prod['product_name'] . ($supplierName ? "({$supplierName})" : '');
            }
        }

        $phoneMap = [];
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

        $out = [];
        foreach ($rows as $row) {
            $id = (int)($row['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }

            $productRaw = $row['product_name'] ?? '';
            $productDisplay = (string)$productRaw;
            if ($productRaw !== '' && is_numeric($productRaw)) {
                $productId = (int)$productRaw;
                $productDisplay = isset($productNameMap[$productId]) ? (string)$productNameMap[$productId] : (string)$productRaw;
            }

            $inquiryId = $row['inquiry_id'] ?? '';
            $portId = $row['port_id'] ?? '';
            $rawKhRank = trim((string)($row['kh_rank'] ?? ''));
            if ($rawKhRank === '') {
                $rankDisplay = '';
            } elseif (preg_match('/^\d+$/', $rawKhRank)) {
                $rankDisplay = isset($rankMap[$rawKhRank]) && trim((string)$rankMap[$rawKhRank]) !== ''
                    ? trim((string)$rankMap[$rawKhRank])
                    : $rawKhRank;
            } else {
                $rankDisplay = $rawKhRank;
            }

            $out[] = [
                'id' => $id,
                'kh_name' => (string)($row['kh_name'] ?? ''),
                'main_phone' => isset($phoneMap[$id]) ? (string)$phoneMap[$id]['main'] : '',
                'aux_phone' => isset($phoneMap[$id]) ? (string)$phoneMap[$id]['aux'] : '',
                'product_name' => $productDisplay,
                'inquiry_name' => isset($inquiryMap[$inquiryId]) ? (string)$inquiryMap[$inquiryId] : (string)$inquiryId,
                'port_name' => isset($portMap[$portId]) ? (string)$portMap[$portId] : (string)$portId,
                'kh_rank' => (string)($row['kh_rank'] ?? ''),
                'kh_rank_name' => $rankDisplay,
                'last_up_records' => (string)($row['last_up_records'] ?? ''),
                'last_up_time' => $this->formatLastUpTimeForDisplay($row['last_up_time'] ?? ''),
                'next_up_time' => $this->formatNextUpTimeForDisplay($row['next_up_time'] ?? ''),
                'pr_user' => (string)($row['pr_user'] ?? ''),
                'bg_color' => isset($markMap[$id]) ? (string)$markMap[$id] : '',
                'remark' => isset($remarkMap[$id]) ? (string)$remarkMap[$id] : '',
            ];
        }

        return $out;
    }

    /**
     * 删除客户跟进记录（逻辑删除）并回写客户最新跟进摘要。
     *
     * @param int|string $commentId
     * @param array $operatorInfo ['admin_id' => int, 'username' => string]
     * @return array
     */
    public function deleteFollowComment($commentId, $operatorInfo = [])
    {
        $commentId = (int)$commentId;
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);

        if ($commentId <= 0) {
            return $this->fail('缺少跟进记录ID');
        }
        if ($operatorId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }

        $comment = Db::table('crm_comment')
            ->where('id', $commentId)
            ->where('is_deleted', 0)
            ->find();
        if (!$comment) {
            return $this->fail('跟进记录不存在或已删除');
        }

        if ($operatorId !== 1 && (int)$comment['user_id'] !== $operatorId) {
            return $this->fail('您无权删除该跟进记录');
        }

        $leadsId = (int)$comment['leads_id'];
        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$client) {
            return $this->fail('客户不存在');
        }

        $nowTimestamp = time();
        $nowDateTime = date('Y-m-d H:i:s', $nowTimestamp);

        Db::startTrans();
        try {
            $affectedRows = Db::table('crm_comment')
                ->where('id', $commentId)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_by' => $operatorId,
                    'deleted_at' => $nowTimestamp,
                ]);
            if ((int)$affectedRows <= 0) {
                throw new \RuntimeException('跟进记录已被删除，请刷新后重试');
            }

            $latestComment = Db::table('crm_comment')
                ->where('leads_id', $leadsId)
                ->where('is_deleted', 0)
                ->order('create_date desc,id desc')
                ->find();

            $leadUpdate = [
                'ut_time' => $nowDateTime,
            ];
            if ($latestComment) {
                $leadUpdate['last_up_records'] = (string)$latestComment['reply_msg'];
                $leadUpdate['last_up_time'] = date('Y-m-d H:i:s', (int)$latestComment['create_date']);
            } else {
                $leadUpdate['last_up_records'] = '';
                $leadUpdate['last_up_time'] = null;
            }

            Db::table('crm_leads')->where('id', $leadsId)->update($leadUpdate);

            Db::commit();
            return ['code' => 0, 'msg' => '删除成功', 'data' => []];
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail('删除失败：' . $e->getMessage());
        }
    }

    /**
     * 待办跟进管理表格数据（layui table）：Model 只负责分页查询，本方法负责全部展示层组装。
     *
     * @param int $page
     * @param int $limit
     * @param array $keyword 与 ClientFollow::buildTodoFollowBaseQuery 一致
     * @param array $currentAdmin 预留，透传 Model
     * @param string $scene todo_manage|team_todo
     * @return array{code:int,msg:string,data:array,count:int,rel:int}
     */
    public function getTodoFollowTableData($page, $limit, $keyword = [], $currentAdmin = [], $scene = 'todo_manage')
    {
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);
        $keyword = $this->normalizeTodoSearchKeyword($keyword);

        $list = (new ClientFollow())->getTodoFollowList($page, $limit, $keyword, $currentAdmin, $scene);
        if ($list === null || empty($list['data'])) {
            return [
                'code' => 0,
                'msg' => '获取成功!',
                'data' => [],
                'count' => 0,
                'rel' => 1,
            ];
        }

        $rows = $list['data'];
        $total = (int)($list['total'] ?? 0);

        $inquiryIds = array_unique(array_filter(array_column($rows, 'inquiry_id')));
        $portIds = array_unique(array_filter(array_column($rows, 'port_id')));
        $productIds = [];
        foreach ($rows as $r) {
            $pv = $r['product_name'] ?? '';
            if ($pv !== '' && $pv !== null && is_numeric($pv)) {
                $productIds[] = (int)$pv;
            }
        }
        $productIds = array_unique(array_filter($productIds));

        $inquiryMap = [];
        if (!empty($inquiryIds)) {
            $inquiryMap = Db::table('crm_inquiry')->whereIn('id', $inquiryIds)->column('inquiry_name', 'id');
        }
        $portMap = [];
        if (!empty($portIds)) {
            $portMap = Db::table('crm_inquiry_port')->whereIn('id', $portIds)->column('port_name', 'id');
        }
        $productMap = [];
        if (!empty($productIds)) {
            $productMap = Db::table('crm_products')->whereIn('id', $productIds)->column('product_name', 'id');
        }

        $jointIdSet = [];
        foreach ($rows as $r) {
            foreach ($this->parseJointPersonIds($r['joint_person'] ?? '') as $jid) {
                if ($jid > 0) {
                    $jointIdSet[$jid] = true;
                }
            }
        }
        $jointAdminMap = [];
        if (!empty($jointIdSet)) {
            $jointAdminMap = Db::table('admin')
                ->whereIn('admin_id', array_keys($jointIdSet))
                ->column('username', 'admin_id');
        }

        $out = [];
        foreach ($rows as $row) {
            $out[] = $this->buildTodoFollowTableRow($row, $inquiryMap, $portMap, $productMap, $jointAdminMap);
        }

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $out,
            'count' => $total,
            'rel' => 1,
        ];
    }

    /**
     * 待办跟进搜索条件标准化（Service 统一入口，Controller 只负责接收与转发）。
     *
     * @param mixed $keyword
     * @return array
     */
    private function normalizeTodoSearchKeyword($keyword)
    {
        $keyword = is_array($keyword) ? $keyword : [];
        $keyword = array_merge([
            'kh_name' => '',
            'phone' => '',
            'pr_user' => '',
            'next_up_start' => '',
            'next_up_end' => '',
            'inquiry_id' => '',
            'port_id' => '',
            'only_overdue' => '',
        ], $keyword);

        $keyword['kh_name'] = $this->normalizeSearchScalar($keyword['kh_name']);
        $keyword['phone'] = $this->normalizeSearchScalar($keyword['phone']);
        $keyword['pr_user'] = $this->normalizeSearchScalar($keyword['pr_user']);
        $keyword['next_up_start'] = $this->normalizeSearchDateTimeBound($keyword['next_up_start'], false);
        $keyword['next_up_end'] = $this->normalizeSearchDateTimeBound($keyword['next_up_end'], true);
        $keyword['inquiry_id'] = $this->normalizeSearchScalar($keyword['inquiry_id']);
        $keyword['port_id'] = $this->normalizeSearchScalar($keyword['port_id']);

        // 起止时间倒置时自动纠正，避免筛选结果异常为空。
        if ($keyword['next_up_start'] !== '' && $keyword['next_up_end'] !== '' && strcmp($keyword['next_up_start'], $keyword['next_up_end']) > 0) {
            $tmp = $keyword['next_up_start'];
            $keyword['next_up_start'] = $keyword['next_up_end'];
            $keyword['next_up_end'] = $tmp;
        }

        $onlyOverdueRaw = $keyword['only_overdue'];
        $keyword['only_overdue'] = $this->isTruthyFlag($onlyOverdueRaw) ? '1' : '';

        return $keyword;
    }

    /**
     * 搜索标量统一标准化：trim + 兜底 null/undefined。
     *
     * @param mixed $value
     * @return string
     */
    private function normalizeSearchScalar($value)
    {
        if ($value === null) {
            return '';
        }

        $s = trim((string)$value);
        $lower = strtolower($s);
        if ($lower === 'null' || $lower === 'undefined') {
            return '';
        }

        return $s;
    }

    /**
     * 搜索时间边界统一标准化：
     * - 支持 Y-m-d：起始补 00:00:00，结束补 23:59:59
     * - 支持可解析的日期时间：统一为 Y-m-d H:i:s
     * - 非法值回退为空字符串（相当于不加该条件）
     *
     * @param mixed $value
     * @param bool $isEndBound
     * @return string
     */
    private function normalizeSearchDateTimeBound($value, $isEndBound)
    {
        $v = $this->normalizeSearchScalar($value);
        if ($v === '') {
            return '';
        }

        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $v)) {
            return $v . ($isEndBound ? ' 23:59:59' : ' 00:00:00');
        }

        $ts = strtotime($v);
        if ($ts === false) {
            return '';
        }

        return date('Y-m-d H:i:s', $ts);
    }

    /**
     * 布尔型筛选统一识别（兼容 1/true/on/yes）。
     *
     * @param mixed $v
     * @return bool
     */
    private function isTruthyFlag($v)
    {
        if ($v === true || $v === 1 || $v === '1') {
            return true;
        }
        if (is_string($v)) {
            $s = strtolower(trim($v));
            return $s === 'true' || $s === 'yes' || $s === 'on';
        }
        return false;
    }

    /**
     * 单行待办列表展示数据（时间、电话、渠道、逾期与前端标签均在 Service 内完成）。
     *
     * @param array $row Model 行
     * @param array $inquiryMap id => inquiry_name
     * @param array $portMap id => port_name
     * @param array $productMap id => product_name
     * @param array $jointAdminMap admin_id => username
     * @return array
     */
    private function buildTodoFollowTableRow(array $row, array $inquiryMap, array $portMap, array $productMap, array $jointAdminMap)
    {
        $nextRaw = array_key_exists('next_up_time', $row) ? $row['next_up_time'] : null;
        $lastRaw = array_key_exists('last_up_time', $row) ? $row['last_up_time'] : null;

        $row['next_up_time_display'] = $this->formatNextUpTimeForDisplay($nextRaw);
        $row['last_up_time_display'] = $this->formatLastUpTimeForDisplay($lastRaw);

        $phones = $this->buildTodoFollowPhoneDisplay(
            $row['main_phone'] ?? null,
            $row['aux_phone'] ?? null
        );
        $row['main_phone_display'] = $phones['main_phone_display'];
        $row['aux_phone_display'] = $phones['aux_phone_display'];
        $row['phone_summary'] = $phones['phone_summary'];

        $iid = $row['inquiry_id'] ?? null;
        $row['inquiry_name'] = $this->mapInquiryName($iid, $inquiryMap);

        $pid = $row['port_id'] ?? null;
        $row['port_name'] = $this->mapPortName($pid, $portMap);

        $prodVal = $row['product_name'] ?? null;
        $row['product_name_display'] = $this->mapProductNameDisplay($prodVal, $productMap);

        $row['kh_name_display'] = $this->scalarToDisplay($row['kh_name'] ?? null, '-');
        $row['pr_user_display'] = $this->scalarToDisplay($row['pr_user'] ?? null, '-');
        $row['last_up_records_display'] = $this->scalarToDisplay($row['last_up_records'] ?? null, '-');

        $row['joint_person_display'] = $this->formatJointPersonNamesForTable($row['joint_person'] ?? '', $jointAdminMap);

        $row['overdue_days'] = $this->calcOverdueDays($nextRaw);
        $od = (int)$row['overdue_days'];
        $row['overdue_days_display'] = $od > 0 ? ($od . '天') : '—';

        $row['is_todo_follow'] = $this->isTodoFollow($nextRaw);

        $tag = $this->buildTodoFollowTag($nextRaw, (int)$row['overdue_days']);
        $row['todo_tag'] = $tag['todo_tag'];
        $row['todo_status_text'] = $tag['todo_status_text'];

        return $row;
    }

    /**
     * 解析 joint_person 为 admin_id 列表（JSON 数组或逗号分隔）。
     *
     * @param mixed $jointRaw
     * @return int[]
     */
    private function parseJointPersonIds($jointRaw)
    {
        $jointRaw = trim((string)$jointRaw);
        if ($jointRaw === '') {
            return [];
        }

        $jointIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jointRaw)) {
            $decoded = json_decode($jointRaw, true);
            if (is_array($decoded)) {
                $jointIds = $decoded;
            }
        } else {
            $jointIds = array_values(array_filter(explode(',', $jointRaw)));
        }

        $out = [];
        foreach ($jointIds as $id) {
            $n = (int)$id;
            if ($n > 0) {
                $out[] = $n;
            }
        }
        return $out;
    }

    /**
     * 协同人展示名（与 buildFollowClientDetailData 口径一致）。
     *
     * @param array $adminIdToName admin_id => username
     */
    private function formatJointPersonNamesForTable($jointRaw, array $adminIdToName)
    {
        $ids = $this->parseJointPersonIds($jointRaw);
        if (empty($ids)) {
            return '-';
        }
        $names = [];
        foreach ($ids as $id) {
            $name = isset($adminIdToName[$id]) ? $adminIdToName[$id] : (isset($adminIdToName[(string)$id]) ? $adminIdToName[(string)$id] : '');
            $name = trim((string)$name);
            $names[] = $name !== '' ? $name : (string)$id;
        }
        return empty($names) ? '-' : implode('、', $names);
    }

    /**
     * 上次跟进时间展示（与下次跟进时间同一套日期时间规范化）。
     *
     * @param mixed $lastUpTime
     * @return string
     */
    public function formatLastUpTimeForDisplay($lastUpTime)
    {
        return $this->formatNextUpTimeForDisplay($lastUpTime);
    }

    /**
     * 主电话 / 辅助电话：Model 已聚合为 main_phone、aux_phone，此处统一去空、去标签、供表格展示。
     *
     * @return array{main_phone_display:string,aux_phone_display:string,phone_summary:string}
     */
    private function buildTodoFollowPhoneDisplay($mainRaw, $auxRaw)
    {
        $main = $this->normalizePhoneChunk($mainRaw);
        $aux = $this->normalizePhoneChunk($auxRaw);
        if ($aux !== '') {
            $aux = str_replace(['<br>', '<br/>', '<br />'], '、', $aux);
            $aux = trim(preg_replace('/\s+/u', ' ', strip_tags($aux)));
        }

        $mainDisp = $main === '' ? '-' : $main;
        $auxDisp = $aux === '' ? '-' : $aux;

        $summary = $mainDisp;
        if ($auxDisp !== '-') {
            $summary = ($mainDisp === '-') ? $auxDisp : ($mainDisp . ' / ' . $auxDisp);
        }
        if ($summary === '-') {
            $summary = '-';
        }

        return [
            'main_phone_display' => $mainDisp,
            'aux_phone_display' => $auxDisp,
            'phone_summary' => $summary,
        ];
    }

    /**
     * @param mixed $chunk
     * @return string
     */
    private function normalizePhoneChunk($chunk)
    {
        if ($chunk === null) {
            return '';
        }
        $s = trim((string)$chunk);
        if ($s === '' || strtolower($s) === 'null' || strtolower($s) === 'undefined') {
            return '';
        }
        return $s;
    }

    /**
     * @param mixed $inquiryId
     * @param array $inquiryMap
     * @return string
     */
    private function mapInquiryName($inquiryId, array $inquiryMap)
    {
        if ($inquiryId === null || $inquiryId === '') {
            return '-';
        }
        if (is_numeric($inquiryId)) {
            $id = (int)$inquiryId;
            if (isset($inquiryMap[$id]) && trim((string)$inquiryMap[$id]) !== '') {
                return trim((string)$inquiryMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$inquiryId);
        return $s === '' ? '-' : $s;
    }

    /**
     * @param mixed $portId
     * @param array $portMap
     * @return string
     */
    private function mapPortName($portId, array $portMap)
    {
        if ($portId === null || $portId === '') {
            return '-';
        }
        if (is_numeric($portId)) {
            $id = (int)$portId;
            if (isset($portMap[$id]) && trim((string)$portMap[$id]) !== '') {
                return trim((string)$portMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$portId);
        return $s === '' ? '-' : $s;
    }

    /**
     * @param mixed $productVal
     * @param array $productMap
     * @return string
     */
    private function mapProductNameDisplay($productVal, array $productMap)
    {
        if ($productVal === null || $productVal === '') {
            return '-';
        }
        if (is_numeric($productVal)) {
            $id = (int)$productVal;
            if (isset($productMap[$id]) && trim((string)$productMap[$id]) !== '') {
                return trim((string)$productMap[$id]);
            }
            return '-';
        }
        $s = trim((string)$productVal);
        return $s === '' ? '-' : $s;
    }

    /**
     * 标量展示：空/null/伪空 统一为占位符。
     *
     * @param mixed $value
     * @param string $placeholder
     * @return string
     */
    private function scalarToDisplay($value, $placeholder = '-')
    {
        if ($value === null) {
            return $placeholder;
        }
        $s = trim((string)$value);
        if ($s === '' || strtolower($s) === 'null' || strtolower($s) === 'undefined') {
            return $placeholder;
        }
        return $s;
    }

    /**
     * 前端列表标签：逾期 N 天 / 今日待办 / 待跟进。
     *
     * @param mixed $nextUpTime
     * @param int $overdueDays
     * @return array{todo_tag:string,todo_status_text:string}
     */
    private function buildTodoFollowTag($nextUpTime, $overdueDays)
    {
        if ($overdueDays > 0) {
            return [
                'todo_tag' => 'overdue',
                'todo_status_text' => '逾期' . $overdueDays . '天',
            ];
        }
        $raw = $nextUpTime === null ? '' : trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return ['todo_tag' => 'unknown', 'todo_status_text' => '-'];
        }
        $ts = strtotime($raw);
        if ($ts !== false && date('Y-m-d', $ts) === date('Y-m-d')) {
            return [
                'todo_tag' => 'today',
                'todo_status_text' => '今日待办',
            ];
        }
        return [
            'todo_tag' => 'due',
            'todo_status_text' => '待跟进',
        ];
    }

    /**
     * 逾期天数：下次跟进日期的「日历日」早于「今天」时，返回相隔天数；否则 0。
     * 与「仅看逾期」口径一致：跨自然日才算逾期天数。
     *
     * @param mixed $nextUpTime
     * @return int
     */
    public function calcOverdueDays($nextUpTime)
    {
        if ($nextUpTime === null) {
            return 0;
        }
        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return 0;
        }
        $ts = strtotime($raw);
        if ($ts === false) {
            return 0;
        }
        $dueDay = date('Y-m-d', $ts);
        $today = date('Y-m-d');
        if ($dueDay >= $today) {
            return 0;
        }
        $dueStart = strtotime($dueDay . ' 00:00:00');
        $todayStart = strtotime($today . ' 00:00:00');
        return (int)(($todayStart - $dueStart) / 86400);
    }

    /**
     * 是否满足「待办时间」语义：有下次跟进时间且已到期（<= 当前时刻）。
     * 不校验 status / issuccess（由列表查询保证）。
     *
     * @param mixed $nextUpTime
     * @return bool
     */
    public function isTodoFollow($nextUpTime)
    {
        if ($nextUpTime === null) {
            return false;
        }
        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null') {
            return false;
        }
        $ts = strtotime($raw);
        if ($ts === false) {
            return false;
        }
        return $ts <= time();
    }

    /**
     * crm_comment.follow_role 是否已就绪（带进程内缓存）
     */
    public function hasFollowRoleColumn()
    {
        if ($this->followRoleColumnExists !== null) {
            return $this->followRoleColumnExists;
        }

        try {
            $column = Db::query("SHOW COLUMNS FROM `crm_comment` LIKE 'follow_role'");
            $this->followRoleColumnExists = !empty($column);
        } catch (Throwable $e) {
            $this->followRoleColumnExists = false;
        }

        return $this->followRoleColumnExists;
    }

    /**
     * 检查客户「全员查看」识别（与 Client 控制器既有名单保持一致）
     */
    private function isCheckClientFullViewer(array $adminContext)
    {
        $adminId = (int)($adminContext['admin_id'] ?? 0);
        $groupId = (int)($adminContext['group_id'] ?? 0);
        // 395李营 350李燕慧 375拜云梦 387张二凤 391范文清 405李鹏 392叶诗龙 407乔亚锋
        $specialAdminIds = [395, 350, 375, 387, 391, 405, 392, 407];

        return (
            $adminId === 1
            || $groupId === 1
            || in_array($adminId, $specialAdminIds, true)
        );
    }

    /**
     * 检查客户可见业务员用户名（与 Client::getCheckClientAllowedUsernames 口径一致）
     *
     * @return string[]
     */
    private function resolveCheckClientVisibleUsernames(array $adminContext)
    {
        $currentAdminId = (int)($adminContext['admin_id'] ?? 0);
        $currentUsername = trim((string)($adminContext['username'] ?? ''));
        $currentGroupId = (int)($adminContext['group_id'] ?? 0);
        $currentTeamName = trim((string)($adminContext['team_name'] ?? ''));

        if ($currentAdminId <= 0 && $currentUsername === '') {
            return [];
        }

        if ($currentGroupId <= 0 || $currentTeamName === '' || $currentUsername === '') {
            if ($currentAdminId > 0) {
                $row = Db::name('admin')
                    ->where('admin_id', $currentAdminId)
                    ->field('admin_id,username,group_id,team_name')
                    ->find();
                if ($row) {
                    $currentUsername = trim((string)($row['username'] ?? $currentUsername));
                    $currentGroupId = (int)($row['group_id'] ?? $currentGroupId);
                    $currentTeamName = trim((string)($row['team_name'] ?? $currentTeamName));
                }
            }
        }

        $specialAdminIds = [1, 395, 350, 375, 387, 391, 405, 407];
        $allVisibleGroupIds = [10, 11, 14, 17, 18, 19, 21, 22];
        $teamVisibleGroupIds = [17, 18];
        $selfVisibleGroupIds = [10, 11, 14, 19, 21, 22];

        $allowed = [];
        if (in_array($currentAdminId, $specialAdminIds, true)) {
            $allowed = Db::name('admin')
                ->where('group_id', 'in', $allVisibleGroupIds)
                ->where('username', '<>', '')
                ->column('username');
        } elseif (in_array($currentGroupId, $teamVisibleGroupIds, true) && $currentTeamName !== '') {
            $allowed = Db::name('admin')
                ->where('team_name', $currentTeamName)
                ->where('username', '<>', '')
                ->column('username');
        } elseif (in_array($currentGroupId, $selfVisibleGroupIds, true) && $currentUsername !== '') {
            $allowed = [$currentUsername];
        } elseif ($currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        $allowed = array_values(array_unique(array_filter(array_map('trim', (array)$allowed))));
        if (empty($allowed) && $currentUsername !== '') {
            $allowed = [$currentUsername];
        }

        return $allowed;
    }

    private function fail($msg)
    {
        return ['code' => 1, 'msg' => $msg, 'data' => []];
    }
}
