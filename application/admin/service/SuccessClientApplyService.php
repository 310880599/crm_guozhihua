<?php

namespace app\admin\service;

use app\admin\controller\Client as ClientController;
use think\Db;
use Throwable;

/**
 * 成交客户申请审核（列表、详情、通过、驳回、提交）
 */
class SuccessClientApplyService
{
    private const TABLE_APPLY = 'crm_success_client_apply';
    private const TABLE_APPLY_CONFIG = 'crm_success_client_apply_config';
    private const TABLE_LEADS = 'crm_leads';
    private const BATCH_APPROVE_MAX = 500;

    private const CHECK_PENDING = 0;
    private const CHECK_APPROVED = 1;
    private const CHECK_REJECTED = 2;

    /**
     * 申请成交时间准入截止点（业务时区 Asia/Shanghai）
     * 规则：crm_leads.at_time 必须严格早于该时刻
     */
    private const APPLY_AT_TIME_DEADLINE = '2025-12-18 00:00:00';

    private const MSG_APPLY_TIME_INELIGIBLE = '该客户不符合申请成交条件：仅允许2025年12月17日及以前创建或导入到CRM的客户申请成交。';
    private const MSG_APPLY_TIME_INVALID = '客户创建时间异常，无法申请成交，请联系管理员核实。';

    /**
     * 判断客户 at_time 是否符合申请成交时间准入（严格早于截止时间）
     *
     * @param mixed $atTime crm_leads.at_time
     * @return bool
     */
    public function isApplyTimeEligible($atTime)
    {
        $dt = $this->parseValidAtTime($atTime);
        if ($dt === null) {
            return false;
        }

        $deadline = $this->getApplyAtTimeDeadline();
        return $dt < $deadline;
    }

    /**
     * 时间准入失败时的业务提示；符合条件返回空字符串
     *
     * @param mixed $atTime crm_leads.at_time
     * @return string
     */
    public function getApplyTimeIneligibleMessage($atTime)
    {
        if ($this->parseValidAtTime($atTime) === null) {
            return self::MSG_APPLY_TIME_INVALID;
        }
        if (!$this->isApplyTimeEligible($atTime)) {
            return self::MSG_APPLY_TIME_INELIGIBLE;
        }

        return '';
    }

    /**
     * 业务员提交成交客户申请（不修改 crm_leads.issuccess）
     */
    public function submitApply($leadsId, $proofImage, $applyRemark, array $lead, $applyUserId, $applyUser)
    {
        $leadsId = (int)$leadsId;
        $proofImage = trim((string)$proofImage);
        $applyRemark = trim((string)$applyRemark);
        $applyUserId = (int)$applyUserId;
        $applyUser = trim((string)$applyUser);

        if ($leadsId <= 0) {
            return $this->fail('客户ID不能为空');
        }
        if ($proofImage === '') {
            return $this->fail('请上传成交凭证');
        }

        Db::startTrans();
        try {
            $leadRow = Db::table(self::TABLE_LEADS)->where('id', $leadsId)->lock(true)->find();
            if (!$leadRow) {
                throw new \RuntimeException('客户不存在');
            }
            if ((int)($leadRow['status'] ?? 0) !== 1) {
                throw new \RuntimeException('客户状态无效，无法提交成交申请');
            }
            if ((int)($leadRow['issuccess'] ?? 0) !== -1) {
                throw new \RuntimeException('仅未成交客户可提交成交申请');
            }

            $timeMsg = $this->getApplyTimeIneligibleMessage($leadRow['at_time'] ?? null);
            if ($timeMsg !== '') {
                throw new \RuntimeException($timeMsg);
            }

            $pending = Db::table(self::TABLE_APPLY)
                ->where('leads_id', $leadsId)
                ->where('check_status', self::CHECK_PENDING)
                ->lock(true)
                ->find();
            if ($pending) {
                throw new \RuntimeException('该客户已有成交申请待审核，请等待审核结果');
            }

            $now = date('Y-m-d H:i:s');
            $insertId = Db::table(self::TABLE_APPLY)->insertGetId([
                'leads_id' => $leadsId,
                'kh_name' => (string)($leadRow['kh_name'] ?? $lead['kh_name'] ?? ''),
                'pr_user' => (string)($leadRow['pr_user'] ?? $lead['pr_user'] ?? ''),
                'proof_image' => $proofImage,
                'apply_remark' => $applyRemark,
                'check_status' => self::CHECK_PENDING,
                'apply_user_id' => $applyUserId,
                'apply_user' => $applyUser,
                'apply_time' => $now,
            ]);

            if (!$insertId) {
                throw new \RuntimeException('提交失败，请重试');
            }

            ClientController::addOperLog($leadsId, '成交客户申请', '提交成交客户申请');

            Db::commit();
            return $this->ok('提交成功，等待审核');
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail($e->getMessage() ?: '提交失败');
        }
    }

    /**
     * 申请列表（layui table）
     */
    public function getApplyList($page, $limit, $keyword = [])
    {
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);
        $keyword = is_array($keyword) ? $keyword : [];
        $currentUserId = (int)session('aid');
        $canAudit = $this->isAuditAdmin($currentUserId);

        $query = Db::table(self::TABLE_APPLY)->alias('a')->order('a.id desc');

        // 非审核管理员仅可查看自己提交的申请
        if (!$canAudit) {
            if ($currentUserId > 0) {
                $query->where('a.apply_user_id', '=', $currentUserId);
            } else {
                $query->where('a.id', '=', 0);
            }
        }

        $khName = trim((string)($keyword['kh_name'] ?? ''));
        if ($khName !== '') {
            $query->where('a.kh_name', 'like', "%{$khName}%");
        }

        $prUser = trim((string)($keyword['pr_user'] ?? ''));
        if ($prUser !== '') {
            $query->where('a.pr_user', 'like', "%{$prUser}%");
        }

        $applyUser = trim((string)($keyword['apply_user'] ?? ''));
        if ($applyUser !== '') {
            $query->where('a.apply_user', 'like', "%{$applyUser}%");
        }

        if (isset($keyword['check_status']) && $keyword['check_status'] !== '' && $keyword['check_status'] !== null) {
            $query->where('a.check_status', (int)$keyword['check_status']);
        }

        $applyTime = trim((string)($keyword['apply_time'] ?? ''));
        if ($applyTime !== '') {
            $parts = explode(' - ', $applyTime);
            if (count($parts) === 2) {
                $start = trim($parts[0]);
                $end = trim($parts[1]);
                if ($start !== '' && $end !== '') {
                    $query->where('a.apply_time', '>=', $start . ' 00:00:00');
                    $query->where('a.apply_time', '<=', $end . ' 23:59:59');
                }
            }
        }

        $list = $query->paginate(['list_rows' => $limit, 'page' => $page])->toArray();
        $rows = $list['data'] ?? [];
        foreach ($rows as &$row) {
            $row['check_status_text'] = $this->mapCheckStatusText($row['check_status'] ?? null);
            $row['can_audit'] = $canAudit;
            $row['current_user_id'] = $currentUserId;
        }
        unset($row);

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $rows,
            'count' => $list['total'] ?? 0,
            'rel' => 1,
            'can_audit' => $canAudit,
            'is_super_admin' => $this->isSuperAdminAccount(),
            'current_user_id' => $currentUserId,
        ];
    }

    /**
     * 申请详情
     */
    public function getApplyDetail($id)
    {
        $id = (int)$id;
        if ($id <= 0) {
            return $this->fail('参数错误');
        }

        $apply = Db::table(self::TABLE_APPLY)->where('id', $id)->find();
        if (!$apply) {
            return $this->fail('申请记录不存在');
        }

        $apply['check_status_text'] = $this->mapCheckStatusText($apply['check_status'] ?? null);

        $lead = Db::table(self::TABLE_LEADS)
            ->where('id', (int)($apply['leads_id'] ?? 0))
            ->field('id,kh_name,pr_user,status,issuccess,ut_time,product_name')
            ->find();

        return $this->ok('获取成功', [
            'apply' => $apply,
            'lead' => $lead ?: null,
        ]);
    }

    /**
     * 审核通过
     */
    public function approve($id)
    {
        $id = (int)$id;
        if ($id <= 0) {
            return $this->fail('参数错误');
        }
        if (!$this->isAuditAdmin((int)session('aid'))) {
            return $this->fail('无成交客户审核权限');
        }

        $checker = $this->getCurrentChecker();
        $now = date('Y-m-d H:i:s');

        try {
            $this->approveOne($id, $checker, $now, '成交客户申请审核通过');
            return $this->ok('审核通过');
        } catch (Throwable $e) {
            return $this->fail($e->getMessage() ?: '审核通过失败');
        }
    }

    /**
     * 批量审核通过（仅当前筛选条件下待审核申请）
     */
    public function batchApprove($keyword = [])
    {
        if (!$this->isSuperAdminAccount()) {
            return $this->fail('仅超级管理员admin可以执行批量审核通过');
        }

        $keyword = is_array($keyword) ? $keyword : [];
        $checker = $this->getCurrentChecker();
        $now = date('Y-m-d H:i:s');

        $query = Db::table(self::TABLE_APPLY)->alias('a')
            ->where('a.check_status', self::CHECK_PENDING)
            ->order('a.id asc');

        $khName = trim((string)($keyword['kh_name'] ?? ''));
        if ($khName !== '') {
            $query->where('a.kh_name', 'like', "%{$khName}%");
        }

        $prUser = trim((string)($keyword['pr_user'] ?? ''));
        if ($prUser !== '') {
            $query->where('a.pr_user', 'like', "%{$prUser}%");
        }

        $applyUser = trim((string)($keyword['apply_user'] ?? ''));
        if ($applyUser !== '') {
            $query->where('a.apply_user', 'like', "%{$applyUser}%");
        }

        $applyTime = trim((string)($keyword['apply_time'] ?? ''));
        if ($applyTime !== '') {
            $parts = explode(' - ', $applyTime);
            if (count($parts) === 2) {
                $start = trim($parts[0]);
                $end = trim($parts[1]);
                if ($start !== '' && $end !== '') {
                    $query->where('a.apply_time', '>=', $start . ' 00:00:00');
                    $query->where('a.apply_time', '<=', $end . ' 23:59:59');
                }
            }
        }

        $idRows = $query
            ->limit(self::BATCH_APPROVE_MAX + 1)
            ->field('a.id')
            ->select();
        $idRows = array_column((array)$idRows, 'id');
        $idRows = array_values(array_filter(array_map('intval', (array)$idRows), function ($v) {
            return $v > 0;
        }));
        if (empty($idRows)) {
            return $this->fail('当前筛选条件下没有待审核申请');
        }

        $hasMore = count($idRows) > self::BATCH_APPROVE_MAX;
        $ids = array_slice($idRows, 0, self::BATCH_APPROVE_MAX);

        $successCount = 0;
        $failCount = 0;
        $failList = [];

        foreach ($ids as $id) {
            try {
                $this->approveOne($id, $checker, $now, '批量审核通过成交客户申请');
                $successCount++;
            } catch (Throwable $e) {
                $failCount++;
                if (count($failList) < 10) {
                    $failList[] = '申请ID ' . $id . '：' . ($e->getMessage() ?: '审核通过失败');
                }
            }
        }

        $processedCount = count($ids);
        $msg = '批量审核完成：成功' . $successCount . '条，失败' . $failCount . '条';
        if ($hasMore) {
            $msg .= '。本次最多处理500条，请继续点击处理剩余待审核申请';
        }

        return $this->ok($msg, [
            'success_count' => $successCount,
            'fail_count' => $failCount,
            'fail_list' => $failList,
            'processed_count' => $processedCount,
            'has_more' => $hasMore,
        ]);
    }

    /**
     * 审核驳回
     */
    public function reject($id, $rejectReason)
    {
        $id = (int)$id;
        $rejectReason = trim((string)$rejectReason);
        if ($id <= 0) {
            return $this->fail('参数错误');
        }
        if (!$this->isAuditAdmin((int)session('aid'))) {
            return $this->fail('无成交客户审核权限');
        }
        if ($rejectReason === '') {
            return $this->fail('请填写驳回原因');
        }

        $checker = $this->getCurrentChecker();

        Db::startTrans();
        try {
            $apply = Db::table(self::TABLE_APPLY)->where('id', $id)->lock(true)->find();
            if (!$apply) {
                throw new \RuntimeException('申请记录不存在');
            }

            $checkStatus = (int)($apply['check_status'] ?? -1);
            if ($checkStatus === self::CHECK_APPROVED) {
                throw new \RuntimeException('该申请已审核通过，不能驳回');
            }
            if ($checkStatus === self::CHECK_REJECTED) {
                throw new \RuntimeException('该申请已被驳回，请勿重复操作');
            }
            if ($checkStatus !== self::CHECK_PENDING) {
                throw new \RuntimeException('仅待审核状态的申请可驳回');
            }

            $leadsId = (int)($apply['leads_id'] ?? 0);

            $applyUpdated = Db::table(self::TABLE_APPLY)->where('id', $id)->where('check_status', self::CHECK_PENDING)->update([
                'check_status' => self::CHECK_REJECTED,
                'reject_reason' => $rejectReason,
                'check_user_id' => $checker['check_user_id'],
                'check_user' => $checker['check_user'],
                'check_time' => $checker['check_time'],
            ]);
            if ($applyUpdated === false || $applyUpdated === 0) {
                throw new \RuntimeException('申请状态更新失败，可能已被其他操作处理');
            }

            if ($leadsId > 0) {
                ClientController::addOperLog($leadsId, '成交客户申请审核', '成交客户申请审核驳回：' . $rejectReason);
            }

            Db::commit();
            return $this->ok('已驳回');
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail($e->getMessage() ?: '审核驳回失败');
        }
    }

    /**
     * 获取成交客户审核员 admin_id 列表（强制包含 admin 账号）。
     *
     * @return int[]
     */
    public function getAuditAdminIds()
    {
        $ids = [];

        $config = Db::table(self::TABLE_APPLY_CONFIG)
            ->order('id desc')
            ->find();
        if (!empty($config)) {
            $ids = $this->parseAuditAdminIds($config['audit_admin_ids'] ?? '');
        }

        $adminId = $this->getSuperAdminId();
        if ($adminId > 0) {
            $ids[] = $adminId;
        }

        $ids = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));

        sort($ids);
        return $ids;
    }

    /**
     * 当前用户是否有成交客户审核权限。
     * 说明：admin 账号始终拥有权限，不依赖配置表。
     *
     * @param int $userId
     * @return bool
     */
    public function isAuditAdmin($userId = 0)
    {
        $userId = (int)$userId;
        if ($userId <= 0) {
            $userId = (int)session('aid');
        }
        if ($userId <= 0) {
            return false;
        }

        if (strtolower((string)session('username')) === 'admin') {
            return true;
        }

        $account = Db::name('admin')
            ->where('admin_id', $userId)
            ->field('admin_id,username')
            ->find();
        if (!empty($account) && strtolower((string)($account['username'] ?? '')) === 'admin') {
            return true;
        }

        return in_array($userId, $this->getAuditAdminIds(), true);
    }

    /**
     * 保存成交客户审核员配置（强制包含 admin，且不可移除）。
     *
     * @param int[] $ids
     * @return array
     */
    public function saveAuditAdminIds(array $ids)
    {
        $cleanIds = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));

        $adminId = $this->getSuperAdminId();
        if ($adminId > 0 && !in_array($adminId, $cleanIds, true)) {
            $cleanIds[] = $adminId;
        }

        sort($cleanIds);

        $payload = [
            'audit_admin_ids' => implode(',', $cleanIds),
            'update_user_id' => (int)session('aid'),
            'update_user' => (string)session('username'),
            'update_time' => date('Y-m-d H:i:s'),
        ];

        Db::startTrans();
        try {
            $latestId = (int)Db::table(self::TABLE_APPLY_CONFIG)->order('id desc')->value('id');
            if ($latestId > 0) {
                Db::table(self::TABLE_APPLY_CONFIG)->where('id', $latestId)->update($payload);
            } else {
                Db::table(self::TABLE_APPLY_CONFIG)->insert($payload);
            }
            Db::commit();
            return $this->ok('保存成功', ['audit_admin_ids' => $cleanIds]);
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail($e->getMessage() ?: '保存失败');
        }
    }

    /**
     * 审核权限配置页面的管理员可选数据（可用于搜索多选）。
     *
     * @return array
     */
    public function getAdminUserList($keyword = '')
    {
        $keyword = trim((string)$keyword);

        $query = Db::name('admin')
            ->where('username', '<>', '');
        if ($keyword !== '') {
            $query->where('username', 'like', '%' . $keyword . '%');
        }

        $list = $query
            ->field('admin_id,username,is_open')
            ->orderRaw("case when username = 'admin' then 0 else 1 end, admin_id asc")
            ->select();

        $rows = [];
        foreach ((array)$list as $item) {
            $rows[] = [
                'admin_id' => (int)($item['admin_id'] ?? 0),
                'username' => (string)($item['username'] ?? ''),
                'is_open' => (int)($item['is_open'] ?? 0),
            ];
        }

        return $this->ok('获取成功', ['list' => $rows]);
    }

    /**
     * 解析并校验 crm_leads.at_time（业务时区 Asia/Shanghai）
     * 无效值一律返回 null，不得放行
     *
     * @param mixed $atTime
     * @return \DateTime|null
     */
    private function parseValidAtTime($atTime)
    {
        if ($atTime === null) {
            return null;
        }

        if (is_string($atTime) || is_numeric($atTime)) {
            $raw = trim((string)$atTime);
        } else {
            return null;
        }

        if ($raw === '' || $raw === '0000-00-00 00:00:00' || $raw === '0000-00-00') {
            return null;
        }

        // 拒绝纯数字时间戳等隐式转换，避免落到 1970 年误判放行
        if (preg_match('/^\d+$/', $raw)) {
            return null;
        }

        if (!preg_match('/^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$/', $raw)) {
            return null;
        }

        $tz = new \DateTimeZone('Asia/Shanghai');
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $raw)) {
            $raw .= ' 00:00:00';
        }

        $dt = \DateTime::createFromFormat('Y-m-d H:i:s', $raw, $tz);
        $errors = \DateTime::getLastErrors();
        if ($dt === false) {
            return null;
        }
        if (is_array($errors) && (((int)($errors['warning_count'] ?? 0) > 0) || ((int)($errors['error_count'] ?? 0) > 0))) {
            return null;
        }
        if ($dt->format('Y-m-d H:i:s') !== $raw) {
            return null;
        }

        return $dt;
    }

    /**
     * @return \DateTime
     */
    private function getApplyAtTimeDeadline()
    {
        $tz = new \DateTimeZone('Asia/Shanghai');
        $deadline = \DateTime::createFromFormat('Y-m-d H:i:s', self::APPLY_AT_TIME_DEADLINE, $tz);
        if ($deadline === false) {
            // 常量非法时宁可全部拒绝，也不要误放行
            return new \DateTime('1970-01-01 00:00:00', $tz);
        }

        return $deadline;
    }

    private function ok($msg, $data = null)
    {
        $result = ['code' => 0, 'msg' => (string)$msg];
        if ($data !== null) {
            $result['data'] = $data;
        }
        return $result;
    }

    private function fail($msg, $code = 1)
    {
        return [
            'code' => (int)$code,
            'msg' => (string)$msg,
        ];
    }

    private function mapCheckStatusText($status)
    {
        $map = [
            self::CHECK_PENDING => '待审核',
            self::CHECK_APPROVED => '已通过',
            self::CHECK_REJECTED => '已驳回',
        ];

        return $map[(int)$status] ?? '未知';
    }

    private function getCurrentChecker()
    {
        return [
            'check_user_id' => (int)session('aid'),
            'check_user' => (string)session('username'),
            'check_time' => date('Y-m-d H:i:s'),
        ];
    }

    /**
     * 当前登录用户是否为超级管理员 admin 账号。
     *
     * @return bool
     */
    private function isSuperAdminAccount()
    {
        if (strtolower((string)session('username')) === 'admin') {
            return true;
        }

        $userId = (int)session('aid');
        if ($userId <= 0) {
            return false;
        }

        $username = Db::name('admin')
            ->where('admin_id', $userId)
            ->value('username');

        return strtolower((string)$username) === 'admin';
    }

    /**
     * 审核通过单条申请（单条事务，供单审/批量复用）。
     *
     * @param int   $id
     * @param array $checker
     * @param string $now
     * @param string $logContent
     * @throws \RuntimeException
     */
    private function approveOne($id, array $checker, $now, $logContent)
    {
        $id = (int)$id;
        if ($id <= 0) {
            throw new \RuntimeException('参数错误');
        }

        Db::startTrans();
        try {
            $apply = Db::table(self::TABLE_APPLY)->where('id', $id)->lock(true)->find();
            if (!$apply) {
                throw new \RuntimeException('申请记录不存在');
            }

            $checkStatus = (int)($apply['check_status'] ?? -1);
            if ($checkStatus === self::CHECK_APPROVED) {
                throw new \RuntimeException('该申请已审核通过，请勿重复操作');
            }
            if ($checkStatus === self::CHECK_REJECTED) {
                throw new \RuntimeException('该申请已被驳回，不能再审核通过');
            }
            if ($checkStatus !== self::CHECK_PENDING) {
                throw new \RuntimeException('仅待审核状态的申请可审核通过');
            }

            $leadsId = (int)($apply['leads_id'] ?? 0);
            if ($leadsId <= 0) {
                throw new \RuntimeException('申请关联客户无效');
            }

            $lead = Db::table(self::TABLE_LEADS)->where('id', $leadsId)->lock(true)->find();
            if (!$lead) {
                throw new \RuntimeException('关联客户不存在');
            }
            if ((int)($lead['status'] ?? 0) !== 1) {
                throw new \RuntimeException('关联客户状态无效，无法审核通过');
            }
            if ((int)($lead['issuccess'] ?? 0) === 1) {
                throw new \RuntimeException('该客户已是成交客户，无需重复审核');
            }
            if ((int)($lead['issuccess'] ?? 0) !== -1) {
                throw new \RuntimeException('关联客户成交状态异常，无法审核通过');
            }

            $applyUpdated = Db::table(self::TABLE_APPLY)->where('id', $id)->where('check_status', self::CHECK_PENDING)->update([
                'check_status' => self::CHECK_APPROVED,
                'check_user_id' => (int)($checker['check_user_id'] ?? 0),
                'check_user' => (string)($checker['check_user'] ?? ''),
                'check_time' => (string)($checker['check_time'] ?? $now),
            ]);
            if ($applyUpdated === false || $applyUpdated === 0) {
                throw new \RuntimeException('申请状态更新失败，可能已被其他操作处理');
            }

            $leadUpdated = Db::table(self::TABLE_LEADS)
                ->where('id', $leadsId)
                ->where('status', 1)
                ->where('issuccess', -1)
                ->update([
                    'issuccess' => 1,
                    'ut_time' => $now,
                ]);
            if ($leadUpdated === false || $leadUpdated === 0) {
                throw new \RuntimeException('客户成交状态更新失败，请刷新后重试');
            }

            ClientController::addOperLog($leadsId, '成交客户申请审核', (string)$logContent);

            Db::commit();
        } catch (Throwable $e) {
            Db::rollback();
            throw new \RuntimeException($e->getMessage() ?: '审核通过失败');
        }
    }

    /**
     * 解析配置里的审核员 ID 串。
     *
     * @param string $raw
     * @return int[]
     */
    private function parseAuditAdminIds($raw)
    {
        $raw = trim((string)$raw);
        if ($raw === '') {
            return [];
        }

        $parts = explode(',', $raw);
        $ids = [];
        foreach ($parts as $part) {
            $id = (int)trim((string)$part);
            if ($id > 0) {
                $ids[] = $id;
            }
        }

        return array_values(array_unique($ids));
    }

    /**
     * 获取超级管理员 admin 账号对应 admin_id。
     *
     * @return int
     */
    private function getSuperAdminId()
    {
        return (int)Db::name('admin')->where('username', 'admin')->value('admin_id');
    }
}
