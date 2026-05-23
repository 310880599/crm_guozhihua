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
    private const TABLE_LEADS = 'crm_leads';

    private const CHECK_PENDING = 0;
    private const CHECK_APPROVED = 1;
    private const CHECK_REJECTED = 2;

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

        $query = Db::table(self::TABLE_APPLY)->alias('a')->order('a.id desc');

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
        }
        unset($row);

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $rows,
            'count' => $list['total'] ?? 0,
            'rel' => 1,
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

        $checker = $this->getCurrentChecker();
        $now = date('Y-m-d H:i:s');

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
                'check_user_id' => $checker['check_user_id'],
                'check_user' => $checker['check_user'],
                'check_time' => $checker['check_time'],
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

            ClientController::addOperLog($leadsId, '成交客户申请审核', '成交客户申请审核通过');

            Db::commit();
            return $this->ok('审核通过');
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail($e->getMessage() ?: '审核通过失败');
        }
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
}
