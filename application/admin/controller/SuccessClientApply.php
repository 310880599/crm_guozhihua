<?php

namespace app\admin\controller;

use app\admin\service\SuccessClientApplyService;

/**
 * 成交客户申请审核
 */
class SuccessClientApply extends Common
{
    /**
     * @return SuccessClientApplyService
     */
    protected function service()
    {
        return new SuccessClientApplyService();
    }

    /**
     * GET：审核列表页；POST/AJAX：申请列表 JSON
     */
    public function index()
    {
        if (request()->isPost() || request()->isAjax()) {
            $page = input('page/d', 1);
            $limit = input('limit/d', 10);
            $keyword = [
                'kh_name' => trim((string)input('kh_name', '')),
                'pr_user' => trim((string)input('pr_user', '')),
                'apply_user' => trim((string)input('apply_user', '')),
                'check_status' => input('check_status', ''),
                'apply_time' => trim((string)input('apply_time', '')),
            ];
            if ($keyword['check_status'] !== '' && $keyword['check_status'] !== null) {
                $keyword['check_status'] = (int)$keyword['check_status'];
            } else {
                $keyword['check_status'] = '';
            }

            return json($this->service()->getApplyList($page, $limit, $keyword));
        }

        return $this->fetch('success_client_apply/index');
    }

    /**
     * 申请详情
     */
    public function detail()
    {
        $id = input('id/d', 0);
        return json($this->service()->getApplyDetail($id));
    }

    /**
     * 审核通过
     */
    public function approve()
    {
        if (!request()->isPost()) {
            return json(['code' => 1, 'msg' => '非法请求']);
        }

        $id = input('id/d', 0);
        return json($this->service()->approve($id));
    }

    /**
     * 审核驳回
     */
    public function reject()
    {
        if (!request()->isPost()) {
            return json(['code' => 1, 'msg' => '非法请求']);
        }

        $id = input('id/d', 0);
        $rejectReason = trim((string)input('reject_reason', ''));

        return json($this->service()->reject($id, $rejectReason));
    }

    /**
     * 成交客户审核配置（审核管理员）
     * GET: 页面
     * POST: 保存配置
     */
    public function config()
    {
        $service = $this->service();

        if (request()->isPost()) {
            $rawIds = input('post.audit_admin_ids/a', null);
            if (!is_array($rawIds)) {
                $rawIds = input('post.audit_admin_ids', '');
                if (is_string($rawIds)) {
                    $rawIds = $rawIds === '' ? [] : explode(',', $rawIds);
                } else {
                    $rawIds = [];
                }
            }

            $ids = array_values(array_unique(array_filter(array_map('intval', (array)$rawIds), function ($id) {
                return $id > 0;
            })));

            $result = $service->saveAuditAdminIds($ids);
            if ((int)($result['code'] ?? 1) === 0) {
                return json(['code' => 0, 'msg' => '保存成功']);
            }
            return json([
                'code' => 1,
                'msg' => (string)($result['msg'] ?? '保存失败'),
            ]);
        }

        $userResult = $service->getAdminUserList();
        $userList = (array)($userResult['data']['list'] ?? []);
        $selectedIds = $service->getAuditAdminIds();

        $adminId = 0;
        foreach ($userList as $row) {
            if (strtolower((string)($row['username'] ?? '')) === 'admin') {
                $adminId = (int)($row['admin_id'] ?? 0);
                break;
            }
        }
        if ($adminId > 0 && !in_array($adminId, $selectedIds, true)) {
            $selectedIds[] = $adminId;
        }

        $this->assign('adminUserListJson', json_encode($userList, JSON_UNESCAPED_UNICODE));
        $this->assign('selectedAuditAdminIdsJson', json_encode(array_values($selectedIds), JSON_UNESCAPED_UNICODE));
        $this->assign('forceAdminId', $adminId);
        return $this->fetch('success_client_apply/config');
    }
}
