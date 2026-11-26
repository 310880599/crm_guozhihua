<?php

namespace app\admin\controller;

use app\admin\service\ShippingApplyService;
use app\admin\service\ShippingConfigService;
use think\facade\Request;

/**
 * 发货管理
 */
class Ship extends Common
{
    /** 请求体上限（字节）：256 KB */
    const SUBMIT_HTTP_MAX_BODY_BYTES = 262144;

    /** items JSON 字符串上限（字节） */
    const SUBMIT_HTTP_MAX_ITEMS_BYTES = 262144;

    /** 发货行上限 */
    const SUBMIT_HTTP_MAX_ITEM_ROWS = 200;

    /** JSON 嵌套深度上限 */
    const SUBMIT_HTTP_MAX_JSON_DEPTH = 16;

    /** 正式提交 CSRF：Session 键（与业务 submit_token 分离） */
    const SUBMIT_CSRF_SESSION_KEY = 'ship_submit_csrf';

    /** 审批决策 CSRF：Session 键（与正式提交 CSRF / 审批幂等严格分离） */
    const DECIDE_CSRF_SESSION_KEY = 'ship_decide_csrf';

    /** CSRF Token：bin2hex(random_bytes(32)) → 64 hex */
    const SUBMIT_CSRF_TOKEN_HEX_LEN = 64;

    /** 审批 CSRF Token 长度（与提交 CSRF 同规格） */
    const DECIDE_CSRF_TOKEN_HEX_LEN = 64;

    /** 审核失败编辑 CSRF：Session 键（与 submit / decide / 业务 token 严格分离） */
    const REJECTED_EDIT_CSRF_SESSION_KEY = 'ship_rejected_edit_csrf';

    /** 审核失败编辑 CSRF Token 长度 */
    const REJECTED_EDIT_CSRF_TOKEN_HEX_LEN = 64;

    /** 审核失败再次提交 CSRF：Session 键（与 submit / decide / rejected_edit / 业务 token 严格分离） */
    const RESUBMIT_CSRF_SESSION_KEY = 'ship_resubmit_csrf';

    /** 审核失败再次提交 CSRF Token 长度 */
    const RESUBMIT_CSRF_TOKEN_HEX_LEN = 64;

    /** 审核失败受控软删除 CSRF：Session 键（与 submit / decide / rejected_edit / resubmit 严格分离） */
    const DELETE_REJECTED_CSRF_SESSION_KEY = 'ship_delete_rejected_csrf';

    /** 审核失败受控软删除 CSRF Token 长度 */
    const DELETE_REJECTED_CSRF_TOKEN_HEX_LEN = 64;

    /**
     * @return ShippingConfigService
     */
    protected function getShippingConfigService()
    {
        return new ShippingConfigService();
    }

    /**
     * @return ShippingApplyService
     */
    protected function getShippingApplyService()
    {
        return new ShippingApplyService();
    }

    /**
     * 发货管理配置
     * GET：展示配置页
     * POST：保存配置
     */
    public function configShip()
    {
        $service = $this->getShippingConfigService();

        // 业务层二次权限：超级管理员（不依赖菜单隐藏）
        if (!$service->isShippingConfigAdmin()) {
            if (request()->isPost() || request()->isAjax()) {
                return json(['code' => -200, 'msg' => '您无权限访问发货管理配置', 'data' => []]);
            }
            return $this->error('您无权限访问发货管理配置');
        }

        if (request()->isPost()) {
            $params = Request::param();
            $result = $service->saveConfig(is_array($params) ? $params : []);
            return json($result);
        }

        $pageData = $service->getPageData();
        $this->assign('config', $pageData['config']);
        $this->assign('adminOptions', $pageData['admin_options']);
        $this->assign('productDirector', $pageData['product_director']);
        $this->assign('financeCcUser', $pageData['finance_cc_user']);
        $this->assign('directorDisabledTip', $pageData['director_disabled_tip']);
        $this->assign('financeDisabledTip', $pageData['finance_disabled_tip']);

        return $this->fetch('ship/config_ship');
    }

    /**
     * 新建发货申请
     * GET：展示页面
     * POST：op 分发 AJAX（search_orders / get_order_detail / save_draft / list_factories）
     */
    public function createShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();

            switch ($op) {
                case 'search_orders':
                    return json($service->searchOrdersForApply(is_array($params) ? $params : []));
                case 'get_order_detail':
                    return json($service->getOrderDetailForApply((int)Request::param('order_id', 0)));
                case 'save_draft':
                    return json($service->saveDraft(is_array($params) ? $params : []));
                case 'list_factories':
                    return json($service->listFactories(is_array($params) ? $params : []));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'data' => []]);
            }
        }

        $draftId = (int)Request::param('id', 0);
        $draftData = null;
        if ($draftId > 0) {
            $draftData = $service->getDraftForEdit($draftId);
        }

        // 页面渲染：输出可信 Session 身份与 CSRF（仅 GET；不参与草稿保存）
        $sessionAid = $this->normalizeSubmitSessionAid($this->getSubmitSessionAid());
        $submitCsrfToken = '';
        if ($sessionAid > 0) {
            try {
                $submitCsrfToken = $this->ensureSubmitCsrfToken($sessionAid);
            } catch (\Throwable $e) {
                $submitCsrfToken = '';
            }
        }

        $this->assign('draftDataJson', $draftData ? json_encode($draftData, JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_QUOT | JSON_HEX_AMP) : '');
        $this->assign('draftId', $draftId);
        $this->assign('sessionAid', $sessionAid);
        // JSON 编码输出到 JS，避免 XSS / 模板转义破坏 Token
        $this->assign('sessionAidJson', json_encode((int)$sessionAid));
        $this->assign('submitCsrfTokenJson', json_encode((string)$submitCsrfToken, JSON_UNESCAPED_SLASHES));
        // 新建/草稿模式：驳回编辑相关变量置空，避免模板未定义
        $this->assign('pageMode', 'create');
        $this->assign('pageModeJson', json_encode('create'));
        $this->assign('rejectedEditDataJson', '');
        $this->assign('rejectedEditCsrfTokenJson', json_encode(''));
        $this->assign('resubmitCsrfTokenJson', json_encode(''));
        $this->assign('saveRejectedUrlJson', json_encode((string)url('Ship/saveRejectedApply'), JSON_UNESCAPED_SLASHES));
        $this->assign('resubmitRejectedUrlJson', json_encode((string)url('Ship/resubmitRejectedApply'), JSON_UNESCAPED_SLASHES));

        return $this->fetch('ship/create_ship');
    }

    /**
     * 草稿箱
     * GET：列表页
     * POST：op 分发（list / delete_draft / M4-1A-4 替代授权相关；无 op 兼容原列表查询）
     */
    public function draftShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();

            switch ($op) {
                case '':
                case 'list':
                    return json($service->listDrafts(is_array($params) ? $params : []));
                case 'delete_draft':
                    return json($service->deleteDraft(Request::param('id')));
                case 'list_pm_substitutes':
                    return json($service->listDraftPmSubstituteTargets(Request::param('apply_id')));
                case 'list_substitute_candidates':
                    return json($service->listPmSubstituteCandidates(is_array($params) ? $params : []));
                case 'authorize_pm_substitute':
                    return json($service->authorizePmSubstitute(is_array($params) ? $params : []));
                case 'revoke_pm_substitute':
                    return json($service->revokePmSubstitute(is_array($params) ? $params : []));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'data' => []]);
            }
        }

        return $this->fetch('ship/draft_ship');
    }

    /**
     * 我的发货申请（M4-2A 只读列表；M4-2C-1B 排除驳回状态）
     * GET：列表页
     * POST：op=list → listMyApplies（仅本人非草稿、非驳回正式申请）
     */
    public function myShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();
            if (!is_array($params)) {
                $params = [];
            }

            switch ($op) {
                case '':
                case 'list':
                    return json($service->listMyApplies($params));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'count' => 0, 'data' => []]);
            }
        }

        return $this->fetch('ship/my_ship');
    }

    /**
     * 全部发货申请（M4-2C-6-2 指定 admin 只读列表）
     * GET：仅 admin_id=1 且启用账号可打开页面
     * POST：仅 op=list → listAllApplies；拒绝任何写操作分发
     * 菜单节点权限不能代替本方法身份校验。
     */
    public function allShip()
    {
        $service = $this->getShippingApplyService();
        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);

        $denyMsg = '无权访问全部发货申请';
        if ($aid <= 0) {
            $denyMsg = ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                ? '请先登录'
                : '登录状态无效';
            if (request()->isPost()) {
                return json(['code' => -200, 'msg' => $denyMsg, 'count' => 0, 'data' => []]);
            }
            return $this->error($denyMsg);
        }

        // GET/POST 均拦截：菜单可见 ≠ 数据授权；身份仅来自 Session
        if (!$service->isDesignatedAllApplyAdmin($aid)) {
            if (request()->isPost()) {
                return json(['code' => -200, 'msg' => $denyMsg, 'count' => 0, 'data' => []]);
            }
            return $this->error($denyMsg);
        }

        if (request()->isPost()) {
            // P1-1：拒绝数组/对象/布尔，避免 (string) 触发 Array to string conversion
            $opRaw = Request::param('op', '');
            if (is_array($opRaw) || is_object($opRaw) || is_bool($opRaw)) {
                return json(['code' => -200, 'msg' => '参数类型无效', 'count' => 0, 'data' => []]);
            }
            $op = trim((string)$opRaw);
            $params = Request::param();
            if (!is_array($params)) {
                $params = [];
            }
            // 丢弃客户端伪造身份字段；申请人筛选字段由 Service 另行校验
            unset(
                $params['user_id'],
                $params['admin_id'],
                $params['is_admin'],
                $params['group_id'],
                $params['role'],
                $params['username'],
                $params['can_view'],
                $params['viewer_role'],
                $params['can_approve'],
                $params['can_delete'],
                $params['can_edit'],
                $params['is_deleted'],
                $params['is_designated_failed_admin'],
                $params['is_designated_all_apply_admin']
            );

            switch ($op) {
                case '':
                case 'list':
                    return json($service->listAllApplies($params));
                default:
                    // 未知 op 一律拒绝；不转发 delete/freeze/cancel 等到其他写接口
                    return json(['code' => -200, 'msg' => '无效的操作', 'count' => 0, 'data' => []]);
            }
        }

        return $this->fetch('ship/all_ship');
    }

    /**
     * 审核失败（M4-2C-1B / M4-2C-5-2）
     * GET：列表页；指定 admin 签发独立删除 CSRF
     * POST：op=list → listFailedApplies（普通本人；指定 admin 全公司失败申请）
     * 编辑入口由前端按钮打开 editRejectedShip；保存走 saveRejectedApply；
     * 删除走独立接口 deleteRejectedApply。
     */
    public function failedShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();
            if (!is_array($params)) {
                $params = [];
            }

            switch ($op) {
                case '':
                case 'list':
                    return json($service->listFailedApplies($params));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'count' => 0, 'data' => []]);
            }
        }

        $sessionAid = $this->normalizeSubmitSessionAid($this->getSubmitSessionAid());
        $deleteRejectedCsrfToken = '';
        $isDesignatedFailedAdmin = 0;
        if ($sessionAid > 0 && $service->isDesignatedFailedApplyAdmin($sessionAid)) {
            $isDesignatedFailedAdmin = 1;
            try {
                $deleteRejectedCsrfToken = $this->ensureDeleteRejectedCsrfToken($sessionAid);
            } catch (\Throwable $e) {
                $deleteRejectedCsrfToken = '';
            }
        }
        $this->assign('isDesignatedFailedAdmin', $isDesignatedFailedAdmin);
        $this->assign(
            'deleteRejectedCsrfTokenJson',
            json_encode((string)$deleteRejectedCsrfToken, JSON_UNESCAPED_SLASHES)
        );
        $this->assign(
            'deleteRejectedUrlJson',
            json_encode((string)url('Ship/deleteRejectedApply'), JSON_UNESCAPED_SLASHES)
        );

        return $this->fetch('ship/failed_ship');
    }

    /**
     * 审核失败申请受控软删除（仅 POST；独立 CSRF；仅指定 admin_id=1）
     *
     * @return \think\response\Json
     */
    public function deleteRejectedApply()
    {
        if (!$this->request->isPost()) {
            return json([
                'code' => -200,
                'msg'  => '仅支持 POST 提交',
                'data' => [],
            ]);
        }

        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);
        if ($aid <= 0) {
            return json([
                'code' => -200,
                'msg'  => ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                    ? '请先登录'
                    : '登录状态无效',
                'data' => [],
            ]);
        }

        $service = $this->getShippingApplyService();
        if (!$service->isDesignatedFailedApplyAdmin($aid)) {
            return json([
                'code' => -200,
                'msg'  => '无权删除该发货申请',
                'data' => [],
            ]);
        }

        $csrfRaw = $this->request->post('csrf_token');
        $csrfCheck = $this->validateDeleteRejectedCsrfToken($csrfRaw, $aid);
        if ($csrfCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $csrfCheck['msg'],
                'data' => [],
            ]);
        }

        $params = $this->request->post();
        if (!is_array($params)) {
            $params = [];
        }
        unset(
            $params['csrf_token'],
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['role'],
            $params['status'],
            $params['approval_round'],
            $params['deleted_by'],
            $params['is_deleted'],
            $params['is_designated_failed_admin'],
            $params['can_delete']
        );

        $applyId = $this->parseDecidePositiveInt(
            array_key_exists('apply_id', $params) ? $params['apply_id'] : null
        );
        if ($applyId <= 0) {
            return json([
                'code' => -200,
                'msg'  => '申请ID无效',
                'data' => [],
            ]);
        }

        $deleteReason = array_key_exists('delete_reason', $params) ? $params['delete_reason'] : '';

        try {
            $result = $service->softDeleteRejectedApply($applyId, $deleteReason);
            if (!is_array($result)) {
                return json([
                    'code' => -200,
                    'msg'  => '删除失败，请稍后重试',
                    'data' => [],
                ]);
            }
            return json($result);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[Ship::deleteRejectedApply] ' . $e->getMessage(), 'error');
            }
            return json([
                'code' => -200,
                'msg'  => '删除失败，请稍后重试',
                'data' => [],
            ]);
        }
    }

    /**
     * 审核失败菜单数字角标 COUNT（M4-2C-4-2）
     * 仅 GET JSON；身份只取 Session aid；不接受客户端 applicant_id。
     * 成功：code=0，data.admin_id / data.total；查询失败不得伪装成 0。
     */
    public function failedShipBadgeCount()
    {
        if (!request()->isGet()) {
            return json(['code' => -200, 'msg' => '仅支持 GET 请求', 'data' => []]);
        }

        $adminId = $this->resolveFailedShipBadgeAdminId();
        if ($adminId < 1) {
            return json(['code' => -200, 'msg' => '登录状态已失效，请重新登录', 'data' => []]);
        }

        try {
            $result = $this->getShippingApplyService()->countFailedAppliesForBadge();
            if (!is_array($result)) {
                return json(['code' => -200, 'msg' => '获取失败，请稍后重试', 'data' => []]);
            }
            if ((int)($result['code'] ?? -1) !== 0) {
                $msg = trim((string)($result['msg'] ?? ''));
                if ($msg === '') {
                    $msg = '获取失败，请稍后重试';
                }
                return json(['code' => -200, 'msg' => $msg, 'data' => []]);
            }

            $data = isset($result['data']) && is_array($result['data']) ? $result['data'] : [];
            $serviceAdminId = (int)($data['admin_id'] ?? 0);
            $total = $data['total'] ?? null;
            if ($serviceAdminId !== $adminId) {
                return json(['code' => -200, 'msg' => '登录状态已失效，请重新登录', 'data' => []]);
            }
            if (!is_int($total) && !(is_string($total) && preg_match('/^(0|[1-9]\d*)$/', $total))) {
                return json(['code' => -200, 'msg' => '获取失败，请稍后重试', 'data' => []]);
            }
            $total = (int)$total;
            if ($total < 0) {
                return json(['code' => -200, 'msg' => '获取失败，请稍后重试', 'data' => []]);
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [
                    'admin_id' => $adminId,
                    'total' => $total,
                ],
            ]);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[Ship::failedShipBadgeCount] ' . $e->getMessage(), 'error');
            }
            return json(['code' => -200, 'msg' => '获取失败，请稍后重试', 'data' => []]);
        }
    }

    /**
     * 角标接口当前登录管理员 ID：仅接受 Session aid 正整数。
     *
     * @return int
     */
    protected function resolveFailedShipBadgeAdminId()
    {
        $raw = session('aid');
        if (is_int($raw)) {
            return $raw > 0 ? $raw : 0;
        }
        if (is_string($raw) && preg_match('/\A[1-9]\d*\z/', $raw) === 1) {
            $adminId = (int)$raw;
            if ($adminId > 0 && (string)$adminId === $raw) {
                return $adminId;
            }
        }
        return 0;
    }

    /**
     * 编辑审核失败发货申请（M4-2C-2B-2C-2）
     * 仅 GET：复用 create_ship 模板，pageMode=rejected_edit。
     * 禁止 POST；保存走 saveRejectedApply。
     */
    public function editRejectedShip()
    {
        if (request()->isPost()) {
            return json(['code' => -200, 'msg' => '编辑页仅支持打开查看，请使用保存修改接口', 'data' => []]);
        }

        $idRaw = Request::param('id', 0);
        if (is_int($idRaw)) {
            $applyId = $idRaw > 0 ? $idRaw : 0;
        } elseif (is_string($idRaw) && preg_match('/^[1-9]\d*$/', trim($idRaw))) {
            $applyId = (int)trim($idRaw);
        } else {
            $applyId = 0;
        }
        if ($applyId <= 0) {
            return $this->error('无权编辑该发货申请，或申请当前不可编辑。');
        }

        $result = $this->getShippingApplyService()->getRejectedApplyForEdit($applyId);
        if (!is_array($result) || (int)($result['code'] ?? -1) !== 0) {
            $msg = is_array($result) ? trim((string)($result['msg'] ?? '')) : '';
            if ($msg === '') {
                $msg = '无权编辑该发货申请，或申请当前不可编辑。';
            }
            return $this->error($msg);
        }

        $editData = isset($result['data']) && is_array($result['data']) ? $result['data'] : [];
        $sessionAid = $this->normalizeSubmitSessionAid($this->getSubmitSessionAid());
        $rejectedEditCsrfToken = '';
        $resubmitCsrfToken = '';
        if ($sessionAid > 0) {
            try {
                $rejectedEditCsrfToken = $this->ensureRejectedEditCsrfToken($sessionAid);
            } catch (\Throwable $e) {
                $rejectedEditCsrfToken = '';
            }
            try {
                $resubmitCsrfToken = $this->ensureResubmitCsrfToken($sessionAid);
            } catch (\Throwable $e) {
                $resubmitCsrfToken = '';
            }
        }
        if ($rejectedEditCsrfToken === '' || $resubmitCsrfToken === '') {
            return $this->error('安全令牌签发失败，请刷新后重试');
        }

        // 复用 create_ship：注入驳回编辑模式，不走草稿 getDraftForEdit
        $this->assign('pageMode', 'rejected_edit');
        $this->assign('pageModeJson', json_encode('rejected_edit'));
        $this->assign('draftDataJson', '');
        $this->assign('draftId', 0);
        $this->assign(
            'rejectedEditDataJson',
            json_encode($editData, JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_QUOT | JSON_HEX_AMP)
        );
        $this->assign('sessionAid', $sessionAid);
        $this->assign('sessionAidJson', json_encode((int)$sessionAid));
        $this->assign('submitCsrfTokenJson', json_encode(''));
        $this->assign('rejectedEditCsrfTokenJson', json_encode((string)$rejectedEditCsrfToken, JSON_UNESCAPED_SLASHES));
        $this->assign('resubmitCsrfTokenJson', json_encode((string)$resubmitCsrfToken, JSON_UNESCAPED_SLASHES));
        $this->assign('saveRejectedUrlJson', json_encode((string)url('Ship/saveRejectedApply'), JSON_UNESCAPED_SLASHES));
        $this->assign('resubmitRejectedUrlJson', json_encode((string)url('Ship/resubmitRejectedApply'), JSON_UNESCAPED_SLASHES));

        return $this->fetch('ship/create_ship');
    }

    /**
     * 审核失败申请再次提交审核（仅 POST；独立 CSRF；不放宽 submitApply）
     *
     * @return \think\response\Json
     */
    public function resubmitRejectedApply()
    {
        // 与首次正式提交共用服务端开关；不得只靠前端按钮
        if (config('shipping_submit.enabled') !== true) {
            return json([
                'code' => -200,
                'msg'  => '再次提交接口尚未开放',
                'data' => [],
            ]);
        }

        if (!$this->request->isPost()) {
            return json([
                'code' => -200,
                'msg'  => '仅支持 POST 提交',
                'data' => [],
            ]);
        }

        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);
        if ($aid <= 0) {
            return json([
                'code' => -200,
                'msg'  => ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                    ? '请先登录'
                    : '登录状态无效',
                'data' => [],
            ]);
        }

        $csrfRaw = $this->request->post('csrf_token');
        $csrfCheck = $this->validateResubmitCsrfToken($csrfRaw, $aid);
        if ($csrfCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $csrfCheck['msg'],
                'data' => [],
            ]);
        }

        $contentLength = isset($_SERVER['CONTENT_LENGTH']) ? (int)$_SERVER['CONTENT_LENGTH'] : 0;
        if ($contentLength > self::SUBMIT_HTTP_MAX_BODY_BYTES) {
            return json([
                'code' => -200,
                'msg'  => '请求体过大，已被拒绝',
                'data' => [],
            ]);
        }

        $params = $this->request->post();
        if (!is_array($params)) {
            $params = [];
        }
        if ($params === [] && $contentLength > 0) {
            return json([
                'code' => -200,
                'msg'  => '请求体无法解析或已超限，已被拒绝',
                'data' => [],
            ]);
        }

        unset(
            $params['shipping_submit'],
            $params['enabled'],
            $params['csrf_token'],
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['role'],
            $params['status'],
            $params['approval_round'],
            $params['order_amount'],
            $params['approval_threshold'],
            $params['approval_route'],
            $params['submit_token'],
            $params['submit_payload_hash'],
            $params['current_stage'],
            $params['product_director_id'],
            $params['product_director_name'],
            $params['items'],
            $params['order_id'],
            $params['can_edit'],
            $params['can_approve']
        );

        $applyIdCheck = $this->parseDecidePositiveInt(
            array_key_exists('apply_id', $params) ? $params['apply_id'] : null
        );
        if ($applyIdCheck <= 0) {
            return json([
                'code' => -200,
                'msg'  => '发货申请ID无效',
                'data' => [],
            ]);
        }
        $expectedRoundCheck = $this->parseDecidePositiveInt(
            array_key_exists('expected_round', $params) ? $params['expected_round'] : null
        );
        if ($expectedRoundCheck <= 0) {
            return json([
                'code' => -200,
                'msg'  => '审批轮次无效',
                'data' => [],
            ]);
        }

        $tokenRaw = array_key_exists('resubmit_token', $params) ? $params['resubmit_token'] : null;
        if ($tokenRaw === null || is_array($tokenRaw) || is_object($tokenRaw) || is_bool($tokenRaw)
            || is_int($tokenRaw) || is_float($tokenRaw) || !is_string($tokenRaw)
        ) {
            return json([
                'code' => -200,
                'msg'  => '再次提交标识无效',
                'data' => [],
            ]);
        }
        $token = trim($tokenRaw);
        if ($token === '' || strlen($token) < 8 || strlen($token) > 64
            || !preg_match('/^[A-Za-z0-9_-]+$/', $token)
        ) {
            return json([
                'code' => -200,
                'msg'  => '再次提交标识格式不正确',
                'data' => [],
            ]);
        }

        $workHash = strtolower(trim((string)(array_key_exists('expected_work_hash', $params) ? $params['expected_work_hash'] : '')));
        if ($workHash === '' || strlen($workHash) !== 64 || !preg_match('/^[a-f0-9]{64}$/', $workHash)) {
            return json([
                'code' => -200,
                'msg'  => '编辑版本校验失效，请刷新后重试。',
                'data' => [],
            ]);
        }

        $params['apply_id'] = $applyIdCheck;
        $params['expected_round'] = $expectedRoundCheck;
        $params['resubmit_token'] = $token;
        $params['expected_work_hash'] = $workHash;

        try {
            $result = $this->getShippingApplyService()->resubmitRejectedApply($params);
            if (!is_array($result)) {
                return json([
                    'code' => -200,
                    'msg'  => '再次提交暂时无法完成，请刷新后核对',
                    'data' => [],
                ]);
            }
            return json($result);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[Ship::resubmitRejectedApply] ' . $e->getMessage(), 'error');
            }
            return json([
                'code' => -200,
                'msg'  => '再次提交暂时无法完成，请刷新后核对',
                'data' => [],
            ]);
        }
    }

    /**
     * 保存审核失败发货申请修改（仅 POST；独立 CSRF）
     *
     * @return \think\response\Json
     */
    public function saveRejectedApply()
    {
        if (!$this->request->isPost()) {
            return json([
                'code' => -200,
                'msg'  => '仅支持 POST 提交',
                'data' => [],
            ]);
        }

        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);
        if ($aid <= 0) {
            return json([
                'code' => -200,
                'msg'  => ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                    ? '请先登录'
                    : '登录状态无效',
                'data' => [],
            ]);
        }

        $csrfRaw = $this->request->post('csrf_token');
        $csrfCheck = $this->validateRejectedEditCsrfToken($csrfRaw, $aid);
        if ($csrfCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $csrfCheck['msg'],
                'data' => [],
            ]);
        }

        $params = $this->request->post();
        if (!is_array($params)) {
            $params = [];
        }

        // 结构闸门：明细列表
        $itemsCheck = $this->parseSubmitApplyItems($params);
        if ($itemsCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $itemsCheck['msg'],
                'data' => [],
            ]);
        }
        $params['items'] = $itemsCheck['items'];

        $applyIdCheck = $this->parseDecidePositiveInt(
            array_key_exists('apply_id', $params) ? $params['apply_id'] : null
        );
        if ($applyIdCheck <= 0) {
            return json([
                'code' => -200,
                'msg'  => '发货申请ID无效',
                'data' => [],
            ]);
        }

        unset(
            $params['csrf_token'],
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['role'],
            $params['status'],
            $params['approval_round'],
            $params['order_amount'],
            $params['approval_threshold'],
            $params['approval_route'],
            $params['submit_token'],
            $params['submit_payload_hash'],
            $params['can_edit'],
            $params['can_approve']
        );
        $params['apply_id'] = $applyIdCheck;

        try {
            $result = $this->getShippingApplyService()->saveRejectedApply($params);
            if (!is_array($result)) {
                return json([
                    'code' => -200,
                    'msg'  => '保存暂时无法完成，请刷新后核对',
                    'data' => [],
                ]);
            }
            return json($result);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write('[Ship::saveRejectedApply] ' . $e->getMessage(), 'error');
            }
            return json([
                'code' => -200,
                'msg'  => '保存暂时无法完成，请刷新后核对',
                'data' => [],
            ]);
        }
    }

    /**
     * 待我审核（M4-2B-2A 只读列表；本轮不提供通过/驳回写操作）
     * GET：列表页
     * POST：op=list → listPendingApplies（仅 session 审批人有效任务）
     */
    public function pendingShip()
    {
        $service = $this->getShippingApplyService();

        if (request()->isPost()) {
            $op = trim((string)Request::param('op', ''));
            $params = Request::param();
            if (!is_array($params)) {
                $params = [];
            }

            switch ($op) {
                case '':
                case 'list':
                    return json($service->listPendingApplies($params));
                default:
                    return json(['code' => -200, 'msg' => '无效的操作', 'count' => 0, 'data' => []]);
            }
        }

        return $this->fetch('ship/pending_ship');
    }

    /**
     * 统一发货申请详情（M4-2A/M4-2B；供「我的发货申请」与「待我审核」复用）
     * GET：鉴权后渲染；禁止写操作。可审上下文来自 Service，不信任前端。
     */
    public function applyDetail()
    {
        if (request()->isPost()) {
            return json(['code' => -200, 'msg' => '详情接口仅支持查看', 'data' => []]);
        }

        $idRaw = Request::param('id', 0);
        // 严格正整数；非法不进入敏感查询组装
        if (is_int($idRaw)) {
            $applyId = $idRaw > 0 ? $idRaw : 0;
        } elseif (is_string($idRaw) && preg_match('/^[1-9]\d*$/', trim($idRaw))) {
            $applyId = (int)trim($idRaw);
        } else {
            $applyId = 0;
        }

        if ($applyId <= 0) {
            return $this->error('无权查看或记录不存在');
        }

        $result = $this->getShippingApplyService()->getApplyDetailForView($applyId);
        if (!is_array($result) || (int)($result['code'] ?? -1) !== 0) {
            $msg = is_array($result) ? trim((string)($result['msg'] ?? '')) : '';
            if ($msg === '') {
                $msg = '无权查看或记录不存在';
            }
            return $this->error($msg);
        }

        $data = isset($result['data']) && is_array($result['data']) ? $result['data'] : [];
        // 仅信任 Service 返回的可审上下文；不因 URL 来源授予写权限
        $canApprove = !empty($data['can_approve']);
        $approvalTaskId = (int)($data['approval_task_id'] ?? 0);
        if (!$canApprove || $approvalTaskId <= 0) {
            $data['can_approve'] = false;
            $data['approval_task_id'] = 0;
            $data['approval_type'] = (int)($data['approval_type'] ?? 0);
            $data['approval_round'] = (int)($data['approval_round'] ?? 0);
            $data['approval_action_label'] = (string)($data['approval_action_label'] ?? '');
        } else {
            $data['can_approve'] = true;
            $data['approval_task_id'] = $approvalTaskId;
        }

        // 审批 CSRF：仅对当前有合法审批资格的详情页签发（独立 Session 键）
        $decideCsrfToken = '';
        if (!empty($data['can_approve'])) {
            $sessionAid = $this->normalizeSubmitSessionAid($this->getSubmitSessionAid());
            if ($sessionAid > 0) {
                try {
                    $decideCsrfToken = $this->ensureDecideCsrfToken($sessionAid);
                } catch (\Throwable $e) {
                    // Token 签发失败：降级为不可审展示，避免无 CSRF 的写入口
                    $decideCsrfToken = '';
                    $data['can_approve'] = false;
                    $data['approval_task_id'] = 0;
                }
            } else {
                $data['can_approve'] = false;
                $data['approval_task_id'] = 0;
            }
        }

        $this->assign('detailJson', json_encode($data, JSON_UNESCAPED_UNICODE | JSON_HEX_TAG | JSON_HEX_APOS | JSON_HEX_QUOT | JSON_HEX_AMP));
        $this->assign('applyId', $applyId);
        $this->assign('decideCsrfTokenJson', json_encode((string)$decideCsrfToken, JSON_UNESCAPED_SLASHES));
        $this->assign('decideUrlJson', json_encode((string)url('Ship/decideApply'), JSON_UNESCAPED_SLASHES));

        return $this->fetch('ship/apply_detail');
    }

    /**
     * 发货申请审批决策（通过/驳回）HTTP 入口
     *
     * 仅负责：POST / Session / 独立 CSRF / 参数入口 / 一次 Service 调用 / JSON 透传。
     * 禁止在此开启事务或复制审批状态机。
     *
     * @return \think\response\Json
     */
    public function decideApply()
    {
        // 1. 仅允许 POST
        if (!$this->request->isPost()) {
            return json([
                'code' => -200,
                'msg'  => '仅支持 POST 提交',
                'data' => [],
            ]);
        }

        // 2. Session 登录校验（身份权威来源仅为 session('aid')）
        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);
        if ($aid <= 0) {
            return json([
                'code' => -200,
                'msg'  => ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                    ? '请先登录'
                    : '登录状态无效',
                'data' => [],
            ]);
        }

        // 3. 独立审批 CSRF
        $csrfRaw = $this->request->post('csrf_token');
        $csrfCheck = $this->validateDecideCsrfToken($csrfRaw, $aid);
        if ($csrfCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $csrfCheck['msg'],
                'data' => [],
            ]);
        }

        // 4. 仅读取 POST（禁止 param() 混合 GET）
        $params = $this->request->post();
        if (!is_array($params)) {
            $params = [];
        }

        // 5. decision 闸门（仅 approve|reject；其余拒绝）
        $decisionRaw = isset($params['decision']) ? $params['decision'] : '';
        if (!is_string($decisionRaw) && !is_numeric($decisionRaw)) {
            return json([
                'code' => -200,
                'msg'  => '审批动作无效',
                'data' => [],
            ]);
        }
        $decision = trim((string)$decisionRaw);
        if ($decision !== 'approve' && $decision !== 'reject') {
            return json([
                'code' => -200,
                'msg'  => '审批动作无效',
                'data' => [],
            ]);
        }

        // 6. 入口级正整数闸门（完整业务校验仍由 Service）
        $applyIdCheck = $this->parseDecidePositiveInt(
            array_key_exists('apply_id', $params) ? $params['apply_id'] : null
        );
        $taskIdCheck = $this->parseDecidePositiveInt(
            array_key_exists('task_id', $params) ? $params['task_id'] : null
        );
        if ($applyIdCheck <= 0) {
            return json([
                'code' => -200,
                'msg'  => '发货申请ID无效',
                'data' => [],
            ]);
        }
        if ($taskIdCheck <= 0) {
            return json([
                'code' => -200,
                'msg'  => '审批任务ID无效',
                'data' => [],
            ]);
        }

        // 显式丢弃伪造身份/权限字段；CSRF 不进入业务载荷
        unset(
            $params['csrf_token'],
            $params['approver_id'],
            $params['applicant_id'],
            $params['user_id'],
            $params['admin_id'],
            $params['is_admin'],
            $params['group_id'],
            $params['role'],
            $params['operator_id'],
            $params['operator_role'],
            $params['approval_round'],
            $params['approval_type'],
            $params['can_approve'],
            $params['can_view'],
            $params['viewer_role']
        );

        $params['apply_id'] = $applyIdCheck;
        $params['task_id'] = $taskIdCheck;
        // 统一意见字段；驳回理由必填由 Service 校验
        if (!array_key_exists('opinion', $params)) {
            $params['opinion'] = '';
        }

        try {
            $result = $this->getShippingApplyService()->decideApply($params, $decision);
            if (!is_array($result)) {
                return json([
                    'code' => -200,
                    'msg'  => '审批暂时无法完成，请刷新后核对',
                    'data' => [],
                ]);
            }
            // 原样透传（含 idempotent_replay / result_uncertain）；不得把失败改成 code=0
            return json($result);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[Ship::decideApply] ' . $e->getMessage(),
                    'error'
                );
            }
            return json([
                'code' => -200,
                'msg'  => '审批暂时无法完成，请刷新后核对',
                'data' => [],
            ]);
        }
    }

    /**
     * 审批入口正整数解析（严格；非法返回 0）
     *
     * @param mixed $raw
     * @return int
     */
    protected function parseDecidePositiveInt($raw)
    {
        if ($raw === null || $raw === '' || is_array($raw) || is_object($raw) || is_bool($raw)) {
            return 0;
        }
        if (is_int($raw)) {
            return $raw > 0 ? $raw : 0;
        }
        if (is_float($raw)) {
            return 0;
        }
        if (!is_numeric($raw)) {
            return 0;
        }
        $text = trim((string)$raw);
        if ($text === '' || !preg_match('/^[1-9]\d*$/', $text)) {
            return 0;
        }
        $n = (int)$text;
        if ($n <= 0 || (string)$n !== $text) {
            return 0;
        }
        return $n;
    }

    /**
     * 正式提交发货申请（HTTP 入口；默认关闭）
     *
     * 仅负责：开关 / POST / Session / 输入结构校验 / 一次 Service 调用 / JSON 透传。
     * 禁止在此开启事务或复制提交业务逻辑。
     *
     * @return \think\response\Json
     */
    public function submitApply()
    {
        // 1. 服务端默认关闭开关（严格布尔；缺省/非 true 一律关闭）
        if (config('shipping_submit.enabled') !== true) {
            return json([
                'code' => -200,
                'msg'  => '正式提交接口尚未开放',
                'data' => [],
            ]);
        }

        // 2. 仅允许 POST（禁止 GET/HEAD/其它方法及 Query 触发提交）
        if (!$this->request->isPost()) {
            return json([
                'code' => -200,
                'msg'  => '仅支持 POST 提交',
                'data' => [],
            ]);
        }

        // 3. Session 登录校验（防御加深；Common::initialize 可能已拦截未登录）
        // 身份权威来源仅为 session('aid')，见 getSubmitSessionAid()
        $aidRaw = $this->getSubmitSessionAid();
        $aid = $this->normalizeSubmitSessionAid($aidRaw);
        if ($aid <= 0) {
            return json([
                'code' => -200,
                'msg'  => ($aidRaw === null || $aidRaw === '' || $aidRaw === false)
                    ? '请先登录'
                    : '登录状态无效',
                'data' => [],
            ]);
        }

        // 4. CSRF 校验（与业务 submit_token 分离；不一次性销毁，兼容幂等重试）
        $csrfRaw = $this->request->post('csrf_token');
        $csrfCheck = $this->validateSubmitCsrfToken($csrfRaw, $aid);
        if ($csrfCheck['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $csrfCheck['msg'],
                'data' => [],
            ]);
        }

        // 5. 请求体大小保护（CONTENT_LENGTH；并兼容 post_max_size 丢弃后的空 POST）
        $contentLength = isset($_SERVER['CONTENT_LENGTH']) ? (int)$_SERVER['CONTENT_LENGTH'] : 0;
        if ($contentLength > self::SUBMIT_HTTP_MAX_BODY_BYTES) {
            return json([
                'code' => -200,
                'msg'  => '请求体过大，已被拒绝',
                'data' => [],
            ]);
        }

        // 6. 仅读取 POST（禁止 param() 混合 GET）
        $params = $this->request->post();
        if (!is_array($params)) {
            $params = [];
        }

        // post_max_size 超限时 PHP 可能清空 $_POST，此时仍有 CONTENT_LENGTH
        if ($params === [] && $contentLength > 0) {
            return json([
                'code' => -200,
                'msg'  => '请求体无法解析或已超限，已被拒绝',
                'data' => [],
            ]);
        }

        // 关闭态以外也不允许请求参数控制开关；CSRF 不进入业务载荷
        unset($params['shipping_submit'], $params['enabled'], $params['csrf_token']);

        // 7. items 结构校验（完整业务校验仍由 Service 负责）
        $itemsResult = $this->parseSubmitApplyItems($params);
        if ($itemsResult['ok'] !== true) {
            return json([
                'code' => -200,
                'msg'  => $itemsResult['msg'],
                'data' => [],
            ]);
        }
        $params['items'] = $itemsResult['items'];

        // 申请人身份仅以 Session 为准；前端 applicant_id 不得成为权威身份
        // Service 内部会再次 unset 并以 session('aid') 解析，此处不信任也不改写为前端值

        try {
            // 8. 仅调用一次 Service；9. 原样透传（含 idempotent_replay / result_uncertain）
            $result = $this->getShippingApplyService()->submitApply($params);
            if (!is_array($result)) {
                return json([
                    'code' => -200,
                    'msg'  => '正式提交暂时无法完成，请稍后查询提交状态',
                    'data' => [],
                ]);
            }
            return json($result);
        } catch (\Throwable $e) {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write(
                    '[Ship::submitApply] ' . $e->getMessage(),
                    'error'
                );
            }
            // 无法断言事务已回滚；不替换 token、不自动重试、不泄露 SQL/堆栈
            return json([
                'code' => -200,
                'msg'  => '正式提交暂时无法完成，请稍后查询提交状态',
                'data' => [],
            ]);
        }
    }

    /**
     * 正式提交身份：仅信任 session('aid')
     *
     * @return mixed
     */
    protected function getSubmitSessionAid()
    {
        return session('aid');
    }

    /**
     * 将 Session aid 规范化为正整数；非法返回 0
     *
     * @param mixed $raw
     * @return int
     */
    protected function normalizeSubmitSessionAid($raw)
    {
        if ($raw === null || $raw === '' || $raw === false || is_array($raw) || is_object($raw) || is_bool($raw)) {
            return 0;
        }
        if (is_int($raw)) {
            return $raw > 0 ? $raw : 0;
        }
        if (is_float($raw)) {
            return 0;
        }
        if (!is_numeric($raw)) {
            return 0;
        }
        $text = trim((string)$raw);
        if ($text === '' || !preg_match('/^[1-9]\d*$/', $text)) {
            return 0;
        }
        $aid = (int)$text;
        if ($aid <= 0 || (string)$aid !== $text) {
            return 0;
        }
        return $aid;
    }

    /**
     * 读取正式提交 CSRF Session 记录（可测试覆盖）
     *
     * @return mixed
     */
    protected function getSubmitCsrfSessionRecord()
    {
        return session(self::SUBMIT_CSRF_SESSION_KEY);
    }

    /**
     * 写入正式提交 CSRF Session 记录（可测试覆盖）
     *
     * @param array $record
     * @return void
     */
    protected function setSubmitCsrfSessionRecord(array $record)
    {
        session(self::SUBMIT_CSRF_SESSION_KEY, $record);
    }

    /**
     * CSRF Token 格式：64 位小写/大写十六进制
     *
     * @param mixed $token
     * @return bool
     */
    protected function isValidSubmitCsrfTokenFormat($token)
    {
        if (!is_string($token)) {
            return false;
        }
        if (strlen($token) !== self::SUBMIT_CSRF_TOKEN_HEX_LEN) {
            return false;
        }
        return (bool)preg_match('/^[a-fA-F0-9]{' . self::SUBMIT_CSRF_TOKEN_HEX_LEN . '}$/', $token);
    }

    /**
     * 生成或复用与当前 aid 绑定的 CSRF Token（同 Session 内可重复校验，不一次性销毁）
     *
     * @param int $aid
     * @return string
     * @throws \RuntimeException
     */
    protected function ensureSubmitCsrfToken($aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            throw new \RuntimeException('登录状态无效');
        }

        $record = $this->getSubmitCsrfSessionRecord();
        if (is_array($record)
            && isset($record['aid'], $record['token'])
            && (int)$record['aid'] === $aid
            && $this->isValidSubmitCsrfTokenFormat($record['token'])
        ) {
            return (string)$record['token'];
        }

        try {
            $token = bin2hex(random_bytes(32));
        } catch (\Throwable $e) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }
        if (!$this->isValidSubmitCsrfTokenFormat($token)) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }

        $this->setSubmitCsrfSessionRecord([
            'aid'   => $aid,
            'token' => $token,
        ]);

        return $token;
    }

    /**
     * 校验正式提交 CSRF（失败不泄露服务端 Token）
     *
     * @param mixed $raw
     * @param int   $aid
     * @return array{ok:bool,msg:string}
     */
    protected function validateSubmitCsrfToken($raw, $aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            return ['ok' => false, 'msg' => '登录状态无效'];
        }
        // 缺键 / 非字符串 / 空 / 格式非法：统一拒绝文案，不泄露服务端 Token
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }
        if (!is_string($raw) || $raw === '' || !$this->isValidSubmitCsrfTokenFormat($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        $record = $this->getSubmitCsrfSessionRecord();
        if (!is_array($record)
            || !isset($record['aid'], $record['token'])
            || !$this->isValidSubmitCsrfTokenFormat($record['token'])
        ) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        // 账号切换：旧 CSRF 立即失效（不清除 uncertain 业务意图；仅拒绝本次请求）
        if ((int)$record['aid'] !== $aid) {
            return ['ok' => false, 'msg' => '登录身份已变化，请刷新页面后重新确认提交状态'];
        }

        if (!hash_equals((string)$record['token'], $raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        return ['ok' => true, 'msg' => ''];
    }

    /**
     * 解析并校验正式提交 items（Controller 层结构闸门）
     *
     * @param array $params
     * @return array{ok:bool,msg:string,items?:array}
     */
    protected function parseSubmitApplyItems(array $params)
    {
        if (!array_key_exists('items', $params)) {
            return ['ok' => false, 'msg' => '发货明细不能为空'];
        }

        $itemsRaw = $params['items'];

        if (is_string($itemsRaw)) {
            if ($itemsRaw === '') {
                return ['ok' => false, 'msg' => '发货明细不能为空'];
            }
            if (strlen($itemsRaw) > self::SUBMIT_HTTP_MAX_ITEMS_BYTES) {
                return ['ok' => false, 'msg' => '发货明细过大，已被拒绝'];
            }
            if (!mb_check_encoding($itemsRaw, 'UTF-8')) {
                return ['ok' => false, 'msg' => '发货明细编码无效'];
            }

            $decoded = json_decode($itemsRaw, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                return ['ok' => false, 'msg' => '发货明细格式不正确'];
            }
            // JSON null / 标量：禁止静默转 []
            if (!is_array($decoded)) {
                return ['ok' => false, 'msg' => '发货明细必须是列表'];
            }
            $items = $decoded;
        } elseif (is_array($itemsRaw)) {
            $items = $itemsRaw;
        } else {
            return ['ok' => false, 'msg' => '发货明细格式不正确'];
        }

        // 拒绝 JSON 对象（关联数组）冒充列表
        if (!$this->isSubmitItemsListArray($items)) {
            return ['ok' => false, 'msg' => '发货明细必须是列表'];
        }

        if ($items === []) {
            return ['ok' => false, 'msg' => '发货明细不能为空'];
        }

        if (count($items) > self::SUBMIT_HTTP_MAX_ITEM_ROWS) {
            return ['ok' => false, 'msg' => '发货明细行数超过限制'];
        }

        if (!$this->isSubmitItemsDepthAllowed($items, 1)) {
            return ['ok' => false, 'msg' => '发货明细结构过深，已被拒绝'];
        }

        foreach ($items as $row) {
            if (!is_array($row)) {
                return ['ok' => false, 'msg' => '发货明细行格式不正确'];
            }
            // 行本身不得再是纯列表伪装；业务字段由 Service 校验
            if (!$this->isSubmitItemRowObject($row)) {
                return ['ok' => false, 'msg' => '发货明细行格式不正确'];
            }
        }

        return ['ok' => true, 'msg' => '', 'items' => $items];
    }

    /**
     * 是否为 0..n-1 连续整数键的列表
     *
     * @param array $arr
     * @return bool
     */
    protected function isSubmitItemsListArray(array $arr)
    {
        if ($arr === []) {
            return true;
        }
        return array_keys($arr) === range(0, count($arr) - 1);
    }

    /**
     * 行对象：允许空对象；拒绝纯列表行（无业务键）
     *
     * @param array $row
     * @return bool
     */
    protected function isSubmitItemRowObject(array $row)
    {
        if ($row === []) {
            return true;
        }
        // 纯 [1,2,3] 视为非法行；关联/混合键视为对象交由 Service
        return !$this->isSubmitItemsListArray($row);
    }

    /**
     * 限制嵌套深度，防止异常深度 JSON 消耗资源
     *
     * @param mixed $value
     * @param int   $depth
     * @return bool
     */
    protected function isSubmitItemsDepthAllowed($value, $depth)
    {
        if ($depth > self::SUBMIT_HTTP_MAX_JSON_DEPTH) {
            return false;
        }
        if (!is_array($value)) {
            return true;
        }
        foreach ($value as $child) {
            if (!$this->isSubmitItemsDepthAllowed($child, $depth + 1)) {
                return false;
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // M4-2B-2B-2B-2：审批独立 CSRF（Session 键 ship_decide_csrf）
    // -------------------------------------------------------------------------

    /**
     * 读取审批 CSRF Session 记录
     *
     * @return mixed
     */
    protected function getDecideCsrfSessionRecord()
    {
        return session(self::DECIDE_CSRF_SESSION_KEY);
    }

    /**
     * 写入审批 CSRF Session 记录
     *
     * @param array $record
     * @return void
     */
    protected function setDecideCsrfSessionRecord(array $record)
    {
        session(self::DECIDE_CSRF_SESSION_KEY, $record);
    }

    /**
     * 审批 CSRF Token 格式校验
     *
     * @param mixed $token
     * @return bool
     */
    protected function isValidDecideCsrfTokenFormat($token)
    {
        if (!is_string($token)) {
            return false;
        }
        if (strlen($token) !== self::DECIDE_CSRF_TOKEN_HEX_LEN) {
            return false;
        }
        return (bool)preg_match('/^[a-fA-F0-9]{' . self::DECIDE_CSRF_TOKEN_HEX_LEN . '}$/', $token);
    }

    /**
     * 生成或复用与当前 aid 绑定的审批 CSRF Token（同 Session 可重复校验；不一次性销毁）
     *
     * @param int $aid
     * @return string
     * @throws \RuntimeException
     */
    protected function ensureDecideCsrfToken($aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            throw new \RuntimeException('登录状态无效');
        }

        $record = $this->getDecideCsrfSessionRecord();
        if (is_array($record)
            && isset($record['aid'], $record['token'])
            && (int)$record['aid'] === $aid
            && $this->isValidDecideCsrfTokenFormat($record['token'])
        ) {
            return (string)$record['token'];
        }

        try {
            $token = bin2hex(random_bytes(32));
        } catch (\Throwable $e) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }
        if (!$this->isValidDecideCsrfTokenFormat($token)) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }

        $this->setDecideCsrfSessionRecord([
            'aid'   => $aid,
            'token' => $token,
        ]);

        return $token;
    }

    /**
     * 校验审批 CSRF（失败不泄露服务端 Token）
     *
     * @param mixed $raw
     * @param int   $aid
     * @return array{ok:bool,msg:string}
     */
    protected function validateDecideCsrfToken($raw, $aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            return ['ok' => false, 'msg' => '登录状态无效'];
        }
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }
        if (!is_string($raw) || $raw === '' || !$this->isValidDecideCsrfTokenFormat($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        $record = $this->getDecideCsrfSessionRecord();
        if (!is_array($record)
            || !isset($record['aid'], $record['token'])
            || !$this->isValidDecideCsrfTokenFormat($record['token'])
        ) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        if ((int)$record['aid'] !== $aid) {
            return ['ok' => false, 'msg' => '登录身份已变化，请刷新页面后重新确认审批状态'];
        }

        if (!hash_equals((string)$record['token'], $raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        return ['ok' => true, 'msg' => ''];
    }

    // -------------------------------------------------------------------------
    // M4-2C-2B-2C-2：审核失败编辑独立 CSRF（Session 键 ship_rejected_edit_csrf）
    // -------------------------------------------------------------------------

    /**
     * @return mixed
     */
    protected function getRejectedEditCsrfSessionRecord()
    {
        return session(self::REJECTED_EDIT_CSRF_SESSION_KEY);
    }

    /**
     * @param array $record
     * @return void
     */
    protected function setRejectedEditCsrfSessionRecord(array $record)
    {
        session(self::REJECTED_EDIT_CSRF_SESSION_KEY, $record);
    }

    /**
     * @param mixed $token
     * @return bool
     */
    protected function isValidRejectedEditCsrfTokenFormat($token)
    {
        if (!is_string($token)) {
            return false;
        }
        if (strlen($token) !== self::REJECTED_EDIT_CSRF_TOKEN_HEX_LEN) {
            return false;
        }
        return (bool)preg_match('/^[a-fA-F0-9]{' . self::REJECTED_EDIT_CSRF_TOKEN_HEX_LEN . '}$/', $token);
    }

    /**
     * @param int $aid
     * @return string
     * @throws \RuntimeException
     */
    protected function ensureRejectedEditCsrfToken($aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            throw new \RuntimeException('登录状态无效');
        }

        $record = $this->getRejectedEditCsrfSessionRecord();
        if (is_array($record)
            && isset($record['aid'], $record['token'])
            && (int)$record['aid'] === $aid
            && $this->isValidRejectedEditCsrfTokenFormat($record['token'])
        ) {
            return (string)$record['token'];
        }

        try {
            $token = bin2hex(random_bytes(32));
        } catch (\Throwable $e) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }
        if (!$this->isValidRejectedEditCsrfTokenFormat($token)) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }

        $this->setRejectedEditCsrfSessionRecord([
            'aid'   => $aid,
            'token' => $token,
        ]);

        return $token;
    }

    /**
     * @param mixed $raw
     * @param int   $aid
     * @return array{ok:bool,msg:string}
     */
    protected function validateRejectedEditCsrfToken($raw, $aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            return ['ok' => false, 'msg' => '登录状态无效'];
        }
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }
        if (!is_string($raw) || $raw === '' || !$this->isValidRejectedEditCsrfTokenFormat($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        $record = $this->getRejectedEditCsrfSessionRecord();
        if (!is_array($record)
            || !isset($record['aid'], $record['token'])
            || !$this->isValidRejectedEditCsrfTokenFormat($record['token'])
        ) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        if ((int)$record['aid'] !== $aid) {
            return ['ok' => false, 'msg' => '登录身份已变化，请刷新页面后重新打开编辑'];
        }

        if (!hash_equals((string)$record['token'], $raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        return ['ok' => true, 'msg' => ''];
    }

    // -------------------------------------------------------------------------
    // M4-2C-3-2：审核失败再次提交独立 CSRF（Session 键 ship_resubmit_csrf）
    // -------------------------------------------------------------------------

    /**
     * @return mixed
     */
    protected function getResubmitCsrfSessionRecord()
    {
        return session(self::RESUBMIT_CSRF_SESSION_KEY);
    }

    /**
     * @param array $record
     * @return void
     */
    protected function setResubmitCsrfSessionRecord(array $record)
    {
        session(self::RESUBMIT_CSRF_SESSION_KEY, $record);
    }

    /**
     * @param mixed $token
     * @return bool
     */
    protected function isValidResubmitCsrfTokenFormat($token)
    {
        if (!is_string($token)) {
            return false;
        }
        if (strlen($token) !== self::RESUBMIT_CSRF_TOKEN_HEX_LEN) {
            return false;
        }
        return (bool)preg_match('/^[a-fA-F0-9]{' . self::RESUBMIT_CSRF_TOKEN_HEX_LEN . '}$/', $token);
    }

    /**
     * @param int $aid
     * @return string
     * @throws \RuntimeException
     */
    protected function ensureResubmitCsrfToken($aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            throw new \RuntimeException('登录状态无效');
        }

        $record = $this->getResubmitCsrfSessionRecord();
        if (is_array($record)
            && isset($record['aid'], $record['token'])
            && (int)$record['aid'] === $aid
            && $this->isValidResubmitCsrfTokenFormat($record['token'])
        ) {
            return (string)$record['token'];
        }

        try {
            $token = bin2hex(random_bytes(32));
        } catch (\Throwable $e) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }
        if (!$this->isValidResubmitCsrfTokenFormat($token)) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }

        $this->setResubmitCsrfSessionRecord([
            'aid'   => $aid,
            'token' => $token,
        ]);

        return $token;
    }

    /**
     * @param mixed $raw
     * @param int   $aid
     * @return array{ok:bool,msg:string}
     */
    protected function validateResubmitCsrfToken($raw, $aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            return ['ok' => false, 'msg' => '登录状态无效'];
        }
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }
        if (!is_string($raw) || $raw === '' || !$this->isValidResubmitCsrfTokenFormat($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        $record = $this->getResubmitCsrfSessionRecord();
        if (!is_array($record)
            || !isset($record['aid'], $record['token'])
            || !$this->isValidResubmitCsrfTokenFormat($record['token'])
        ) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        if ((int)$record['aid'] !== $aid) {
            return ['ok' => false, 'msg' => '登录身份已变化，请刷新页面后重新打开编辑'];
        }

        if (!hash_equals((string)$record['token'], $raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        return ['ok' => true, 'msg' => ''];
    }

    // -------------------------------------------------------------------------
    // M4-2C-5-2：审核失败受控软删除独立 CSRF（Session 键 ship_delete_rejected_csrf）
    // -------------------------------------------------------------------------

    /**
     * @return mixed
     */
    protected function getDeleteRejectedCsrfSessionRecord()
    {
        return session(self::DELETE_REJECTED_CSRF_SESSION_KEY);
    }

    /**
     * @param array $record
     * @return void
     */
    protected function setDeleteRejectedCsrfSessionRecord(array $record)
    {
        session(self::DELETE_REJECTED_CSRF_SESSION_KEY, $record);
    }

    /**
     * @param mixed $token
     * @return bool
     */
    protected function isValidDeleteRejectedCsrfTokenFormat($token)
    {
        if (!is_string($token)) {
            return false;
        }
        if (strlen($token) !== self::DELETE_REJECTED_CSRF_TOKEN_HEX_LEN) {
            return false;
        }
        return (bool)preg_match('/^[a-fA-F0-9]{' . self::DELETE_REJECTED_CSRF_TOKEN_HEX_LEN . '}$/', $token);
    }

    /**
     * @param int $aid
     * @return string
     * @throws \RuntimeException
     */
    protected function ensureDeleteRejectedCsrfToken($aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            throw new \RuntimeException('登录状态无效');
        }

        $record = $this->getDeleteRejectedCsrfSessionRecord();
        if (is_array($record)
            && isset($record['aid'], $record['token'])
            && (int)$record['aid'] === $aid
            && $this->isValidDeleteRejectedCsrfTokenFormat($record['token'])
        ) {
            return (string)$record['token'];
        }

        try {
            $token = bin2hex(random_bytes(32));
        } catch (\Throwable $e) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }
        if (!$this->isValidDeleteRejectedCsrfTokenFormat($token)) {
            throw new \RuntimeException('无法生成安全 CSRF Token');
        }

        $this->setDeleteRejectedCsrfSessionRecord([
            'aid'   => $aid,
            'token' => $token,
        ]);

        return $token;
    }

    /**
     * @param mixed $raw
     * @param int   $aid
     * @return array{ok:bool,msg:string}
     */
    protected function validateDeleteRejectedCsrfToken($raw, $aid)
    {
        $aid = (int)$aid;
        if ($aid <= 0) {
            return ['ok' => false, 'msg' => '登录状态无效'];
        }
        if ($raw === null || is_array($raw) || is_object($raw) || is_bool($raw) || is_int($raw) || is_float($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }
        if (!is_string($raw) || $raw === '' || !$this->isValidDeleteRejectedCsrfTokenFormat($raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        $record = $this->getDeleteRejectedCsrfSessionRecord();
        if (!is_array($record)
            || !isset($record['aid'], $record['token'])
            || !$this->isValidDeleteRejectedCsrfTokenFormat($record['token'])
        ) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        if ((int)$record['aid'] !== $aid) {
            return ['ok' => false, 'msg' => '登录身份已变化，请刷新页面后重试'];
        }

        if (!hash_equals((string)$record['token'], $raw)) {
            return ['ok' => false, 'msg' => '安全校验失败，请刷新页面后重试'];
        }

        return ['ok' => true, 'msg' => ''];
    }
}
