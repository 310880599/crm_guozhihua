<?php
namespace app\admin\controller;

use app\admin\service\ClientFollowService;
use think\Db;

/**
 * 待办跟进管理。
 *
 * 访问控制：沿用 Common::initialize() 的登录与 auth_rule 菜单权限。
 * 列表数据范围：由 ClientFollowTodoPermissionService 与客户列表口径对齐，并包含协同人。
 */
class ClientFollow extends Common
{
    public function index()
    {
        if (request()->isPost()) {
            return $this->followSearch();
        }

        $this->assign('inquiryList', Db::table('crm_inquiry')->select());
        $this->assign('portList', Db::table('crm_inquiry_port')->select());

        return $this->fetch();
    }

    /**
     * 待办列表（layui table POST）
     */
    public function followSearch()
    {
        $page = input('page/d', 1);
        $limit = input('limit/d', 10);

        $keyword = [
            'kh_name' => $this->normalizeSearchInput('kh_name'),
            'phone' => $this->normalizeSearchInput('phone'),
            'pr_user' => $this->normalizeSearchInput('pr_user'),
            'next_up_start' => $this->normalizeSearchInput('next_up_start'),
            'next_up_end' => $this->normalizeSearchInput('next_up_end'),
            'inquiry_id' => $this->normalizeSearchInput('inquiry_id'),
            'port_id' => $this->normalizeSearchInput('port_id'),
            'only_overdue' => $this->normalizeSearchInput('only_overdue'),
        ];

        $service = new ClientFollowService();
        return json($service->getTodoFollowTableData($page, $limit, $keyword, []));
    }

    /**
     * 搜索参数标准化：统一转字符串并处理 null/undefined。
     *
     * @param string $key
     * @return string
     */
    private function normalizeSearchInput($key)
    {
        $value = input($key, '');
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
}
