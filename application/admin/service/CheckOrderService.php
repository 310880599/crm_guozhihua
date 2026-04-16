<?php

namespace app\admin\service;

use app\admin\model\Admin;
use app\admin\model\ClientOrder;
use app\admin\model\CrmInquiry;
use app\admin\model\CrmInquiryPort;
use app\admin\model\Lead;
use app\admin\model\OrderItem;

/**
 * 客户管理 - 检查订单（业务编排：筛选、权限范围、统计、凭证图字段）
 */
class CheckOrderService
{
    /**
     * 检查订单页 GET 所需 assign 数据（不含 customer_type 常量，由 Controller 赋值）
     *
     * @param string[] $allowedUsernames getCheckClientAllowedUsernames
     * @param callable(): array $teamListProvider Controller->getTeamList
     * @return array<string, mixed>
     */
    public function getPageAssignData(array $allowedUsernames, callable $teamListProvider): array
    {
        return [
            'channelList' => CrmInquiry::listEnabledNames(),
            'portList'      => CrmInquiryPort::listEnabledNames(),
            'adminResult'   => Admin::listByUsernamesForSelect($allowedUsernames),
            'teamList'      => $teamListProvider(),
        ];
    }

    /**
     * 检查订单列表 JSON（原 checkOrderSearch）
     *
     * @param array $keyword
     * @param int|string $page
     * @param int|string $limit
     * @param array $visibleUsers getCheckClientAllowedUsernames
     * @param string $currentUsername
     * @param callable(string $timeCondition, string $field): array $buildTimeWhere Controller->buildTimeWhere
     * @param callable(string $org, string $alias = ''): \Closure $getOrgWhere Controller->getOrgWhere
     * @return array{code:int,msg:string,data:array,count:int,rel:int,totalInquiries:int,successRate:string,totalMoney:string,totalProfit:string}
     */
    public function search(
        array $keyword,
        $page,
        $limit,
        array $visibleUsers,
        string $currentUsername,
        callable $buildTimeWhere,
        callable $getOrgWhere
    ): array {
        $where = [];
        $client_where = [];

        $where[] = ['o.check_status', '=', 2];

        $visibleUsers = array_values(array_unique(array_filter(array_map('trim', $visibleUsers))));
        $currentUsername = trim($currentUsername);
        if (empty($visibleUsers) && $currentUsername !== '') {
            $visibleUsers = [$currentUsername];
        }

        if ($keyword) {
            $keyword = array_filter($keyword);
        }

        if (isset($keyword['order_no'])) {
            $where[] = ['order_no', 'like', "%{$keyword['order_no']}%"];
        }

        $timeCondition = null;
        if (isset($keyword['timebucket']) && $keyword['timebucket'] !== '') {
            $timeCondition = $keyword['timebucket'];
        } elseif (isset($keyword['at_time']) && $keyword['at_time'] !== '') {
            $timeCondition = $keyword['at_time'];
        }

        if ($timeCondition) {
            $where[] = $buildTimeWhere($timeCondition, 'order_time');
            $timeWhere = [];
            $timeWhere['at_time'] = $buildTimeWhere($timeCondition, 'at_time');
            $timeWhere['to_kh_time'] = $buildTimeWhere($timeCondition, 'to_kh_time');
            $client_where[] = function ($query) use ($timeWhere) {
                $query->where(...$timeWhere['at_time']);
                $query->whereOr(...$timeWhere['to_kh_time']);
            };
        }

        if (isset($keyword['min_money'])) {
            $where[] = ['money', '>', $keyword['min_money']];
        }
        if (isset($keyword['max_money'])) {
            $where[] = ['money', '<', $keyword['max_money']];
        }
        if (isset($keyword['min_profit'])) {
            $where[] = ['profit', '>', $keyword['min_profit']];
        }
        if (isset($keyword['max_profit'])) {
            $where[] = ['profit', '<', $keyword['max_profit']];
        }
        if (isset($keyword['min_margin_rate'])) {
            $where[] = ['margin_rate', '>', $keyword['min_margin_rate']];
        }
        if (isset($keyword['max_margin_rate'])) {
            $where[] = ['margin_rate', '<', $keyword['max_margin_rate']];
        }
        if (isset($keyword['cname'])) {
            $where[] = ['cname', 'like', "%{$keyword['cname']}%"];
        }
        if (isset($keyword['contact'])) {
            $where[] = ['contact', 'like', "%{$keyword['contact']}%"];
        }
        if (isset($keyword['customer_type'])) {
            $where[] = ['customer_type', '=', $keyword['customer_type']];
        }
        if (isset($keyword['product_name'])) {
            $where[] = ['product_name', 'like', "%{$keyword['product_name']}%"];
        }

        $filter_team_name = '';
        if (isset($keyword['team_name']) && $keyword['team_name'] !== '') {
            $where[] = ['team_name', '=', $keyword['team_name']];
            $filter_team_name = $keyword['team_name'];
        }

        $org_where = [];
        if (!empty($keyword['org'])) {
            $org_where[] = $getOrgWhere($keyword['org']);
        }

        $scopedUsers = $visibleUsers;
        if ($filter_team_name || !empty($org_where)) {
            $filteredUsernames = Admin::columnUsernamesAfterTeamOrgFilter(
                $visibleUsers,
                $filter_team_name !== '' ? $filter_team_name : null,
                $org_where
            );

            if (empty($filteredUsernames)) {
                return $this->emptyGridResponse();
            }

            $scopedUsers = $filteredUsernames;
        }

        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        if (isset($keyword['source_port'])) {
            $where[] = ['source_port', '=', $keyword['source_port']];
        }

        $selectedPrUser = '';
        if (isset($keyword['pr_user'])) {
            $selectedPrUser = trim((string) $keyword['pr_user']);
        }

        if ($selectedPrUser !== '') {
            if (empty($scopedUsers) || !in_array($selectedPrUser, $scopedUsers, true)) {
                return $this->emptyGridResponse();
            }

            $where[] = ['pr_user', '=', $selectedPrUser];
            $client_where[] = ['pr_user', '=', $selectedPrUser];
            $scopedUsers = [$selectedPrUser];
        }

        if (!empty($scopedUsers)) {
            $where[] = ['pr_user', 'in', $scopedUsers];
            $client_where[] = ['pr_user', 'in', $scopedUsers];
        } elseif ($currentUsername !== '') {
            $where[] = ['pr_user', '=', $currentUsername];
            $client_where[] = ['pr_user', '=', $currentUsername];
        }

        $client_where[] = ['l.status', '=', 1];

        $orderModel = new ClientOrder();
        $list = $orderModel->paginatePersonSearch($where, $page, $limit);

        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = OrderItem::getProductNamesGroupedByOrderIds($orderIds);

        foreach ($list['data'] as &$order) {
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
        }
        unset($order);

        $list['data'] = OrderImageService::appendOrderImageFields($list['data'] ?? []);

        $leadModel = new Lead();
        $totalInquiries = $leadModel->countWithAlias($client_where);

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $orderModel->sumMoneyByWhere($where);
        $totalProfit = $orderModel->sumProfitByWhere($where);

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
            'totalInquiries' => $totalInquiries,
            'successRate' => number_format($successRate, 2),
            'totalMoney' => number_format($totalMoney, 2),
            'totalProfit' => number_format($totalProfit, 2),
        ];
    }

    /**
     * @return array{code:int,msg:string,data:array,count:int,rel:int,totalInquiries:int,successRate:string,totalMoney:string,totalProfit:string}
     */
    private function emptyGridResponse(): array
    {
        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => [],
            'count' => 0,
            'rel' => 1,
            'totalInquiries' => 0,
            'successRate' => number_format(0, 2),
            'totalMoney' => number_format(0, 2),
            'totalProfit' => number_format(0, 2),
        ];
    }
}
