<?php

namespace app\admin\service;

use app\admin\model\Admin;
use app\admin\model\ClientOrder;
use app\admin\model\OrderItem;
use think\Db;

/**
 * 运营管理 - 成交订单列表（personOrderSearch）业务编排
 *
 * 凭证图 wechat_receipt_images / inquiry_assign_images 由 {@see \app\admin\controller\Operator::personOrderSearch} 调用
 * {@see VoucherImageParseService::parseList} 写入，本类只返回含库字段的列表数据。
 */
class PersonOrderService
{
    /** @var ClientOrder */
    protected $clientOrderModel;

    public function __construct()
    {
        $this->clientOrderModel = new ClientOrder();
    }

    /**
     * @param array $params 一般为 Request::param()
     * @param int|string $page
     * @param int|string $limit
     * @param callable $buildTimeWhere function (string $timebucket, string $field): array
     * @param callable $getOrgWhere function (string $org, string $alias = ''): \Closure
     * @return array{code:int,msg:string,data:array,count:int,rel:int,totalInquiries:int,successRate:string,totalMoney:string,totalProfit:string}
     */
    public function search(array $params, $page, $limit, callable $buildTimeWhere, callable $getOrgWhere)
    {
        $where = [];
        $client_where = [];

        $current_admin = Admin::getMyInfo();

        $isSuper = (
            (int)session('aid') === 1
            || ($current_admin['username'] ?? '') === 'admin'
            || (int)($current_admin['group_id'] ?? 0) === 12
            || (int)($current_admin['group_id'] ?? 0) === 13
            || (int)($current_admin['group_id'] ?? 0) === 16
        );

        $keyword = $params['keyword'] ?? null;
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

        $current_username = $current_admin['username'] ?? '';

        if ($isSuper === true) {
            // 超级管理员：不添加 pr_user 限制
        } else {
            if ($current_username) {
                $where[] = ['pr_user', '=', $current_username];
                $client_where[] = ['pr_user', '=', $current_username];
            }
        }

        if (isset($keyword['team_name']) && !empty($keyword['team_name'])) {
            $where[] = ['team_name', '=', $keyword['team_name']];
        }

        $org_where = [];
        if (!empty($keyword['org'])) {
            $org_where[] = $getOrgWhere($keyword['org']);
        }

        $filter_team_name = $keyword['team_name'] ?? '';
        if ($filter_team_name || !empty($org_where)) {
            $filter_query = Db::table('admin');
            if ($filter_team_name) {
                $filter_query->where('team_name', $filter_team_name);
            }
            if (!empty($org_where)) {
                $filter_query->where($org_where);
            }
            $filtered_usernames = $filter_query->column('username');

            if (empty($filtered_usernames)) {
                $client_where[] = ['pr_user', '=', time()];
                $where[] = ['pr_user', '=', time()];
            } else {
                if ($isSuper) {
                    $client_where[] = ['pr_user', 'in', $filtered_usernames];
                    $where[] = ['pr_user', 'in', $filtered_usernames];
                } else {
                    if (!in_array($current_username, $filtered_usernames)) {
                        $client_where[] = ['pr_user', '=', time()];
                        $where[] = ['pr_user', '=', time()];
                    }
                }
            }
        }

        if (isset($keyword['source'])) {
            $where[] = ['source', '=', $keyword['source']];
            $kh_source = strtolower($keyword['source']);
            $client_where[] = ['kh_status', 'like', "%$kh_source%"];
        }
        if (isset($keyword['source_port'])) {
            $where[] = ['source_port', '=', $keyword['source_port']];
        }

        if (isset($keyword['pr_user'])) {
            if ($isSuper) {
                $where[] = ['pr_user', '=', $keyword['pr_user']];
                $client_where[] = ['pr_user', '=', $keyword['pr_user']];
            } else {
                if ($keyword['pr_user'] !== $current_username) {
                    $client_where[] = ['pr_user', '=', time()];
                    $where[] = ['pr_user', '=', time()];
                }
            }
        }

        $client_where[] = ['l.status', '=', 1];

        $list = $this->clientOrderModel->paginatePersonSearch($where, $page, $limit);

        $orderIds = array_column($list['data'], 'id');
        $orderItemsMap = OrderItem::getProductNamesGroupedByOrderIds($orderIds);

        foreach ($list['data'] as &$order) {
            $order['order_items'] = $orderItemsMap[$order['id']] ?? [];
        }
        unset($order);

        $totalInquiries = (int)Db::table('crm_leads')->alias('l')->where($client_where)->count();

        $successOrders = $list['total'];
        $successRate = $totalInquiries > 0 ? ($successOrders / $totalInquiries * 100) : 0;
        $totalMoney = $this->clientOrderModel->sumMoneyByWhere($where);
        $totalProfit = $this->clientOrderModel->sumProfitByWhere($where);

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
}
