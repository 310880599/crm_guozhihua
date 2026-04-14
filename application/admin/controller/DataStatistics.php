<?php
namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use app\admin\model\Admin;

class DataStatistics extends Common
{
    /**
     * 权限边界约定：
     * 1) 保留菜单层权限（本文件不实现）与 initialize() 模块访问权限判断；
     * 2) 业务数据统一按公开统计口径查询，不做“按当前登录人数据范围收口”。
     */
    public function initialize()
    {
        parent::initialize();
        $currentAdmin = Admin::getMyInfo();
        // [权限边界-A] 模块访问权限：允许保留（本模块唯一后端访问权限判断）
        if ($currentAdmin['group_id'] != 1
            && $currentAdmin['group_id'] != 11
            && $currentAdmin['group_id'] != 12
            && $currentAdmin['group_id'] != 13
            && $currentAdmin['group_id'] != 19
            && $currentAdmin['group_id'] != 21
            && $currentAdmin['group_id'] != 22
            && $currentAdmin['username'] != 'admin') {
            $this->error('您无权限访问该模块');
        }
    }

    public function index()
    {
        return $this->fetch();
    }

    /**
     * 业务人员业绩表 - 页面入口
     */
    public function performanceTable()
    {
        return $this->fetch('performance_table');
    }

    /**
     * 团队业绩表 - 页面入口
     */
    public function teamPerformanceTable()
    {
        return $this->fetch('team_performance_table');
    }

    /**
     * 业务询盘汇总表 - 页面入口
     */
    public function inquirySummaryTable()
    {
        return $this->fetch('inquiry_summary_table');
    }

    // =========================================================
    // 业务人员业绩表
    // =========================================================

    /**
     * 获取业务人员业绩数据
     * 合计与分组均以「订单列表」同一套 where 为口径；补零成员仅增加展示行，不改变 summary。
     */
    public function getPerformanceData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        $filter_username = Request::param('username', '');

        $empty_summary = [
            'total_profit' => number_format(0, 2),
            'total_money'  => number_format(0, 2),
            'profit_rate'  => number_format(0, 2),
        ];

        try {

        // 需要从业绩表中隐藏的业务员（仅影响表格展示行，不影响访问权限/接口权限）
        // 后续只需要在这里增删名字即可控制“展示隐藏”范围
        $excludeUsernames = [
            // '张三',
            // '李四',
            '范文清',
            '郭志华',
            '郭志华2',
            '付淑雅',
            '叶诗龙'
        ];
        // 清洗隐藏名单：去空格、去空值、去重
        $excludeUsernames = array_values(array_unique(array_filter(array_map(function ($name) {
            return trim((string)$name);
        }, $excludeUsernames))));
        $hiddenUsernameMap = array_fill_keys($excludeUsernames, true);

        // 注意：$excludeUsernames 不参与 where 构建，仅在最终输出前过滤展示行
        $where = $this->buildOrderListAlignedOrderWhere($timebucket, $at_time, $filter_username, $month_keys);

        // 1) summary：与订单列表 totalProfit/totalMoney 同源（对整批 where 求和，不受补零行影响）
        $totalsQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($totalsQuery, $where);
        $totals_row = $totalsQuery
            ->field('SUM(profit) AS total_profit, SUM(money) AS total_money')
            ->find();
        $sum_profit_all = round((float)($totals_row['total_profit'] ?? 0), 2);
        $sum_money_all = round((float)($totals_row['total_money'] ?? 0), 2);
        $sum_rate_all = $sum_money_all > 0 ? round(($sum_profit_all / $sum_money_all) * 100, 2) : 0;

        // 2) 按 pr_user 聚合（空 pr_user 单独成桶，避免丢单）
        // MySQL 5.7: GROUP_CONCAT 的 SEPARATOR 需使用字符串字面量，不能用 CHAR(30)
        $teamNameSeparator = '||#||';
        $orderStatsQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($orderStatsQuery, $where);
        $order_stats = $orderStatsQuery
            ->fieldRaw(
                "IFNULL(NULLIF(TRIM(pr_user),''), '__PR_EMPTY__') AS pr_bucket, "
                . 'COUNT(id) AS order_count, SUM(profit) AS total_profit, SUM(money) AS total_money, '
                . "SUBSTRING_INDEX("
                . "GROUP_CONCAT(NULLIF(TRIM(team_name), '') ORDER BY order_time DESC, id DESC SEPARATOR '{$teamNameSeparator}')"
                . ", '{$teamNameSeparator}', 1"
                . ") AS snap_team_name"
            )
            ->group('pr_bucket')
            ->select();

        $admin_map = [];
        $adminQuery = Db::table('admin')->field('username,team_name');
        foreach ($adminQuery->select() as $ar) {
            if (!empty($ar['username'])) {
                $admin_map[$ar['username']] = $ar;
            }
        }

        // 3) 补零：仅展示用（启用 + 业务组），不参与上面的 SUM
        $business_users_query = Db::table('admin')
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14])
            ->where('is_open', '=', 1)
            ->where('username', '<>', '')
            ->whereNotNull('username');
        if ($filter_username !== '') {
            $business_users_query->where('username', '=', $filter_username);
        }
        $business_users = $business_users_query
            ->field('admin_id,username,team_name')
            ->order('team_name,username')
            ->limit(500)
            ->select();

        $agg_map = [];
        foreach ($order_stats as $stat) {
            $bucket = (string)$stat['pr_bucket'];
            $is_empty_pr = ($bucket === '__PR_EMPTY__');
            $display_username = $is_empty_pr ? '未知业务员' : $bucket;
            $agg_map[$bucket] = [
                'username' => $display_username,
                'pr_bucket' => $bucket,
                'order_count' => (int)($stat['order_count'] ?? 0),
                'total_profit' => round((float)($stat['total_profit'] ?? 0), 2),
                'total_money' => round((float)($stat['total_money'] ?? 0), 2),
                'snap_team_name' => trim((string)($stat['snap_team_name'] ?? '')),
            ];
        }

        foreach ($business_users as $u) {
            $un = trim((string)($u['username'] ?? ''));
            if ($un === '') {
                continue;
            }
            if (!isset($agg_map[$un])) {
                $agg_map[$un] = [
                    'username' => $un,
                    'pr_bucket' => $un,
                    'order_count' => 0,
                    'total_profit' => 0.0,
                    'total_money' => 0.0,
                    'snap_team_name' => '',
                ];
            }
        }

        $result = [];
        foreach ($agg_map as $row) {
            $username = trim((string)($row['username'] ?? ''));
            // 统一在输出前过滤，确保有订单/补零人员都能正确排除
            if ($username !== '' && isset($hiddenUsernameMap[$username])) {
                continue;
            }

            $bucket = $row['pr_bucket'];
            $total_profit = (float)$row['total_profit'];
            $total_money = (float)$row['total_money'];
            $profit_rate = $total_money > 0 ? round(($total_profit / $total_money) * 100, 2) : 0;

            $snap = trim((string)$row['snap_team_name']);
            $admin_team = '';
            if ($bucket !== '__PR_EMPTY__' && isset($admin_map[$bucket])) {
                $admin_team = trim((string)($admin_map[$bucket]['team_name'] ?? ''));
            }
            if ($snap !== '') {
                $team_display = $snap;
            } elseif ($admin_team !== '') {
                $team_display = $admin_team;
            } else {
                $team_display = '未分组';
            }

            $result[] = [
                'username' => $row['username'],
                'team_name' => $team_display,
                'order_count' => (int)$row['order_count'],
                'profit_raw' => $total_profit,
                'profit' => number_format($total_profit, 2),
                'total_money_raw' => $total_money,
                'total_money' => number_format($total_money, 2),
                'profit_rate_raw' => $profit_rate,
                'profit_rate' => number_format($profit_rate, 2),
            ];
        }

        usort($result, function ($a, $b) {
            return ((float)$b['profit_raw']) <=> ((float)$a['profit_raw']);
        });

        $rank = 1;
        foreach ($result as &$item) {
            $item['rank'] = $rank++;
        }
        unset($item);

        return json([
            'code' => 0,
            'msg' => '获取成功',
            'data' => $result,
            'summary' => [
                'total_profit' => number_format($sum_profit_all, 2),
                'total_money' => number_format($sum_money_all, 2),
                'profit_rate' => number_format($sum_rate_all, 2),
            ],
        ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[Performance] getPerformanceData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取业务人员业绩数据失败：' . $e->getMessage(),
                'data' => [],
                'summary' => $empty_summary,
            ]);
        }
    }

    /**
     * 临时调试：对比「订单列表口径」与「旧业绩表口径」命中订单差集，用于核对利润差额来源。
     * 调用示例：POST admin/data_statistics/debugPerformanceDiff 参数 timebucket、at_time 与业绩表一致。
     */
    public function debugPerformanceDiff()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        $where_new = $this->buildOrderListAlignedOrderWhere($timebucket, $at_time, '', $month_keys);

        $effective_timebucket = $timebucket;
        if ($at_time === '' && $effective_timebucket === '') {
            $effective_timebucket = 'month';
        }

        $old_usernames = Db::table('admin')
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14])
            ->where('is_open', '=', 1)
            ->where('username', '<>', '')
            ->whereNotNull('username')
            ->limit(500)
            ->column('username');
        $old_usernames = $old_usernames ? array_values(array_filter($old_usernames)) : [];

        $cols = 'id,order_no,pr_user,team_name,order_time,money,profit,check_status';

        $ordersNewQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($ordersNewQuery, $where_new);
        $orders_new = $ordersNewQuery->field($cols)->select();
        $ids_new = array_column($orders_new ?: [], 'id');

        if ($old_usernames === []) {
            $orders_old = [];
            $ids_old = [];
        } else {
            $orders_old = $this->buildPerformanceOrderQuery($effective_timebucket, $at_time, [], '', $month_keys)
                ->where('pr_user', 'in', $old_usernames)
                ->field($cols)
                ->select();
            $ids_old = array_column($orders_old ?: [], 'id');
        }

        $set_new = array_fill_keys($ids_new, true);
        $set_old = array_fill_keys($ids_old, true);

        $only_new = array_values(array_diff($ids_new, $ids_old));
        $only_old = array_values(array_diff($ids_old, $ids_new));

        $only_new_rows = [];
        $diff_profit = 0.0;
        foreach ($orders_new ?: [] as $o) {
            if (isset($set_old[$o['id']])) {
                continue;
            }
            $only_new_rows[] = $o;
            $diff_profit += (float)($o['profit'] ?? 0);
        }
        $diff_profit = round($diff_profit, 2);

        $sumNewQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($sumNewQuery, $where_new);
        $sum_new = round((float)$sumNewQuery->sum('profit'), 2);
        $sum_old = $old_usernames === []
            ? 0.0
            : round((float)$this->buildPerformanceOrderQuery($effective_timebucket, $at_time, [], '', $month_keys)
                ->where('pr_user', 'in', $old_usernames)
                ->sum('profit'), 2);

        return json([
            'code' => 0,
            'msg' => '调试数据：only_new 为旧业绩表漏掉的订单（其利润合计应接近列表与旧表差额）',
            'criteria' => [
                'new_where_note' => '统一公开口径：check_status=2 + order_time',
                'old_where_note' => '历史口径：同时间 + pr_user in (启用且业务组 admin)',
            ],
            'counts' => [
                'new_orders' => count($ids_new),
                'old_orders' => count($ids_old),
                'only_in_new' => count($only_new),
                'only_in_old' => count($only_old),
            ],
            'sums' => [
                'profit_new_aligned' => $sum_new,
                'profit_old_legacy' => $sum_old,
                'delta_legacy_minus_new' => round($sum_old - $sum_new, 2),
            ],
            'diff_only_in_new_profit_sum' => $diff_profit,
            'orders_in_new_criteria' => $orders_new,
            'orders_in_old_criteria' => $orders_old,
            'order_ids_only_in_new' => $only_new,
            'order_ids_only_in_old' => $only_old,
            'orders_only_in_new_detail' => $only_new_rows,
        ]);
    }

    /**
     * 导出：业务人员业绩表（多 Sheet）。
     */
    public function exportBusinessPerformance()
    {
        try {
            if (!$this->isSpreadsheetExportReady()) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $username = trim((string)Request::param('username', ''));

            $exportData = $this->collectBusinessPerformanceExportData($timebucket, $at_time, $month_keys, $username);
            $totalRows = count($exportData['summary_rows']) + count($exportData['detail_rows']) + count($exportData['raw_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = $this->buildMultiSheetSpreadsheet([
                [
                    'title' => '业务员业绩汇总',
                    'headers' => ['排名', '业务员', '团队', '订单数', '利润', '总金额', '利润率(%)'],
                    'rows' => $exportData['summary_rows'],
                ],
                [
                    'title' => '业务员明细数据',
                    'headers' => ['业务员', '团队', '订单数', '利润', '总金额', '利润率(%)'],
                    'rows' => $exportData['detail_rows'],
                ],
                [
                    'title' => '原始订单明细',
                    'headers' => ['订单ID', '订单号', '下单时间', '客户名称', '业务员', '团队', '订单金额', '利润', '审核状态'],
                    'rows' => $exportData['raw_rows'],
                ],
            ]);

            $this->outputSpreadsheet($spreadsheet, '业务人员业绩表', 'business_performance');
        } catch (\Throwable $e) {
            \think\facade\Log::error('[BusinessPerformance] exportBusinessPerformance failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    // =========================================================
    // 团队业绩表
    // =========================================================

    /**
     * 获取【团队】业绩数据
     * 统计口径：统一订单口径（crm_client_order，check_status=2，order_time）
     * 团队名称：只使用订单表 crm_client_order.team_name（订单快照团队）
     * 人员范围：全量订单数据（不按当前登录人收口）
     */
    public function getTeamPerformanceData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        $emptySummary = ['total_profit' => '0.00', 'total_money' => '0.00', 'profit_rate' => '0.00'];

        try {
            $rows = $this->buildPerformanceOrderQuery($timebucket, $at_time, [], '', $month_keys)
                ->field('team_name, SUM(profit) as total_profit, SUM(money) as total_money')
                ->group('team_name')
                ->order('total_profit desc')
                ->select();

            $result = [];
            $sum_profit = 0;
            $sum_money  = 0;
            foreach ($rows as $r) {
                $profit = (float)($r['total_profit'] ?: 0);
                $money  = (float)($r['total_money'] ?: 0);
                $sum_profit += $profit;
                $sum_money  += $money;

                $rate = $money > 0 ? round($profit / $money * 100, 2) : 0;
                $result[] = [
                    'team_name'   => ($r['team_name'] ?? '') !== '' ? $r['team_name'] : '未分组',
                    'profit'      => number_format($profit, 2),
                    'total_money' => number_format($money, 2),
                    'profit_rate' => number_format($rate, 2),
                ];
            }

            $rank = 1;
            foreach ($result as &$item) {
                $item['rank'] = $rank++;
            }
            unset($item);

            $sum_rate = $sum_money > 0 ? round($sum_profit / $sum_money * 100, 2) : 0;
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => [
                    'total_profit' => number_format($sum_profit, 2),
                    'total_money'  => number_format($sum_money, 2),
                    'profit_rate'  => number_format($sum_rate, 2),
                ]
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[Performance] getTeamPerformanceData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取团队业绩数据失败：' . $e->getMessage(),
                'data' => [],
                'summary' => $emptySummary
            ]);
        }
    }

    /**
     * 团队成员业绩排行页面
     */
    public function teamMemberPerformancePage()
    {
        $team_name  = Request::param('team_name', '');
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        $this->assign('team_name',  $team_name);
        $this->assign('timebucket', $timebucket);
        $this->assign('at_time',    $at_time);
        $this->assign('month_keys', $month_keys);

        return $this->fetch('team_member_performance_table');
    }

    /**
     * 获取团队成员业绩数据
     * 返回指定团队下业务员的业绩排行数据
     */
    public function getTeamMemberPerformanceData()
    {
        $team_name_raw = Request::param('team_name', '');
        $team_name  = trim((string)$team_name_raw);
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        $emptySummary = ['total_profit' => '0.00', 'total_money' => '0.00', 'profit_rate' => '0.00'];

        try {
            $is_ungrouped = ($team_name === '' || $team_name === '未分组');
            $team_usernames_query = Db::name('admin');
            if ($is_ungrouped) {
                $team_usernames_query->where(function ($q) {
                    $q->whereNull('team_name')->whereOr('team_name', '=', '');
                });
            } else {
                $team_usernames_query->where('team_name', '=', $team_name);
            }
            $team_usernames = $team_usernames_query->column('username');
            $team_usernames = $team_usernames ? array_values(array_filter($team_usernames)) : [];

            if (empty($team_usernames)) {
                return json([
                    'code' => 200,
                    'msg' => 'ok',
                    'data' => [],
                    'summary' => $emptySummary
                ]);
            }

            $order_stats = $this->buildPerformanceOrderQuery($timebucket, $at_time, [], '', $month_keys)
                ->whereIn('pr_user', $team_usernames)
                ->field('pr_user, SUM(profit) as total_profit, SUM(money) as total_money')
                ->group('pr_user')
                ->order('total_profit desc')
                ->select();

            $result = [];
            $sum_profit = 0.0;
            $sum_money  = 0.0;
            $hasUser = [];
            foreach ($order_stats as $stat) {
                $u = (string)($stat['pr_user'] ?? '');
                if ($u === '') {
                    continue;
                }
                $profit = (float)($stat['total_profit'] ?: 0);
                $money  = (float)($stat['total_money'] ?: 0);
                $sum_profit += $profit;
                $sum_money  += $money;
                $hasUser[$u] = true;
                $rate = $money > 0 ? round($profit / $money * 100, 2) : 0;
                $result[] = [
                    'username'    => $u,
                    'profit'      => number_format($profit, 2),
                    'total_money' => number_format($money, 2),
                    'profit_rate' => number_format($rate, 2),
                ];
            }

            foreach ($team_usernames as $u) {
                $u = (string)$u;
                if ($u === '' || isset($hasUser[$u])) {
                    continue;
                }
                $result[] = [
                    'username'    => $u,
                    'profit'      => number_format(0, 2),
                    'total_money' => number_format(0, 2),
                    'profit_rate' => number_format(0, 2),
                ];
            }

            usort($result, function ($a, $b) {
                $profit_a = floatval(str_replace(',', '', $a['profit']));
                $profit_b = floatval(str_replace(',', '', $b['profit']));
                return $profit_b <=> $profit_a;
            });

            $rank = 1;
            foreach ($result as &$item) {
                $item['rank'] = $rank++;
            }
            unset($item);

            $sum_rate = $sum_money > 0 ? round($sum_profit / $sum_money * 100, 2) : 0;
            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => $result,
                'summary' => [
                    'total_profit' => number_format($sum_profit, 2),
                    'total_money'  => number_format($sum_money, 2),
                    'profit_rate'  => number_format($sum_rate, 2),
                ]
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[Performance] getTeamMemberPerformanceData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '获取团队成员业绩数据失败：' . $e->getMessage(),
                'data' => [],
                'summary' => $emptySummary
            ]);
        }
    }

    /**
     * 获取指定业务员的订单明细（个人业绩清单）
     */
    public function getMemberPerformanceDetail()
    {
        $username   = Request::param('username', '');
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        if (empty($username)) {
            return json([
                'code' => 200,
                'msg'  => 'ok',
                'data' => [],
                'summary' => ['total_money' => '0.00', 'total_profit' => '0.00']
            ]);
        }

        $extra  = ['pr_user' => $username];
        $orders = $this->buildPerformanceOrderQuery($timebucket, $at_time, $extra, '', $month_keys)
            ->field('id,order_time,cname,money,profit')
            ->order('order_time desc,id desc')
            ->limit(1000)
            ->select();

        $result     = [];
        $sum_money  = 0;
        $sum_profit = 0;
        foreach ($orders as $order) {
            $money_val  = (float)($order['money'] ?: 0);
            $profit_val = (float)($order['profit'] ?: 0);
            $sum_money  += $money_val;
            $sum_profit += $profit_val;

            $result[] = [
                'order_time' => $order['order_time'] ?: '',
                'cname'      => $order['cname'] ?: '',
                'money'      => number_format($money_val, 2),
                'profit'     => number_format($profit_val, 2),
            ];
        }

        return json([
            'code' => 200,
            'msg'  => 'ok',
            'data' => $result,
            'summary' => [
                'total_money'  => number_format($sum_money, 2),
                'total_profit' => number_format($sum_profit, 2),
            ]
        ]);
    }

    /**
     * 导出：团队业绩表（多 Sheet）。
     */
    public function exportTeamPerformanceSummary()
    {
        try {
            if (!$this->isSpreadsheetExportReady()) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time    = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $team_name  = trim((string)Request::param('team_name', ''));
            $username   = trim((string)Request::param('username', ''));

            $exportData = $this->collectTeamPerformanceExportData($timebucket, $at_time, $month_keys, $team_name, $username);
            $totalRows  = count($exportData['team_rows']) + count($exportData['member_rows']) + count($exportData['detail_rows']) + count($exportData['raw_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = $this->buildMultiSheetSpreadsheet([
                [
                    'title'   => '团队业绩汇总',
                    'headers' => ['排名', '团队名称', '订单数', '总利润', '总金额', '利润率(%)'],
                    'rows'    => $exportData['team_rows'],
                ],
                [
                    'title'   => '团队成员业绩汇总',
                    'headers' => ['团队名称', '排名', '成员', '订单数', '总利润', '总金额', '利润率(%)'],
                    'rows'    => $exportData['member_rows'],
                ],
                [
                    'title'   => '个人业绩明细',
                    'headers' => ['团队', '成员', '订单数', '总利润', '总金额', '利润率(%)', '最近下单时间'],
                    'rows'    => $exportData['detail_rows'],
                ],
                [
                    'title'   => '原始订单明细',
                    'headers' => ['订单ID', '订单号', '下单时间', '客户名称', '业务员', '团队', '订单金额', '利润', '审核状态'],
                    'rows'    => $exportData['raw_rows'],
                ],
            ]);

            $this->outputSpreadsheet($spreadsheet, '团队业绩表', 'team_performance');
        } catch (\Throwable $e) {
            \think\facade\Log::error('[TeamPerformance] exportTeamPerformanceSummary failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    // =========================================================
    // 业务人员业绩表 —— 私有辅助方法
    // =========================================================

    private function buildOrderListAlignedOrderWhere(string $timebucket, string $at_time, string $filterPrUser = '', string $month_keys = ''): array
    {
        $where = [];
        $where[] = ['check_status', '=', 2];

        // 时间优先级统一：month_keys > at_time > timebucket > month
        $where[] = function ($query) use ($timebucket, $at_time, $month_keys) {
            $this->applyMonthShortcutTimeFilterSafe($query, 'order_time', $timebucket, $at_time, $month_keys);
        };

        if ($filterPrUser !== '') {
            $where[] = ['pr_user', '=', $filterPrUser];
        }

        return $where;
    }

    private function applyPerformanceWhereToQuery($query, array $where)
    {
        foreach ($where as $condition) {
            if (is_callable($condition)) {
                $query->where($condition);
                continue;
            }

            if (!is_array($condition)) {
                continue;
            }

            // 兼容 buildTimeWhere 可能返回的嵌套数组格式
            if (isset($condition[0]) && is_array($condition[0]) && isset($condition[0][0])) {
                foreach ($condition as $sub) {
                    if (is_array($sub) && isset($sub[0])) {
                        $query->where($sub[0], $sub[1] ?? '=', $sub[2] ?? null);
                    }
                }
                continue;
            }

            if (isset($condition[0])) {
                $query->where($condition[0], $condition[1] ?? '=', $condition[2] ?? null);
            }
        }

        return $query;
    }

    private function buildPerformanceOrderWhere(string $timebucket = '', string $at_time = '', array $extra = [], string $fieldPrefix = '', string $month_keys = ''): array
    {
        $prefix = $fieldPrefix ? rtrim($fieldPrefix, '.') . '.' : '';
        $where = [];
        $where[] = [$prefix . 'check_status', '=', 2];
        // 时间优先级统一：month_keys > at_time > timebucket > month
        $where[] = function ($query) use ($prefix, $timebucket, $at_time, $month_keys) {
            $this->applyMonthShortcutTimeFilterSafe($query, $prefix . 'order_time', $timebucket, $at_time, $month_keys);
        };

        foreach ($extra as $k => $v) {
            if (is_int($k) && is_array($v)) {
                $where[] = $v;
            } else {
                $where[] = [$prefix . $k, '=', $v];
            }
        }

        return $where;
    }

    private function buildPerformanceOrderQuery(string $timebucket = '', string $at_time = '', array $extra = [], string $alias = '', string $month_keys = '')
    {
        $query = $alias === '' ? Db::table('crm_client_order') : Db::table('crm_client_order')->alias($alias);
        $where = $this->buildPerformanceOrderWhere($timebucket, $at_time, $extra, $alias, $month_keys);
        return $this->applyPerformanceWhereToQuery($query, $where);
    }

    private function parseMonthKeysToRanges(string $month_keys = ''): array
    {
        $month_keys = trim($month_keys);
        if ($month_keys === '') {
            return [];
        }

        $items = explode(',', $month_keys);
        $ranges = [];
        $seen = [];
        foreach ($items as $item) {
            $monthKey = trim((string)$item);
            if ($monthKey === '' || isset($seen[$monthKey])) {
                continue;
            }
            if (!preg_match('/^\d{4}-(0[1-9]|1[0-2])$/', $monthKey)) {
                continue;
            }
            $monthStartTs = strtotime($monthKey . '-01 00:00:00');
            if ($monthStartTs === false) {
                continue;
            }
            $ranges[] = [
                'key' => $monthKey,
                'start' => date('Y-m-01 00:00:00', $monthStartTs),
                'end' => date('Y-m-t 23:59:59', $monthStartTs),
            ];
            $seen[$monthKey] = true;
        }

        return $ranges;
    }

    /**
     * 通用：解析 at_time 为 [startDate, endDate]，支持 "YYYY-MM-DD,YYYY-MM-DD" 与 "YYYY-MM-DD - YYYY-MM-DD"。
     */
    private function parseCustomDateRange(string $at_time = ''): array
    {
        $at_time = trim((string)$at_time);
        if ($at_time === '') {
            return [];
        }

        $startDate = '';
        $endDate = '';
        if (strpos($at_time, ',') !== false) {
            $dateParts = explode(',', $at_time, 2);
            $startDate = trim((string)($dateParts[0] ?? ''));
            $endDate = trim((string)($dateParts[1] ?? ''));
        } elseif (strpos($at_time, ' - ') !== false) {
            $dateParts = explode(' - ', $at_time, 2);
            $startDate = trim((string)($dateParts[0] ?? ''));
            $endDate = trim((string)($dateParts[1] ?? ''));
        }

        $isValidDate = function ($date) {
            $dt = \DateTime::createFromFormat('Y-m-d', (string)$date);
            return $dt && $dt->format('Y-m-d') === $date;
        };

        if ($startDate === '' || $endDate === '' || !$isValidDate($startDate) || !$isValidDate($endDate)) {
            return [];
        }
        if ($startDate > $endDate) {
            $tmp = $startDate;
            $startDate = $endDate;
            $endDate = $tmp;
        }

        return [$startDate, $endDate];
    }

    /**
     * 统一格式化可安全拼接到 whereRaw 的日期字段表达式。
     * 仅允许 `field` 或 `alias.field` 形态，避免触发框架字段元数据解析（如 SHOW COLUMNS FROM o）。
     */
    private function normalizeSafeDateFieldExpression(string $dateField, string $fallbackField = 'order_time'): string
    {
        $dateField = trim((string)$dateField);
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/', $dateField)) {
            return $dateField;
        }
        return $fallbackField;
    }

    /**
     * 安全追加单段时间范围过滤（包含边界）。
     */
    private function applySafeDateRangeWhereRaw($query, string $fieldExpr, string $startAt, string $endAt)
    {
        // 避免 whereRaw 占位符绑定错位，统一走构造器参数绑定
        $query->where($fieldExpr, '>=', $startAt)->where($fieldExpr, '<=', $endAt);
        return $query;
    }

    /**
     * timebucket 转换为 [startDatetime, endDatetime]，统一补齐 00:00:00 / 23:59:59。
     */
    private function resolveTimebucketDateRange(string $timebucket = ''): array
    {
        $bucket = strtolower(trim((string)$timebucket));
        if ($bucket === '' || $bucket === 'custom') {
            $bucket = 'month';
        }

        switch ($bucket) {
            case 'today':
                $startTs = strtotime(date('Y-m-d') . ' 00:00:00');
                $endTs = strtotime(date('Y-m-d') . ' 23:59:59');
                break;
            case 'yesterday':
                $startTs = strtotime(date('Y-m-d', strtotime('-1 day')) . ' 00:00:00');
                $endTs = strtotime(date('Y-m-d', strtotime('-1 day')) . ' 23:59:59');
                break;
            case 'week':
                $startTs = strtotime('monday this week');
                $endTs = strtotime('sunday this week');
                if ($startTs !== false) {
                    $startTs = strtotime(date('Y-m-d', $startTs) . ' 00:00:00');
                }
                if ($endTs !== false) {
                    $endTs = strtotime(date('Y-m-d', $endTs) . ' 23:59:59');
                }
                break;
            case 'year':
                $year = date('Y');
                $startTs = strtotime($year . '-01-01 00:00:00');
                $endTs = strtotime($year . '-12-31 23:59:59');
                break;
            case 'last month':
            case 'last_month':
                $monthTs = strtotime(date('Y-m-01', strtotime('-1 month')) . ' 00:00:00');
                $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
                $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
                break;
            case 'month':
            default:
                $monthTs = strtotime(date('Y-m-01') . ' 00:00:00');
                $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
                $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
                break;
        }

        if ($startTs === false || $endTs === false) {
            $monthTs = strtotime(date('Y-m-01') . ' 00:00:00');
            $startTs = strtotime(date('Y-m-01 00:00:00', $monthTs));
            $endTs = strtotime(date('Y-m-t 23:59:59', $monthTs));
        }

        return [
            'start' => date('Y-m-d H:i:s', $startTs),
            'end' => date('Y-m-d H:i:s', $endTs),
        ];
    }

    /**
     * 通用：时间筛选优先级 month_keys > at_time > timebucket > month。
     * 仅使用 whereRaw + 参数绑定，兼容带表别名字段（order_time/o.order_time/l.at_time/src.at_time...）。
     */
    private function applyMonthShortcutTimeFilterSafe($query, string $dateField, string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        $fieldExpr = $this->normalizeSafeDateFieldExpression($dateField, 'order_time');

        $monthRanges = $this->parseMonthKeysToRanges($month_keys);
        if (!empty($monthRanges)) {
            $query->where(function ($monthOrQuery) use ($monthRanges, $fieldExpr) {
                foreach ($monthRanges as $idx => $range) {
                    if ($idx === 0) {
                        $monthOrQuery->where(function ($subQuery) use ($fieldExpr, $range) {
                            $subQuery->where($fieldExpr, '>=', $range['start'])->where($fieldExpr, '<=', $range['end']);
                        });
                    } else {
                        $monthOrQuery->whereOr(function ($subQuery) use ($fieldExpr, $range) {
                            $subQuery->where($fieldExpr, '>=', $range['start'])->where($fieldExpr, '<=', $range['end']);
                        });
                    }
                }
            });
            return $query;
        }

        $dateRange = $this->parseCustomDateRange($at_time);
        if (!empty($dateRange)) {
            return $this->applySafeDateRangeWhereRaw($query, $fieldExpr, $dateRange[0] . ' 00:00:00', $dateRange[1] . ' 23:59:59');
        }

        $bucketRange = $this->resolveTimebucketDateRange($timebucket);
        return $this->applySafeDateRangeWhereRaw($query, $fieldExpr, $bucketRange['start'], $bucketRange['end']);
    }

    /**
     * 通用 Sheet 写入（首行为表头）。
     */
    private function fillExportSheet($sheet, array $headers, array $rows): void
    {
        foreach ($headers as $colIdx => $header) {
            $sheet->setCellValueByColumnAndRow($colIdx + 1, 1, (string)$header);
        }

        $rowNum = 2;
        foreach ($rows as $row) {
            $colNum = 1;
            foreach ((array)$row as $cellVal) {
                if (is_int($cellVal) || is_float($cellVal)) {
                    $sheet->setCellValueByColumnAndRow($colNum, $rowNum, $cellVal);
                } else {
                    $sheet->setCellValueByColumnAndRow($colNum, $rowNum, (string)$cellVal);
                }
                $colNum++;
            }
            $rowNum++;
        }

        $colCount = count($headers);
        for ($i = 1; $i <= $colCount; $i++) {
            $colLetter = \PhpOffice\PhpSpreadsheet\Cell\Coordinate::stringFromColumnIndex($i);
            $sheet->getColumnDimension($colLetter)->setAutoSize(true);
        }
    }

    /**
     * 检查 PhpSpreadsheet 依赖。
     */
    private function isSpreadsheetExportReady(): bool
    {
        return class_exists('\PhpOffice\PhpSpreadsheet\Spreadsheet')
            && class_exists('\PhpOffice\PhpSpreadsheet\Writer\Xlsx');
    }

    /**
     * 统一按多 Sheet 结构创建 Spreadsheet。
     *
     * @param array $sheets
     */
    private function buildMultiSheetSpreadsheet(array $sheets)
    {
        $spreadsheet = new \PhpOffice\PhpSpreadsheet\Spreadsheet();
        $sheetIndex = 0;
        foreach ($sheets as $sheetDef) {
            $sheet = $sheetIndex === 0 ? $spreadsheet->getActiveSheet() : $spreadsheet->createSheet();
            $title = isset($sheetDef['title']) ? trim((string)$sheetDef['title']) : ('Sheet' . ($sheetIndex + 1));
            if ($title === '') {
                $title = 'Sheet' . ($sheetIndex + 1);
            }
            $sheet->setTitle(mb_substr($title, 0, 31));
            $headers = isset($sheetDef['headers']) && is_array($sheetDef['headers']) ? $sheetDef['headers'] : [];
            $rows = isset($sheetDef['rows']) && is_array($sheetDef['rows']) ? $sheetDef['rows'] : [];
            $this->fillExportSheet($sheet, $headers, $rows);
            $sheetIndex++;
        }
        $spreadsheet->setActiveSheetIndex(0);
        return $spreadsheet;
    }

    /**
     * 统一输出 Excel 下载流。
     */
    private function outputSpreadsheet($spreadsheet, string $displayNamePrefix, string $asciiPrefix = 'export'): void
    {
        $fileName = $displayNamePrefix . '_' . date('Ymd_His') . '.xlsx';
        $safeAsciiPrefix = preg_replace('/[^a-zA-Z0-9_]+/', '_', $asciiPrefix);
        $safeAsciiPrefix = trim((string)$safeAsciiPrefix, '_');
        if ($safeAsciiPrefix === '') {
            $safeAsciiPrefix = 'export';
        }
        $asciiFileName = $safeAsciiPrefix . '_' . date('Ymd_His') . '.xlsx';
        $encodedFileName = rawurlencode($fileName);

        while (ob_get_level() > 0) {
            @ob_end_clean();
        }

        header('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        header("Content-Disposition: attachment; filename=\"{$asciiFileName}\"; filename*=UTF-8''{$encodedFileName}");
        header('Cache-Control: max-age=0');
        header('Pragma: public');

        $writer = new \PhpOffice\PhpSpreadsheet\Writer\Xlsx($spreadsheet);
        $writer->save('php://output');
        exit;
    }

    /**
     * 组装导出数据：团队业绩表。
     */
    private function collectTeamPerformanceExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', string $team_name = '', string $username = ''): array
    {
        $teamName  = trim((string)$team_name);
        $username  = trim((string)$username);
        $teamExpr  = "IFNULL(NULLIF(TRIM(team_name), ''), '未分组')";
        $baseQuery = $this->buildPerformanceOrderQuery($timebucket, $at_time, [], '', $month_keys);
        if ($teamName !== '') {
            $normTeam = $teamName === '未分组' ? '未分组' : $teamName;
            $baseQuery->whereRaw($teamExpr . " = :team_name", ['team_name' => $normTeam]);
        }
        if ($username !== '') {
            $baseQuery->where('pr_user', '=', $username);
        }

        $teamRowsRaw = (clone $baseQuery)
            ->field($teamExpr . " as team_name, COUNT(id) as order_count, SUM(profit) as total_profit, SUM(money) as total_money")
            ->group($teamExpr)
            ->order('total_profit desc, team_name asc')
            ->select();
        $teamRows = [];
        foreach ((array)$teamRowsRaw as $idx => $row) {
            $profit = (float)($row['total_profit'] ?? 0);
            $money  = (float)($row['total_money'] ?? 0);
            $rate   = $money > 0 ? round(($profit / $money) * 100, 2) : 0.0;
            $teamRows[] = [
                $idx + 1,
                (string)($row['team_name'] ?? '未分组'),
                (int)($row['order_count'] ?? 0),
                number_format($profit, 2, '.', ''),
                number_format($money, 2, '.', ''),
                number_format($rate, 2, '.', ''),
            ];
        }

        $memberRowsRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, TRIM(pr_user) as username, COUNT(id) as order_count, SUM(profit) as total_profit, SUM(money) as total_money")
            ->group($teamExpr . ',TRIM(pr_user)')
            ->order('team_name asc, total_profit desc, username asc')
            ->select();
        $memberRows       = [];
        $memberDetailRows = [];
        $rankByTeam       = [];
        foreach ((array)$memberRowsRaw as $row) {
            $team = (string)($row['team_name'] ?? '未分组');
            if (!isset($rankByTeam[$team])) {
                $rankByTeam[$team] = 0;
            }
            $rankByTeam[$team]++;
            $profit = (float)($row['total_profit'] ?? 0);
            $money  = (float)($row['total_money'] ?? 0);
            $rate   = $money > 0 ? round(($profit / $money) * 100, 2) : 0.0;
            $memberRows[] = [
                $team,
                $rankByTeam[$team],
                (string)($row['username'] ?? ''),
                (int)($row['order_count'] ?? 0),
                number_format($profit, 2, '.', ''),
                number_format($money, 2, '.', ''),
                number_format($rate, 2, '.', ''),
            ];
        }

        $detailRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, TRIM(pr_user) as username, COUNT(id) as order_count, SUM(profit) as total_profit, SUM(money) as total_money, MAX(order_time) as latest_order_time")
            ->group($teamExpr . ',TRIM(pr_user)')
            ->order('team_name asc, total_profit desc, username asc')
            ->select();
        foreach ((array)$detailRaw as $row) {
            $profit = (float)($row['total_profit'] ?? 0);
            $money  = (float)($row['total_money'] ?? 0);
            $rate   = $money > 0 ? round(($profit / $money) * 100, 2) : 0.0;
            $memberDetailRows[] = [
                (string)($row['team_name'] ?? '未分组'),
                (string)($row['username'] ?? ''),
                (int)($row['order_count'] ?? 0),
                number_format($profit, 2, '.', ''),
                number_format($money, 2, '.', ''),
                number_format($rate, 2, '.', ''),
                (string)($row['latest_order_time'] ?? ''),
            ];
        }

        $rawOrderRows = (clone $baseQuery)
            ->fieldRaw("id,order_no,order_time,cname,TRIM(pr_user) as pr_user,{$teamExpr} as team_name,money,profit,check_status")
            ->order('order_time desc,id desc')
            ->limit(20000)
            ->select();
        $rawRows = [];
        foreach ((array)$rawOrderRows as $row) {
            $rawRows[] = [
                (int)($row['id'] ?? 0),
                (string)($row['order_no'] ?? ''),
                (string)($row['order_time'] ?? ''),
                (string)($row['cname'] ?? ''),
                (string)($row['pr_user'] ?? ''),
                (string)($row['team_name'] ?? '未分组'),
                number_format((float)($row['money'] ?? 0), 2, '.', ''),
                number_format((float)($row['profit'] ?? 0), 2, '.', ''),
                (int)($row['check_status'] ?? 0),
            ];
        }

        return [
            'team_rows'   => $teamRows,
            'member_rows' => $memberRows,
            'detail_rows' => $memberDetailRows,
            'raw_rows'    => $rawRows,
        ];
    }

    // =========================================================
    // 业务询盘汇总表（三连屏）
    // =========================================================

    /**
     * 第一屏：团队询盘汇总（复用业务询盘口径）。
     */
    public function getInquiryTeamSummaryData()
    {
        try {
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            $normalizedTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys);
            $baseTotal = (int)(clone $baseQuery)->count();

            \think\facade\Log::info('[InquirySummaryDebug] params=' . json_encode([
                'timebucket' => $timebucket,
                'at_time' => $at_time,
                'excludedTeams' => [],
                'excludedUsers' => [],
            ], JSON_UNESCAPED_UNICODE));
            \think\facade\Log::info('[InquirySummaryDebug] base_total=' . $baseTotal);

            $query = (clone $baseQuery)->leftJoin('admin a', 'l.pr_user = a.username');
            $rawTotal = (int)(clone $query)->count();
            \think\facade\Log::info('[InquirySummaryDebug] raw_total_before_excludes=' . $rawTotal);
            \think\facade\Log::info('[InquirySummaryDebug] after_exclude_team_total=' . $rawTotal);
            \think\facade\Log::info('[InquirySummaryDebug] after_excludes_total=' . $rawTotal);

            $rows = $query
                ->group($normalizedTeamExpr)
                ->field($normalizedTeamExpr . ' as team_name,count(*) as yw_num')
                ->order('yw_num desc')
                ->order('team_name')
                ->select();

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $result[] = [
                    'rank' => $idx + 1,
                    'team_name' => trim((string)($row['team_name'] ?? '')) !== '' ? $row['team_name'] : '未分组',
                    'yw_num' => $count,
                ];
            }
            \think\facade\Log::info('[InquirySummaryDebug] grouped_total=' . $total);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryTeamSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第二屏顶部：团队来源汇总（复用第一屏团队询盘统计口径）。
     */
    public function getInquiryTeamSourceSummaryData()
    {
        try {
            $team_name = trim((string)Request::param('team_name', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            if ($team_name === '') {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => [
                        'team_name' => '',
                        'total_count' => 0,
                    ],
                ]);
            }

            $sourceSummary = $this->buildInquiryTeamSourceSummaryRows($timebucket, $at_time, $month_keys, $team_name);
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $sourceSummary['rows'],
                'summary' => $sourceSummary['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryTeamSourceSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队来源汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => [
                    'team_name' => trim((string)Request::param('team_name', '')),
                    'total_count' => 0,
                ],
            ]);
        }
    }

    /**
     * 临时调试接口：对比"客户列表真实总数"与"询盘汇总基础集总数"，并给出 id 差异样本（最多20条）
     */
    public function debugInquirySummaryCompare()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');

        $keyword = [];
        if ($timebucket !== '') {
            $keyword['timebucket'] = $this->buildTimeWhere($timebucket, 'at_time');
        }
        if ($at_time !== '') {
            $keyword['timebucket'] = $this->buildTimeWhere($at_time, 'at_time');
        }

        $clientList = model('Client')->getClientSearchListAll(1, 1, $keyword);
        $clientTotal = (int)($clientList['total'] ?? 0);

        $inquiryBaseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time);
        $inquiryBaseTotal = (int)(clone $inquiryBaseQuery)->count();

        $clientFinalIdSql = model('Client')->buildClientSearchListAllFinalIdQuery($keyword)->field('l.id')->buildSql();
        $inquiryFinalIdSql = (clone $inquiryBaseQuery)->field('l.id')->buildSql();

        $onlyInClient = Db::table([$clientFinalIdSql => 'c'])
            ->leftJoin([$inquiryFinalIdSql => 'i'], 'c.id = i.id')
            ->whereNull('i.id')
            ->limit(20)
            ->column('c.id');

        $onlyInInquiry = Db::table([$inquiryFinalIdSql => 'i'])
            ->leftJoin([$clientFinalIdSql => 'c'], 'i.id = c.id')
            ->whereNull('c.id')
            ->limit(20)
            ->column('i.id');

        return json([
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'params' => ['timebucket' => $timebucket, 'at_time' => $at_time],
                'client_total' => $clientTotal,
                'inquiry_base_total' => $inquiryBaseTotal,
                'only_in_client_ids' => array_values(array_map('intval', (array)$onlyInClient)),
                'only_in_inquiry_ids' => array_values(array_map('intval', (array)$onlyInInquiry)),
            ],
        ]);
    }

    /**
     * 第二屏：团队下个人询盘汇总（复用业务询盘口径）。
     */
    public function getInquiryMemberSummaryData()
    {
        try {
            $team_name = trim((string)Request::param('team_name', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            if ($team_name === '') {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys);
            $query = (clone $baseQuery)
                ->leftJoin('admin a', 'l.pr_user = a.username');

            if ($team_name === '未分组') {
                $query->whereRaw("(a.username IS NULL OR a.team_name IS NULL OR TRIM(a.team_name) = '')");
            } else {
                $query->whereRaw("TRIM(a.team_name) = :team_name", ['team_name' => $team_name]);
            }

            $rows = $query
                ->group('l.pr_user')
                ->field('l.pr_user as username,count(distinct l.id) as yw_num')
                ->order('yw_num desc')
                ->order('username')
                ->select();

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $result[] = [
                    'rank' => $idx + 1,
                    'username' => (string)($row['username'] ?? ''),
                    'team_name' => $team_name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryMemberSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '成员汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：个人询盘渠道分类汇总（优先 inquiry_id 关联渠道名）。
     */
    public function getInquiryChannelSummaryData()
    {
        try {
            $username = trim((string)Request::param('username', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            if ($username === '') {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys);
            $rows = (clone $baseQuery)
                ->where('l.pr_user', '=', $username)
                ->group('l.inquiry_id')
                ->field('l.inquiry_id,count(distinct l.id) as yw_num')
                ->order('yw_num desc')
                ->select();

            $inquiry_ids = [];
            foreach ($rows as $row) {
                $iid = (int)($row['inquiry_id'] ?? 0);
                if ($iid > 0) {
                    $inquiry_ids[] = $iid;
                }
            }
            $inquiry_ids = array_values(array_unique($inquiry_ids));
            $inquiry_map = [];
            if (!empty($inquiry_ids)) {
                $inquiry_map = Db::table('crm_inquiry')->where('id', 'in', $inquiry_ids)->column('inquiry_name', 'id');
            }

            $result = [];
            $total = 0;
            foreach ($rows as $idx => $row) {
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                $iid = (int)($row['inquiry_id'] ?? 0);
                if ($iid > 0 && !empty($inquiry_map[$iid])) {
                    $channel_name = $inquiry_map[$iid];
                } elseif ($iid <= 0) {
                    $channel_name = '未分类';
                } else {
                    $channel_name = '其他';
                }
                $result[] = [
                    'rank' => $idx + 1,
                    'channel_name' => $channel_name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] getInquiryChannelSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '渠道汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 导出：业务询盘汇总表三连屏完整数据（多 Sheet）。
     */
    public function exportInquirySummary()
    {
        try {
            if (!class_exists('\PhpOffice\PhpSpreadsheet\Spreadsheet') || !class_exists('\PhpOffice\PhpSpreadsheet\Writer\Xlsx')) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $team_name = trim((string)Request::param('team_name', ''));

            $exportData = $this->collectInquirySummaryExportData($timebucket, $at_time, $month_keys, $team_name);
            $totalRows = count($exportData['team_rows']) + count($exportData['member_rows']) + count($exportData['channel_rows']) + count($exportData['raw_rows']) + count($exportData['team_source_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = new \PhpOffice\PhpSpreadsheet\Spreadsheet();

            $teamSheet = $spreadsheet->getActiveSheet();
            $teamSheet->setTitle('团队询盘汇总');
            $teamMatrix = [];
            foreach ($exportData['team_rows'] as $row) {
                $teamMatrix[] = [
                    (int)($row['rank'] ?? 0),
                    (string)($row['team_name'] ?? ''),
                    (int)($row['yw_num'] ?? 0),
                ];
            }
            $this->fillInquirySummaryExportSheet($teamSheet, ['排名', '团队名称', '询盘数量'], $teamMatrix);

            $memberSheet = $spreadsheet->createSheet();
            $memberSheet->setTitle('成员询盘汇总');
            $memberMatrix = [];
            foreach ($exportData['member_rows'] as $row) {
                $memberMatrix[] = [
                    (string)($row['team_name'] ?? ''),
                    (int)($row['rank'] ?? 0),
                    (string)($row['username'] ?? ''),
                    (int)($row['yw_num'] ?? 0),
                ];
            }
            $this->fillInquirySummaryExportSheet($memberSheet, ['团队名称', '排名', '业务员名称', '询盘数量'], $memberMatrix);

            $channelSheet = $spreadsheet->createSheet();
            $channelSheet->setTitle('渠道分类明细');
            $channelMatrix = [];
            foreach ($exportData['channel_rows'] as $row) {
                $channelMatrix[] = [
                    (string)($row['team_name'] ?? ''),
                    (string)($row['username'] ?? ''),
                    (int)($row['rank'] ?? 0),
                    (string)($row['channel_name'] ?? ''),
                    (int)($row['yw_num'] ?? 0),
                ];
            }
            $this->fillInquirySummaryExportSheet($channelSheet, ['团队名称', '业务员名称', '排名', '渠道名称', '询盘数量'], $channelMatrix);

            $rawSheet = $spreadsheet->createSheet();
            $rawSheet->setTitle('原始明细数据');
            $rawMatrix = [];
            foreach ($exportData['raw_rows'] as $row) {
                $rawMatrix[] = [
                    (int)($row['lead_id'] ?? 0),
                    (string)($row['kh_name'] ?? ''),
                    (string)($row['contact_text'] ?? ''),
                    (string)($row['username'] ?? ''),
                    (string)($row['team_name'] ?? ''),
                    (string)($row['channel_name'] ?? ''),
                    (string)($row['port_name'] ?? ''),
                    (string)($row['product_name'] ?? ''),
                    (string)($row['at_time'] ?? ''),
                    (string)($row['to_kh_time'] ?? ''),
                ];
            }
            $this->fillInquirySummaryExportSheet($rawSheet, ['客户ID', '客户名称', '手机号/电话', '负责人/业务员', '团队名称', '询盘来源', '运营端口', '产品名称', '录入时间', '转客户时间'], $rawMatrix);

            $teamSourceSheet = $spreadsheet->createSheet();
            $teamSourceSheet->setTitle('团队来源汇总');
            $teamSourceMatrix = [];
            foreach ($exportData['team_source_rows'] as $row) {
                $teamSourceMatrix[] = [
                    (string)($row['team_name'] ?? ''),
                    (string)($row['inquiry_name'] ?? '未知来源'),
                    (int)($row['yw_num'] ?? 0),
                ];
            }
            $this->fillInquirySummaryExportSheet($teamSourceSheet, ['团队名称', '渠道名称', '询盘数量'], $teamSourceMatrix);

            $spreadsheet->setActiveSheetIndex(0);

            $fileName = '业务询盘汇总_' . date('Ymd_His') . '.xlsx';
            $asciiFileName = 'inquiry_summary_' . date('Ymd_His') . '.xlsx';
            $encodedFileName = rawurlencode($fileName);

            while (ob_get_level() > 0) {
                @ob_end_clean();
            }

            header('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
            header("Content-Disposition: attachment; filename=\"{$asciiFileName}\"; filename*=UTF-8''{$encodedFileName}");
            header('Cache-Control: max-age=0');
            header('Pragma: public');

            $writer = new \PhpOffice\PhpSpreadsheet\Writer\Xlsx($spreadsheet);
            $writer->save('php://output');
            exit;
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquirySummary] exportInquirySummary failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    // =========================================================
    // 业务询盘汇总表 —— 私有辅助方法
    // =========================================================

    /**
     * 统一解析时间参数：与 buildPerformanceOrderWhere 保持同一规则。
     */
    private function resolveInquirySummaryTimeParams(string $timebucket = '', string $at_time = '', string $month_keys = ''): array
    {
        $month_ranges = $this->parseMonthKeysToRanges($month_keys);
        if (!empty($month_ranges)) {
            return [
                'is_month_keys' => true,
                'month_ranges' => $month_ranges,
                'is_custom' => false,
                'start_date' => '',
                'end_date' => '',
                'timebucket' => '',
            ];
        }

        $date_range = $this->parseCustomDateRange($at_time);
        if (!empty($date_range)) {
            return [
                'is_month_keys' => false,
                'month_ranges' => [],
                'is_custom' => true,
                'start_date' => $date_range[0],
                'end_date' => $date_range[1],
                'timebucket' => '',
            ];
        }

        return [
            'is_month_keys' => false,
            'month_ranges' => [],
            'is_custom' => false,
            'start_date' => '',
            'end_date' => '',
            'timebucket' => $timebucket !== '' ? $timebucket : 'month',
        ];
    }

    /**
     * 获取 crm_leads 字段列表（缓存）。
     */
    private function getInquirySummaryLeadsColumns(): array
    {
        static $cached = null;
        if ($cached !== null) {
            return $cached;
        }

        $fields = [];
        try {
            $columns = Db::query('SHOW COLUMNS FROM `crm_leads`');
            foreach ((array)$columns as $col) {
                if (!empty($col['Field'])) {
                    $fields[] = (string)$col['Field'];
                }
            }
        } catch (\Throwable $e) {
            $fields = [];
        }

        $cached = array_values(array_unique($fields));
        return $cached;
    }

    /**
     * 向后兼容：返回首选来源字段（source 优先，其次 xs_source）。
     */
    private function getInquirySummarySourceField(): string
    {
        $fields = $this->getInquirySummaryLeadsColumns();
        if (in_array('source', $fields, true)) {
            return 'source';
        }
        if (in_array('xs_source', $fields, true)) {
            return 'xs_source';
        }
        return '';
    }

    /**
     * 获取可用于"返单"过滤的来源字段集合（同时支持 source + xs_source）。
     */
    private function getInquirySummaryRepeatSourceFields(): array
    {
        $fields = $this->getInquirySummaryLeadsColumns();
        $out = [];
        if (in_array('source', $fields, true)) {
            $out[] = 'source';
        }
        if (in_array('xs_source', $fields, true)) {
            $out[] = 'xs_source';
        }
        return $out;
    }

    /**
     * 动态查询"返单"渠道 ID（crm_inquiry.inquiry_name='返单'）。
     */
    private function getInquirySummaryRepeatInquiryIds(): array
    {
        static $cached = null;
        if ($cached !== null) {
            return $cached;
        }

        try {
            $ids = Db::table('crm_inquiry')
                ->whereRaw("TRIM(IFNULL(inquiry_name, '')) = '返单'")
                ->column('id');
        } catch (\Throwable $e) {
            $ids = [];
        }

        $normalized = [];
        foreach ((array)$ids as $id) {
            $iid = (int)$id;
            if ($iid > 0) {
                $normalized[$iid] = $iid;
            }
        }
        $cached = array_values($normalized);
        return $cached;
    }

    /**
     * 统一应用单字段"排除返单"过滤（source/xs_source 可复用）。
     */
    private function applyInquirySummarySourceExclude($query, string $field, string $alias = '')
    {
        $field = trim($field);
        if ($field === '' || !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $field)) {
            return $query;
        }

        $column = $field;
        $alias = trim($alias);
        if ($alias !== '' && preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $alias)) {
            $column = $alias . '.' . $field;
        }

        $query->where(function ($q) use ($column) {
            $q->whereNull($column)
                ->whereOrRaw("TRIM(IFNULL({$column}, '')) <> '返单'");
        });

        return $query;
    }

    /**
     * 统一排除"返单"客户（三列统计共用）。
     */
    private function applyInquirySummaryExcludeRepeatLead($query, string $leadAlias = 'l')
    {
        $leadAlias = trim($leadAlias);
        if ($leadAlias === '' || !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $leadAlias)) {
            $leadAlias = 'l';
        }

        $sourceFields = $this->getInquirySummaryRepeatSourceFields();
        foreach ($sourceFields as $field) {
            $this->applyInquirySummarySourceExclude($query, $field, $leadAlias);
        }

        $columns = $this->getInquirySummaryLeadsColumns();
        $repeatInquiryIds = $this->getInquirySummaryRepeatInquiryIds();
        if (in_array('inquiry_id', $columns, true) && !empty($repeatInquiryIds)) {
            $inquiryColumn = $leadAlias . '.inquiry_id';
            $query->where(function ($q) use ($inquiryColumn, $repeatInquiryIds) {
                $q->whereNull($inquiryColumn)
                    ->whereOr($inquiryColumn, 'not in', $repeatInquiryIds);
            });
        }

        return $query;
    }

    /**
     * 业务询盘汇总三连屏：复用客户列表真实口径基础查询。
     * 公开口径：直接基于 crm_leads 构建，不再继承登录人数据权限。
     */
    private function buildInquirySummaryClientBaseQuery(string $timebucket = '', string $at_time = '', bool $excludeRepeatSource = true, string $month_keys = '')
    {
        $clientBaseQuery = Db::table('crm_leads')->alias('l');
        $leadColumns = $this->getInquirySummaryLeadsColumns();
        if (in_array('is_delete', $leadColumns, true)) {
            $clientBaseQuery->where('l.is_delete', '=', 0);
        }
        $this->applyMonthShortcutTimeFilterSafe($clientBaseQuery, 'l.at_time', $timebucket, $at_time, $month_keys);
        if ($excludeRepeatSource) {
            $this->applyInquirySummaryExcludeRepeatLead($clientBaseQuery, 'l');
        }

        $finalIdQuerySql = (clone $clientBaseQuery)
            ->leftJoin('crm_contacts c', "c.leads_id = l.id AND c.is_delete = 0 AND c.contact_type IN (1,3)")
            ->field(array_merge([
                'l.id',
                'l.pr_user',
                'l.inquiry_id',
                'l.port_id',
            ], in_array('oper_user', $leadColumns, true) ? ['l.oper_user'] : []))
            ->group('l.id')
            ->buildSql();

        $query = Db::table([$finalIdQuerySql => 'l']);
        return $query;
    }

    /**
     * 需要从「团队询盘汇总」排除的团队名称（第一屏/联动查询均生效）。
     */
    private function getExcludedInquiryTeamNames(): array
    {
        $items = [
            // '测试团队',
        ];

        $items = array_map(function ($v) {
            return trim((string)$v);
        }, $items);
        $items = array_filter($items, function ($v) {
            return $v !== '';
        });
        return array_values(array_unique($items));
    }

    /**
     * 需要从「团队询盘汇总」排除的业务员名称（第二屏/第三屏均生效，并且从第一屏统计口径扣除）。
     */
    private function getExcludedInquiryUsernames(): array
    {
        $items = [
            // '测试账号',
        ];

        $items = array_map(function ($v) {
            return trim((string)$v);
        }, $items);
        $items = array_filter($items, function ($v) {
            return $v !== '';
        });
        return array_values(array_unique($items));
    }

    /**
     * 统一应用团队/业务员排除条件（优先 SQL 层过滤）。
     */
    private function applyInquirySummaryExcludes($query, array $excludedTeams = [], array $excludedUsers = [], string $normalizedTeamExpr = '')
    {
        // 团队/成员排除用于固定业务口径筛选，不依赖当前登录人身份
        if (!empty($excludedTeams)) {
            if ($normalizedTeamExpr !== '') {
                $escaped = array_map(function ($v) {
                    return "'" . addslashes((string)$v) . "'";
                }, $excludedTeams);
                $inSql = implode(',', $escaped);
                $query->whereRaw("({$normalizedTeamExpr}) NOT IN ({$inSql})");
            } else {
                $query->where('a.team_name', 'not in', $excludedTeams);
            }
        }
        if (!empty($excludedUsers)) {
            $query->where('l.pr_user', 'not in', $excludedUsers);
        }
        return $query;
    }

    /**
     * 统一构建"团队来源汇总"数据（第二屏展示 + 导出第5个sheet共用）。
     */
    private function buildInquiryTeamSourceSummaryRows(string $timebucket = '', string $at_time = '', string $month_keys = '', string $team_name = ''): array
    {
        $team_name = trim($team_name);

        $normalizedTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";
        $inquiryNameExpr = "CASE WHEN ci.inquiry_name IS NULL OR TRIM(ci.inquiry_name) = '' THEN '未知来源' ELSE TRIM(ci.inquiry_name) END";

        $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys);
        $query = (clone $baseQuery)
            ->leftJoin('admin a', 'l.pr_user = a.username')
            ->leftJoin('crm_inquiry ci', 'l.inquiry_id = ci.id');
        if ($team_name !== '') {
            $query->whereRaw("{$normalizedTeamExpr} = :team_name", ['team_name' => $team_name]);
        }

        $rows = $query
            ->group($normalizedTeamExpr . ',l.inquiry_id,' . $inquiryNameExpr)
            ->field([
                $normalizedTeamExpr . ' as team_name',
                'l.inquiry_id',
                $inquiryNameExpr . ' as inquiry_name',
                'count(*) as yw_num',
            ])
            ->order('team_name asc')
            ->order('yw_num desc')
            ->order('inquiry_name asc')
            ->select();

        $result = [];
        $total = 0;
        $rankByTeam = [];
        foreach ($rows as $row) {
            $teamName = trim((string)($row['team_name'] ?? ''));
            if ($teamName === '') {
                $teamName = '未分组';
            }
            if (!isset($rankByTeam[$teamName])) {
                $rankByTeam[$teamName] = 0;
            }
            $rankByTeam[$teamName]++;

            $count = (int)($row['yw_num'] ?? 0);
            $total += $count;
            $result[] = [
                'team_name' => $teamName,
                'inquiry_id' => (int)($row['inquiry_id'] ?? 0),
                'inquiry_name' => trim((string)($row['inquiry_name'] ?? '')) !== '' ? (string)$row['inquiry_name'] : '未知来源',
                'yw_num' => $count,
                'rank' => $rankByTeam[$teamName],
            ];
        }

        return [
            'rows' => $result,
            'summary' => [
                'team_name' => $team_name,
                'total_count' => $total,
            ],
        ];
    }

    /**
     * 组装导出数据：业务询盘汇总表。
     */
    private function collectInquirySummaryExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', string $team_name = ''): array
    {
        $normalizedTeamExpr = "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";

        $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys);
        $baseJoinAdmin = (clone $baseQuery)->leftJoin('admin a', 'l.pr_user = a.username');

        $teamRowsRaw = (clone $baseJoinAdmin)
            ->group($normalizedTeamExpr)
            ->field($normalizedTeamExpr . ' as team_name,count(*) as yw_num')
            ->order('yw_num desc')
            ->order('team_name asc')
            ->select();
        $teamRows = [];
        foreach ($teamRowsRaw as $idx => $row) {
            $teamRows[] = [
                'rank' => $idx + 1,
                'team_name' => trim((string)($row['team_name'] ?? '')) !== '' ? (string)$row['team_name'] : '未分组',
                'yw_num' => (int)($row['yw_num'] ?? 0),
            ];
        }

        $memberRowsRaw = (clone $baseJoinAdmin)
            ->group($normalizedTeamExpr . ',l.pr_user')
            ->field($normalizedTeamExpr . ' as team_name,l.pr_user as username,count(distinct l.id) as yw_num')
            ->order('team_name asc')
            ->order('yw_num desc')
            ->order('username asc')
            ->select();
        $memberRows = [];
        $memberRankByTeam = [];
        foreach ($memberRowsRaw as $row) {
            $teamName = trim((string)($row['team_name'] ?? ''));
            if ($teamName === '') {
                $teamName = '未分组';
            }
            if (!isset($memberRankByTeam[$teamName])) {
                $memberRankByTeam[$teamName] = 0;
            }
            $memberRankByTeam[$teamName]++;
            $memberRows[] = [
                'team_name' => $teamName,
                'rank' => $memberRankByTeam[$teamName],
                'username' => (string)($row['username'] ?? ''),
                'yw_num' => (int)($row['yw_num'] ?? 0),
            ];
        }

        $channelGroupRows = (clone $baseJoinAdmin)
            ->group($normalizedTeamExpr . ',l.pr_user,l.inquiry_id')
            ->field($normalizedTeamExpr . ' as team_name,l.pr_user as username,l.inquiry_id,count(distinct l.id) as yw_num')
            ->order('team_name asc')
            ->order('username asc')
            ->order('yw_num desc')
            ->order('l.inquiry_id asc')
            ->select();
        $inquiryIds = [];
        foreach ($channelGroupRows as $row) {
            $iid = (int)($row['inquiry_id'] ?? 0);
            if ($iid > 0) {
                $inquiryIds[$iid] = $iid;
            }
        }
        $inquiryMap = [];
        if (!empty($inquiryIds)) {
            $inquiryMap = Db::table('crm_inquiry')->where('id', 'in', array_values($inquiryIds))->column('inquiry_name', 'id');
        }
        $channelRows = [];
        $channelRankByMember = [];
        foreach ($channelGroupRows as $row) {
            $teamName = trim((string)($row['team_name'] ?? ''));
            if ($teamName === '') {
                $teamName = '未分组';
            }
            $username = (string)($row['username'] ?? '');
            $iid = (int)($row['inquiry_id'] ?? 0);
            if ($iid > 0 && !empty($inquiryMap[$iid])) {
                $channelName = (string)$inquiryMap[$iid];
            } elseif ($iid <= 0) {
                $channelName = '未分类';
            } else {
                $channelName = '其他';
            }

            $rankKey = $teamName . '||' . $username;
            if (!isset($channelRankByMember[$rankKey])) {
                $channelRankByMember[$rankKey] = 0;
            }
            $channelRankByMember[$rankKey]++;

            $channelRows[] = [
                'team_name' => $teamName,
                'username' => $username,
                'rank' => $channelRankByMember[$rankKey],
                'channel_name' => $channelName,
                'yw_num' => (int)($row['yw_num'] ?? 0),
            ];
        }

        $leadColumns = $this->getInquirySummaryLeadsColumns();
        $hasKhName = in_array('kh_name', $leadColumns, true);
        $hasAtTime = in_array('at_time', $leadColumns, true);
        $hasToKhTime = in_array('to_kh_time', $leadColumns, true);
        $hasSource = in_array('source', $leadColumns, true);
        $hasXsSource = in_array('xs_source', $leadColumns, true);
        $hasProductName = in_array('product_name', $leadColumns, true);
        $hasMobile = in_array('mobile', $leadColumns, true);
        $hasPhone = in_array('phone', $leadColumns, true);
        $hasTel = in_array('tel', $leadColumns, true);

        $contactSubSql = Db::table('crm_contacts')
            ->where('is_delete', '=', 0)
            ->field("leads_id, GROUP_CONCAT(CONCAT(IFNULL(contact_extra,''),IFNULL(contact_value,'')) SEPARATOR ' / ') as contact_text")
            ->group('leads_id')
            ->buildSql();

        $detailQuery = (clone $baseJoinAdmin)
            ->leftJoin('crm_leads cl', 'cl.id = l.id')
            ->leftJoin('crm_inquiry ci', 'l.inquiry_id = ci.id')
            ->leftJoin('crm_inquiry_port cp', 'l.port_id = cp.id')
            ->leftJoin([$contactSubSql => 'ct'], 'ct.leads_id = l.id');

        if ($hasProductName) {
            $detailQuery->leftJoin('crm_products p', 'cl.product_name = p.id');
        }

        $detailFields = [
            'l.id as lead_id',
            $normalizedTeamExpr . ' as team_name',
            'l.pr_user as username',
            "IFNULL(ci.inquiry_name, '') as inquiry_name",
            "IFNULL(cp.port_name, '') as port_name",
            "IFNULL(ct.contact_text, '') as contact_text",
            $hasKhName ? 'cl.kh_name as kh_name' : "'' as kh_name",
            $hasAtTime ? 'cl.at_time as at_time' : "'' as at_time",
            $hasToKhTime ? 'cl.to_kh_time as to_kh_time' : "'' as to_kh_time",
            $hasSource ? 'cl.source as source' : "'' as source",
            $hasXsSource ? 'cl.xs_source as xs_source' : "'' as xs_source",
            $hasMobile ? 'cl.mobile as mobile' : "'' as mobile",
            $hasPhone ? 'cl.phone as phone' : "'' as phone",
            $hasTel ? 'cl.tel as tel' : "'' as tel",
        ];
        if ($hasProductName) {
            $detailFields[] = 'cl.product_name as product_raw';
            $detailFields[] = "IFNULL(p.product_name, '') as product_name";
        } else {
            $detailFields[] = "'' as product_raw";
            $detailFields[] = "'' as product_name";
        }

        $rawRowsRaw = $detailQuery
            ->field($detailFields)
            ->order('l.id desc')
            ->select();

        $rawRows = [];
        foreach ($rawRowsRaw as $row) {
            $teamName = trim((string)($row['team_name'] ?? ''));
            if ($teamName === '') {
                $teamName = '未分组';
            }

            $channelName = trim((string)($row['inquiry_name'] ?? ''));
            if ($channelName === '') {
                $channelName = trim((string)($row['source'] ?? ''));
            }
            if ($channelName === '') {
                $channelName = trim((string)($row['xs_source'] ?? ''));
            }
            if ($channelName === '') {
                $channelName = '未分类';
            }

            $contact = trim((string)($row['contact_text'] ?? ''));
            if ($contact === '') {
                $contact = trim((string)($row['mobile'] ?? ''));
            }
            if ($contact === '') {
                $contact = trim((string)($row['phone'] ?? ''));
            }
            if ($contact === '') {
                $contact = trim((string)($row['tel'] ?? ''));
            }

            $productName = trim((string)($row['product_name'] ?? ''));
            if ($productName === '') {
                $productName = trim((string)($row['product_raw'] ?? ''));
            }

            $rawRows[] = [
                'lead_id' => (int)($row['lead_id'] ?? 0),
                'kh_name' => (string)($row['kh_name'] ?? ''),
                'contact_text' => $contact,
                'username' => (string)($row['username'] ?? ''),
                'team_name' => $teamName,
                'channel_name' => $channelName,
                'port_name' => (string)($row['port_name'] ?? ''),
                'product_name' => $productName,
                'at_time' => (string)($row['at_time'] ?? ''),
                'to_kh_time' => (string)($row['to_kh_time'] ?? ''),
            ];
        }

        $teamSourceSummary = $this->buildInquiryTeamSourceSummaryRows($timebucket, $at_time, $month_keys, '');

        return [
            'team_rows' => $teamRows,
            'member_rows' => $memberRows,
            'channel_rows' => $channelRows,
            'raw_rows' => $rawRows,
            'team_source_rows' => $teamSourceSummary['rows'],
        ];
    }

    /**
     * 写入导出 Sheet（首行为表头）。
     */
    private function fillInquirySummaryExportSheet($sheet, array $headers, array $rows): void
    {
        $this->fillExportSheet($sheet, $headers, $rows);
    }

    // =========================================================
    // 运营询盘汇总表
    // =========================================================

    /**
     * 运营询盘汇总表 - 页面入口
     */
    public function operationInquirySummaryTable()
    {
        return $this->fetch('operation_inquiry_summary_table');
    }

    /**
     * 第一屏：按询盘来源汇总。
     */
    public function getOperationInquirySummarySourceData()
    {
        try {
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);

            $bucketExpr = 'CASE WHEN l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), \'\')) = \'\' THEN 0 ELSE CAST(l.inquiry_id AS UNSIGNED) END';

            $rows = (clone $baseQuery)
                ->field($bucketExpr . ' AS inquiry_bucket, COUNT(*) AS yw_num')
                ->group([$bucketExpr])
                ->order('yw_num', 'desc')
                ->order('inquiry_bucket', 'asc')
                ->select();

            $buckets = [];
            foreach ($rows as $row) {
                $buckets[] = (int)($row['inquiry_bucket'] ?? 0);
            }
            $buckets = array_values(array_unique(array_filter($buckets, function ($v) {
                return $v > 0;
            })));
            $inquiry_map = [];
            if (!empty($buckets)) {
                $inquiry_map = Db::table('crm_inquiry')->where('id', 'in', $buckets)->column('inquiry_name', 'id');
            }

            $result = [];
            $total = 0;
            $idx = 0;
            foreach ($rows as $row) {
                $bucket = (int)($row['inquiry_bucket'] ?? 0);
                $count = (int)($row['yw_num'] ?? 0);
                $total += $count;
                if ($bucket <= 0) {
                    $name = '未知来源';
                    $iid = 0;
                } else {
                    $name = $inquiry_map[$bucket] ?? ('ID:' . $bucket);
                    $iid = $bucket;
                }
                $idx++;
                $result[] = [
                    'rank' => $idx,
                    'inquiry_id' => $iid,
                    'inquiry_name' => $name,
                    'yw_num' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummarySourceData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '询盘来源汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第二屏：指定来源下各运营人员询盘数。
     */
    public function getOperationInquirySummaryStaffData()
    {
        try {
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);

            // 关键修复：不能再按 leads.oper_user/pr_user 直接分组，否则会出现“未分配运营”误归类。
            // 统一对齐控制面板口径：先取当前组织下、运营组、启用、且负责当前 inquiry_id 的真实运营账号，再按账号配置的 port_id 与 leads 交集计数。
            $staffRows = $this->fetchOperationStaffRowsForInquirySummary($inquiry_id);
            if (empty($staffRows)) {
                return json([
                    'code' => 0,
                    'msg' => '获取成功',
                    'data' => [],
                    'summary' => ['total_count' => 0],
                ]);
            }

            $result = [];
            $total = 0;
            foreach ((array)$staffRows as $op) {
                $uname = trim((string)($op['username'] ?? ''));
                if ($uname === '') {
                    continue;
                }
                $ports = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
                $portWhere = $this->buildLeadPortIntersectAdminPortsWhere($ports);
                $cnt = (int)(clone $baseQuery)->whereRaw($portWhere)->count('l.id');
                $total += $cnt;
                $result[] = [
                    'username' => $uname,
                    'yw_num' => $cnt,
                ];
            }

            usort($result, function ($a, $b) {
                if ($a['yw_num'] !== $b['yw_num']) {
                    return $b['yw_num'] <=> $a['yw_num'];
                }
                return strcmp($a['username'], $b['username']);
            });
            foreach ($result as $i => &$r) {
                $r['rank'] = $i + 1;
            }
            unset($r);

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $result,
                'summary' => ['total_count' => $total],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummaryStaffData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '运营人员汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：端口询盘明细。
     */
    public function getOperationInquirySummaryPortData()
    {
        try {
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $username = trim((string)Request::param('username', ''));
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);
            $validPortMap = $this->getInquiryPortNameMapForSummary($inquiry_id);
            $leadScopeQuery = clone $baseQuery;

            // 关键修复：第三屏必须先锁定“当前来源 + 当前运营人员配置端口”，不能仅按字符串用户名过滤 leads。
            if ($username !== '') {
                $op = $this->findOperationStaffForPortSummary($username, $inquiry_id);
                if ($op === null) {
                    return json([
                        'code' => 0,
                        'msg' => '获取成功',
                        'data' => [],
                        'summary' => ['total_count' => 0],
                    ]);
                }

                $portIds = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
                if (empty($portIds)) {
                    return json([
                        'code' => 0,
                        'msg' => '获取成功',
                        'data' => [],
                        'summary' => ['total_count' => 0],
                    ]);
                }

                $portWhere = $this->buildLeadPortIntersectAdminPortsWhere($portIds);
                $leadScopeQuery->whereRaw($portWhere);
            }

            $leadRows = $leadScopeQuery->field('l.id,l.port_id')->select();
            if (is_array($leadRows)) {
                $leadRowsArr = $leadRows;
            } elseif (is_object($leadRows) && method_exists($leadRows, 'toArray')) {
                $leadRowsArr = $leadRows->toArray();
            } else {
                $leadRowsArr = iterator_to_array($leadRows);
            }
            $portSummary = $this->buildOperationInquiryPortSummaryFromLeadRows($leadRowsArr, $validPortMap, '未分配端口');

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $portSummary['data'],
                'summary' => $portSummary['summary'],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquirySummaryPortData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '运营端口汇总获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
            ]);
        }
    }

    /**
     * 未知来源第三屏：异常原因明细。
     */
    public function getOperationInquiryUnknownSourceDetailData()
    {
        try {
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $timebucket = Request::param('timebucket', '');
            $at_time = Request::param('at_time', '');
            $month_keys = trim((string)Request::param('month_keys', ''));

            $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);

            $leadsColumns = $this->getInquirySummaryLeadsColumns();
            $selectFields = [
                'src.id AS leads_id',
                'src.pr_user',
                'src.inquiry_id',
                'src.port_id',
            ];
            foreach (['kh_name', 'source_port', 'oper_user'] as $field) {
                if (in_array($field, $leadsColumns, true)) {
                    $selectFields[] = 'src.' . $field;
                }
            }

            $leadRows = (clone $baseQuery)
                ->leftJoin('crm_leads src', 'src.id = l.id')
                ->field($selectFields)
                ->select();

            $reasonDefs = [
                'inquiry_null' => 'inquiry_id 为 NULL',
                'inquiry_empty' => 'inquiry_id 为空字符串',
                'inquiry_zero' => 'inquiry_id 为 0',
                'inquiry_not_exists' => 'inquiry_id 在 crm_inquiry 表中不存在',
                'no_staff' => 'inquiry_id 正常但没有匹配到运营人员',
                'no_port' => 'inquiry_id 正常但没有匹配到运营端口',
                'other' => '其他异常',
            ];
            $groups = [];
            foreach ($reasonDefs as $reasonKey => $reasonType) {
                $groups[$reasonKey] = [
                    'reason_key' => $reasonKey,
                    'reason_type' => $reasonType,
                    'count' => 0,
                    'samples' => [],
                ];
            }

            $inquiryIds = [];
            foreach ($leadRows as $row) {
                $raw = isset($row['inquiry_id']) ? trim((string)$row['inquiry_id']) : '';
                if ($raw === '' || !preg_match('/^-?\d+$/', $raw)) {
                    continue;
                }
                $iid = (int)$raw;
                if ($iid > 0) {
                    $inquiryIds[$iid] = $iid;
                }
            }
            $inquiryIds = array_values($inquiryIds);

            $inquiryExistsMap = [];
            if (!empty($inquiryIds)) {
                $existsIds = Db::table('crm_inquiry')->where('id', 'in', $inquiryIds)->column('id');
                foreach ((array)$existsIds as $eid) {
                    $eid = (int)$eid;
                    if ($eid > 0) {
                        $inquiryExistsMap[$eid] = true;
                    }
                }
            }

            $staffInquiryMap = [];
            $staffRows = Db::table('admin')
                ->where('group_id', '=', $this->yygid)
                ->where('is_open', '=', 1)
                ->field('inquiry_id')
                ->select();
            foreach ((array)$staffRows as $srow) {
                $sid = isset($srow['inquiry_id']) ? (int)$srow['inquiry_id'] : 0;
                if ($sid > 0) {
                    $staffInquiryMap[$sid] = true;
                }
            }

            $portInquiryMap = [];
            $portInquiryIds = Db::table('crm_inquiry_port')->where('inquiry_id', '>', 0)->column('inquiry_id');
            foreach ((array)$portInquiryIds as $pid) {
                $pid = (int)$pid;
                if ($pid > 0) {
                    $portInquiryMap[$pid] = true;
                }
            }

            foreach ($leadRows as $row) {
                $reasonKey = $this->classifyOperationInquiryUnknownReason($row, $inquiryExistsMap, $staffInquiryMap, $portInquiryMap);
                if (!isset($groups[$reasonKey])) {
                    $reasonKey = 'other';
                }
                $groups[$reasonKey]['count']++;
                if (count($groups[$reasonKey]['samples']) < 10) {
                    $groups[$reasonKey]['samples'][] = [
                        'leads_id' => isset($row['leads_id']) ? (int)$row['leads_id'] : 0,
                        'kh_name' => isset($row['kh_name']) ? (string)$row['kh_name'] : '',
                        'pr_user' => isset($row['pr_user']) ? (string)$row['pr_user'] : '',
                        'inquiry_id' => isset($row['inquiry_id']) ? (string)$row['inquiry_id'] : '',
                        'port_id' => isset($row['port_id']) ? (string)$row['port_id'] : '',
                        'source_port' => isset($row['source_port']) ? (string)$row['source_port'] : '',
                        'oper_user' => isset($row['oper_user']) ? (string)$row['oper_user'] : '',
                    ];
                }
            }

            $data = [];
            $reasonStats = [];
            $total = 0;
            foreach ($reasonDefs as $reasonKey => $reasonType) {
                $group = $groups[$reasonKey];
                $total += (int)$group['count'];
                $reasonStats[] = [
                    'reason_key' => $reasonKey,
                    'reason_type' => $reasonType,
                    'count' => (int)$group['count'],
                ];
                $data[] = $group;
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $data,
                'summary' => ['total_count' => $total],
                'reason_stats' => $reasonStats,
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] getOperationInquiryUnknownSourceDetailData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '未知来源异常明细获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['total_count' => 0],
                'reason_stats' => [],
            ]);
        }
    }

    /**
     * 导出：运营询盘汇总表（多 Sheet）。
     */
    public function exportOperationInquirySummary()
    {
        try {
            if (!$this->isSpreadsheetExportReady()) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $inquiry_id = (int)Request::param('inquiry_id', 0);
            $username = trim((string)Request::param('username', ''));

            $exportData = $this->collectOperationInquiryExportData($timebucket, $at_time, $month_keys, $inquiry_id, $username);
            $totalRows = count($exportData['source_rows']) + count($exportData['staff_rows']) + count($exportData['port_rows']) + count($exportData['raw_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = $this->buildMultiSheetSpreadsheet([
                [
                    'title' => '来源汇总',
                    'headers' => ['排名', '询盘来源ID', '询盘来源', '询盘数量'],
                    'rows' => $exportData['source_rows'],
                ],
                [
                    'title' => '运营人员汇总',
                    'headers' => ['询盘来源', '排名', '运营人员', '询盘数量'],
                    'rows' => $exportData['staff_rows'],
                ],
                [
                    'title' => '运营端口明细',
                    'headers' => ['询盘来源', '运营人员', '排名', '运营端口', '询盘数量'],
                    'rows' => $exportData['port_rows'],
                ],
                [
                    'title' => '原始客户询盘明细',
                    'headers' => ['客户ID', '客户名称', '手机号/电话', '负责人', '团队', '询盘来源', '运营端口', '录入时间', '转客户时间'],
                    'rows' => $exportData['raw_rows'],
                ],
            ]);

            $this->outputSpreadsheet($spreadsheet, '运营询盘汇总表', 'operation_inquiry_summary');
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OperationInquirySummary] exportOperationInquirySummary failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    // =========================================================
    // 运营询盘汇总表 —— 私有辅助方法
    // =========================================================

    private function parseCsvPortIdsForOperatorInquiry($raw): array
    {
        $raw = trim((string)$raw);
        if ($raw === '') {
            return [];
        }
        $parts = preg_split('/\s*,\s*/', $raw);
        $out = [];
        foreach ($parts as $p) {
            $p = trim((string)$p);
            if ($p === '' || !ctype_digit($p)) {
                continue;
            }
            $v = (int)$p;
            if ($v > 0) {
                $out[$v] = $v;
            }
        }
        return array_values($out);
    }

    private function fetchOperationStaffRowsForInquirySummary(int $inquiryIdToken): array
    {
        $current_admin = Admin::getMyInfo();
        $q = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('group_id', '=', $this->yygid)
            ->where('is_open', '=', 1);

        if ($inquiryIdToken <= 0) {
            $q->where(function ($sub) {
                $sub->whereNull('inquiry_id')
                    ->whereOr('inquiry_id', '=', '')
                    ->whereOr('inquiry_id', '=', 0);
            });
        } else {
            $q->whereRaw('CAST(inquiry_id AS UNSIGNED) = ?', [$inquiryIdToken]);
        }

        return $q->field('username,port_id,inquiry_id')->order('username')->select();
    }

    private function applyOperatorInquiryLeadsSourceBucket($query, int $inquiryIdToken)
    {
        if ($inquiryIdToken <= 0) {
            $query->whereRaw("(l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), '')) = '')");
        } else {
            $query->where('l.inquiry_id', '=', $inquiryIdToken);
        }
        return $query;
    }

    /**
     * 判断 leads.port_id 与运营人员端口配置是否有交集。
     */
    private function buildLeadPortIntersectAdminPortsWhere(array $adminPortIds): string
    {
        if (empty($adminPortIds)) {
            return '1=0';
        }
        $parts = [];
        foreach ($adminPortIds as $pid) {
            $pid = (int)$pid;
            if ($pid <= 0) {
                continue;
            }
            $parts[] = "FIND_IN_SET('{$pid}', l.port_id) > 0";
        }
        return empty($parts) ? '1=0' : '(' . implode(' OR ', $parts) . ')';
    }

    private function getOperationInquiryStaffNameExpr(): string
    {
        $leadColumns = $this->getInquirySummaryLeadsColumns();
        if (in_array('oper_user', $leadColumns, true)) {
            return "IFNULL(NULLIF(TRIM(l.oper_user), ''), '未分配运营')";
        }
        return "IFNULL(NULLIF(TRIM(l.pr_user), ''), '未分配运营')";
    }

    private function findOperationStaffForPortSummary(string $username, int $inquiryIdToken): ?array
    {
        $username = trim($username);
        if ($username === '') {
            return null;
        }
        $current_admin = Admin::getMyInfo();
        $row = Db::table('admin')
            ->where($this->getOrgWhere($current_admin['org']))
            ->where('username', '=', $username)
            ->where('group_id', '=', $this->yygid)
            ->where('is_open', '=', 1)
            ->field('username,port_id,inquiry_id')
            ->find();
        if (empty($row)) {
            return null;
        }
        if ($inquiryIdToken <= 0) {
            $ok = ($row['inquiry_id'] === null || $row['inquiry_id'] === '' || (int)$row['inquiry_id'] === 0);
        } else {
            $ok = ((int)$row['inquiry_id'] === $inquiryIdToken) || ((string)(int)$row['inquiry_id'] === (string)$inquiryIdToken);
        }
        return $ok ? $row : null;
    }

    private function getInquiryPortNameMapForSummary(int $inquiryId): array
    {
        if ($inquiryId <= 0) {
            return [];
        }
        $rows = Db::table('crm_inquiry_port')
            ->where('inquiry_id', '=', $inquiryId)
            ->field('id,port_name')
            ->select();
        $map = [];
        foreach ($rows as $row) {
            $pid = (int)($row['id'] ?? 0);
            if ($pid <= 0) {
                continue;
            }
            $name = trim((string)($row['port_name'] ?? ''));
            if ($name === '') {
                $name = 'ID:' . $pid;
            }
            $map[$pid] = $name;
        }
        return $map;
    }

    private function buildOperationInquiryPortSummaryFromLeadRows(array $leadRows, array $validPortMap, string $fallbackName = '未分配端口'): array
    {
        $groups = [];
        $total = 0;
        foreach ($leadRows as $row) {
            $total++;
            $leadPortIds = $this->parseCsvPortIdsForOperatorInquiry($row['port_id'] ?? '');
            $bucketPortId = 0;
            foreach ($leadPortIds as $pid) {
                $pid = (int)$pid;
                if ($pid > 0 && isset($validPortMap[$pid])) {
                    $bucketPortId = $pid;
                    break;
                }
            }

            if ($bucketPortId > 0) {
                $bucketKey = 'p_' . $bucketPortId;
                $bucketName = $validPortMap[$bucketPortId] ?? ('ID:' . $bucketPortId);
            } else {
                $bucketKey = 'p_0';
                $bucketName = $fallbackName;
            }

            if (!isset($groups[$bucketKey])) {
                $groups[$bucketKey] = [
                    'port_id' => $bucketPortId,
                    'port_name' => $bucketName,
                    'yw_num' => 0,
                ];
            }
            $groups[$bucketKey]['yw_num']++;
        }

        $result = array_values($groups);
        usort($result, function ($a, $b) {
            if ((int)$a['yw_num'] !== (int)$b['yw_num']) {
                return (int)$b['yw_num'] <=> (int)$a['yw_num'];
            }
            return strcmp((string)$a['port_name'], (string)$b['port_name']);
        });
        foreach ($result as $i => &$r) {
            $r['rank'] = $i + 1;
        }
        unset($r);

        return [
            'data' => $result,
            'summary' => ['total_count' => $total],
        ];
    }

    private function resolveOperationInquiryNameById(int $inquiryId): string
    {
        if ($inquiryId <= 0) {
            return '未知来源';
        }
        $name = Db::table('crm_inquiry')->where('id', '=', $inquiryId)->value('inquiry_name');
        $name = trim((string)$name);
        return $name !== '' ? $name : ('来源ID:' . $inquiryId);
    }

    private function collectOperationInquiryExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', int $inquiry_id = 0, string $username = ''): array
    {
        $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
        if ($inquiry_id > 0) {
            $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiry_id);
        }

        $sourceRowsRaw = (clone $baseQuery)
            ->field("CASE WHEN l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), '')) = '' THEN 0 ELSE CAST(l.inquiry_id AS UNSIGNED) END AS inquiry_bucket, COUNT(*) AS yw_num")
            ->group("CASE WHEN l.inquiry_id IS NULL OR l.inquiry_id = 0 OR TRIM(IFNULL(CAST(l.inquiry_id AS CHAR), '')) = '' THEN 0 ELSE CAST(l.inquiry_id AS UNSIGNED) END")
            ->order('yw_num', 'desc')
            ->order('inquiry_bucket', 'asc')
            ->select();
        $bucketIds = [];
        foreach ((array)$sourceRowsRaw as $row) {
            $iid = (int)($row['inquiry_bucket'] ?? 0);
            if ($iid > 0) {
                $bucketIds[$iid] = $iid;
            }
        }
        $inquiryMap = [];
        if (!empty($bucketIds)) {
            $inquiryMap = Db::table('crm_inquiry')->where('id', 'in', array_values($bucketIds))->column('inquiry_name', 'id');
        }
        $sourceRows = [];
        foreach ((array)$sourceRowsRaw as $idx => $row) {
            $iid = (int)($row['inquiry_bucket'] ?? 0);
            $sourceRows[] = [
                $idx + 1,
                $iid,
                $iid > 0 ? (string)($inquiryMap[$iid] ?? ('ID:' . $iid)) : '未知来源',
                (int)($row['yw_num'] ?? 0),
            ];
        }

        $sourceNameMap = [];
        foreach ($sourceRows as $sr) {
            $sourceNameMap[(int)$sr[1]] = (string)$sr[2];
        }
        if ($inquiry_id > 0 && !isset($sourceNameMap[$inquiry_id])) {
            $sourceNameMap[$inquiry_id] = '来源ID:' . $inquiry_id;
        }

        $staffRows = [];
        $portRows = [];
        if ($inquiry_id > 0) {
            $staffRes = $this->collectOperationStaffRowsForExport($inquiry_id, $timebucket, $at_time, $month_keys, $username);
            $staffRows = $staffRes['staff_rows'];
            $portRows = $this->collectOperationPortRowsForExport($inquiry_id, $timebucket, $at_time, $month_keys, $username, $sourceNameMap[$inquiry_id] ?? '未知来源');
        } else {
            foreach ($sourceNameMap as $iid => $name) {
                $staffRes = $this->collectOperationStaffRowsForExport((int)$iid, $timebucket, $at_time, $month_keys, $username);
                foreach ($staffRes['staff_rows'] as $srow) {
                    $staffRows[] = $srow;
                }
                $portPartRows = $this->collectOperationPortRowsForExport((int)$iid, $timebucket, $at_time, $month_keys, $username, $name);
                foreach ($portPartRows as $prow) {
                    $portRows[] = $prow;
                }
            }
        }

        $contactSubSql = Db::table('crm_contacts')
            ->where('is_delete', '=', 0)
            ->field("leads_id, GROUP_CONCAT(CONCAT(IFNULL(contact_extra,''),IFNULL(contact_value,'')) SEPARATOR ' / ') as contact_text")
            ->group('leads_id')
            ->buildSql();
        $detailQuery = (clone $baseQuery)
            ->leftJoin('crm_leads cl', 'cl.id = l.id')
            ->leftJoin('admin a', 'l.pr_user = a.username')
            ->leftJoin('crm_inquiry ci', 'l.inquiry_id = ci.id')
            ->leftJoin('crm_inquiry_port cp', 'l.port_id = cp.id')
            ->leftJoin([$contactSubSql => 'ct'], 'ct.leads_id = l.id');
        if ($inquiry_id > 0) {
            $this->applyOperatorInquiryLeadsSourceBucket($detailQuery, $inquiry_id);
        }
        if ($username !== '') {
            // 导出与页面保持一致：按“运营账号配置端口”过滤，不再按 leads 的 oper_user/pr_user 文本过滤。
            $op = $this->findOperationStaffForPortSummary($username, $inquiry_id);
            if ($op === null) {
                return [
                    'source_rows' => $sourceRows,
                    'staff_rows' => $staffRows,
                    'port_rows' => $portRows,
                    'raw_rows' => [],
                ];
            }
            $portIds = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
            if (empty($portIds)) {
                return [
                    'source_rows' => $sourceRows,
                    'staff_rows' => $staffRows,
                    'port_rows' => $portRows,
                    'raw_rows' => [],
                ];
            }
            $detailQuery->whereRaw($this->buildLeadPortIntersectAdminPortsWhere($portIds));
        }
        $rawRowsRaw = $detailQuery
            ->field([
                'l.id as lead_id',
                "IFNULL(cl.kh_name, '') as kh_name",
                "IFNULL(ct.contact_text, '') as contact_text",
                "IFNULL(l.pr_user, '') as username",
                "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END as team_name",
                "IFNULL(ci.inquiry_name, '未知来源') as inquiry_name",
                "IFNULL(cp.port_name, '') as port_name",
                "IFNULL(cl.at_time, '') as at_time",
                "IFNULL(cl.to_kh_time, '') as to_kh_time",
            ])
            ->order('l.id desc')
            ->limit(20000)
            ->select();
        $rawRows = [];
        foreach ((array)$rawRowsRaw as $row) {
            $rawRows[] = [
                (int)($row['lead_id'] ?? 0),
                (string)($row['kh_name'] ?? ''),
                (string)($row['contact_text'] ?? ''),
                (string)($row['username'] ?? ''),
                (string)($row['team_name'] ?? '未分组'),
                (string)($row['inquiry_name'] ?? '未知来源'),
                (string)($row['port_name'] ?? ''),
                (string)($row['at_time'] ?? ''),
                (string)($row['to_kh_time'] ?? ''),
            ];
        }

        return [
            'source_rows' => $sourceRows,
            'staff_rows' => $staffRows,
            'port_rows' => $portRows,
            'raw_rows' => $rawRows,
        ];
    }

    private function collectOperationStaffRowsForExport(int $inquiryId, string $timebucket = '', string $at_time = '', string $month_keys = '', string $username = ''): array
    {
        $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
        $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiryId);
        $staffRowsRaw = $this->fetchOperationStaffRowsForInquirySummary($inquiryId);
        $sourceName = $this->resolveOperationInquiryNameById($inquiryId);
        $rows = [];
        foreach ((array)$staffRowsRaw as $row) {
            $uname = trim((string)($row['username'] ?? ''));
            if ($uname === '') {
                continue;
            }
            if ($username !== '' && $uname !== $username) {
                continue;
            }
            $ports = $this->parseCsvPortIdsForOperatorInquiry($row['port_id'] ?? '');
            $portWhere = $this->buildLeadPortIntersectAdminPortsWhere($ports);
            $cnt = (int)(clone $baseQuery)->whereRaw($portWhere)->count('l.id');
            $rows[] = [
                'source_name' => $sourceName,
                'username' => $uname,
                'yw_num' => $cnt,
            ];
        }
        usort($rows, function ($a, $b) {
            if ((int)$a['yw_num'] !== (int)$b['yw_num']) {
                return (int)$b['yw_num'] <=> (int)$a['yw_num'];
            }
            return strcmp((string)$a['username'], (string)$b['username']);
        });
        $out = [];
        foreach ($rows as $idx => $row) {
            $out[] = [
                (string)$row['source_name'],
                $idx + 1,
                (string)$row['username'],
                (int)$row['yw_num'],
            ];
        }
        return ['staff_rows' => $out];
    }

    private function collectOperationPortRowsForExport(int $inquiryId, string $timebucket = '', string $at_time = '', string $month_keys = '', string $username = '', string $sourceName = ''): array
    {
        $baseQuery = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, false, $month_keys);
        $this->applyOperatorInquiryLeadsSourceBucket($baseQuery, $inquiryId);
        $validPortMap = $this->getInquiryPortNameMapForSummary($inquiryId);
        $leadScopeQuery = clone $baseQuery;
        $displayUsername = '';

        if ($username !== '') {
            $op = $this->findOperationStaffForPortSummary($username, $inquiryId);
            if ($op === null) {
                return [];
            }
            $displayUsername = $username;
            $portIds = $this->parseCsvPortIdsForOperatorInquiry($op['port_id'] ?? '');
            if (empty($portIds)) {
                return [];
            }
            $portWhere = $this->buildLeadPortIntersectAdminPortsWhere($portIds);
            $leadScopeQuery->whereRaw($portWhere);
        }

        $leadRows = $leadScopeQuery->field('l.id,l.port_id')->select();
        $leadRowsArr = is_array($leadRows)
            ? $leadRows
            : ((is_object($leadRows) && method_exists($leadRows, 'toArray')) ? $leadRows->toArray() : iterator_to_array($leadRows));
        $summary = $this->buildOperationInquiryPortSummaryFromLeadRows($leadRowsArr, $validPortMap, '未分配端口');

        $rows = [];
        foreach ((array)($summary['data'] ?? []) as $row) {
            $rows[] = [
                (string)$sourceName,
                (string)$displayUsername,
                (int)($row['rank'] ?? 0),
                (string)($row['port_name'] ?? ''),
                (int)($row['yw_num'] ?? 0),
            ];
        }
        return $rows;
    }

    private function classifyOperationInquiryUnknownReason(array $leadRow, array $inquiryExistsMap, array $staffInquiryMap, array $portInquiryMap): string
    {
        if (!array_key_exists('inquiry_id', $leadRow) || $leadRow['inquiry_id'] === null) {
            return 'inquiry_null';
        }

        $rawStr = trim((string)$leadRow['inquiry_id']);
        if ($rawStr === '') {
            return 'inquiry_empty';
        }

        if (preg_match('/^-?\d+$/', $rawStr)) {
            $iid = (int)$rawStr;
            if ($iid === 0) {
                return 'inquiry_zero';
            }
            if ($iid > 0) {
                if (empty($inquiryExistsMap[$iid])) {
                    return 'inquiry_not_exists';
                }
                if (empty($staffInquiryMap[$iid])) {
                    return 'no_staff';
                }
                if (empty($portInquiryMap[$iid])) {
                    return 'no_port';
                }
            }
        }

        return 'other';
    }

    /**
     * 组装导出数据：业务人员业绩表。
     */
    private function collectBusinessPerformanceExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', string $username = ''): array
    {
        // 导出口径与页面口径一致：复用同一套公开 where 条件
        $where = $this->buildOrderListAlignedOrderWhere($timebucket, $at_time, $username, $month_keys);
        $teamNameSeparator = '||#||';

        $orderStatsQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($orderStatsQuery, $where);
        $orderStats = $orderStatsQuery
            ->fieldRaw(
                "IFNULL(NULLIF(TRIM(pr_user),''), '__PR_EMPTY__') AS pr_bucket, "
                . 'COUNT(id) AS order_count, SUM(profit) AS total_profit, SUM(money) AS total_money, '
                . "SUBSTRING_INDEX("
                . "GROUP_CONCAT(NULLIF(TRIM(team_name), '') ORDER BY order_time DESC, id DESC SEPARATOR '{$teamNameSeparator}')"
                . ", '{$teamNameSeparator}', 1"
                . ") AS snap_team_name"
            )
            ->group('pr_bucket')
            ->select();

        $adminMap = [];
        $adminQuery = Db::table('admin')->field('username,team_name');
        foreach ((array)$adminQuery->select() as $ar) {
            $uname = trim((string)($ar['username'] ?? ''));
            if ($uname !== '') {
                $adminMap[$uname] = $ar;
            }
        }

        $businessUsersQuery = Db::table('admin')
            ->where('group_id', 'in', [$this->ywgid, $this->ywzgid, $this->pdgid, 14])
            ->where('is_open', '=', 1)
            ->where('username', '<>', '')
            ->whereNotNull('username');
        if ($username !== '') {
            $businessUsersQuery->where('username', '=', $username);
        }
        $businessUsers = $businessUsersQuery
            ->field('username')
            ->limit(500)
            ->select();

        $excludeUsernames = array_values(array_filter(array_unique(array_map(function ($name) {
            return trim((string)$name);
        }, ['范文清', '郭志华', '郭志华2', '付淑雅', '叶诗龙']))));
        $excludeMap = array_fill_keys($excludeUsernames, true);

        $aggMap = [];
        foreach ((array)$orderStats as $stat) {
            $bucket = (string)($stat['pr_bucket'] ?? '');
            if ($bucket === '') {
                continue;
            }
            $isEmptyPr = ($bucket === '__PR_EMPTY__');
            $displayUsername = $isEmptyPr ? '未知业务员' : $bucket;
            $aggMap[$bucket] = [
                'username' => $displayUsername,
                'order_count' => (int)($stat['order_count'] ?? 0),
                'total_profit' => round((float)($stat['total_profit'] ?? 0), 2),
                'total_money' => round((float)($stat['total_money'] ?? 0), 2),
                'snap_team_name' => trim((string)($stat['snap_team_name'] ?? '')),
            ];
        }

        foreach ((array)$businessUsers as $u) {
            $un = trim((string)($u['username'] ?? ''));
            if ($un === '' || isset($aggMap[$un])) {
                continue;
            }
            $aggMap[$un] = [
                'username' => $un,
                'order_count' => 0,
                'total_profit' => 0.0,
                'total_money' => 0.0,
                'snap_team_name' => '',
            ];
        }

        $rows = [];
        foreach ($aggMap as $bucket => $row) {
            $displayUsername = trim((string)($row['username'] ?? ''));
            if ($displayUsername !== '' && isset($excludeMap[$displayUsername])) {
                continue;
            }
            $totalProfit = (float)($row['total_profit'] ?? 0);
            $totalMoney = (float)($row['total_money'] ?? 0);
            $rate = $totalMoney > 0 ? round(($totalProfit / $totalMoney) * 100, 2) : 0.0;

            $teamName = trim((string)($row['snap_team_name'] ?? ''));
            if ($teamName === '' && $bucket !== '__PR_EMPTY__' && isset($adminMap[$bucket])) {
                $teamName = trim((string)($adminMap[$bucket]['team_name'] ?? ''));
            }
            if ($teamName === '') {
                $teamName = '未分组';
            }

            $rows[] = [
                'username' => $displayUsername,
                'team_name' => $teamName,
                'order_count' => (int)($row['order_count'] ?? 0),
                'total_profit' => $totalProfit,
                'total_money' => $totalMoney,
                'profit_rate' => $rate,
            ];
        }
        usort($rows, function ($a, $b) {
            return ((float)$b['total_profit']) <=> ((float)$a['total_profit']);
        });

        $summaryRows = [];
        $detailRows = [];
        foreach ($rows as $idx => $r) {
            $summaryRows[] = [
                $idx + 1,
                (string)$r['username'],
                (string)$r['team_name'],
                (int)$r['order_count'],
                number_format((float)$r['total_profit'], 2, '.', ''),
                number_format((float)$r['total_money'], 2, '.', ''),
                number_format((float)$r['profit_rate'], 2, '.', ''),
            ];
            $detailRows[] = [
                (string)$r['username'],
                (string)$r['team_name'],
                (int)$r['order_count'],
                number_format((float)$r['total_profit'], 2, '.', ''),
                number_format((float)$r['total_money'], 2, '.', ''),
                number_format((float)$r['profit_rate'], 2, '.', ''),
            ];
        }

        $rawQuery = Db::table('crm_client_order');
        $this->applyPerformanceWhereToQuery($rawQuery, $where);
        $rawOrderRows = $rawQuery
            ->field('id,order_no,order_time,cname,pr_user,team_name,money,profit,check_status')
            ->order('order_time desc,id desc')
            ->limit(20000)
            ->select();
        $rawRows = [];
        foreach ((array)$rawOrderRows as $r) {
            $rawRows[] = [
                (int)($r['id'] ?? 0),
                (string)($r['order_no'] ?? ''),
                (string)($r['order_time'] ?? ''),
                (string)($r['cname'] ?? ''),
                (string)($r['pr_user'] ?? ''),
                trim((string)($r['team_name'] ?? '')) !== '' ? (string)$r['team_name'] : '未分组',
                number_format((float)($r['money'] ?? 0), 2, '.', ''),
                number_format((float)($r['profit'] ?? 0), 2, '.', ''),
                (int)($r['check_status'] ?? 0),
            ];
        }

        return [
            'summary_rows' => $summaryRows,
            'detail_rows' => $detailRows,
            'raw_rows' => $rawRows,
        ];
    }

    // =========================================================
    // 订单产品汇总表
    // =========================================================

    /**
     * 订单产品汇总表 - 页面入口
     */
    public function orderProductSummaryTable()
    {
        return $this->fetch('order_product_summary_table');
    }

    /**
     * 订单产品汇总：团队名标准化（空值统一为"未分组"）
     */
    private function normalizeOrderProductTeamName(string $teamName): string
    {
        $teamName = trim($teamName);
        return $teamName === '' ? '未分组' : $teamName;
    }

    /**
     * 订单产品汇总：统一团队名称口径
     * 优先订单快照 o.team_name，其次 admin.team_name，最后"未分组"
     */
    private function getOrderProductSummaryTeamExpr(): string
    {
        return "IFNULL(NULLIF(TRIM(o.team_name),''), IFNULL(NULLIF(TRIM(a.team_name),''), '未分组'))";
    }

    /**
     * 订单产品汇总：专用时间过滤（避免通用 where 结构在 join/alias 场景下歧义）
     */
    private function parseOrderProductSummaryMonthKeys(string $month_keys = ''): array
    {
        return $this->parseMonthKeysToRanges($month_keys);
    }

    private function applyOrderProductSummaryTimeFilter($query, string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        return $this->applyMonthShortcutTimeFilterSafe($query, 'o.order_time', $timebucket, $at_time, $month_keys);
    }

    // ===== [开始] 订单产品汇总第一屏利润/销量排序改造 =====
    /**
     * 订单产品汇总：公司/个人产品销量聚合
     */
    private function buildOrderProductSummaryProductSalesQuery(string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        return $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys)
            ->field('oi.product_name, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit')
            ->group('oi.product_name')
            ->order('total_profit desc, sale_qty desc, oi.product_name asc');
    }

    /**
     * 订单产品汇总：公共基础查询
     * - 主表：crm_order_item
     * - 关联：crm_client_order
     * - 时间/状态：专用过滤（check_status=2 + order_time）
     */
    private function buildOrderProductSummaryBaseQuery(string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        $query = Db::table('crm_order_item')->alias('oi')
            ->join('crm_client_order o', 'oi.order_id = o.id', 'INNER')
            ->leftJoin('admin a', 'o.pr_user = a.username');

        $query->where('o.check_status', '=', 2);
        $this->applyOrderProductSummaryTimeFilter($query, $timebucket, $at_time, $month_keys);
        $query->whereRaw("TRIM(IFNULL(oi.product_name, '')) <> ''");

        return $query;
    }

    /**
     * 订单产品汇总：按团队过滤（支持"未分组"）
     */
    private function applyOrderProductSummaryTeamFilter($query, string $teamName)
    {
        $teamName = $this->normalizeOrderProductTeamName($teamName);
        $teamExpr = $this->getOrderProductSummaryTeamExpr();
        $query->whereRaw($teamExpr . " = :team_name", ['team_name' => $teamName]);
        return $query;
    }

    /**
     * 第一屏：全公司产品销量排行 + 第二屏团队列表
     */
    public function getOrderProductSummaryData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        try {
            $teamExpr = $this->getOrderProductSummaryTeamExpr();

            $productRows = $this->buildOrderProductSummaryProductSalesQuery($timebucket, $at_time, $month_keys)
                ->select();

            $products = [];
            $totalSalesQty = 0.0;
            $totalProfit = 0.0;
            $rank = 1;
            foreach ((array)$productRows as $row) {
                $qty = (float)($row['sale_qty'] ?? 0);
                $profit = (float)($row['total_profit'] ?? 0);
                $totalSalesQty += $qty;
                $totalProfit += $profit;
                $products[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'sale_qty_raw' => $qty,
                    'sale_qty' => $qty,
                    'total_profit_raw' => $profit,
                    'total_profit' => number_format($profit, 2, '.', ','),
                ];
            }

            $teamRows = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys)
                ->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''")
                ->field($teamExpr . " as team_name, SUM(IFNULL(oi.qty,0)) as sale_qty, COUNT(DISTINCT NULLIF(TRIM(o.pr_user), '')) as member_count")
                ->group($teamExpr)
                ->order('sale_qty desc, team_name asc')
                ->select();

            $teams = [];
            $teamRank = 1;
            foreach ((array)$teamRows as $row) {
                $teamName = $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? ''));
                $teams[] = [
                    'rank' => $teamRank++,
                    'team_name' => $teamName,
                    'sale_qty' => (float)($row['sale_qty'] ?? 0),
                    'member_count' => (int)($row['member_count'] ?? 0),
                ];
            }

            $memberRows = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys)
                ->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''")
                ->field("TRIM(o.pr_user) as username")
                ->group("TRIM(o.pr_user)")
                ->select();
            $memberCount = count((array)$memberRows);

            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => [
                    'products' => $products,
                    'teams' => $teams,
                ],
                'summary' => [
                    'company' => [
                        'total_product_count' => count($products),
                        'total_sales_qty' => $totalSalesQty,
                        'total_profit_raw' => $totalProfit,
                        'total_profit' => number_format($totalProfit, 2, '.', ','),
                    ],
                    'team' => [
                        'team_count' => count($teams),
                        'member_count' => $memberCount,
                    ],
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OrderProductSummary] getOrderProductSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '订单产品汇总获取失败：' . $e->getMessage(),
                'data' => ['products' => [], 'teams' => []],
                'summary' => [
                    'company' => [
                        'total_product_count' => 0,
                        'total_sales_qty' => 0,
                        'total_profit_raw' => 0,
                        'total_profit' => number_format(0, 2, '.', ','),
                    ],
                    'team' => ['team_count' => 0, 'member_count' => 0],
                ],
            ]);
        }
    }
    // ===== [结束] 订单产品汇总第一屏利润/销量排序改造 =====

    /**
     * 第二屏：指定团队的成员列表（按销量排序）
     */
    public function getOrderProductTeamMembers()
    {
        $team_name = trim((string)Request::param('team_name', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        $team_name = $this->normalizeOrderProductTeamName($team_name);

        try {
            $query = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $this->applyOrderProductSummaryTeamFilter($query, $team_name);
            $query->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''");

            $rows = $query
                ->field("TRIM(o.pr_user) as username, SUM(IFNULL(oi.qty,0)) as sale_qty")
                ->group("TRIM(o.pr_user)")
                ->order('sale_qty desc, username asc')
                ->select();

            $data = [];
            $rank = 1;
            $totalSalesQty = 0.0;
            foreach ((array)$rows as $row) {
                $qty = (float)($row['sale_qty'] ?? 0);
                $totalSalesQty += $qty;
                $data[] = [
                    'rank' => $rank++,
                    'username' => trim((string)($row['username'] ?? '')),
                    'sale_qty' => $qty,
                ];
            }

            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => $data,
                'summary' => [
                    'team_count' => empty($data) ? 0 : 1,
                    'member_count' => count($data),
                    'total_sales_qty' => $totalSalesQty,
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OrderProductSummary] getOrderProductTeamMembers failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队成员获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['team_count' => 0, 'member_count' => 0, 'total_sales_qty' => 0],
            ]);
        }
    }

    /**
     * 第三屏：指定团队的产品利润/销量排行
     */
    public function getOrderProductTeamProducts()
    {
        $team_name = trim((string)Request::param('team_name', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        $team_name = $this->normalizeOrderProductTeamName($team_name);

        try {
            $query = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $this->applyOrderProductSummaryTeamFilter($query, $team_name);
            $query->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''");

            $rows = $query
                ->field("oi.product_name, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit")
                ->group("oi.product_name")
                ->order("total_profit desc, sale_qty desc, oi.product_name asc")
                ->select();

            $data = [];
            $rank = 1;
            $totalSalesQty = 0.0;
            $totalProfit = 0.0;
            foreach ((array)$rows as $row) {
                $qty = (float)($row['sale_qty'] ?? 0);
                $profit = (float)($row['total_profit'] ?? 0);
                $totalSalesQty += $qty;
                $totalProfit += $profit;
                $data[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'sale_qty_raw' => $qty,
                    'sale_qty' => $qty,
                    'total_profit_raw' => $profit,
                    'total_profit' => number_format($profit, 2, '.', ','),
                ];
            }

            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => $data,
                'summary' => [
                    'team_product_count' => count($data),
                    'total_product_count' => count($data),
                    'total_sales_qty' => $totalSalesQty,
                    'total_profit_raw' => $totalProfit,
                    'total_profit' => number_format($totalProfit, 2, '.', ','),
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OrderProductSummary] getOrderProductTeamProducts failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队产品排行获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => [
                    'team_product_count' => 0,
                    'total_product_count' => 0,
                    'total_sales_qty' => 0,
                    'total_profit_raw' => 0,
                    'total_profit' => number_format(0, 2, '.', ','),
                ],
            ]);
        }
    }

    /**
     * 第三屏：指定业务员的产品利润/销量排行
     */
    public function getOrderProductUserProducts()
    {
        $username = trim((string)Request::param('username', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));
        if ($username === '') {
            return json([
                'code' => 422,
                'msg' => '请先选择业务员',
                'data' => [],
                'summary' => [
                    'user_product_count' => 0,
                    'total_product_count' => 0,
                    'total_sales_qty' => 0,
                    'total_profit_raw' => 0,
                    'total_profit' => number_format(0, 2, '.', ','),
                ],
            ]);
        }

        try {
            $query = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $query->where('o.pr_user', '=', $username);

            $rows = $query
                ->field("oi.product_name, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit")
                ->group("oi.product_name")
                ->order('total_profit desc, sale_qty desc, oi.product_name asc')
                ->select();

            $data = [];
            $rank = 1;
            $totalSalesQty = 0.0;
            $totalProfit = 0.0;
            foreach ((array)$rows as $row) {
                $qty = (float)($row['sale_qty'] ?? 0);
                $profit = (float)($row['total_profit'] ?? 0);
                $totalSalesQty += $qty;
                $totalProfit += $profit;
                $data[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'sale_qty_raw' => $qty,
                    'sale_qty' => $qty,
                    'total_profit_raw' => $profit,
                    'total_profit' => number_format($profit, 2, '.', ','),
                ];
            }

            return json([
                'code' => 200,
                'msg' => 'ok',
                'data' => $data,
                'summary' => [
                    'user_product_count' => count($data),
                    'total_product_count' => count($data),
                    'total_sales_qty' => $totalSalesQty,
                    'total_profit_raw' => $totalProfit,
                    'total_profit' => number_format($totalProfit, 2, '.', ','),
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OrderProductSummary] getOrderProductUserProducts failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '个人产品排行获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => [
                    'user_product_count' => 0,
                    'total_product_count' => 0,
                    'total_sales_qty' => 0,
                    'total_profit_raw' => 0,
                    'total_profit' => number_format(0, 2, '.', ','),
                ],
            ]);
        }
    }

    /**
     * 导出：订单产品汇总表（多 Sheet）。
     */
    public function exportOrderProductSummary()
    {
        try {
            if (!$this->isSpreadsheetExportReady()) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $team_name = trim((string)Request::param('team_name', ''));
            $username = trim((string)Request::param('username', ''));

            $exportData = $this->collectOrderProductExportData($timebucket, $at_time, $month_keys, $team_name, $username);
            $totalRows = count($exportData['company_rows']) + count($exportData['team_rows']) + count($exportData['user_rows']) + count($exportData['raw_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = $this->buildMultiSheetSpreadsheet([
                [
                    'title' => '公司产品汇总',
                    'headers' => ['排名', '产品名称', '总利润', '总销量'],
                    'rows' => $exportData['company_rows'],
                ],
                [
                    'title' => '团队产品汇总',
                    'headers' => ['排名', '团队名称', '总利润', '总销量', '成员数'],
                    'rows' => $exportData['team_rows'],
                ],
                [
                    'title' => '个人产品汇总',
                    'headers' => ['团队', '成员', '产品名称', '订单数', '总利润', '总销量'],
                    'rows' => $exportData['user_rows'],
                ],
                [
                    'title' => '原始订单产品明细',
                    'headers' => ['订单ID', '订单号', '下单时间', '业务员', '团队', '产品名称', '销量', '利润'],
                    'rows' => $exportData['raw_rows'],
                ],
            ]);

            $this->outputSpreadsheet($spreadsheet, '订单产品汇总表', 'order_product_summary');
        } catch (\Throwable $e) {
            \think\facade\Log::error('[OrderProductSummary] exportOrderProductSummary failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    // =========================================================
    // 询盘产品汇总表
    // =========================================================

    /**
     * 询盘产品汇总表 - 页面入口
     */
    public function inquiryProductSummaryTable()
    {
        return $this->fetch('inquiry_product_summary_table');
    }

    /**
     * 询盘产品汇总：统一团队名称口径（空值归并"未分组"）
     */
    private function getInquiryProductSummaryTeamExpr(): string
    {
        return "CASE WHEN a.team_name IS NULL OR TRIM(a.team_name) = '' THEN '未分组' ELSE TRIM(a.team_name) END";
    }

    /**
     * 询盘产品汇总：产品名称口径
     * - crm_leads.product_name 为数字ID时，优先映射 crm_products.product_name
     */
    private function getInquiryProductSummaryProductExpr(): string
    {
        return "CASE " .
            "WHEN TRIM(IFNULL(l.product_name, '')) = '' THEN '' " .
            "WHEN TRIM(IFNULL(l.product_name, '')) REGEXP '^[0-9]+$' " .
            "     AND p.id IS NOT NULL " .
            "     AND TRIM(IFNULL(p.product_name, '')) <> '' " .
            "THEN TRIM(p.product_name) " .
            "ELSE TRIM(l.product_name) END";
    }

    /**
     * 询盘产品汇总：统一产品排行查询写法（四个接口共用）
     */
    private function applyInquiryProductSummaryProductAgg($query)
    {
        $productExpr = $this->getInquiryProductSummaryProductExpr();
        return $query
            ->field($productExpr . " as product_name, COUNT(DISTINCT l.id) as inquiry_count")
            ->group($productExpr)
            ->order('inquiry_count desc, product_name asc');
    }

    /**
     * 询盘产品汇总：基础数据集（严格复用客户列表口径 + 返单排除）
     */
    private function buildInquiryProductSummaryBaseQuery(string $timebucket = '', string $at_time = '', string $month_keys = '')
    {
        $baseIdSql = $this->buildInquirySummaryClientBaseQuery($timebucket, $at_time, true, $month_keys)
            ->field('l.id')
            ->buildSql();

        $query = Db::table([$baseIdSql => 'lb'])
            ->join('crm_leads l', 'lb.id = l.id')
            ->leftJoin('admin a', 'l.pr_user = a.username')
            ->leftJoin('crm_products p', "TRIM(IFNULL(l.product_name, '')) REGEXP '^[0-9]+$' AND p.id = CAST(TRIM(l.product_name) AS UNSIGNED)");

        $query->whereRaw("TRIM(IFNULL(l.product_name, '')) <> ''");
        return $query;
    }

    /**
     * 询盘产品汇总：按团队过滤（支持"未分组"）
     */
    private function applyInquiryProductSummaryTeamFilter($query, string $teamName)
    {
        $teamName = $this->normalizeOrderProductTeamName($teamName);
        $teamExpr = $this->getInquiryProductSummaryTeamExpr();
        $query->whereRaw($teamExpr . " = :team_name", ['team_name' => $teamName]);
        return $query;
    }

    /**
     * 第一屏：公司产品咨询数量排行 + 第二屏团队列表
     */
    public function getInquiryProductSummaryData()
    {
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        try {
            $teamExpr = $this->getInquiryProductSummaryTeamExpr();
            $baseQuery = $this->buildInquiryProductSummaryBaseQuery($timebucket, $at_time, $month_keys);

            $productRows = $this->applyInquiryProductSummaryProductAgg(clone $baseQuery)
                ->select();

            $products = [];
            $totalInquiryCount = 0;
            $rank = 1;
            foreach ((array)$productRows as $row) {
                $count = (int)($row['inquiry_count'] ?? 0);
                $totalInquiryCount += $count;
                $products[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'inquiry_count' => $count,
                ];
            }

            $teamRows = (clone $baseQuery)
                ->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''")
                ->field($teamExpr . " as team_name, COUNT(DISTINCT l.id) as inquiry_count, COUNT(DISTINCT NULLIF(TRIM(l.pr_user), '')) as member_count")
                ->group($teamExpr)
                ->order('inquiry_count desc, team_name asc')
                ->select();

            $teams = [];
            $teamRank = 1;
            foreach ((array)$teamRows as $row) {
                $teamName = $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? ''));
                $teams[] = [
                    'rank' => $teamRank++,
                    'team_name' => $teamName,
                    'inquiry_count' => (int)($row['inquiry_count'] ?? 0),
                    'member_count' => (int)($row['member_count'] ?? 0),
                ];
            }

            $memberRows = (clone $baseQuery)
                ->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''")
                ->field("TRIM(l.pr_user) as username")
                ->group("TRIM(l.pr_user)")
                ->select();

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [
                    'products' => $products,
                    'teams' => $teams,
                ],
                'summary' => [
                    'company' => [
                        'total_product_count' => count($products),
                        'total_inquiry_count' => $totalInquiryCount,
                    ],
                    'team' => [
                        'team_count' => count($teams),
                        'member_count' => count((array)$memberRows),
                    ],
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquiryProductSummary] getInquiryProductSummaryData failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '询盘产品汇总获取失败：' . $e->getMessage(),
                'data' => ['products' => [], 'teams' => []],
                'summary' => [
                    'company' => ['total_product_count' => 0, 'total_inquiry_count' => 0],
                    'team' => ['team_count' => 0, 'member_count' => 0],
                ],
            ]);
        }
    }

    /**
     * 第二屏：指定团队成员列表（按咨询数量排序）
     */
    public function getInquiryProductTeamMembers()
    {
        $team_name  = trim((string)Request::param('team_name', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        if ($team_name === '') {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [],
                'summary' => ['member_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }

        $team_name = $this->normalizeOrderProductTeamName($team_name);

        try {
            $query = $this->buildInquiryProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $this->applyInquiryProductSummaryTeamFilter($query, $team_name);
            $query->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''");

            $rows = $query
                ->field("TRIM(l.pr_user) as username, COUNT(DISTINCT l.id) as inquiry_count")
                ->group("TRIM(l.pr_user)")
                ->order('inquiry_count desc, username asc')
                ->select();

            $data = [];
            $rank = 1;
            $totalInquiryCount = 0;
            foreach ((array)$rows as $row) {
                $count = (int)($row['inquiry_count'] ?? 0);
                $totalInquiryCount += $count;
                $data[] = [
                    'rank' => $rank++,
                    'username' => trim((string)($row['username'] ?? '')),
                    'inquiry_count' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $data,
                'summary' => [
                    'member_count' => count($data),
                    'total_inquiry_count' => $totalInquiryCount,
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquiryProductSummary] getInquiryProductTeamMembers failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队成员获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['member_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：指定团队产品咨询数量排行
     */
    public function getInquiryProductTeamProducts()
    {
        $team_name  = trim((string)Request::param('team_name', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        if ($team_name === '') {
            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => [],
                'summary' => ['team_product_count' => 0, 'total_product_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }

        $team_name = $this->normalizeOrderProductTeamName($team_name);

        try {
            $query = $this->buildInquiryProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $this->applyInquiryProductSummaryTeamFilter($query, $team_name);
            $query->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''");

            $rows = $this->applyInquiryProductSummaryProductAgg($query)->select();

            $data = [];
            $rank = 1;
            $totalInquiryCount = 0;
            foreach ((array)$rows as $row) {
                $count = (int)($row['inquiry_count'] ?? 0);
                $totalInquiryCount += $count;
                $data[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'inquiry_count' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $data,
                'summary' => [
                    'team_product_count' => count($data),
                    'total_product_count' => count($data),
                    'total_inquiry_count' => $totalInquiryCount,
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquiryProductSummary] getInquiryProductTeamProducts failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '团队产品排行获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['team_product_count' => 0, 'total_product_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }
    }

    /**
     * 第三屏：指定成员产品咨询数量排行
     */
    public function getInquiryProductUserProducts()
    {
        $username   = trim((string)Request::param('username', ''));
        $timebucket = Request::param('timebucket', '');
        $at_time    = Request::param('at_time', '');
        $month_keys = trim((string)Request::param('month_keys', ''));

        if ($username === '') {
            return json([
                'code' => 422,
                'msg' => '请先选择成员',
                'data' => [],
                'summary' => ['user_product_count' => 0, 'total_product_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }

        try {
            $query = $this->buildInquiryProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
            $query->where('l.pr_user', '=', $username);

            $rows = $this->applyInquiryProductSummaryProductAgg($query)->select();

            $data = [];
            $rank = 1;
            $totalInquiryCount = 0;
            foreach ((array)$rows as $row) {
                $count = (int)($row['inquiry_count'] ?? 0);
                $totalInquiryCount += $count;
                $data[] = [
                    'rank' => $rank++,
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'inquiry_count' => $count,
                ];
            }

            return json([
                'code' => 0,
                'msg' => '获取成功',
                'data' => $data,
                'summary' => [
                    'user_product_count' => count($data),
                    'total_product_count' => count($data),
                    'total_inquiry_count' => $totalInquiryCount,
                ],
            ]);
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquiryProductSummary] getInquiryProductUserProducts failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '成员产品排行获取失败：' . $e->getMessage(),
                'data' => [],
                'summary' => ['user_product_count' => 0, 'total_product_count' => 0, 'total_inquiry_count' => 0],
            ]);
        }
    }

    /**
     * 导出：询盘产品汇总表（多 Sheet）。
     */
    public function exportInquiryProductSummary()
    {
        try {
            if (!$this->isSpreadsheetExportReady()) {
                return json([
                    'code' => 500,
                    'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出',
                ]);
            }

            $timebucket = trim((string)Request::param('timebucket', ''));
            $at_time    = trim((string)Request::param('at_time', ''));
            $month_keys = trim((string)Request::param('month_keys', ''));
            $team_name  = trim((string)Request::param('team_name', ''));
            $username   = trim((string)Request::param('username', ''));

            $exportData = $this->collectInquiryProductExportData($timebucket, $at_time, $month_keys, $team_name, $username);
            $totalRows = count($exportData['company_rows']) + count($exportData['team_rows']) + count($exportData['user_rows']) + count($exportData['raw_rows']);
            if ($totalRows <= 0) {
                return json([
                    'code' => 404,
                    'msg' => '当前筛选条件下暂无可导出数据',
                ]);
            }

            $spreadsheet = $this->buildMultiSheetSpreadsheet([
                [
                    'title' => '公司询盘产品汇总',
                    'headers' => ['排名', '产品名称', '询盘数量'],
                    'rows' => $exportData['company_rows'],
                ],
                [
                    'title' => '团队询盘产品汇总',
                    'headers' => ['排名', '团队名称', '产品名称', '询盘数量'],
                    'rows' => $exportData['team_rows'],
                ],
                [
                    'title' => '个人询盘产品汇总',
                    'headers' => ['团队', '成员', '产品名称', '询盘数量'],
                    'rows' => $exportData['user_rows'],
                ],
                [
                    'title' => '原始询盘明细',
                    'headers' => ['客户ID', '客户名称', '负责人', '团队', '产品名称', '询盘来源', '运营端口ID', '录入时间', '转客户时间'],
                    'rows' => $exportData['raw_rows'],
                ],
            ]);

            $this->outputSpreadsheet($spreadsheet, '询盘产品汇总表', 'inquiry_product_summary');
        } catch (\Throwable $e) {
            \think\facade\Log::error('[InquiryProductSummary] exportInquiryProductSummary failed: ' . $e->getMessage());
            return json([
                'code' => 500,
                'msg' => '导出失败：' . $e->getMessage(),
            ]);
        }
    }

    /**
     * 组装导出数据：询盘产品汇总表。
     */
    private function collectInquiryProductExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', string $team_name = '', string $username = ''): array
    {
        $teamExpr    = $this->getInquiryProductSummaryTeamExpr();
        $productExpr = $this->getInquiryProductSummaryProductExpr();
        $baseQuery   = $this->buildInquiryProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
        if (trim($team_name) !== '') {
            $this->applyInquiryProductSummaryTeamFilter($baseQuery, trim($team_name));
        }
        if (trim($username) !== '') {
            $baseQuery->where('l.pr_user', '=', trim($username));
        }

        $companyRowsRaw = $this->applyInquiryProductSummaryProductAgg(clone $baseQuery)->select();
        $companyRows = [];
        foreach ((array)$companyRowsRaw as $idx => $row) {
            $companyRows[] = [
                $idx + 1,
                (string)($row['product_name'] ?? ''),
                (int)($row['inquiry_count'] ?? 0),
            ];
        }

        $teamRowsRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, {$productExpr} as product_name, COUNT(DISTINCT l.id) as inquiry_count")
            ->group($teamExpr . "," . $productExpr)
            ->order('team_name asc, inquiry_count desc, product_name asc')
            ->select();
        $teamRows = [];
        foreach ((array)$teamRowsRaw as $idx => $row) {
            $teamRows[] = [
                $idx + 1,
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                (string)($row['product_name'] ?? ''),
                (int)($row['inquiry_count'] ?? 0),
            ];
        }

        $userRowsRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(l.pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, TRIM(l.pr_user) as username, {$productExpr} as product_name, COUNT(DISTINCT l.id) as inquiry_count")
            ->group($teamExpr . ",TRIM(l.pr_user)," . $productExpr)
            ->order('team_name asc, username asc, inquiry_count desc, product_name asc')
            ->select();
        $userRows = [];
        foreach ((array)$userRowsRaw as $row) {
            $userRows[] = [
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                (string)($row['username'] ?? ''),
                (string)($row['product_name'] ?? ''),
                (int)($row['inquiry_count'] ?? 0),
            ];
        }

        $leadColumns  = $this->getInquirySummaryLeadsColumns();
        $hasKhName    = in_array('kh_name', $leadColumns, true);
        $hasAtTime    = in_array('at_time', $leadColumns, true);
        $hasToKhTime  = in_array('to_kh_time', $leadColumns, true);
        $rawRowsRaw = (clone $baseQuery)
            ->leftJoin('crm_inquiry ci', 'l.inquiry_id = ci.id')
            ->field([
                'l.id as lead_id',
                $teamExpr . ' as team_name',
                "TRIM(IFNULL(l.pr_user, '')) as username",
                $productExpr . ' as product_name',
                "IFNULL(ci.inquiry_name, '未知来源') as inquiry_name",
                "TRIM(IFNULL(l.port_id, '')) as port_ids",
                $hasKhName   ? "IFNULL(l.kh_name, '') as kh_name"       : "'' as kh_name",
                $hasAtTime   ? "IFNULL(l.at_time, '') as at_time"        : "'' as at_time",
                $hasToKhTime ? "IFNULL(l.to_kh_time, '') as to_kh_time" : "'' as to_kh_time",
            ])
            ->order('l.id desc')
            ->limit(20000)
            ->select();
        $rawRows = [];
        foreach ((array)$rawRowsRaw as $row) {
            $rawRows[] = [
                (int)($row['lead_id'] ?? 0),
                (string)($row['kh_name'] ?? ''),
                (string)($row['username'] ?? ''),
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                (string)($row['product_name'] ?? ''),
                (string)($row['inquiry_name'] ?? '未知来源'),
                (string)($row['port_ids'] ?? ''),
                (string)($row['at_time'] ?? ''),
                (string)($row['to_kh_time'] ?? ''),
            ];
        }

        return [
            'company_rows' => $companyRows,
            'team_rows'    => $teamRows,
            'user_rows'    => $userRows,
            'raw_rows'     => $rawRows,
        ];
    }

    /**
     * 组装导出数据：订单产品汇总表。
     */
    private function collectOrderProductExportData(string $timebucket = '', string $at_time = '', string $month_keys = '', string $team_name = '', string $username = ''): array
    {
        $teamExpr = $this->getOrderProductSummaryTeamExpr();

        $baseQuery = $this->buildOrderProductSummaryBaseQuery($timebucket, $at_time, $month_keys);
        if (trim($team_name) !== '') {
            $this->applyOrderProductSummaryTeamFilter($baseQuery, trim($team_name));
        }
        if (trim($username) !== '') {
            $baseQuery->where('o.pr_user', '=', trim($username));
        }

        $companyRowsRaw = (clone $baseQuery)
            ->field("oi.product_name, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit")
            ->group('oi.product_name')
            ->order('total_profit desc, sale_qty desc, oi.product_name asc')
            ->select();
        $companyRows = [];
        foreach ((array)$companyRowsRaw as $idx => $row) {
            $companyRows[] = [
                $idx + 1,
                (string)($row['product_name'] ?? ''),
                number_format((float)($row['total_profit'] ?? 0), 2, '.', ''),
                (float)($row['sale_qty'] ?? 0),
            ];
        }

        $teamRowsRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit, COUNT(DISTINCT NULLIF(TRIM(o.pr_user), '')) as member_count")
            ->group($teamExpr)
            ->order('total_profit desc, sale_qty desc, team_name asc')
            ->select();
        $teamRows = [];
        foreach ((array)$teamRowsRaw as $idx => $row) {
            $teamRows[] = [
                $idx + 1,
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                number_format((float)($row['total_profit'] ?? 0), 2, '.', ''),
                (float)($row['sale_qty'] ?? 0),
                (int)($row['member_count'] ?? 0),
            ];
        }

        $userRowsRaw = (clone $baseQuery)
            ->whereRaw("TRIM(IFNULL(o.pr_user, '')) <> ''")
            ->field($teamExpr . " as team_name, TRIM(o.pr_user) as username, oi.product_name, COUNT(DISTINCT o.id) as order_count, SUM(IFNULL(oi.qty,0)) as sale_qty, SUM(IFNULL(oi.sub_profit,0)) as total_profit")
            ->group($teamExpr . ",TRIM(o.pr_user),oi.product_name")
            ->order('team_name asc, username asc, total_profit desc, sale_qty desc')
            ->select();
        $userRows = [];
        foreach ((array)$userRowsRaw as $row) {
            $userRows[] = [
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                (string)($row['username'] ?? ''),
                (string)($row['product_name'] ?? ''),
                (int)($row['order_count'] ?? 0),
                number_format((float)($row['total_profit'] ?? 0), 2, '.', ''),
                (float)($row['sale_qty'] ?? 0),
            ];
        }

        $rawRowsRaw = (clone $baseQuery)
            ->field($teamExpr . " as team_name, o.id as order_id, o.order_no, o.order_time, TRIM(o.pr_user) as username, oi.product_name, IFNULL(oi.qty,0) as sale_qty, IFNULL(oi.sub_profit,0) as total_profit")
            ->order('o.id desc')
            ->limit(20000)
            ->select();
        $rawRows = [];
        foreach ((array)$rawRowsRaw as $row) {
            $rawRows[] = [
                (int)($row['order_id'] ?? 0),
                (string)($row['order_no'] ?? ''),
                (string)($row['order_time'] ?? ''),
                (string)($row['username'] ?? ''),
                $this->normalizeOrderProductTeamName((string)($row['team_name'] ?? '')),
                (string)($row['product_name'] ?? ''),
                (float)($row['sale_qty'] ?? 0),
                number_format((float)($row['total_profit'] ?? 0), 2, '.', ''),
            ];
        }

        return [
            'company_rows' => $companyRows,
            'team_rows' => $teamRows,
            'user_rows' => $userRows,
            'raw_rows' => $rawRows,
        ];
    }

}
