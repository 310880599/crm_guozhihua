<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use app\admin\model\Admin;

class Achievement extends Common
{
    /**
     * 旺春PK小组 - 业绩看板配置（后续可在此调整）
     */
    // 活动标题
    private $wcDashboardTitle = '旺春PK小组 · 业绩看板';
    // 活动标题
    private $wcDashboardTitlePermanent = '旺春现有团队 · 业绩看板';

    // 统计周期文本（展示用）
    private $wcStatPeriodText = '2026.03.01 - 2026.04.15';

    // 统计时间范围（用于筛选订单，建议与上面文本保持一致）
    private $wcStatStartTime = '2026-03-01 00:00:00'; // 统计开始时间
    private $wcStatEndTime   = '2026-04-15 23:59:59'; // 统计结束时间

    /**
     * 订单时间字段：用于统计周期过滤
     * order_time = 成交时间（业务含义，与统计周期一致，推荐）
     * create_time = 订单创建时间（若需按录入时间统计可改为 create_time）
     */
    private $wcOrderTimeField = 'order_time';

    // 是否包含零业绩成员（true：展示所有配置成员；false：仅展示有业绩的成员）
    private $wcIncludeZeroMember = true;

    // 左侧榜单：显示前几名小组（0 表示全部）
    private $wcLeftGroupTopLimit = 0;

    // 右侧每组成员是否按业绩降序排列（true：按业绩从高到低排序；false：按配置顺序）
    private $wcRightMemberSortDesc = true;

    // 右侧小组块排序字段：total_amount / avg_amount
    private $wcRightGroupSortField = 'total_amount';

    // 右侧小组块排序方向：desc / asc
    private $wcRightGroupSortDirection = 'desc';

    // 小组人均业绩排序方向：desc / asc
    private $wcGroupAvgSortDirection = 'desc';

    /**
     * 活动小组名单配置（请严格根据 Excel 名单维护）
     *
     * 键为小组名称，值为该小组成员数组。
     * 成员可为：纯姓名字符串，或 ['id' => 管理员ID, 'name' => '姓名']（优先按 pr_user_id 匹配）。
     * 姓名与订单表 pr_user 匹配时会做 trim。
     */
    private $pkGroups = [
        '破局队' => ['周岚', '李燕慧', '卢慧贤', '曹玲艳', '冯婷婷', '张杰', '宋蒙', '闫雪'],
        '飞马队' => ['张红玲', '田丽园', '杨惠岚', '钱秀霞', '张甜甜', '胡沙沙', '司月鹅', '杨青青'],
        '领航队' => ['张二凤', '贾聪乐', '魏红岩', '赵静', '刘丹丹', '张艳艳', '张书芹', '陈培培'],
        '启胜队' => ['胡晓惠', '张珊珊', '职利杰', '周燕', '谢园园', '刘小方', '安晓娜', '罗亭'],
        '冲锋队' => ['刘燕燕', '韩利敏', '樊培培', '张春辉', '常绍瑞', '陈义笑', '张景春', '田瑞云'],
        '亿马当先' => ['苗雪会', '王静', '刘玉杰', '张莹', '王俊', '李剪阁', '彭玉', '邵朋杰', '李小萌'],
        '金马队' => ['张岩', '李灵燕', '李营', '连书会', '冯燕平', '周勤勤', '毛卫娟', '范晶晶'],
        '战神队' => ['拜云梦', '王亚丽', '高慢慢', '袁璟', '申振宇', '何文可', '吴银霞', '宋秀丽'],
    ];


    private $pkGroupsPermanent = [
        '勇敢者' => ['张春辉', '宋秀丽', '韩利敏', '王静', '周勤勤', '王俊', '刘燕燕', '闫雪'],
        '追光者' => ['谢园园', '安晓娜', '田丽园', '常绍瑞', '司月鹅', '李灵燕', '刘丹丹', '罗亭','冯婷婷'],
        '扬帆队' => ['魏红岩', '王亚丽', '申振宇', '张书芹', '苗雪会', '张红玲', '何文可','范晶晶'],
        '超越队' => ['李营', '袁璟', '李燕慧', '周燕', '杨青青', '胡晓惠',  '曹玲艳','职利杰'],
        '步步高' => ['张杰', '张珊珊', '连书会', '赵静', '冯燕平', '张甜甜', '张艳艳'],
        '齐心聚力' => ['卢慧贤', '樊培培', '高慢慢', '陈义笑', '周岚', '张岩', '杨惠岚', '宋蒙', '钱秀霞'],
        '疯狂蚂蚁' => ['吴银霞', '拜云梦', '陈培培', '李剪阁', '张莹', '邵朋杰', '李小萌', '刘玉杰'],
        '无畏蜜蜂' => ['彭玉', '刘小方', '胡沙沙', '田瑞云', '张二凤', '贾聪乐', '张景春', '毛卫娟'],
    ];

    public function temporaryAchievement()
    {
        // 首次进入页面仍然采用服务端渲染完整数据
        $data  = $this->buildTemporaryAchievementData();
        $stamp = $this->getTemporaryAchievementStamp();

        $this->assign('dashboardTitle', $this->wcDashboardTitle);
        $this->assign('periodText', $this->wcStatPeriodText);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        // 首次渲染时把当前版本标识传给前端，便于心跳对比
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/temporary_achievement');
    }

    public function permanentAchievement()
    {
        // 首次进入页面仍然采用服务端渲染完整数据；使用永久团队专属 stamp，解决复制 PK 小组后错误复用 temporary 轮询链路的问题
        $data  = $this->buildPermanentAchievementData();
        $stamp = $this->getPermanentAchievementStamp();

        $this->assign('dashboardTitle', $this->wcDashboardTitlePermanent);
        $this->assign('periodText', $this->wcStatPeriodText);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        // 首次渲染时把当前版本标识传给前端，便于心跳对比
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/permanent_achievement');
    }

    /**
     * 构建旺春PK小组临时业绩看板数据
     * 数据来源：crm_client_order，仅 check_status=2，按配置名单与统计周期汇总。
     *
     * @return array{groupAvgRankList:array, memberRankGroupList:array}
     */
    private function buildTemporaryAchievementData()
    {
        $groupAvgRankList    = [];
        $memberRankGroupList = [];

        if (empty($this->pkGroups) || !is_array($this->pkGroups)) {
            return [
                'groupAvgRankList'    => $groupAvgRankList,
                'memberRankGroupList' => $memberRankGroupList,
            ];
        }

        $startTime = $this->wcStatStartTime;
        $endTime   = $this->wcStatEndTime;
        $timeField = $this->wcOrderTimeField;

        // 时间字段白名单，防止被错误值污染
        $allowedTimeFields = ['order_time', 'create_time'];
        if (!in_array($timeField, $allowedTimeFields, true)) {
            $timeField = 'order_time';
        }

        // 一次查询：当前周期内审核通过订单，按 pr_user_id 聚合纯利润（profit 作为业绩口径，避免 N+1）
        $orderRows = Db::table('crm_client_order')
            ->alias('o')
            ->field('o.pr_user_id, o.pr_user, SUM(COALESCE(o.profit, 0)) AS total_profit')
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->group('o.pr_user_id, o.pr_user')
            ->select();

        // 按 pr_user_id 与 pr_user（trim）建立业绩映射，此处业绩统计口径为 profit 纯利润，空值按 0
        $achievementByUserId = [];
        $achievementByName   = [];
        if (is_array($orderRows)) {
            foreach ($orderRows as $row) {
                $uid    = isset($row['pr_user_id']) ? (int)$row['pr_user_id'] : 0;
                $name   = isset($row['pr_user']) ? trim((string)$row['pr_user']) : '';
                $amount = isset($row['total_profit']) ? (float)$row['total_profit'] : 0.0;
                $amount = round($amount, 2);
                if ($uid > 0) {
                    if (!isset($achievementByUserId[$uid])) {
                        $achievementByUserId[$uid] = 0.0;
                    }
                    $achievementByUserId[$uid] += $amount;
                }
                if ($name !== '') {
                    if (!isset($achievementByName[$name])) {
                        $achievementByName[$name] = 0.0;
                    }
                    $achievementByName[$name] += $amount;
                }
            }
        }
        // 同一人可能多条 group 记录（pr_user_id 不同），已按 id 分组；按 id 取时用 id 对应金额，按 name 取时用 name 对应金额（可能重复加，这里以 DB 的 group by pr_user_id 为准，所以按 id 汇总正确；按 name 汇总可能重复，但业务上通常一人一个 id）—— 为简单起见，查询已按 pr_user_id 分组，所以每个 id 一条，每个 name 可能对应多条（不同 id 同 name 的情况少见）。我们按 id 优先匹配，没有 id 再按 name。
        // 上面按 id 分组后，同一 id 只有一条，所以 achievementByUserId 每个 id 一个值；按 name 时，若多条记录同一 name 不同 id，会重复累加。这里保持：一个 pr_user_id 一条，name 可能重复。为准确，按 id 聚合后，同一 name 可能对应多个 id，我们不再按 name 二次聚合，而是：每条 row 的 pr_user 可能相同（重名），所以 achievementByName 按 name 累加会重复。正确做法：只按 pr_user_id 聚合，然后建立 id->amount, id->name；匹配时优先 id，无 id 时用 name，但 name 对应的金额我们只能从“按 name 再聚一次”得到。所以查询改为 group by pr_user_id，得到 (pr_user_id, pr_user, total_money)。然后 achievementByUserId[id] = amount。对于 achievementByName：同一 name 可能对应多个 id（重名），我们按 name 汇总时应该把同一 name 的多个 id 的金额加起来。所以先按 id 聚，再遍历每条 row，按 name 累加：achievementByName[name] += row.total_money。但这样同一 name 多 id 会加多次，正确。所以上面代码逻辑是对的：每条 row 是 group by pr_user_id 的结果，一个 id 一条；我们既按 id 存又按 name 存，按 name 存时多条 row 可能同一 name（不同 id），所以 name 会累加多次，正确。但按 id 存时我们写的是 +=，其实每个 id 只有一条，所以没问题。OK。

        foreach ($this->pkGroups as $groupName => $memberList) {
            $groupName = (string)$groupName;
            $members   = is_array($memberList) ? $memberList : [];
            $memberCountConfig = count($members);

            $groupMembersStat = [];
            $groupTotalAmount = 0.0;

            $resolvedMembers = []; // [ ['name'=>展示名, 'amount'=>金额], ... ]
            foreach ($members as $member) {
                $name  = '';
                $uid   = null;
                if (is_array($member)) {
                    $name = isset($member['name']) ? trim((string)$member['name']) : '';
                    $uid  = isset($member['id']) ? (int)$member['id'] : null;
                } else {
                    $name = trim((string)$member);
                }
                if ($name === '' && $uid === null) {
                    continue;
                }
                if ($name === '' && $uid !== null) {
                    $name = (string)$uid; // 占位，避免空名
                }

                $amount = 0.0;
                if ($uid !== null && $uid > 0 && isset($achievementByUserId[$uid])) {
                    $amount = (float)$achievementByUserId[$uid];
                } elseif ($name !== '' && isset($achievementByName[$name])) {
                    $amount = (float)$achievementByName[$name];
                }
                $amount = round($amount, 2);

                $groupTotalAmount += $amount;

                if (!$this->wcIncludeZeroMember && $amount <= 0) {
                    continue;
                }
                $resolvedMembers[] = ['name' => $name, 'amount' => $amount];
            }

            $groupTotalAmount = round($groupTotalAmount, 2);
            $avgAmount        = 0.0;
            if ($memberCountConfig > 0) {
                $avgAmount = round($groupTotalAmount / $memberCountConfig, 2);
            }

            // 成员排序与排名
            if (!empty($resolvedMembers) && $this->wcRightMemberSortDesc) {
                usort($resolvedMembers, function ($a, $b) {
                    if ($a['amount'] === $b['amount']) return 0;
                    return $a['amount'] < $b['amount'] ? 1 : -1;
                });
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            } else {
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            }

            $groupAvgRankList[] = [
                'rank'          => 0,
                'group_name'    => $groupName,
                'total_amount'  => $groupTotalAmount,
                'member_count'  => $memberCountConfig,
                'avg_amount'    => $avgAmount,
            ];

            $memberRankGroupList[] = [
                'group_name'   => $groupName,
                'total_amount' => $groupTotalAmount,
                'avg_amount'   => $avgAmount,
                'members'      => $resolvedMembers,
            ];
        }

        // 小组人均排序与左侧排名
        if (!empty($groupAvgRankList)) {
            // 左侧小组人均排行排序规则：
            // 1）按 avg_amount 降序
            // 2）avg_amount 相同则按 total_amount 降序
            // 3）avg_amount 和 total_amount 都相同则按 group_name 升序，保证顺序稳定
            usort($groupAvgRankList, function ($a, $b) {
                $aAvg = isset($a['avg_amount']) ? (float)$a['avg_amount'] : 0.0;
                $bAvg = isset($b['avg_amount']) ? (float)$b['avg_amount'] : 0.0;
                if ($aAvg !== $bAvg) {
                    return ($aAvg < $bAvg) ? 1 : -1; // avg_amount 降序
                }

                $aTotal = isset($a['total_amount']) ? (float)$a['total_amount'] : 0.0;
                $bTotal = isset($b['total_amount']) ? (float)$b['total_amount'] : 0.0;
                if ($aTotal !== $bTotal) {
                    return ($aTotal < $bTotal) ? 1 : -1; // total_amount 降序
                }

                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName); // group_name 升序
            });
            $rank = 1;
            foreach ($groupAvgRankList as $idx => $item) {
                $groupAvgRankList[$idx]['rank'] = $rank++;
            }
        }

        // 右侧小组块排序：默认按总业绩从高到低，同业绩时按人均业绩从高到低，再按小组名升序
        // 当前实现：优先使用配置字段（wcRightGroupSortField），仅允许 total_amount / avg_amount；
        // 如果配置异常会自动回退到 total_amount，方向默认按 wcRightGroupSortDirection（非法值回退为 desc）。
        if (!empty($memberRankGroupList)) {
            $sortField = $this->wcRightGroupSortField;
            $allowedFields = ['total_amount', 'avg_amount'];
            if (!in_array($sortField, $allowedFields, true)) {
                $sortField = 'total_amount';
            }

            $direction = strtolower($this->wcRightGroupSortDirection) === 'asc' ? 'asc' : 'desc';

            usort($memberRankGroupList, function ($a, $b) use ($sortField, $direction) {
                $aPrimary = isset($a[$sortField]) ? (float)$a[$sortField] : 0.0;
                $bPrimary = isset($b[$sortField]) ? (float)$b[$sortField] : 0.0;

                if ($aPrimary !== $bPrimary) {
                    if ($direction === 'asc') {
                        return ($aPrimary < $bPrimary) ? -1 : 1;
                    }
                    return ($aPrimary < $bPrimary) ? 1 : -1;
                }

                // 主字段相同，使用另一业绩字段做二次排序（始终按降序）
                $secondaryField = ($sortField === 'total_amount') ? 'avg_amount' : 'total_amount';
                $aSecondary = isset($a[$secondaryField]) ? (float)$a[$secondaryField] : 0.0;
                $bSecondary = isset($b[$secondaryField]) ? (float)$b[$secondaryField] : 0.0;

                if ($aSecondary !== $bSecondary) {
                    return ($aSecondary < $bSecondary) ? 1 : -1; // 二级字段降序
                }

                // 两个业绩字段都相同，按 group_name 升序保证顺序稳定
                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName);
            });
        }

        $leftLimit = (int)$this->wcLeftGroupTopLimit;
        if ($leftLimit > 0) {
            $groupAvgRankList = array_slice($groupAvgRankList, 0, $leftLimit);
        }

        return [
            'groupAvgRankList'    => $groupAvgRankList,
            'memberRankGroupList' => $memberRankGroupList,
        ];
    }



        /**
     * 构建旺春PK小组永久业绩看板数据
     * 数据来源：crm_client_order，仅 check_status=2，按配置名单与统计周期汇总。
     *
     * @return array{groupAvgRankList:array, memberRankGroupList:array}
     */
    private function buildPermanentAchievementData()
    {
        $groupAvgRankList    = [];
        $memberRankGroupList = [];

        if (empty($this->pkGroupsPermanent) || !is_array($this->pkGroupsPermanent)) {
            return [
                'groupAvgRankList'    => $groupAvgRankList,
                'memberRankGroupList' => $memberRankGroupList,
            ];
        }

        $startTime = $this->wcStatStartTime;
        $endTime   = $this->wcStatEndTime;
        $timeField = $this->wcOrderTimeField;

        // 时间字段白名单，防止被错误值污染
        $allowedTimeFields = ['order_time', 'create_time'];
        if (!in_array($timeField, $allowedTimeFields, true)) {
            $timeField = 'order_time';
        }

        // 一次查询：当前周期内审核通过订单，按 pr_user_id 聚合纯利润（profit 作为业绩口径，避免 N+1）
        $orderRows = Db::table('crm_client_order')
            ->alias('o')
            ->field('o.pr_user_id, o.pr_user, SUM(COALESCE(o.profit, 0)) AS total_profit')
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->group('o.pr_user_id, o.pr_user')
            ->select();

        // 按 pr_user_id 与 pr_user（trim）建立业绩映射，此处业绩统计口径为 profit 纯利润，空值按 0
        $achievementByUserId = [];
        $achievementByName   = [];
        if (is_array($orderRows)) {
            foreach ($orderRows as $row) {
                $uid    = isset($row['pr_user_id']) ? (int)$row['pr_user_id'] : 0;
                $name   = isset($row['pr_user']) ? trim((string)$row['pr_user']) : '';
                $amount = isset($row['total_profit']) ? (float)$row['total_profit'] : 0.0;
                $amount = round($amount, 2);
                if ($uid > 0) {
                    if (!isset($achievementByUserId[$uid])) {
                        $achievementByUserId[$uid] = 0.0;
                    }
                    $achievementByUserId[$uid] += $amount;
                }
                if ($name !== '') {
                    if (!isset($achievementByName[$name])) {
                        $achievementByName[$name] = 0.0;
                    }
                    $achievementByName[$name] += $amount;
                }
            }
        }
        // 同一人可能多条 group 记录（pr_user_id 不同），已按 id 分组；按 id 取时用 id 对应金额，按 name 取时用 name 对应金额（可能重复加，这里以 DB 的 group by pr_user_id 为准，所以按 id 汇总正确；按 name 汇总可能重复，但业务上通常一人一个 id）—— 为简单起见，查询已按 pr_user_id 分组，所以每个 id 一条，每个 name 可能对应多条（不同 id 同 name 的情况少见）。我们按 id 优先匹配，没有 id 再按 name。
        // 上面按 id 分组后，同一 id 只有一条，所以 achievementByUserId 每个 id 一个值；按 name 时，若多条记录同一 name 不同 id，会重复累加。这里保持：一个 pr_user_id 一条，name 可能重复。为准确，按 id 聚合后，同一 name 可能对应多个 id，我们不再按 name 二次聚合，而是：每条 row 的 pr_user 可能相同（重名），所以 achievementByName 按 name 累加会重复。正确做法：只按 pr_user_id 聚合，然后建立 id->amount, id->name；匹配时优先 id，无 id 时用 name，但 name 对应的金额我们只能从“按 name 再聚一次”得到。所以查询改为 group by pr_user_id，得到 (pr_user_id, pr_user, total_money)。然后 achievementByUserId[id] = amount。对于 achievementByName：同一 name 可能对应多个 id（重名），我们按 name 汇总时应该把同一 name 的多个 id 的金额加起来。所以先按 id 聚，再遍历每条 row，按 name 累加：achievementByName[name] += row.total_money。但这样同一 name 多 id 会加多次，正确。所以上面代码逻辑是对的：每条 row 是 group by pr_user_id 的结果，一个 id 一条；我们既按 id 存又按 name 存，按 name 存时多条 row 可能同一 name（不同 id），所以 name 会累加多次，正确。但按 id 存时我们写的是 +=，其实每个 id 只有一条，所以没问题。OK。

        foreach ($this->pkGroupsPermanent as $groupName => $memberList) {
            $groupName = (string)$groupName;
            $members   = is_array($memberList) ? $memberList : [];
            $memberCountConfig = count($members);

            $groupMembersStat = [];
            $groupTotalAmount = 0.0;

            $resolvedMembers = []; // [ ['name'=>展示名, 'amount'=>金额], ... ]
            foreach ($members as $member) {
                $name  = '';
                $uid   = null;
                if (is_array($member)) {
                    $name = isset($member['name']) ? trim((string)$member['name']) : '';
                    $uid  = isset($member['id']) ? (int)$member['id'] : null;
                } else {
                    $name = trim((string)$member);
                }
                if ($name === '' && $uid === null) {
                    continue;
                }
                if ($name === '' && $uid !== null) {
                    $name = (string)$uid; // 占位，避免空名
                }

                $amount = 0.0;
                if ($uid !== null && $uid > 0 && isset($achievementByUserId[$uid])) {
                    $amount = (float)$achievementByUserId[$uid];
                } elseif ($name !== '' && isset($achievementByName[$name])) {
                    $amount = (float)$achievementByName[$name];
                }
                $amount = round($amount, 2);

                $groupTotalAmount += $amount;

                if (!$this->wcIncludeZeroMember && $amount <= 0) {
                    continue;
                }
                $resolvedMembers[] = ['name' => $name, 'amount' => $amount];
            }

            $groupTotalAmount = round($groupTotalAmount, 2);
            $avgAmount        = 0.0;
            if ($memberCountConfig > 0) {
                $avgAmount = round($groupTotalAmount / $memberCountConfig, 2);
            }

            // 成员排序与排名
            if (!empty($resolvedMembers) && $this->wcRightMemberSortDesc) {
                usort($resolvedMembers, function ($a, $b) {
                    if ($a['amount'] === $b['amount']) return 0;
                    return $a['amount'] < $b['amount'] ? 1 : -1;
                });
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            } else {
                $rank = 1;
                foreach ($resolvedMembers as $idx => $item) {
                    $resolvedMembers[$idx]['rank'] = $rank++;
                }
            }

            $groupAvgRankList[] = [
                'rank'          => 0,
                'group_name'    => $groupName,
                'total_amount'  => $groupTotalAmount,
                'member_count'  => $memberCountConfig,
                'avg_amount'    => $avgAmount,
            ];

            $memberRankGroupList[] = [
                'group_name'   => $groupName,
                'total_amount' => $groupTotalAmount,
                'avg_amount'   => $avgAmount,
                'members'      => $resolvedMembers,
            ];
        }

        // 小组人均排序与左侧排名
        if (!empty($groupAvgRankList)) {
            // 左侧小组人均排行排序规则：
            // 1）按 avg_amount 降序
            // 2）avg_amount 相同则按 total_amount 降序
            // 3）avg_amount 和 total_amount 都相同则按 group_name 升序，保证顺序稳定
            usort($groupAvgRankList, function ($a, $b) {
                $aAvg = isset($a['avg_amount']) ? (float)$a['avg_amount'] : 0.0;
                $bAvg = isset($b['avg_amount']) ? (float)$b['avg_amount'] : 0.0;
                if ($aAvg !== $bAvg) {
                    return ($aAvg < $bAvg) ? 1 : -1; // avg_amount 降序
                }

                $aTotal = isset($a['total_amount']) ? (float)$a['total_amount'] : 0.0;
                $bTotal = isset($b['total_amount']) ? (float)$b['total_amount'] : 0.0;
                if ($aTotal !== $bTotal) {
                    return ($aTotal < $bTotal) ? 1 : -1; // total_amount 降序
                }

                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName); // group_name 升序
            });
            $rank = 1;
            foreach ($groupAvgRankList as $idx => $item) {
                $groupAvgRankList[$idx]['rank'] = $rank++;
            }
        }

        // 右侧小组块排序：默认按总业绩从高到低，同业绩时按人均业绩从高到低，再按小组名升序
        // 当前实现：优先使用配置字段（wcRightGroupSortField），仅允许 total_amount / avg_amount；
        // 如果配置异常会自动回退到 total_amount，方向默认按 wcRightGroupSortDirection（非法值回退为 desc）。
        if (!empty($memberRankGroupList)) {
            $sortField = $this->wcRightGroupSortField;
            $allowedFields = ['total_amount', 'avg_amount'];
            if (!in_array($sortField, $allowedFields, true)) {
                $sortField = 'total_amount';
            }

            $direction = strtolower($this->wcRightGroupSortDirection) === 'asc' ? 'asc' : 'desc';

            usort($memberRankGroupList, function ($a, $b) use ($sortField, $direction) {
                $aPrimary = isset($a[$sortField]) ? (float)$a[$sortField] : 0.0;
                $bPrimary = isset($b[$sortField]) ? (float)$b[$sortField] : 0.0;

                if ($aPrimary !== $bPrimary) {
                    if ($direction === 'asc') {
                        return ($aPrimary < $bPrimary) ? -1 : 1;
                    }
                    return ($aPrimary < $bPrimary) ? 1 : -1;
                }

                // 主字段相同，使用另一业绩字段做二次排序（始终按降序）
                $secondaryField = ($sortField === 'total_amount') ? 'avg_amount' : 'total_amount';
                $aSecondary = isset($a[$secondaryField]) ? (float)$a[$secondaryField] : 0.0;
                $bSecondary = isset($b[$secondaryField]) ? (float)$b[$secondaryField] : 0.0;

                if ($aSecondary !== $bSecondary) {
                    return ($aSecondary < $bSecondary) ? 1 : -1; // 二级字段降序
                }

                // 两个业绩字段都相同，按 group_name 升序保证顺序稳定
                $aName = isset($a['group_name']) ? (string)$a['group_name'] : '';
                $bName = isset($b['group_name']) ? (string)$b['group_name'] : '';
                if ($aName === $bName) {
                    return 0;
                }
                return strcmp($aName, $bName);
            });
        }

        $leftLimit = (int)$this->wcLeftGroupTopLimit;
        if ($leftLimit > 0) {
            $groupAvgRankList = array_slice($groupAvgRankList, 0, $leftLimit);
        }

        return [
            'groupAvgRankList'    => $groupAvgRankList,
            'memberRankGroupList' => $memberRankGroupList,
        ];
    }

    /**
     * 构建旺春PK小组临时业绩看板的签名摘要数据
     *
     * 说明：
     * 1）第一层摘要 strictly 按当前统计周期 + check_status = 2 汇总，与最终看板口径保持一致；
     * 2）第二层摘要只做“近期变更捕捉”，用于触发前端刷新，允许包含未审核或统计周期边界附近的订单，
     *    即使最终排行榜数据没有变化，最多只是多拉一次完整接口，服务器可以接受。
     *
     * 这样可以避免仅依赖 order_time 导致：
     * - 新建订单但尚未审核、或审核后未改动 order_time 时，stamp 不变化；
     * - 订单利润 profit / 审核状态 check_status / 负责人等更新仅改 ut_time / audit_time 时，stamp 不变化。
     *
     * @return array
     */
    private function buildTemporaryAchievementSignatureData()
    {
        $startTime = $this->wcStatStartTime;
        $endTime   = $this->wcStatEndTime;
        $timeField = $this->wcOrderTimeField;

        // 时间字段白名单，防止被错误值污染（需与 buildTemporaryAchievementData 中保持一致）
        $allowedTimeFields = ['order_time', 'create_time'];
        if (!in_array($timeField, $allowedTimeFields, true)) {
            $timeField = 'order_time';
        }

        // 第一层：当前统计周期内、审核通过订单的聚合摘要（完全与看板统计口径一致）
        $periodRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS cnt',
                'SUM(COALESCE(o.profit,0)) AS sum_profit',
                'MAX(o.id) AS max_id',
                'MAX(o.' . $timeField . ') AS max_order_time',
                'MAX(o.create_time) AS max_create_time',
                'MAX(o.ut_time) AS max_ut_time',
                'MAX(o.audit_time) AS max_audit_time',
            ])
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->find();

        $periodSummary = [
            // 以下字段直接反映“榜单最终结果”是否发生变化（数量 / 业绩 / 最大ID / 时间戳）
            'cnt'             => isset($periodRow['cnt']) ? (int)$periodRow['cnt'] : 0,
            'sum_profit'      => isset($periodRow['sum_profit']) ? (float)$periodRow['sum_profit'] : 0.0,
            'max_id'          => isset($periodRow['max_id']) ? (int)$periodRow['max_id'] : 0,
            'max_order_time'  => isset($periodRow['max_order_time']) ? (string)$periodRow['max_order_time'] : '',
            'max_create_time' => isset($periodRow['max_create_time']) ? (string)$periodRow['max_create_time'] : '',
            'max_ut_time'     => isset($periodRow['max_ut_time']) ? (string)$periodRow['max_ut_time'] : '',
            'max_audit_time'  => isset($periodRow['max_audit_time']) ? (string)$periodRow['max_audit_time'] : '',
        ];

        // 第二层：最近一段时间内的订单变更摘要（主要用于捕捉“新增 / 审核 / 修改”动作）
        // 说明：
        // - 这里不限制 check_status = 2，是为了在订单从未审核 -> 已审核（check_status 变为 2）时也能及时触发刷新；
        // - 仍然只关心与看板相关的订单表 crm_client_order，避免对其他业务表做全表扫描。
        $recentStart = date('Y-m-d H:i:s', strtotime('-3 days')); // 近 3 天变更即可触发刷新，窗口可根据需要调整

        $recentRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS recent_cnt',
                'MAX(o.id) AS recent_max_id',
                'MAX(o.create_time) AS recent_max_create_time',
                'MAX(o.ut_time) AS recent_max_ut_time',
                'MAX(o.audit_time) AS recent_max_audit_time',
            ])
            ->where(function ($query) use ($recentStart) {
                // 使用 create_time / ut_time / audit_time 任一字段的近期变动来捕捉订单“新增 / 修改 / 审核”行为
                $query->where('o.create_time', '>=', $recentStart)
                      ->whereOr('o.ut_time', '>=', $recentStart)
                      ->whereOr('o.audit_time', '>=', $recentStart);
            })
            ->find();

        $recentSummary = [
            'recent_cnt'             => isset($recentRow['recent_cnt']) ? (int)$recentRow['recent_cnt'] : 0,
            'recent_max_id'          => isset($recentRow['recent_max_id']) ? (int)$recentRow['recent_max_id'] : 0,
            'recent_max_create_time' => isset($recentRow['recent_max_create_time']) ? (string)$recentRow['recent_max_create_time'] : '',
            'recent_max_ut_time'     => isset($recentRow['recent_max_ut_time']) ? (string)$recentRow['recent_max_ut_time'] : '',
            'recent_max_audit_time'  => isset($recentRow['recent_max_audit_time']) ? (string)$recentRow['recent_max_audit_time'] : '',
        ];

        return [
            'period_summary' => $periodSummary,
            'recent_changes' => $recentSummary,
        ];
    }

    /**
     * 生成旺春PK小组临时业绩看板的轻量级变化标识
     *
     * 这里不再只依赖 order_time，而是综合：
     * - 当前统计周期内、审核通过订单的数量 / 纯利润汇总 / 最大 ID / 各类时间戳；
     * - 近几天内订单的新增 / 修改 / 审核时间变化摘要。
     *
     * @return string
     */
    private function getTemporaryAchievementStamp()
    {
        $signatureData = $this->buildTemporaryAchievementSignatureData();

        // 统一使用 json_encode 后 md5，确保字段扩展时前端无需改动
        return md5(json_encode($signatureData, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * 构建旺春现有团队（永久）业绩看板的签名摘要数据
     * 与临时 PK 小组解耦：永久团队页面有独立的 stamp 计算链路，避免轮询串数据。
     * 统计口径与 buildPermanentAchievementData 一致：同一统计周期、check_status=2、同一订单表。
     *
     * @return array
     */
    private function buildPermanentAchievementSignatureData()
    {
        $startTime = $this->wcStatStartTime;
        $endTime   = $this->wcStatEndTime;
        $timeField = $this->wcOrderTimeField;

        $allowedTimeFields = ['order_time', 'create_time'];
        if (!in_array($timeField, $allowedTimeFields, true)) {
            $timeField = 'order_time';
        }

        // 第一层：当前统计周期内、审核通过订单的聚合摘要（与永久团队看板统计口径一致）
        $periodRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS cnt',
                'SUM(COALESCE(o.profit,0)) AS sum_profit',
                'MAX(o.id) AS max_id',
                'MAX(o.' . $timeField . ') AS max_order_time',
                'MAX(o.create_time) AS max_create_time',
                'MAX(o.ut_time) AS max_ut_time',
                'MAX(o.audit_time) AS max_audit_time',
            ])
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, '<=', $endTime)
            ->find();

        $periodSummary = [
            'cnt'             => isset($periodRow['cnt']) ? (int)$periodRow['cnt'] : 0,
            'sum_profit'      => isset($periodRow['sum_profit']) ? (float)$periodRow['sum_profit'] : 0.0,
            'max_id'          => isset($periodRow['max_id']) ? (int)$periodRow['max_id'] : 0,
            'max_order_time'  => isset($periodRow['max_order_time']) ? (string)$periodRow['max_order_time'] : '',
            'max_create_time' => isset($periodRow['max_create_time']) ? (string)$periodRow['max_create_time'] : '',
            'max_ut_time'     => isset($periodRow['max_ut_time']) ? (string)$periodRow['max_ut_time'] : '',
            'max_audit_time'  => isset($periodRow['max_audit_time']) ? (string)$periodRow['max_audit_time'] : '',
        ];

        $recentStart = date('Y-m-d H:i:s', strtotime('-3 days'));

        $recentRow = Db::table('crm_client_order')
            ->alias('o')
            ->field([
                'COUNT(1) AS recent_cnt',
                'MAX(o.id) AS recent_max_id',
                'MAX(o.create_time) AS recent_max_create_time',
                'MAX(o.ut_time) AS recent_max_ut_time',
                'MAX(o.audit_time) AS recent_max_audit_time',
            ])
            ->where(function ($query) use ($recentStart) {
                $query->where('o.create_time', '>=', $recentStart)
                      ->whereOr('o.ut_time', '>=', $recentStart)
                      ->whereOr('o.audit_time', '>=', $recentStart);
            })
            ->find();

        $recentSummary = [
            'recent_cnt'             => isset($recentRow['recent_cnt']) ? (int)$recentRow['recent_cnt'] : 0,
            'recent_max_id'          => isset($recentRow['recent_max_id']) ? (int)$recentRow['recent_max_id'] : 0,
            'recent_max_create_time' => isset($recentRow['recent_max_create_time']) ? (string)$recentRow['recent_max_create_time'] : '',
            'recent_max_ut_time'     => isset($recentRow['recent_max_ut_time']) ? (string)$recentRow['recent_max_ut_time'] : '',
            'recent_max_audit_time'  => isset($recentRow['recent_max_audit_time']) ? (string)$recentRow['recent_max_audit_time'] : '',
        ];

        return [
            'period_summary' => $periodSummary,
            'recent_changes' => $recentSummary,
        ];
    }

    /**
     * 生成旺春现有团队（永久）业绩看板的轻量级变化标识
     * 与临时 PK 小组独立，保证永久团队页面只在自己的数据变化时刷新。
     *
     * @return string
     */
    private function getPermanentAchievementStamp()
    {
        $signatureData = $this->buildPermanentAchievementSignatureData();

        return md5(json_encode($signatureData, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * 旺春PK小组临时业绩心跳接口
     * 仅返回轻量级变化标识 stamp，用于前端轮询检测是否需要刷新数据。
     *
     * 访问路径示例：/admin/achievement/temporaryAchievementHeartbeat
     */
    public function temporaryAchievementHeartbeat()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $stamp = $this->getTemporaryAchievementStamp();

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'stamp' => $stamp,
                ],
            ]);
        } catch (\Throwable $e) {
            return json([
                'code' => 0,
                'msg'  => '心跳检测失败：' . $e->getMessage(),
                'data' => [],
            ]);
        }
    }

    /**
     * 旺春PK小组临时业绩完整数据接口
     * 前端在心跳检测到 stamp 变化后再调用本接口获取完整排行榜数据。
     *
     * 访问路径示例：/admin/achievement/temporaryAchievementData
     */
    public function temporaryAchievementData()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $data  = $this->buildTemporaryAchievementData();
            $stamp = $this->getTemporaryAchievementStamp();

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'      => $this->wcDashboardTitle,
                    'periodText'          => $this->wcStatPeriodText,
                    'groupAvgRankList'    => $data['groupAvgRankList'],
                    'memberRankGroupList' => $data['memberRankGroupList'],
                    'stamp'               => $stamp,
                ],
            ]);
        } catch (\Throwable $e) {
            return json([
                'code' => 0,
                'msg'  => '数据获取失败：' . $e->getMessage(),
                'data' => [],
            ]);
        }
    }

    /**
     * 旺春现有团队（永久）业绩心跳接口
     * 仅返回永久团队专属的 stamp，供永久团队页面轮询检测，与临时 PK 小组接口互不串用。
     *
     * 访问路径示例：/admin/achievement/permanentAchievementHeartbeat
     */
    public function permanentAchievementHeartbeat()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $stamp = $this->getPermanentAchievementStamp();

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'stamp' => $stamp,
                ],
            ]);
        } catch (\Throwable $e) {
            return json([
                'code' => 0,
                'msg'  => '心跳检测失败：' . $e->getMessage(),
                'data' => [],
            ]);
        }
    }

    /**
     * 旺春现有团队（永久）业绩完整数据接口
     * 永久团队页面在心跳检测到 stamp 变化后调用本接口获取完整榜单，与 temporaryAchievementData 独立。
     *
     * 访问路径示例：/admin/achievement/permanentAchievementData
     */
    public function permanentAchievementData()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $data  = $this->buildPermanentAchievementData();
            $stamp = $this->getPermanentAchievementStamp();

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'      => $this->wcDashboardTitlePermanent,
                    'periodText'          => $this->wcStatPeriodText,
                    'groupAvgRankList'    => $data['groupAvgRankList'],
                    'memberRankGroupList' => $data['memberRankGroupList'],
                    'stamp'               => $stamp,
                ],
            ]);
        } catch (\Throwable $e) {
            return json([
                'code' => 0,
                'msg'  => '数据获取失败：' . $e->getMessage(),
                'data' => [],
            ]);
        }
    }
}
