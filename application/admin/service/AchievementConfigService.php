<?php

namespace app\admin\service;

/**
 * 旺春业绩看板：集中配置（时间、标题、名单、排序等），按页面 key 取用。
 *
 * @see WangchunAchievementService 业务统计与 stamp
 */
class AchievementConfigService
{
    /** PK 旺春系列（与 permanent 心跳/stamp 完全隔离，禁止串用配置） */
    public const SERIES_TEMPORARY = 'temporary';

    /** 现有团队系列 */
    public const SERIES_PERMANENT = 'permanent';

    public const KEY_TEMPORARY_GROUP = 'temporary_group';
    public const KEY_PERMANENT_GROUP = 'permanent_group';
    public const KEY_TEMPORARY_PERSON = 'temporary_person';
    public const KEY_PERMANENT_PERSON = 'permanent_person';

    /** @var array<string,array> */
    private $pkGroupsTemporary;

    /** @var array<string,array> */
    private $pkGroupsPermanent;

    public function __construct()
    {
        $this->pkGroupsTemporary = [
            '破局队' => ['周岚', '李燕慧', '卢慧贤', '曹玲艳', '冯婷婷', '张杰', '宋蒙', '闫雪'],
            '飞马队' => ['张红玲', '田丽园', '杨惠岚', '钱秀霞', '张甜甜', '胡沙沙', '司月鹅', '杨青青'],
            '领航队' => ['张二凤', '贾聪乐', '魏红岩', '赵静', '刘丹丹', '张艳艳', '张书芹', '陈培培'],
            '启胜队' => ['胡晓惠', '张珊珊', '职利杰', '周燕', '谢园园', '刘小方', '安晓娜', '罗亭'],
            '冲锋队' => ['刘燕燕', '韩利敏', '樊培培', '张春辉', '常绍瑞', '陈义笑', '张景春', '田瑞云'],
            '亿马当先' => ['苗雪会', '王静', '刘玉杰', '张莹', '王俊', '李剪阁', '彭玉', '邵朋杰', '李小萌'],
            '金马队' => ['张岩', '李灵燕', '李营', '连书会', '冯燕平', '周勤勤', '毛卫娟', '范晶晶'],
            '战神队' => ['拜云梦', '王亚丽', '高慢慢', '袁璟', '申振宇', '何文可', '吴银霞', '宋秀丽'],
        ];

        $this->pkGroupsPermanent = [
            '勇敢者' => ['张春辉', '宋秀丽', '韩利敏', '王静', '周勤勤', '王俊', '刘燕燕', '闫雪'],
            '追光者' => ['谢园园', '安晓娜', '田丽园', '常绍瑞', '司月鹅', '李灵燕', '刘丹丹', '罗亭','冯婷婷'],
            '扬帆队' => ['魏红岩', '王亚丽', '申振宇', '张书芹', '苗雪会', '张红玲', '何文可','范晶晶'],
            '超越队' => ['李营', '袁璟', '李燕慧', '周燕', '杨青青', '胡晓惠',  '曹玲艳','职利杰'],
            '步步高' => ['张杰', '张珊珊', '连书会', '赵静', '冯燕平', '张甜甜', '张艳艳', '余玲芳'],
            '齐心聚力' => ['卢慧贤', '樊培培', '高慢慢', '陈义笑', '周岚', '张岩', '杨惠岚', '宋蒙', '钱秀霞'],
            '疯狂蚂蚁' => ['吴银霞', '拜云梦', '陈培培', '李剪阁', '张莹', '邵朋杰', '李小萌', '刘玉杰'],
            '无畏蜜蜂' => ['彭玉', '刘小方', '胡沙沙', '田瑞云', '张二凤', '贾聪乐', '张景春', '毛卫娟'],
        ];
    }

    /**
     * 四菜单共用的榜单展示/排序默认值（若需分菜单覆盖，可在 definitions 里按 key 覆盖键）。
     *
     * @return array<string,mixed>
     */
    private function baseRankOptions()
    {
        return [
            'include_zero_member'        => true,
            'left_group_top_limit'       => 0,
            'right_member_sort_desc'     => true,
            'right_group_sort_field'     => 'total_amount',
            'right_group_sort_direction' => 'desc',
            'group_avg_sort_direction'   => 'desc',
        ];
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    private function definitions()
    {
        $b = $this->baseRankOptions();

        return [
            self::KEY_TEMPORARY_GROUP => array_merge($b, [
                'dashboard_title'     => '旺春PK小组 · 业绩看板',
                'period_text'         => '2026.04.16 - 2026.05.31',
                'stat_start'          => '2026-04-16 00:00:00',
                'stat_end'            => '2026-05-31 23:59:59',
                'order_time_field'    => 'order_time',
                'recent_capture_days' => 3,
                'stamp_scope'         => 'wc_temporary_team',
                'pk_groups_ref'       => 'temporary',
            ]),
            self::KEY_TEMPORARY_PERSON => array_merge($b, [
                'dashboard_title'     => '旺春PK小组个人 · 业绩看板',
                'period_text'         => '2026.04.16 - 2026.05.31',
                'stat_start'          => '2026-04-16 00:00:00',
                'stat_end'            => '2026-05-31 23:59:59',
                'order_time_field'    => 'order_time',
                'recent_capture_days' => 3,
                'stamp_scope'         => 'wc_temporary_person',
                'pk_groups_ref'       => 'temporary',
            ]),
            self::KEY_PERMANENT_GROUP => array_merge($b, [
                'dashboard_title'     => '旺春团队 · 业绩看板',
                'period_text'         => '2026.03.01 - 2026.05.31',
                'stat_start'          => '2026-03-01 00:00:00',
                'stat_end'            => '2026-05-31 23:59:59',
                'order_time_field'    => 'order_time',
                'recent_capture_days' => 3,
                'stamp_scope'         => 'wc_permanent_team',
                'pk_groups_ref'       => 'permanent',
            ]),
            self::KEY_PERMANENT_PERSON => array_merge($b, [
                'dashboard_title'     => '旺春现有团队个人 · 业绩看板',
                'period_text'         => '2026.03.01 - 2026.05.31',
                'stat_start'          => '2026-03-01 00:00:00',
                'stat_end'            => '2026-05-31 23:59:59',
                'order_time_field'    => 'order_time',
                'recent_capture_days' => 3,
                'stamp_scope'         => 'wc_permanent_person',
                'pk_groups_ref'       => 'permanent',
            ]),
        ];
    }

    /**
     * 内部：解析名单后的原始行（snake_case）。
     *
     * @param string $key
     * @return array<string,mixed>
     */
    private function getRawConfig($key)
    {
        $all = $this->definitions();
        if (!isset($all[$key])) {
            throw new \InvalidArgumentException('Unknown achievement dashboard key: ' . $key);
        }
        $row = $all[$key];
        $ref = $row['pk_groups_ref'];
        unset($row['pk_groups_ref']);
        $row['pk_groups'] = $ref === 'permanent' ? $this->pkGroupsPermanent : $this->pkGroupsTemporary;

        $row['config_key'] = $key;
        $row['series']     = (strpos($key, 'temporary_') === 0) ? self::SERIES_TEMPORARY : self::SERIES_PERMANENT;

        return $row;
    }

    /**
     * 对外统一结构（camelCase），并附带 stamp / 心跳 / 名单等业务字段。
     *
     * 必选业务字段：dashboardTitle、periodText、startTime、endTime、orderTimeField、includeZeroMember、排序字段。
     * 扩展：recentCaptureDays、stampScope、pkGroups。
     *
     * @param array<string,mixed> $raw
     * @return array<string,mixed>
     */
    private function normalizeConfig(array $raw)
    {
        return [
            'configKey'                => (string)$raw['config_key'],
            'series'                   => (string)$raw['series'],
            'dashboardTitle'           => (string)$raw['dashboard_title'],
            'periodText'               => (string)$raw['period_text'],
            'startTime'                => (string)$raw['stat_start'],
            'endTime'                  => (string)$raw['stat_end'],
            'orderTimeField'           => (string)$raw['order_time_field'],
            'includeZeroMember'        => (bool)$raw['include_zero_member'],
            'leftGroupTopLimit'        => (int)$raw['left_group_top_limit'],
            'rightMemberSortDesc'      => (bool)$raw['right_member_sort_desc'],
            'rightGroupSortField'      => (string)$raw['right_group_sort_field'],
            'rightGroupSortDirection'  => (string)$raw['right_group_sort_direction'],
            'groupAvgSortDirection'    => (string)$raw['group_avg_sort_direction'],
            'recentCaptureDays'        => (int)$raw['recent_capture_days'],
            'stampScope'               => (string)$raw['stamp_scope'],
            'pkGroups'                 => $raw['pk_groups'],
        ];
    }

    /**
     * 获取全部看板配置（key => 配置），结构与 {@see getConfig} 单条一致。
     *
     * @return array<string,array<string,mixed>>
     */
    public function getAllConfigs()
    {
        $keys = [
            self::KEY_TEMPORARY_GROUP,
            self::KEY_TEMPORARY_PERSON,
            self::KEY_PERMANENT_GROUP,
            self::KEY_PERMANENT_PERSON,
        ];
        $out = [];
        foreach ($keys as $key) {
            $out[$key] = $this->getConfig($key);
        }

        return $out;
    }

    /**
     * 根据 key 获取单个看板完整配置。
     *
     * @param string $key self::KEY_* 常量
     * @return array<string,mixed>
     */
    public function getConfig($key)
    {
        return $this->normalizeConfig($this->getRawConfig($key));
    }
}
