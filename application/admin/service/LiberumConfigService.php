<?php

namespace app\admin\service;

use app\admin\model\LiberumConfig;
use think\Db;

class LiberumConfigService
{
    /**
     * @var array
     */
    protected $defaultConfig = [
        'daily_pick_limit' => 10,
        'first_follow_days' => 7,
        'follow_interval_days' => 90,
        'operator_release_days' => 90,
        'enable_operator_pool' => 1,
        'rule_effective_date' => '2026-06-01',
        'first_timeout_only_once' => 1,
    ];

    /**
     * 获取配置值（字符串）
     *
     * @param string $key
     * @param string $default
     * @return string
     */
    public function getConfigValue($key, $default = '')
    {
        $key = trim((string)$key);
        if ($key === '') {
            return (string)$default;
        }

        try {
            $row = LiberumConfig::where('config_key', $key)->find();
            if (!$row) {
                return (string)$default;
            }

            $value = trim((string)$row['config_value']);
            if ($value === '') {
                return (string)$default;
            }

            return $value;
        } catch (\Throwable $e) {
            return (string)$default;
        }
    }

    /**
     * 获取整数配置值（含边界校验）
     *
     * @param string $key
     * @param int $default
     * @param int $min
     * @param int $max
     * @return int
     */
    public function getIntValue($key, $default = 0, $min = 1, $max = 999999)
    {
        $default = (int)$default;
        $min = (int)$min;
        $max = (int)$max;
        if ($min < 0) {
            $min = 0;
        }
        if ($max < $min) {
            $max = $min;
        }
        if ($default < $min || $default > $max) {
            $default = $min;
        }

        $raw = $this->getConfigValue($key, (string)$default);
        if ($raw === '' || !preg_match('/^\d+$/', $raw)) {
            return $default;
        }

        $value = (int)$raw;
        if ($value < $min || $value > $max) {
            return $default;
        }

        return $value;
    }

    /**
     * 获取日期配置值（YYYY-MM-DD）
     *
     * @param string $key
     * @param string $default
     * @return string
     */
    public function getDateValue($key, $default = '2026-06-01')
    {
        $default = trim((string)$default);
        if (!$this->isValidDate($default)) {
            $default = '2026-06-01';
        }

        $raw = trim($this->getConfigValue($key, $default));
        if (!$this->isValidDate($raw)) {
            return $default;
        }

        return $raw;
    }

    /**
     * 校验日期格式与有效性
     *
     * @param string $date
     * @return bool
     */
    protected function isValidDate($date)
    {
        $date = trim((string)$date);
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) {
            return false;
        }

        $timestamp = strtotime($date);
        if ($timestamp === false) {
            return false;
        }

        return date('Y-m-d', $timestamp) === $date;
    }

    /**
     * 获取配置映射（已兜底）
     *
     * @return array
     */
    public function getConfigMap()
    {
        return [
            'daily_pick_limit' => $this->getIntValue('daily_pick_limit', (int)$this->defaultConfig['daily_pick_limit'], 1, 999),
            'first_follow_days' => $this->getIntValue('first_follow_days', (int)$this->defaultConfig['first_follow_days'], 1, 365),
            'follow_interval_days' => $this->getIntValue('follow_interval_days', (int)$this->defaultConfig['follow_interval_days'], 1, 3650),
            'operator_release_days' => $this->getIntValue('operator_release_days', (int)$this->defaultConfig['operator_release_days'], 1, 3650),
            'enable_operator_pool' => $this->getIntValue('enable_operator_pool', (int)$this->defaultConfig['enable_operator_pool'], 0, 1),
            'rule_effective_date' => $this->getDateValue('rule_effective_date', (string)$this->defaultConfig['rule_effective_date']),
            'first_timeout_only_once' => $this->getIntValue('first_timeout_only_once', (int)$this->defaultConfig['first_timeout_only_once'], 0, 1),
        ];
    }

    /**
     * 保存配置
     *
     * @param array $data
     * @return array
     */
    public function saveConfig($data = [])
    {
        $data = is_array($data) ? $data : [];

        $dailyPickLimitRaw = trim((string)($data['daily_pick_limit'] ?? ''));
        $firstFollowDaysRaw = trim((string)($data['first_follow_days'] ?? ''));
        $followIntervalDaysRaw = trim((string)($data['follow_interval_days'] ?? ''));
        $operatorReleaseDaysRaw = trim((string)($data['operator_release_days'] ?? ''));
        $enableOperatorPoolRaw = trim((string)($data['enable_operator_pool'] ?? ''));
        $ruleEffectiveDateRaw = trim((string)($data['rule_effective_date'] ?? ''));
        $firstTimeoutOnlyOnceRaw = trim((string)($data['first_timeout_only_once'] ?? ''));

        if ($dailyPickLimitRaw === '' || !preg_match('/^\d+$/', $dailyPickLimitRaw)) {
            return ['code' => -200, 'msg' => '每人每天领取上限必须是正整数', 'data' => []];
        }
        if ($firstFollowDaysRaw === '' || !preg_match('/^\d+$/', $firstFollowDaysRaw)) {
            return ['code' => -200, 'msg' => '首次跟进超时天数必须是正整数', 'data' => []];
        }
        if ($followIntervalDaysRaw === '' || !preg_match('/^\d+$/', $followIntervalDaysRaw)) {
            return ['code' => -200, 'msg' => '持续跟进超期间隔必须是正整数', 'data' => []];
        }
        if ($operatorReleaseDaysRaw === '' || !preg_match('/^\d+$/', $operatorReleaseDaysRaw)) {
            return ['code' => -200, 'msg' => '运营池自动释放天数必须是正整数', 'data' => []];
        }
        if (!in_array($enableOperatorPoolRaw, ['0', '1'], true)) {
            return ['code' => -200, 'msg' => '是否启用首次超时运营池只能是0或1', 'data' => []];
        }
        if (!in_array($firstTimeoutOnlyOnceRaw, ['0', '1'], true)) {
            return ['code' => -200, 'msg' => '首次超时运营池只触发一次只能是0或1', 'data' => []];
        }
        if (!$this->isValidDate($ruleEffectiveDateRaw)) {
            return ['code' => -200, 'msg' => '公海新规则生效日期格式错误，请使用YYYY-MM-DD', 'data' => []];
        }

        $dailyPickLimit = (int)$dailyPickLimitRaw;
        $firstFollowDays = (int)$firstFollowDaysRaw;
        $followIntervalDays = (int)$followIntervalDaysRaw;
        $operatorReleaseDays = (int)$operatorReleaseDaysRaw;
        $enableOperatorPool = (int)$enableOperatorPoolRaw;
        $firstTimeoutOnlyOnce = (int)$firstTimeoutOnlyOnceRaw;
        $ruleEffectiveDate = $ruleEffectiveDateRaw;

        if ($dailyPickLimit < 1 || $dailyPickLimit > 999) {
            return ['code' => -200, 'msg' => '每人每天领取上限需在1-999之间', 'data' => []];
        }
        if ($firstFollowDays < 1 || $firstFollowDays > 365) {
            return ['code' => -200, 'msg' => '首次跟进超时天数需在1-365之间', 'data' => []];
        }
        if ($followIntervalDays < 1 || $followIntervalDays > 3650) {
            return ['code' => -200, 'msg' => '持续跟进超期间隔需在1-3650之间', 'data' => []];
        }
        if ($operatorReleaseDays < 1 || $operatorReleaseDays > 3650) {
            return ['code' => -200, 'msg' => '运营池自动释放天数需在1-3650之间', 'data' => []];
        }

        $saveMap = [
            'daily_pick_limit' => (string)$dailyPickLimit,
            'first_follow_days' => (string)$firstFollowDays,
            'follow_interval_days' => (string)$followIntervalDays,
            'operator_release_days' => (string)$operatorReleaseDays,
            'enable_operator_pool' => (string)$enableOperatorPool,
            'rule_effective_date' => $ruleEffectiveDate,
            'first_timeout_only_once' => (string)$firstTimeoutOnlyOnce,
        ];

        $configMeta = [
            'daily_pick_limit' => [
                'config_name' => '每日领取上限',
                'description' => '每人每天最多可领取的公海客户数量',
                'sort' => 10,
            ],
            'first_follow_days' => [
                'config_name' => '首次跟进超时天数',
                'description' => '新客户创建后首次跟进超时天数，超过后进入首次超时运营池',
                'sort' => 20,
            ],
            'follow_interval_days' => [
                'config_name' => '持续跟进超期间隔天数',
                'description' => '客户两次跟进间隔或最后一次跟进距今超过该天数，进入普通公海',
                'sort' => 30,
            ],
            'operator_release_days' => [
                'config_name' => '运营池自动释放天数',
                'description' => '客户进入首次超时运营池后，超过该天数仍未分配，则自动转入普通公海',
                'sort' => 40,
            ],
            'enable_operator_pool' => [
                'config_name' => '是否启用首次超时运营池',
                'description' => '1启用，0关闭；关闭后首次超时客户可直接进入普通公海',
                'sort' => 50,
            ],
            'rule_effective_date' => [
                'config_name' => '公海新规则生效日期',
                'description' => '该日期之后创建的客户执行首次7天规则；该日期之前历史客户仅校验跟进间隔与最后跟进时间',
                'sort' => 60,
            ],
            'first_timeout_only_once' => [
                'config_name' => '首次超时运营池只触发一次',
                'description' => '1表示客户仅首次超时进入运营池；重新分配或重新领取后再次超时直接进入普通公海',
                'sort' => 70,
            ],
        ];

        Db::startTrans();
        try {
            $now = time();
            foreach ($saveMap as $configKey => $configValue) {
                $row = LiberumConfig::where('config_key', $configKey)->find();
                if ($row) {
                    $row->config_value = $configValue;
                    $row->update_time = $now;
                    $row->save();
                } else {
                    $meta = $configMeta[$configKey] ?? [];
                    LiberumConfig::create([
                        'config_key' => $configKey,
                        'config_value' => $configValue,
                        'config_name' => (string)($meta['config_name'] ?? $configKey),
                        'description' => (string)($meta['description'] ?? ''),
                        'sort' => (int)($meta['sort'] ?? 0),
                        'create_time' => $now,
                        'update_time' => $now,
                    ]);
                }
            }

            Db::commit();
            return ['code' => 0, 'msg' => '配置保存成功', 'data' => $this->getConfigMap()];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '配置保存失败', 'data' => []];
        }
    }
}
