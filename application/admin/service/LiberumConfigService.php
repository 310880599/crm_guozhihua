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
        'auto_liberum_days' => 90,
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
        if ($min <= 0) {
            $min = 1;
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
     * 获取配置映射（已兜底）
     *
     * @return array
     */
    public function getConfigMap()
    {
        return [
            'daily_pick_limit' => $this->getIntValue('daily_pick_limit', (int)$this->defaultConfig['daily_pick_limit'], 1, 999),
            'auto_liberum_days' => $this->getIntValue('auto_liberum_days', (int)$this->defaultConfig['auto_liberum_days'], 1, 3650),
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
        $autoLiberumDaysRaw = trim((string)($data['auto_liberum_days'] ?? ''));

        if ($dailyPickLimitRaw === '' || !preg_match('/^\d+$/', $dailyPickLimitRaw)) {
            return ['code' => -200, 'msg' => '每人每天领取上限必须是正整数', 'data' => []];
        }
        if ($autoLiberumDaysRaw === '' || !preg_match('/^\d+$/', $autoLiberumDaysRaw)) {
            return ['code' => -200, 'msg' => '自动公海超期天数必须是正整数', 'data' => []];
        }

        $dailyPickLimit = (int)$dailyPickLimitRaw;
        $autoLiberumDays = (int)$autoLiberumDaysRaw;

        if ($dailyPickLimit < 1 || $dailyPickLimit > 999) {
            return ['code' => -200, 'msg' => '每人每天领取上限需在1-999之间', 'data' => []];
        }
        if ($autoLiberumDays < 1 || $autoLiberumDays > 3650) {
            return ['code' => -200, 'msg' => '自动公海超期天数需在1-3650之间', 'data' => []];
        }

        $saveMap = [
            'daily_pick_limit' => (string)$dailyPickLimit,
            'auto_liberum_days' => (string)$autoLiberumDays,
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
                    LiberumConfig::create([
                        'config_key' => $configKey,
                        'config_value' => $configValue,
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
