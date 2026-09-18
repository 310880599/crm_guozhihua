<?php

namespace app\admin\service;

/**
 * 金秋大战独立配置（与旺春 temporary/permanent 完全隔离）
 */
class AutumnBattleConfigService
{
    public const SERIES_AUTUMN = 'autumn';
    public const KEY_AUTUMN_PERSON = 'autumn_person';

    /** @var array<string,mixed>|null */
    private $rawData;

    /**
     * 正式配置文件绝对路径（禁止写死盘符）。
     *
     * @return string
     */
    public function getConfigPath()
    {
        return dirname(__DIR__) . DIRECTORY_SEPARATOR . 'data' . DIRECTORY_SEPARATOR . 'autumn_battle_person.php';
    }

    /**
     * 读取原始配置（请求内缓存；forceReload 时重新 include）。
     *
     * @param bool $forceReload
     * @return array<string,mixed>
     */
    public function getRawConfig($forceReload = false)
    {
        if ($forceReload) {
            $this->rawData = null;
        }

        return $this->loadRawData();
    }

    /**
     * @return array<string,mixed>
     */
    private function loadRawData()
    {
        if ($this->rawData !== null) {
            return $this->rawData;
        }
        $path = $this->getConfigPath();
        if (!is_file($path)) {
            throw new \RuntimeException('金秋大战配置文件缺失：application/admin/data/autumn_battle_person.php');
        }
        $data = include $path;
        if (!is_array($data) || empty($data['challengers']) || empty($data['bettors'])) {
            throw new \RuntimeException('金秋大战配置无效或为空，禁止回退到其他活动数据');
        }
        $this->rawData = $data;

        return $this->rawData;
    }

    /**
     * 对任意原始配置数组计算指纹（与 getConfigFingerprint 口径一致）。
     *
     * @param array<string,mixed> $raw
     * @return string
     */
    public function getFingerprintForRawConfig(array $raw)
    {
        $payload = [
            'config_version' => isset($raw['config_version']) ? $raw['config_version'] : '',
            'activity'       => isset($raw['activity']) ? $raw['activity'] : [],
            'name_aliases'   => isset($raw['name_aliases']) ? $raw['name_aliases'] : [],
            'challengers'    => isset($raw['challengers']) ? $raw['challengers'] : [],
            'bettors'        => isset($raw['bettors']) ? $raw['bettors'] : [],
            'bet_details'    => isset($raw['bet_details']) ? $raw['bet_details'] : [],
            'checksums'      => isset($raw['checksums']) ? $raw['checksums'] : [],
        ];

        return md5(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * 配置指纹：名单/目标/押注/别名变化时必须变化。
     *
     * @return string
     */
    public function getConfigFingerprint()
    {
        return $this->getFingerprintForRawConfig($this->loadRawData());
    }

    /**
     * @return array<string,mixed>
     */
    public function getPersonConfig()
    {
        $raw = $this->loadRawData();
        $activity = isset($raw['activity']) && is_array($raw['activity']) ? $raw['activity'] : [];

        return [
            'configKey'            => self::KEY_AUTUMN_PERSON,
            'series'               => self::SERIES_AUTUMN,
            'configVersion'        => (string)(isset($raw['config_version']) ? $raw['config_version'] : ''),
            'configFingerprint'    => $this->getConfigFingerprint(),
            'dashboardTitle'       => (string)(isset($activity['title']) ? $activity['title'] : '金秋大战·押注战况'),
            'periodText'           => (string)(isset($activity['period_text']) ? $activity['period_text'] : ''),
            'startTime'            => (string)(isset($activity['start_time']) ? $activity['start_time'] : ''),
            'endTimeExclusive'     => (string)(isset($activity['end_time_exclusive']) ? $activity['end_time_exclusive'] : ''),
            'orderTimeField'       => (string)(isset($activity['order_time_field']) ? $activity['order_time_field'] : 'order_time'),
            'checkStatus'          => (int)(isset($activity['check_status']) ? $activity['check_status'] : 2),
            'recentCaptureDays'    => (int)(isset($activity['recent_capture_days']) ? $activity['recent_capture_days'] : 3),
            'stampScope'           => (string)(isset($activity['stamp_scope']) ? $activity['stamp_scope'] : 'jq_person'),
            'nameAliases'          => isset($raw['name_aliases']) && is_array($raw['name_aliases']) ? $raw['name_aliases'] : [],
            'challengers'          => isset($raw['challengers']) && is_array($raw['challengers']) ? $raw['challengers'] : [],
            'bettors'              => isset($raw['bettors']) && is_array($raw['bettors']) ? $raw['bettors'] : [],
            'betDetails'           => isset($raw['bet_details']) && is_array($raw['bet_details']) ? $raw['bet_details'] : [],
            'checksums'            => isset($raw['checksums']) && is_array($raw['checksums']) ? $raw['checksums'] : [],
            'sourceFiles'          => isset($raw['source_files']) && is_array($raw['source_files']) ? $raw['source_files'] : [],
        ];
    }
}
