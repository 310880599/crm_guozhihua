<?php

namespace app\admin\controller;

use app\admin\service\AchievementConfigService;
use app\admin\service\AutumnBattleConfigService;
use app\admin\service\AutumnBattleConfigWriteService;
use app\admin\service\AutumnBattleService;
use app\admin\service\WangchunAchievementService;
use think\Db;

class Achievement extends Common
{
    /** @var AchievementConfigService|null */
    private $achievementConfigService;

    /** @var WangchunAchievementService|null */
    private $wangchunAchievementService;

    /** @var AutumnBattleConfigService|null */
    private $autumnBattleConfigService;

    /** @var AutumnBattleService|null */
    private $autumnBattleService;

    /** @var AutumnBattleConfigWriteService|null */
    private $autumnBattleConfigWriteService;

    private function achievementConfigService()
    {
        if ($this->achievementConfigService === null) {
            $this->achievementConfigService = new AchievementConfigService();
        }

        return $this->achievementConfigService;
    }

    private function wangchunAchievementService()
    {
        if ($this->wangchunAchievementService === null) {
            $this->wangchunAchievementService = new WangchunAchievementService($this->achievementConfigService());
        }

        return $this->wangchunAchievementService;
    }

    private function autumnBattleConfigService()
    {
        if ($this->autumnBattleConfigService === null) {
            $this->autumnBattleConfigService = new AutumnBattleConfigService();
        }

        return $this->autumnBattleConfigService;
    }

    private function autumnBattleService()
    {
        if ($this->autumnBattleService === null) {
            $this->autumnBattleService = new AutumnBattleService($this->autumnBattleConfigService());
        }

        return $this->autumnBattleService;
    }

    private function autumnBattleConfigWriteService()
    {
        if ($this->autumnBattleConfigWriteService === null) {
            $this->autumnBattleConfigWriteService = new AutumnBattleConfigWriteService(
                $this->autumnBattleConfigService()
            );
        }

        return $this->autumnBattleConfigWriteService;
    }

    /**
     * 金秋大战配置页显式权限（auth_rule id=384）。
     * aid=1 超级管理员放行；其他账号必须拥有 384。
     * 与 Common 鉴权叠加，避免仅靠前端隐藏。
     *
     * @return void
     */
    private function assertAutumnAchievementConfigAuth()
    {
        $writeService = $this->autumnBattleConfigWriteService();
        if ($writeService->canAccessConfig((int)session('aid'))) {
            return;
        }

        if (request()->isPost() || request()->isAjax()) {
            // 由调用方处理 JSON；此处抛错统一转消息
            throw new \RuntimeException('您无此操作权限');
        }

        $this->error('您无此操作权限');
    }

    /** 旺春 PK 小组团队页 → 配置 key：temporary_group */
    public function temporaryAchievement()
    {
        $keyTemporaryGroup = AchievementConfigService::KEY_TEMPORARY_GROUP;
        $config = $this->achievementConfigService()->getConfig($keyTemporaryGroup);
        $svc = $this->wangchunAchievementService();
        $data  = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
        $stamp = $svc->getTemporaryAchievementStampByConfig($config);

        $this->assign('dashboardTitle', $config['dashboardTitle']);
        $this->assign('periodText', $config['periodText']);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        $this->assign('globalMemberRankList', $data['globalMemberRankList']);
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/temporary_achievement');
    }

    /** 旺春现有团队页 → 配置 key：permanent_group */
    public function permanentAchievement()
    {
        $keyPermanentGroup = AchievementConfigService::KEY_PERMANENT_GROUP;
        $config = $this->achievementConfigService()->getConfig($keyPermanentGroup);
        $svc = $this->wangchunAchievementService();
        $data  = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
        $stamp = $svc->getPermanentAchievementStampByConfig($config);

        $this->assign('dashboardTitle', $config['dashboardTitle']);
        $this->assign('periodText', $config['periodText']);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        $this->assign('globalMemberRankList', $data['globalMemberRankList']);
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/permanent_achievement');
    }

    /** 旺春 PK 小组个人页 → 配置 key：temporary_person */
    public function temporaryAchievementPerson()
    {
        $keyTemporaryPerson = AchievementConfigService::KEY_TEMPORARY_PERSON;
        $config = $this->achievementConfigService()->getConfig($keyTemporaryPerson);
        $svc = $this->wangchunAchievementService();
        $data  = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
        $stamp = $svc->getTemporaryAchievementStampByConfig($config);

        $this->assign('dashboardTitle', $config['dashboardTitle']);
        $this->assign('periodText', $config['periodText']);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        $this->assign('globalMemberRankList', $data['globalMemberRankList']);
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/temporary_achievement_person');
    }

    /** 旺春现有团队个人页 → 配置 key：permanent_person */
    public function permanentAchievementPerson()
    {
        $keyPermanentPerson = AchievementConfigService::KEY_PERMANENT_PERSON;
        $config = $this->achievementConfigService()->getConfig($keyPermanentPerson);
        $svc = $this->wangchunAchievementService();
        $data  = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
        $stamp = $svc->getPermanentAchievementStampByConfig($config);

        $this->assign('dashboardTitle', $config['dashboardTitle']);
        $this->assign('periodText', $config['periodText']);
        $this->assign('groupAvgRankList', $data['groupAvgRankList']);
        $this->assign('memberRankGroupList', $data['memberRankGroupList']);
        $this->assign('globalMemberRankList', $data['globalMemberRankList']);
        $this->assign('wcStamp', $stamp);

        return $this->fetch('achievement/permanent_achievement_person');
    }

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
            $keyTemporaryGroup = AchievementConfigService::KEY_TEMPORARY_GROUP;
            $config = $this->achievementConfigService()->getConfig($keyTemporaryGroup);
            $stamp = $this->wangchunAchievementService()->getTemporaryAchievementStampByConfig($config);

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

    public function temporaryAchievementPersonHeartbeat()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $keyTemporaryPerson = AchievementConfigService::KEY_TEMPORARY_PERSON;
            $config = $this->achievementConfigService()->getConfig($keyTemporaryPerson);
            $stamp = $this->wangchunAchievementService()->getTemporaryAchievementStampByConfig($config);

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
            $keyTemporaryGroup = AchievementConfigService::KEY_TEMPORARY_GROUP;
            $config = $this->achievementConfigService()->getConfig($keyTemporaryGroup);
            $svc = $this->wangchunAchievementService();
            $data = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
            $stamp = $svc->getTemporaryAchievementStampByConfig($config);

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'       => $config['dashboardTitle'],
                    'periodText'           => $config['periodText'],
                    'groupAvgRankList'     => $data['groupAvgRankList'],
                    'memberRankGroupList'  => $data['memberRankGroupList'],
                    'globalMemberRankList' => $data['globalMemberRankList'],
                    'stamp'                => $stamp,
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

    public function temporaryAchievementPersonData()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $keyTemporaryPerson = AchievementConfigService::KEY_TEMPORARY_PERSON;
            $config = $this->achievementConfigService()->getConfig($keyTemporaryPerson);
            $svc = $this->wangchunAchievementService();
            $data = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
            $stamp = $svc->getTemporaryAchievementStampByConfig($config);

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'       => $config['dashboardTitle'],
                    'periodText'           => $config['periodText'],
                    'groupAvgRankList'     => $data['groupAvgRankList'],
                    'memberRankGroupList'  => $data['memberRankGroupList'],
                    'globalMemberRankList' => $data['globalMemberRankList'],
                    'stamp'                => $stamp,
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
            $keyPermanentGroup = AchievementConfigService::KEY_PERMANENT_GROUP;
            $config = $this->achievementConfigService()->getConfig($keyPermanentGroup);
            $stamp = $this->wangchunAchievementService()->getPermanentAchievementStampByConfig($config);

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

    public function permanentAchievementPersonHeartbeat()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $keyPermanentPerson = AchievementConfigService::KEY_PERMANENT_PERSON;
            $config = $this->achievementConfigService()->getConfig($keyPermanentPerson);
            $stamp = $this->wangchunAchievementService()->getPermanentAchievementStampByConfig($config);

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
            $keyPermanentGroup = AchievementConfigService::KEY_PERMANENT_GROUP;
            $config = $this->achievementConfigService()->getConfig($keyPermanentGroup);
            $svc = $this->wangchunAchievementService();
            $data = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
            $stamp = $svc->getPermanentAchievementStampByConfig($config);

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'       => $config['dashboardTitle'],
                    'periodText'           => $config['periodText'],
                    'groupAvgRankList'     => $data['groupAvgRankList'],
                    'memberRankGroupList'  => $data['memberRankGroupList'],
                    'globalMemberRankList' => $data['globalMemberRankList'],
                    'stamp'                => $stamp,
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

    public function permanentAchievementPersonData()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $keyPermanentPerson = AchievementConfigService::KEY_PERMANENT_PERSON;
            $config = $this->achievementConfigService()->getConfig($keyPermanentPerson);
            $svc = $this->wangchunAchievementService();
            $data = $svc->buildAchievementDataByGroups($config['pkGroups'], $config);
            $stamp = $svc->getPermanentAchievementStampByConfig($config);

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'       => $config['dashboardTitle'],
                    'periodText'           => $config['periodText'],
                    'groupAvgRankList'     => $data['groupAvgRankList'],
                    'memberRankGroupList'  => $data['memberRankGroupList'],
                    'globalMemberRankList' => $data['globalMemberRankList'],
                    'stamp'                => $stamp,
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
     * 金秋大战配置（可视化编辑入口）
     * 菜单：Achievement/autumnAchievementConfig（auth_rule id=384）
     * GET=页面，POST=保存；唯一正式数据源：autumn_battle_person.php
     */
    public function autumnAchievementConfig()
    {
        try {
            $this->assertAutumnAchievementConfigAuth();
        } catch (\RuntimeException $e) {
            if (request()->isPost() || request()->isAjax()) {
                return json([
                    'code' => 0,
                    'msg'  => $e->getMessage(),
                    'data' => [],
                ]);
            }
            $this->error($e->getMessage());
        }

        if (request()->isPost()) {
            $payload = [];
            $rawBody = file_get_contents('php://input');
            if (is_string($rawBody) && $rawBody !== '') {
                $decoded = json_decode($rawBody, true);
                if (is_array($decoded)) {
                    $payload = $decoded;
                }
            }
            if (empty($payload)) {
                $payload = input('post.');
                if (!is_array($payload)) {
                    $payload = [];
                }
            }

            $operator = [
                'admin_id' => (int)session('aid'),
                'username' => (string)session('username'),
            ];

            $result = $this->autumnBattleConfigWriteService()->save($payload, $operator);

            return json($result);
        }

        try {
            $raw = $this->autumnBattleConfigService()->getRawConfig(true);
            $fingerprint = $this->autumnBattleConfigService()->getFingerprintForRawConfig($raw);
        } catch (\Throwable $e) {
            $this->error('金秋大战配置加载失败：' . $e->getMessage());
        }

        $adminList = Db::name('admin')
            ->field('admin_id,username,is_open')
            ->order('admin_id', 'asc')
            ->select();
        if (!is_array($adminList)) {
            $adminList = [];
        }

        $activity = isset($raw['activity']) && is_array($raw['activity']) ? $raw['activity'] : [];
        $startDate = '';
        $endDate = '';
        if (!empty($activity['start_time'])) {
            $startDate = substr((string)$activity['start_time'], 0, 10);
        }
        if (!empty($activity['end_time_exclusive'])) {
            $endTs = strtotime((string)$activity['end_time_exclusive']);
            if ($endTs !== false) {
                $endDate = date('Y-m-d', $endTs - 86400);
            }
        }

        $checksums = isset($raw['checksums']) && is_array($raw['checksums']) ? $raw['checksums'] : [];
        $summary = [
            'config_version'         => isset($raw['config_version']) ? (string)$raw['config_version'] : '',
            'fingerprint_short'      => substr($fingerprint, 0, 12),
            'challenger_count'       => isset($checksums['challenger_count']) ? (int)$checksums['challenger_count'] : count($raw['challengers'] ?? []),
            'bettor_count'           => isset($checksums['bettor_count']) ? (int)$checksums['bettor_count'] : count($raw['bettors'] ?? []),
            'bet_total'              => isset($checksums['bet_total']) ? $checksums['bet_total'] : 0,
            'goal_total_all'         => isset($checksums['goal_total_all']) ? $checksums['goal_total_all'] : 0,
            'challenger_goal_total'  => isset($checksums['challenger_goal_total']) ? $checksums['challenger_goal_total'] : 0,
            'bet_detail_count'       => isset($checksums['bet_detail_count']) ? (int)$checksums['bet_detail_count'] : count($raw['bet_details'] ?? []),
        ];

        $this->assign('configRaw', $raw);
        $this->assign('configJson', json_encode($raw, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $this->assign('adminListJson', json_encode($adminList, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        $this->assign('configFingerprint', $fingerprint);
        $this->assign('activityStartDate', $startDate);
        $this->assign('activityEndDate', $endDate);
        $this->assign('summary', $summary);
        $this->assign('saveUrl', url('Achievement/autumnAchievementConfig'));

        return $this->fetch('achievement/autumn_achievement_config');
    }

    /**
     * 金秋大战个人页（员工查看版，左右双屏手动滚动，无自动轮播）
     * 菜单：Achievement/autumnAchievementPerson（auth_rule id=383）
     */
    public function autumnAchievementPerson()
    {
        try {
            $pageData = $this->autumnBattleService()->buildPersonPageData();
        } catch (\Throwable $e) {
            $this->error('金秋大战个人页加载失败：' . $e->getMessage());
        }

        $this->assign('dashboardTitle', $pageData['dashboardTitle']);
        $this->assign('periodText', $pageData['periodText']);
        $this->assign('challengerRankList', $pageData['challengerRankList']);
        $this->assign('bettorRankList', $pageData['bettorRankList']);
        $this->assign('summary', $pageData['summary']);
        $this->assign('jqStamp', $pageData['stamp']);

        return $this->fetch('achievement/autumn_achievement_person');
    }

    public function autumnAchievementPersonHeartbeat()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $stamp = $this->autumnBattleService()->getPersonStamp();

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

    public function autumnAchievementPersonData()
    {
        if (!request()->isAjax()) {
            return json([
                'code' => 0,
                'msg'  => '非法请求',
                'data' => [],
            ]);
        }

        try {
            $pageData = $this->autumnBattleService()->buildPersonPageData();

            return json([
                'code' => 1,
                'msg'  => 'success',
                'data' => [
                    'dashboardTitle'     => $pageData['dashboardTitle'],
                    'periodText'         => $pageData['periodText'],
                    'challengerRankList' => $pageData['challengerRankList'],
                    'bettorRankList'     => $pageData['bettorRankList'],
                    'summary'            => $pageData['summary'],
                    'stamp'              => $pageData['stamp'],
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
