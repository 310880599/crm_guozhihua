<?php

namespace app\admin\controller;

use app\admin\service\AchievementConfigService;
use app\admin\service\WangchunAchievementService;

class Achievement extends Common
{
    /** @var AchievementConfigService|null */
    private $achievementConfigService;

    /** @var WangchunAchievementService|null */
    private $wangchunAchievementService;

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
}
