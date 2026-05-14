<?php

namespace app\admin\service;

class LiberumLogService extends BaseAdminService
{
    protected $pickLogService;
    protected $inLogService;

    public function getPickLogService()
    {
        if (!$this->pickLogService instanceof LiberumPickLogService) {
            $this->pickLogService = new LiberumPickLogService();
        }

        return $this->pickLogService;
    }

    public function getInLogService()
    {
        if (!$this->inLogService instanceof LiberumInLogService) {
            $this->inLogService = new LiberumInLogService();
        }

        return $this->inLogService;
    }

    public function getPickLogList($params = [])
    {
        return $this->getPickLogService()->getPickLogList($params);
    }

    public function getInLogList($params = [])
    {
        return $this->getInLogService()->getInLogList($params);
    }

    public function batchReturnToLiberum($ids = [], $operatorInfo = [])
    {
        return $this->getPickLogService()->batchReturnToLiberum($ids, $operatorInfo);
    }

    public function batchRestoreOwner($ids = [], $operatorInfo = [])
    {
        return $this->getInLogService()->batchRestoreOwner($ids, $operatorInfo);
    }
}
