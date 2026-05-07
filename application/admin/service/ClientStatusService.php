<?php

namespace app\admin\service;

use app\admin\controller\Client as ClientController;
use app\admin\model\Client as ClientModel;
use think\Db;
use Throwable;

class ClientStatusService
{
    /**
     * 批量客户成交：未成交(-1) -> 成交(1)
     */
    public function batchClientSuccess(array $ids): array
    {
        return $this->batchChangeStatus($ids, -1, 1, '客户状态：未成交 -> 已成交');
    }

    /**
     * 批量客户未成交：成交(1) -> 未成交(-1)
     */
    public function batchClientUnSuccess(array $ids): array
    {
        return $this->batchChangeStatus($ids, 1, -1, '客户状态：已成交 -> 未成交');
    }

    /**
     * 统一批量变更客户成交状态
     */
    private function batchChangeStatus(array $ids, int $fromIsSuccess, int $toIsSuccess, string $logDescription): array
    {
        // 统一过滤 ids，避免空值/重复/非法值进入 SQL
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids))));
        if (empty($ids)) {
            return ['code' => 1, 'msg' => '请选择客户'];
        }

        $clientModel = new ClientModel();
        $clientRows = $clientModel->getBatchStatusClients($ids, $fromIsSuccess, 1);
        if (empty($clientRows)) {
            return ['code' => 1, 'msg' => '没有可操作的客户'];
        }

        $targetIds = array_column($clientRows, 'id');
        $utTime = time();

        Db::startTrans();
        try {
            $updated = $clientModel->batchUpdateIsSuccess($targetIds, $fromIsSuccess, $toIsSuccess, $utTime, 1);
            if ($updated === false) {
                throw new \RuntimeException('客户状态更新失败');
            }

            // 保留现有日志体系：逐条写入客户操作日志
            foreach ($clientRows as $row) {
                ClientController::addOperLog((int)$row['id'], '编辑', $logDescription);
            }

            Db::commit();
            return ['code' => 0, 'msg' => '操作成功，共处理' . count($targetIds) . '个客户'];
        } catch (Throwable $e) {
            Db::rollback();
            return ['code' => 1, 'msg' => $e->getMessage() ?: '操作失败'];
        }
    }
}
