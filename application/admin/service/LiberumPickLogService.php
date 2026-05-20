<?php

namespace app\admin\service;

use app\admin\behavior\ContactMap;
use app\admin\model\LiberumPickLog;
use think\Db;

class LiberumPickLogService extends BaseAdminService
{
    const EXPORT_LIMIT = 5000;
    protected static $phoneMapCache = [];
    protected static $phoneSearchCache = [];

    public function getPickLogList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        try {
            $hasActiveLeadsId = $this->hasColumn('crm_liberum_pick_log', 'active_leads_id');
            $hasOperatorName = $this->hasColumn('crm_liberum_pick_log', 'operator_name');
            $hasPickOperatorId = $this->hasColumn('crm_liberum_pick_log', 'operator_id');
            $hasIsDeleted = $this->hasColumn('crm_liberum_pick_log', 'is_deleted');
            $hasLogPrGhType = $this->hasColumn('crm_liberum_pick_log', 'pr_gh_type');
            $hasLogGhTypeId = $this->hasColumn('crm_liberum_pick_log', 'gh_type_id');
            $hasLogGhType = $this->hasColumn('crm_liberum_pick_log', 'gh_type');
            $hasLeadPrGhType = $this->hasColumn('crm_leads', 'pr_gh_type');

            $leadJoin = $hasActiveLeadsId
                ? 'l.id = IFNULL(NULLIF(pl.leads_id, 0), pl.active_leads_id)'
                : 'l.id = pl.leads_id';

            $activeLeadsIdField = $hasActiveLeadsId
                ? 'IFNULL(MAX(pl.active_leads_id), 0) AS active_leads_id'
                : '0 AS active_leads_id';
            $pickOperatorNameField = $hasOperatorName
                ? 'IFNULL(MAX(pl.operator_name), "") AS operator_name'
                : '"" AS operator_name';
            $pickOperatorIdField = $hasPickOperatorId
                ? 'IFNULL(MAX(pl.operator_id), 0) AS operator_id'
                : '0 AS operator_id';
            $logPrGhTypeField = $hasLogPrGhType
                ? 'IFNULL(MAX(pl.pr_gh_type), 0) AS log_pr_gh_type'
                : '0 AS log_pr_gh_type';
            $logGhTypeIdField = $hasLogGhTypeId
                ? 'IFNULL(MAX(pl.gh_type_id), 0) AS log_gh_type_id'
                : '0 AS log_gh_type_id';
            $logGhTypeField = $hasLogGhType
                ? 'IFNULL(MAX(pl.gh_type), "") AS gh_type'
                : '"" AS gh_type';
            $leadPrGhTypeField = $hasLeadPrGhType
                ? 'IFNULL(MAX(l.pr_gh_type), 0) AS lead_pr_gh_type'
                : '0 AS lead_pr_gh_type';

            $model = new LiberumPickLog();
            $query = $model->alias('pl')
                ->leftJoin('crm_leads l', $leadJoin);
            if ($hasIsDeleted) {
                $query->where('pl.is_deleted', 0);
            }

            if (!empty($params['pick_date'])) {
                $query->where('pl.pick_date', trim((string)$params['pick_date']));
            }

            if (!empty($params['pick_user_keyword'])) {
                $pickUserKeyword = trim((string)$params['pick_user_keyword']);
                $query->where(function ($subQuery) use ($pickUserKeyword, $hasOperatorName) {
                    $subQuery->whereLike('pl.pick_user', '%' . $pickUserKeyword . '%');
                    if ($hasOperatorName) {
                        $subQuery->whereOr('pl.operator_name', 'like', '%' . $pickUserKeyword . '%');
                    }
                });
            }

            if (!empty($params['client_keyword'])) {
                $clientKeyword = trim((string)$params['client_keyword']);
                $query->where(function ($subQuery) use ($clientKeyword) {
                    $subQuery->whereLike('l.kh_name', '%' . $clientKeyword . '%')
                        ->whereOr('pl.kh_name', 'like', '%' . $clientKeyword . '%');
                });
            }

            if (!empty($params['phone'])) {
                $phone = trim((string)$params['phone']);
                $matchedLeadIds = $this->findLeadIdsByPhone($phone);
                if (empty($matchedLeadIds)) {
                    return [
                        'count' => 0,
                        'data' => [],
                    ];
                }
                if ($hasActiveLeadsId) {
                    $query->where(function ($subQuery) use ($matchedLeadIds) {
                        $subQuery->whereIn('pl.leads_id', $matchedLeadIds)
                            ->whereOr(function ($orQuery) use ($matchedLeadIds) {
                                $orQuery->where('pl.leads_id', 0)
                                    ->whereIn('pl.active_leads_id', $matchedLeadIds);
                            })
                            ->whereOr(function ($orQuery) use ($matchedLeadIds) {
                                $orQuery->whereNull('pl.leads_id')
                                    ->whereIn('pl.active_leads_id', $matchedLeadIds);
                            });
                    });
                } else {
                    $query->whereIn('pl.leads_id', $matchedLeadIds);
                }
            }

            if (array_key_exists('is_returned', $params) && $params['is_returned'] !== '') {
                $query->where('pl.is_returned', (int)$params['is_returned']);
            }

            $count = (int)(clone $query)->count('pl.id');
            $data = $query
                ->field([
                    'pl.id',
                    'IFNULL(MAX(pl.leads_id), 0) AS leads_id',
                    $activeLeadsIdField,
                    'IFNULL(MAX(l.id), IFNULL(MAX(pl.leads_id), 0)) AS client_id',
                    $hasActiveLeadsId
                        ? 'IFNULL(MAX(l.id), IFNULL(NULLIF(MAX(pl.leads_id), 0), IFNULL(MAX(pl.active_leads_id), 0))) AS contact_leads_id'
                        : 'IFNULL(MAX(l.id), IFNULL(MAX(pl.leads_id), 0)) AS contact_leads_id',
                    'IFNULL(MAX(l.kh_name), IFNULL(MAX(pl.kh_name), "")) AS client_name',
                    '"" AS client_phone',
                    'IFNULL(MAX(pl.pick_user), "") AS pick_user',
                    'IFNULL(MAX(pl.pick_date), "") AS pick_date',
                    'IFNULL(MAX(pl.pick_time), "") AS pick_time',
                    'IFNULL(MAX(pl.before_pr_user), "") AS before_pr_user',
                    'IFNULL(MAX(l.pr_user), "") AS current_pr_user',
                    'IFNULL(MAX(pl.is_returned), 0) AS is_returned',
                    $pickOperatorNameField,
                    $pickOperatorIdField,
                    $logPrGhTypeField,
                    $logGhTypeIdField,
                    $logGhTypeField,
                    $leadPrGhTypeField,
                ])
                ->group('pl.id')
                ->order('pl.id desc')
                ->page($page, $limit)
                ->select();
            if (is_object($data) && method_exists($data, 'toArray')) {
                $data = $data->toArray();
            } elseif (!is_array($data)) {
                $data = [];
            }
            if (!empty($data)) {
                $leadIds = [];
                foreach ($data as $row) {
                    $leadIds[] = (int)($row['contact_leads_id'] ?? 0);
                }
                $phoneMap = $this->buildPhoneMap($leadIds);
                $ghTypeNameMap = $this->buildGhTypeNameMap($data);
                foreach ($data as &$row) {
                    $mapLeadId = (int)($row['contact_leads_id'] ?? 0);
                    $row['client_phone'] = $phoneMap[$mapLeadId] ?? '';
                    $row['before_gh_type_id'] = (int)($row['log_pr_gh_type'] ?? 0);
                    if ($row['before_gh_type_id'] <= 0) {
                        $row['before_gh_type_id'] = (int)($row['log_gh_type_id'] ?? 0);
                    }
                    if ($row['before_gh_type_id'] <= 0) {
                        $row['before_gh_type_id'] = (int)($row['lead_pr_gh_type'] ?? 0);
                    }
                    $row['before_gh_type_name'] = $this->resolveBeforeGhTypeName($row, $ghTypeNameMap);
                    unset($row['contact_leads_id']);
                }
                unset($row);
            }
        } catch (\Throwable $e) {
            $count = 0;
            $data = [];
        }

        return [
            'count' => $count,
            'data' => $data,
        ];
    }

    protected function buildGhTypeNameMap($rows = [])
    {
        $rows = is_array($rows) ? $rows : [];
        $typeIds = [];
        foreach ($rows as $row) {
            $logPrGhType = (int)($row['log_pr_gh_type'] ?? 0);
            $logGhTypeId = (int)($row['log_gh_type_id'] ?? 0);
            $leadPrGhType = (int)($row['lead_pr_gh_type'] ?? 0);
            if ($logPrGhType > 0) {
                $typeIds[] = $logPrGhType;
            }
            if ($logGhTypeId > 0) {
                $typeIds[] = $logGhTypeId;
            }
            if ($leadPrGhType > 0) {
                $typeIds[] = $leadPrGhType;
            }
        }
        $typeIds = array_values(array_unique($typeIds));
        if (empty($typeIds)) {
            return [];
        }

        $rows = Db::table('crm_liberum_type')
            ->where('is_deleted', 0)
            ->whereIn('id', $typeIds)
            ->column('type_name', 'id');
        if (!is_array($rows)) {
            return [];
        }

        $map = [];
        foreach ($rows as $id => $name) {
            $map[(int)$id] = trim((string)$name);
        }
        return $map;
    }

    protected function resolveBeforeGhTypeName($row = [], $ghTypeNameMap = [])
    {
        $row = is_array($row) ? $row : [];
        $ghTypeNameMap = is_array($ghTypeNameMap) ? $ghTypeNameMap : [];

        $snapshotName = trim((string)($row['gh_type'] ?? ''));
        if ($snapshotName !== '') {
            return $snapshotName;
        }

        $fallbackId = (int)($row['log_pr_gh_type'] ?? 0);
        if ($fallbackId <= 0) {
            $fallbackId = (int)($row['log_gh_type_id'] ?? 0);
        }
        if ($fallbackId <= 0) {
            $fallbackId = (int)($row['lead_pr_gh_type'] ?? 0);
        }

        if ($fallbackId > 0 && isset($ghTypeNameMap[$fallbackId])) {
            return (string)$ghTypeNameMap[$fallbackId];
        }

        return '';
    }

    protected function buildPhoneMap($leadIds = [])
    {
        $leadIds = is_array($leadIds) ? $leadIds : [];
        $leadIds = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));
        if (empty($leadIds)) {
            return [];
        }

        $result = [];
        $needQueryLeadIds = [];
        foreach ($leadIds as $leadId) {
            if (array_key_exists($leadId, self::$phoneMapCache)) {
                $result[$leadId] = (string)self::$phoneMapCache[$leadId];
            } else {
                $needQueryLeadIds[] = $leadId;
            }
        }

        if (empty($needQueryLeadIds)) {
            return $result;
        }

        $hasContactType = $this->hasColumn('crm_contacts', 'contact_type');
        $phoneType = (int)ContactMap::CONTACT_MAP['phone'];
        $firstMap = [];
        $phoneMap = [];
        $leadIdChunks = array_chunk($needQueryLeadIds, 500);
        foreach ($leadIdChunks as $leadIdChunk) {
            if (empty($leadIdChunk)) {
                continue;
            }

            $contacts = Db::table('crm_contacts')
                ->field('leads_id, contact_value' . ($hasContactType ? ', contact_type' : ''))
                ->whereIn('leads_id', $leadIdChunk)
                ->order('id asc')
                ->select();
            if (is_object($contacts) && method_exists($contacts, 'toArray')) {
                $contacts = $contacts->toArray();
            } elseif (!is_array($contacts)) {
                $contacts = [];
            }

            foreach ($contacts as $contact) {
                $leadId = (int)($contact['leads_id'] ?? 0);
                $contactValue = trim((string)($contact['contact_value'] ?? ''));
                if ($leadId <= 0 || $contactValue === '') {
                    continue;
                }
                if (!isset($firstMap[$leadId])) {
                    $firstMap[$leadId] = $contactValue;
                }
                if ($hasContactType && (int)($contact['contact_type'] ?? -1) === $phoneType && !isset($phoneMap[$leadId])) {
                    $phoneMap[$leadId] = $contactValue;
                }
            }
        }

        foreach ($needQueryLeadIds as $leadId) {
            if (isset($phoneMap[$leadId])) {
                self::$phoneMapCache[$leadId] = $phoneMap[$leadId];
            } elseif (isset($firstMap[$leadId])) {
                self::$phoneMapCache[$leadId] = $firstMap[$leadId];
            } else {
                self::$phoneMapCache[$leadId] = '';
            }
            $result[$leadId] = (string)self::$phoneMapCache[$leadId];
        }

        return $result;
    }

    protected function findLeadIdsByPhone($phone = '')
    {
        $phone = strtolower(trim((string)$phone));
        if ($phone === '') {
            return [];
        }

        if (array_key_exists($phone, self::$phoneSearchCache)) {
            return self::$phoneSearchCache[$phone];
        }

        $leadIds = Db::table('crm_contacts')
            ->whereLike('contact_value', '%' . $phone . '%')
            ->column('DISTINCT leads_id');
        if (!is_array($leadIds)) {
            $leadIds = [];
        }

        $result = array_values(array_unique(array_filter(array_map('intval', $leadIds), function ($id) {
            return $id > 0;
        })));

        self::$phoneSearchCache[$phone] = $result;

        return $result;
    }

    public function exportPickLog($params = [])
    {
        $params = is_array($params) ? $params : [];
        unset($params['page'], $params['limit']);
        $params['page'] = 1;
        $params['limit'] = self::EXPORT_LIMIT;

        $list = $this->getPickLogList($params);
        $data = isset($list['data']) && is_array($list['data']) ? $list['data'] : [];
        if (count($data) > self::EXPORT_LIMIT) {
            $data = array_slice($data, 0, self::EXPORT_LIMIT);
        }

        return [
            'count' => min((int)($list['count'] ?? 0), self::EXPORT_LIMIT),
            'data' => $data,
        ];
    }

    /**
     * 批量隐藏客户提取记录（软隐藏，不做物理删除）。
     *
     * @param array $ids 需要隐藏的日志ID数组
     * @param array $operatorInfo 当前操作人信息（admin_id、username）
     * @return array
     */
    public function batchHidePickLog($ids = [], $operatorInfo = [])
    {
        $ids = is_array($ids) ? $ids : [];
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '请选择需要隐藏的记录', 'data' => []];
        }

        $hasIsDeleted = $this->hasColumn('crm_liberum_pick_log', 'is_deleted');
        if (!$hasIsDeleted) {
            return ['code' => -200, 'msg' => '当前表缺少 is_deleted 字段，无法执行隐藏操作', 'data' => []];
        }

        $hasDeletedTime = $this->hasColumn('crm_liberum_pick_log', 'deleted_time');
        $hasDeletedBy = $this->hasColumn('crm_liberum_pick_log', 'deleted_by');
        $hasDeleteRemark = $this->hasColumn('crm_liberum_pick_log', 'delete_remark');

        $nowDateTime = date('Y-m-d H:i:s');
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $updateData = [
            'is_deleted' => 1,
        ];
        if ($hasDeletedTime) {
            $updateData['deleted_time'] = $nowDateTime;
        }
        if ($hasDeletedBy) {
            $updateData['deleted_by'] = $operatorId;
        }
        if ($hasDeleteRemark) {
            $updateData['delete_remark'] = '管理员批量隐藏提取记录';
        }

        try {
            $affected = (int)Db::table('crm_liberum_pick_log')
                ->whereIn('id', $ids)
                ->where('is_deleted', 0)
                ->update($updateData);
        } catch (\Throwable $e) {
            return ['code' => -200, 'msg' => '隐藏失败：' . $e->getMessage(), 'data' => []];
        }

        if ($affected <= 0) {
            return ['code' => -200, 'msg' => '没有可隐藏的记录', 'data' => []];
        }

        return ['code' => 0, 'msg' => '隐藏成功', 'data' => ['count' => $affected]];
    }

    public function batchReturnToLiberum($ids = [], $operatorInfo = [])
    {
        $ids = is_array($ids) ? $ids : [];
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '请选择需要退回的记录', 'data' => []];
        }

        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));
        $nowDateTime = date('Y-m-d H:i:s');
        $nowTimestamp = time();

        $hasLogReturnedTime = $this->hasColumn('crm_liberum_pick_log', 'returned_time');
        $hasLogReturnOperatorId = $this->hasColumn('crm_liberum_pick_log', 'return_operator_id');
        $hasLogReturnOperatorName = $this->hasColumn('crm_liberum_pick_log', 'return_operator_name');
        $hasLogReturnRemark = $this->hasColumn('crm_liberum_pick_log', 'return_remark');
        $hasLogPrGhType = $this->hasColumn('crm_liberum_pick_log', 'pr_gh_type');
        $hasLogGhTypeId = $this->hasColumn('crm_liberum_pick_log', 'gh_type_id');
        $hasLogGhType = $this->hasColumn('crm_liberum_pick_log', 'gh_type');
        $hasLeadsToGhTime = $this->hasColumn('crm_leads', 'to_gh_time');
        $hasLeadsPrUserId = $this->hasColumn('crm_leads', 'pr_user_id');
        $hasLeadsPrGhType = $this->hasColumn('crm_leads', 'pr_gh_type');
        $hasInLogLeadsId = $this->hasColumn('crm_liberum_in_log', 'leads_id');
        $hasInLogKhName = $this->hasColumn('crm_liberum_in_log', 'kh_name');
        $hasInLogBeforePrUser = $this->hasColumn('crm_liberum_in_log', 'before_pr_user');
        $hasInLogOperatorId = $this->hasColumn('crm_liberum_in_log', 'operator_id');
        $hasInLogOperatorName = $this->hasColumn('crm_liberum_in_log', 'operator_name');
        $hasInLogReason = $this->hasColumn('crm_liberum_in_log', 'reason');
        $hasInLogInTime = $this->hasColumn('crm_liberum_in_log', 'in_time');
        $hasInLogIsRecovered = $this->hasColumn('crm_liberum_in_log', 'is_recovered');
        $hasInLogCreateTime = $this->hasColumn('crm_liberum_in_log', 'create_time');
        $hasInLogSourceType = $this->hasColumn('crm_liberum_in_log', 'source_type');
        $hasInLogReturnSource = $this->hasColumn('crm_liberum_in_log', 'return_source');
        $hasInLogLiberumType = $this->hasColumn('crm_liberum_in_log', 'liberum_type');

        $successCount = 0;
        $skipCount = 0;
        $failCount = 0;

        foreach ($ids as $logId) {
            Db::startTrans();
            try {
                $log = Db::table('crm_liberum_pick_log')
                    ->where('id', $logId)
                    ->lock(true)
                    ->find();

                if (empty($log)) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                if ((int)($log['is_returned'] ?? 0) === 1) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $leadId = 0;
                if (!empty($log['leads_id'])) {
                    $leadId = (int)$log['leads_id'];
                }
                if ($leadId <= 0 && !empty($log['active_leads_id'])) {
                    $leadId = (int)$log['active_leads_id'];
                }
                if ($leadId <= 0) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $lead = Db::table('crm_leads')
                    ->where('id', $leadId)
                    ->lock(true)
                    ->find();

                if (empty($lead)) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                if (isset($lead['issuccess']) && (int)$lead['issuccess'] === 1) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                if ((int)($lead['status'] ?? 0) === 2) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                if ((int)($lead['status'] ?? 0) !== 1) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $pickUser = trim((string)($log['pick_user'] ?? ''));
                $currentPrUser = trim((string)($lead['pr_user'] ?? ''));
                if ($pickUser === '' || $currentPrUser !== $pickUser) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $restoredGhType = '手动退回公海';
                $restoredGhTypeId = 0;
                if ($hasLogPrGhType && array_key_exists('pr_gh_type', $log) && trim((string)$log['pr_gh_type']) !== '') {
                    $restoredGhTypeId = (int)$log['pr_gh_type'];
                } elseif ($hasLogGhTypeId && array_key_exists('gh_type_id', $log) && trim((string)$log['gh_type_id']) !== '') {
                    $restoredGhTypeId = (int)$log['gh_type_id'];
                }
                if ($restoredGhTypeId > 0) {
                    $restoredGhType = $restoredGhTypeId;
                } elseif ($hasLogGhType && array_key_exists('gh_type', $log) && trim((string)$log['gh_type']) !== '') {
                    $restoredGhType = $log['gh_type'];
                }

                $leadKhName = isset($lead['kh_name']) ? (string)$lead['kh_name'] : '';
                $leadUpdate = [
                    'status' => 2,
                    'pr_user_bef' => $currentPrUser,
                    'pr_user' => '',
                    'ut_time' => $nowDateTime,
                ];
                if ($hasLeadsPrUserId) {
                    $leadUpdate['pr_user_id'] = 0;
                }
                if ($hasLeadsPrGhType) {
                    $leadUpdate['pr_gh_type'] = $restoredGhType;
                }
                if ($hasLeadsToGhTime) {
                    $leadUpdate['to_gh_time'] = $nowDateTime;
                }

                $leadUpdatedRows = Db::table('crm_leads')
                    ->where('id', $leadId)
                    ->where('status', 1)
                    ->where('pr_user', $pickUser)
                    ->update($leadUpdate);

                if ($leadUpdatedRows !== 1) {
                    Db::rollback();
                    $failCount++;
                    continue;
                }

                $logUpdate = [
                    'is_returned' => 1,
                ];
                if ($hasLogReturnedTime) {
                    $logUpdate['returned_time'] = $nowDateTime;
                }
                if ($hasLogReturnOperatorId) {
                    $logUpdate['return_operator_id'] = $operatorId;
                }
                if ($hasLogReturnOperatorName) {
                    $logUpdate['return_operator_name'] = $operatorName;
                }
                if ($hasLogReturnRemark) {
                    $logUpdate['return_remark'] = '客户提取记录批量退回公海';
                }

                $logUpdatedRows = Db::table('crm_liberum_pick_log')
                    ->where('id', $logId)
                    ->where('is_returned', 0)
                    ->update($logUpdate);
                if ($logUpdatedRows !== 1) {
                    Db::rollback();
                    $failCount++;
                    continue;
                }

                $inLogData = [];
                if ($hasInLogLeadsId) {
                    $inLogData['leads_id'] = $leadId;
                }
                if ($hasInLogKhName) {
                    $inLogData['kh_name'] = $leadKhName !== '' ? $leadKhName : '';
                }
                if ($hasInLogBeforePrUser) {
                    $inLogData['before_pr_user'] = $currentPrUser;
                }
                if ($hasInLogOperatorId) {
                    $inLogData['operator_id'] = $operatorId;
                }
                if ($hasInLogOperatorName) {
                    $inLogData['operator_name'] = $operatorName;
                }
                if ($hasInLogReason) {
                    $inLogData['reason'] = '客户提取记录退回公海（原领取人：' . $currentPrUser . '）';
                }
                if ($hasInLogInTime) {
                    $inLogData['in_time'] = $nowDateTime;
                }
                if ($hasInLogIsRecovered) {
                    $inLogData['is_recovered'] = 0;
                }
                if ($hasInLogCreateTime) {
                    $inLogData['create_time'] = $nowTimestamp;
                }
                if ($hasInLogSourceType) {
                    $inLogData['source_type'] = 'pick_return';
                }
                if ($hasInLogReturnSource) {
                    $inLogData['return_source'] = 'pick_log_batch_return';
                }
                if ($hasInLogLiberumType) {
                    $inLogData['liberum_type'] = $restoredGhTypeId > 0 ? $restoredGhTypeId : 0;
                }

                if (!empty($inLogData)) {
                    $inLogInsertedRows = Db::table('crm_liberum_in_log')->insert($inLogData);
                    if ((int)$inLogInsertedRows !== 1) {
                        Db::rollback();
                        $failCount++;
                        continue;
                    }
                }

                Db::commit();
                $successCount++;
            } catch (\Throwable $e) {
                Db::rollback();
                $failCount++;
            }
        }

        $msg = '成功退回 ' . $successCount . ' 条，跳过 ' . $skipCount . ' 条，失败 ' . $failCount . ' 条。';
        if ($successCount > 0) {
            return [
                'code' => 0,
                'msg' => $msg,
                'data' => [
                    'success_count' => $successCount,
                    'skip_count' => $skipCount,
                    'fail_count' => $failCount,
                    'operate_time' => $nowTimestamp,
                ],
            ];
        }

        return [
            'code' => -200,
            'msg' => $msg,
            'data' => [
                'success_count' => $successCount,
                'skip_count' => $skipCount,
                'fail_count' => $failCount,
                'operate_time' => $nowTimestamp,
            ],
        ];
    }
}
