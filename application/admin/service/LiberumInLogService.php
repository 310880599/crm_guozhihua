<?php

namespace app\admin\service;

use app\admin\behavior\ContactMap;
use app\admin\model\LiberumInLog;
use think\Db;

class LiberumInLogService extends BaseAdminService
{
    const EXPORT_LIMIT = 5000;
    protected static $phoneMapCache = [];
    protected static $phoneSearchCache = [];

    public function getInLogList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        try {
            $hasInOperatorName = $this->hasColumn('crm_liberum_in_log', 'operator_name');
            $hasInOperatorId = $this->hasColumn('crm_liberum_in_log', 'operator_id');
            $hasInSourceType = $this->hasColumn('crm_liberum_in_log', 'source_type');
            $hasRecoverOperatorName = $this->hasColumn('crm_liberum_in_log', 'recover_operator_name');
            $hasRecoverOperatorId = $this->hasColumn('crm_liberum_in_log', 'recover_operator_id');
            $hasRecoverTime = $this->hasColumn('crm_liberum_in_log', 'recover_time');
            $hasRecoveredTime = $this->hasColumn('crm_liberum_in_log', 'recovered_time');

            $inOperatorNameField = $hasInOperatorName
                ? 'IFNULL(MAX(il.operator_name), "") AS operator_name'
                : '"" AS operator_name';
            $inOperatorIdField = $hasInOperatorId
                ? 'IFNULL(MAX(il.operator_id), 0) AS operator_id'
                : '0 AS operator_id';
            $recoverOperatorNameField = $hasRecoverOperatorName
                ? 'IFNULL(MAX(il.recover_operator_name), "") AS recover_operator_name'
                : '"" AS recover_operator_name';
            $recoverOperatorIdField = $hasRecoverOperatorId
                ? 'IFNULL(MAX(il.recover_operator_id), 0) AS recover_operator_id'
                : '0 AS recover_operator_id';
            $sourceTypeField = $hasInSourceType
                ? 'IFNULL(MAX(il.source_type), "") AS source_type'
                : '"" AS source_type';
            if ($hasRecoverTime) {
                $recoveredTimeField = 'IFNULL(MAX(il.recover_time), "") AS recovered_time';
            } elseif ($hasRecoveredTime) {
                $recoveredTimeField = 'IFNULL(MAX(il.recovered_time), "") AS recovered_time';
            } else {
                $recoveredTimeField = '"" AS recovered_time';
            }

            $model = new LiberumInLog();
            $query = $model->alias('il')
                ->leftJoin('crm_leads l', 'il.leads_id = l.id');

            if (!empty($params['in_date'])) {
                $inDate = trim((string)$params['in_date']);
                $query->whereBetweenTime('il.in_time', $inDate . ' 00:00:00', $inDate . ' 23:59:59');
            }

            if (!empty($params['before_pr_user_keyword'])) {
                $query->whereLike('il.before_pr_user', '%' . trim((string)$params['before_pr_user_keyword']) . '%');
            }

            if (!empty($params['operator_keyword'])) {
                $operatorKeyword = trim((string)$params['operator_keyword']);
                if ($hasInOperatorName || $hasInOperatorId) {
                    $query->where(function ($subQuery) use ($operatorKeyword, $hasInOperatorName, $hasInOperatorId) {
                        if ($hasInOperatorName) {
                            $subQuery->whereLike('il.operator_name', '%' . $operatorKeyword . '%');
                        }
                        if ($hasInOperatorId) {
                            if ($hasInOperatorName) {
                                $subQuery->whereOr('il.operator_id', 'like', '%' . $operatorKeyword . '%');
                            } else {
                                $subQuery->whereLike('il.operator_id', '%' . $operatorKeyword . '%');
                            }
                        }
                    });
                }
            }

            if (!empty($params['reason'])) {
                $query->whereLike('il.reason', '%' . trim((string)$params['reason']) . '%');
            }

            if (!empty($params['client_keyword'])) {
                $clientKeyword = trim((string)$params['client_keyword']);
                $query->where(function ($subQuery) use ($clientKeyword) {
                    $subQuery->whereLike('l.kh_name', '%' . $clientKeyword . '%')
                        ->whereOr('il.kh_name', 'like', '%' . $clientKeyword . '%');
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
                $query->whereIn('il.leads_id', $matchedLeadIds);
            }

            if (array_key_exists('is_recovered', $params) && $params['is_recovered'] !== '') {
                $query->where('il.is_recovered', (int)$params['is_recovered']);
            }

            $count = (int)(clone $query)->count('il.id');
            $data = $query
                ->field([
                    'il.id',
                    'IFNULL(MAX(il.leads_id), 0) AS leads_id',
                    'IFNULL(MAX(l.id), IFNULL(MAX(il.leads_id), 0)) AS client_id',
                    'IFNULL(MAX(l.id), IFNULL(MAX(il.leads_id), 0)) AS contact_leads_id',
                    'IFNULL(MAX(l.kh_name), IFNULL(MAX(il.kh_name), "")) AS client_name',
                    '"" AS client_phone',
                    'IFNULL(MAX(il.before_pr_user), "") AS before_pr_user',
                    'IFNULL(MAX(l.pr_user), "") AS current_pr_user',
                    'IFNULL(MAX(il.reason), "") AS reason',
                    'IFNULL(MAX(il.in_time), "") AS in_time',
                    $inOperatorIdField,
                    $inOperatorNameField,
                    $recoverOperatorIdField,
                    $recoverOperatorNameField,
                    'IFNULL(MAX(il.is_recovered), 0) AS is_recovered',
                    $recoveredTimeField,
                    'IFNULL(MAX(l.status), "") AS current_status',
                    'IFNULL(MAX(l.pr_gh_type), "") AS current_gh_type',
                    $sourceTypeField,
                ])
                ->group('il.id')
                ->order('il.id desc')
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
                foreach ($data as &$row) {
                    $mapLeadId = (int)($row['contact_leads_id'] ?? 0);
                    $row['client_phone'] = $phoneMap[$mapLeadId] ?? '';
                    unset($row['contact_leads_id']);
                }
                unset($row);
            }
        } catch (\Throwable $e) {
            \think\Log::record('客户流入公海记录查询失败：' . $e->getMessage(), 'error');
            $count = 0;
            $data = [];
        }

        return [
            'count' => $count,
            'data' => $data,
        ];
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

    public function exportInLog($params = [])
    {
        $params = is_array($params) ? $params : [];
        unset($params['page'], $params['limit']);
        $params['page'] = 1;
        $params['limit'] = self::EXPORT_LIMIT;

        $list = $this->getInLogList($params);
        $data = isset($list['data']) && is_array($list['data']) ? $list['data'] : [];
        if (count($data) > self::EXPORT_LIMIT) {
            $data = array_slice($data, 0, self::EXPORT_LIMIT);
        }

        return [
            'count' => min((int)($list['count'] ?? 0), self::EXPORT_LIMIT),
            'data' => $data,
        ];
    }

    public function batchRestoreOwner($ids = [], $operatorInfo = [])
    {
        $ids = is_array($ids) ? $ids : [];
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids), function ($id) {
            return $id > 0;
        })));
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '请选择需要恢复的记录', 'data' => []];
        }

        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));
        $nowDateTime = date('Y-m-d H:i:s');
        $nowTimestamp = time();

        $hasLogRecoverTime = $this->hasColumn('crm_liberum_in_log', 'recover_time');
        $hasLogRecoveredTime = !$hasLogRecoverTime && $this->hasColumn('crm_liberum_in_log', 'recovered_time');
        $hasLogRecoverOperatorId = $this->hasColumn('crm_liberum_in_log', 'recover_operator_id');
        $hasLogRecoverOperatorName = $this->hasColumn('crm_liberum_in_log', 'recover_operator_name');

        $successCount = 0;
        $skipCount = 0;
        $failCount = 0;

        foreach ($ids as $logId) {
            Db::startTrans();
            try {
                $log = Db::table('crm_liberum_in_log')
                    ->where('id', $logId)
                    ->lock(true)
                    ->find();

                if (empty($log)) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                if ((int)($log['is_recovered'] ?? 0) !== 0) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $leadId = (int)($log['leads_id'] ?? 0);
                if ($leadId <= 0) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $beforePrUser = trim((string)($log['before_pr_user'] ?? ''));
                if ($beforePrUser === '') {
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

                if ((int)($lead['status'] ?? 0) !== 2) {
                    $skipCount++;
                    Db::commit();
                    continue;
                }

                $leadUpdatedRows = Db::table('crm_leads')
                    ->where('id', $leadId)
                    ->where('status', 2)
                    ->update([
                        'status' => 1,
                        'pr_user' => $beforePrUser,
                        'ut_time' => $nowDateTime,
                    ]);
                if ((int)$leadUpdatedRows !== 1) {
                    Db::rollback();
                    $failCount++;
                    continue;
                }

                $logUpdate = [
                    'is_recovered' => 1,
                ];
                if ($hasLogRecoverTime) {
                    $logUpdate['recover_time'] = $nowDateTime;
                } elseif ($hasLogRecoveredTime) {
                    $logUpdate['recovered_time'] = $nowDateTime;
                }
                if ($hasLogRecoverOperatorId) {
                    $logUpdate['recover_operator_id'] = $operatorId;
                }
                if ($hasLogRecoverOperatorName) {
                    $logUpdate['recover_operator_name'] = $operatorName;
                }

                $logUpdatedRows = Db::table('crm_liberum_in_log')
                    ->where('id', $logId)
                    ->where('is_recovered', 0)
                    ->update($logUpdate);
                if ((int)$logUpdatedRows !== 1) {
                    Db::rollback();
                    $failCount++;
                    continue;
                }

                Db::commit();
                $successCount++;
            } catch (\Throwable $e) {
                Db::rollback();
                \think\Log::record('批量恢复原负责人失败，log_id=' . $logId . '，错误：' . $e->getMessage(), 'error');
                $failCount++;
            }
        }

        $msg = '成功恢复 ' . $successCount . ' 条，跳过 ' . $skipCount . ' 条，失败 ' . $failCount . ' 条。';
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
