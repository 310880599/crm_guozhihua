<?php

namespace app\admin\service;

use app\admin\behavior\ContactMap;
use app\admin\model\LiberumInLog;
use app\admin\model\LiberumPickLog;
use think\Db;

class LiberumLogService
{
    protected function hasColumn($table, $column)
    {
        try {
            $table = trim((string)$table);
            $column = trim((string)$column);
            if ($table === '' || $column === '') {
                return false;
            }
            $result = Db::query("SHOW COLUMNS FROM `{$table}` LIKE '{$column}'");
            return !empty($result);
        } catch (\Throwable $e) {
            return false;
        }
    }

    public function getPickLogList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        try {
            $hasActiveLeadsId = $this->hasColumn('crm_liberum_pick_log', 'active_leads_id');
            $hasOperatorName = $this->hasColumn('crm_liberum_pick_log', 'operator_name');
            $hasPickOperatorId = $this->hasColumn('crm_liberum_pick_log', 'operator_id');
            $hasContactType = $this->hasColumn('crm_contacts', 'contact_type');
            $phoneType = (int)ContactMap::CONTACT_MAP['phone'];

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
            $phoneField = $hasContactType
                ? 'IFNULL(SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ' . $phoneType . ' THEN c.contact_value END), ",", 1), "") AS client_phone'
                : 'IFNULL(SUBSTRING_INDEX(GROUP_CONCAT(c.contact_value), ",", 1), "") AS client_phone';

            $model = new LiberumPickLog();
            $query = $model->alias('pl')
                ->leftJoin('crm_leads l', $leadJoin)
                ->leftJoin('crm_contacts c', 'c.leads_id = l.id');

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
                $query->whereLike('c.contact_value', '%' . $phone . '%');
            }

            if (array_key_exists('is_returned', $params) && $params['is_returned'] !== '') {
                $query->where('pl.is_returned', (int)$params['is_returned']);
            }

            $count = (int)(clone $query)->count('DISTINCT pl.id');
            $data = $query
                ->field([
                    'pl.id',
                    'IFNULL(MAX(pl.leads_id), 0) AS leads_id',
                    $activeLeadsIdField,
                    'IFNULL(MAX(l.id), IFNULL(MAX(pl.leads_id), 0)) AS client_id',
                    'IFNULL(MAX(l.kh_name), IFNULL(MAX(pl.kh_name), "")) AS client_name',
                    $phoneField,
                    'IFNULL(MAX(pl.pick_user), "") AS pick_user',
                    'IFNULL(MAX(pl.pick_date), "") AS pick_date',
                    'IFNULL(MAX(pl.pick_time), "") AS pick_time',
                    'IFNULL(MAX(pl.before_pr_user), "") AS before_pr_user',
                    'IFNULL(MAX(l.pr_user), "") AS current_pr_user',
                    'IFNULL(MAX(pl.is_returned), 0) AS is_returned',
                    $pickOperatorNameField,
                    $pickOperatorIdField,
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
        } catch (\Throwable $e) {
            $count = 0;
            $data = [];
        }

        return [
            'count' => $count,
            'data' => $data,
        ];
    }

    public function getInLogList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        try {
            $hasInOperatorName = $this->hasColumn('crm_liberum_in_log', 'operator_name');
            $hasInOperatorId = $this->hasColumn('crm_liberum_in_log', 'operator_id');
            $hasContactType = $this->hasColumn('crm_contacts', 'contact_type');
            $phoneType = (int)ContactMap::CONTACT_MAP['phone'];

            $inOperatorNameField = $hasInOperatorName
                ? 'IFNULL(MAX(il.operator_name), "") AS operator_name'
                : '"" AS operator_name';
            $inOperatorIdField = $hasInOperatorId
                ? 'IFNULL(MAX(il.operator_id), 0) AS operator_id'
                : '0 AS operator_id';
            $phoneField = $hasContactType
                ? 'IFNULL(SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ' . $phoneType . ' THEN c.contact_value END), ",", 1), "") AS client_phone'
                : 'IFNULL(SUBSTRING_INDEX(GROUP_CONCAT(c.contact_value), ",", 1), "") AS client_phone';

            $model = new LiberumInLog();
            $query = $model->alias('il')
                ->leftJoin('crm_leads l', 'il.leads_id = l.id')
                ->leftJoin('crm_contacts c', 'c.leads_id = l.id');

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
                $query->whereLike('c.contact_value', '%' . trim((string)$params['phone']) . '%');
            }

            if (array_key_exists('is_recovered', $params) && $params['is_recovered'] !== '') {
                $query->where('il.is_recovered', (int)$params['is_recovered']);
            }

            $count = (int)(clone $query)->count('DISTINCT il.id');
            $data = $query
                ->field([
                    'il.id',
                    'IFNULL(MAX(il.leads_id), 0) AS leads_id',
                    'IFNULL(MAX(l.id), IFNULL(MAX(il.leads_id), 0)) AS client_id',
                    'IFNULL(MAX(l.kh_name), IFNULL(MAX(il.kh_name), "")) AS client_name',
                    $phoneField,
                    'IFNULL(MAX(il.before_pr_user), "") AS before_pr_user',
                    'IFNULL(MAX(l.pr_user), "") AS current_pr_user',
                    'IFNULL(MAX(il.reason), "") AS reason',
                    'IFNULL(MAX(il.in_time), "") AS in_time',
                    $inOperatorIdField,
                    $inOperatorNameField,
                    'IFNULL(MAX(il.is_recovered), 0) AS is_recovered',
                    'IFNULL(MAX(il.recovered_time), "") AS recovered_time',
                    'IFNULL(MAX(l.status), "") AS current_status',
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
        } catch (\Throwable $e) {
            $count = 0;
            $data = [];
        }

        return [
            'count' => $count,
            'data' => $data,
        ];
    }
}
