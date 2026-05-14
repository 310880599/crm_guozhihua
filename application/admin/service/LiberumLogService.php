<?php

namespace app\admin\service;

use app\admin\model\LiberumInLog;
use app\admin\model\LiberumPickLog;

class LiberumLogService
{
    public function getPickLogList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        try {
            $model = new LiberumPickLog();
            $query = $model->order('id desc');
            $count = (int)(clone $query)->count();
            $data = $query->page($page, $limit)->select();
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
            $model = new LiberumInLog();
            $query = $model->order('id desc');
            $count = (int)(clone $query)->count();
            $data = $query->page($page, $limit)->select();
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
