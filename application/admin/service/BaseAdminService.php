<?php

namespace app\admin\service;

use think\Db;

class BaseAdminService
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
}
