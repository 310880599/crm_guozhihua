<?php

namespace app\admin\service;

use think\Db;
use think\Log;

class BaseAdminService
{
    /**
     * 当前请求生命周期内的字段缓存
     * [
     *   'crm_leads' => [
     *      'status' => true,
     *      'pr_user' => true
     *   ]
     * ]
     *
     * @var array
     */
    protected static $columnCache = [];

    protected function hasColumn($table, $column)
    {
        try {
            $table = strtolower(trim((string)$table));
            $column = strtolower(trim((string)$column));
            if ($table === '' || $column === '') {
                return false;
            }

            if (!isset(self::$columnCache[$table])) {
                $this->getTableColumns($table);
            }

            return isset(self::$columnCache[$table][$column]);
        } catch (\Throwable $e) {
            Log::record(
                'hasColumn error: table=' . (string)$table
                . ', column=' . (string)$column
                . ', message=' . $e->getMessage(),
                'error'
            );
            return false;
        }
    }

    /**
     * 批量读取并缓存表字段
     *
     * @param string $table
     * @return array
     */
    protected function getTableColumns($table)
    {
        $table = strtolower(trim((string)$table));
        if ($table === '') {
            self::$columnCache[$table] = [];
            return self::$columnCache[$table];
        }

        if (isset(self::$columnCache[$table])) {
            return self::$columnCache[$table];
        }

        $rows = Db::query("SHOW COLUMNS FROM `{$table}`");
        $columns = [];
        foreach ($rows as $row) {
            if (!empty($row['Field'])) {
                $columns[strtolower($row['Field'])] = true;
            }
        }

        self::$columnCache[$table] = $columns;

        return self::$columnCache[$table];
    }
}
