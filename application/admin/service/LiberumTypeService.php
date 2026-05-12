<?php

namespace app\admin\service;

use app\admin\model\LiberumType as LiberumTypeModel;
use think\Db;
use think\facade\Session;

class LiberumTypeService
{
    /**
     * 公海类型列表（仅未删除）
     *
     * @param array $params
     * @param int $page
     * @param int $limit
     * @return array
     */
    public function search($params = [], $page = 1, $limit = 10)
    {
        $params = is_array($params) ? $params : [];
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);
        $typeName = trim((string)($params['type_name'] ?? ''));

        $query = LiberumTypeModel::where('is_deleted', 0)->order('id desc');
        if ($typeName !== '') {
            $query->where('type_name', 'like', "%{$typeName}%");
        }

        $list = $query->paginate(['list_rows' => $limit, 'page' => $page])->toArray();
        $total = (int)($list['total'] ?? 0);

        return [
            'data' => $list['data'] ?? [],
            'total' => $total,
            'count' => $total,
        ];
    }

    /**
     * 新增公海类型（支持恢复同名已删除）
     *
     * @param array $data
     * @return array
     */
    public function add($data = [])
    {
        $data = is_array($data) ? $data : [];
        $typeName = trim((string)($data['type_name'] ?? ''));
        if ($typeName === '') {
            return ['code' => -200, 'msg' => '公海类型名称不能为空'];
        }

        $creator = trim((string)Session::get('username'));
        $nowTs = time();

        $existsActive = LiberumTypeModel::where('type_name', $typeName)
            ->where('is_deleted', 0)
            ->find();
        if ($existsActive) {
            return ['code' => -200, 'msg' => '公海类型已存在'];
        }

        $existsDeleted = LiberumTypeModel::where('type_name', $typeName)
            ->where('is_deleted', 1)
            ->order('id desc')
            ->find();
        if ($existsDeleted) {
            $existsDeleted->is_deleted = 0;
            $existsDeleted->deleted_time = null;
            $existsDeleted->deleted_by = null;
            $existsDeleted->creator = $creator;
            $existsDeleted->update_time = $nowTs;
            $res = $existsDeleted->save();
            return $res ? ['code' => 0, 'msg' => '添加成功！'] : ['code' => -200, 'msg' => '添加失败！'];
        }

        $insertData = [
            'type_name' => $typeName,
            'creator' => $creator,
            'is_deleted' => 0,
            'deleted_time' => null,
            'deleted_by' => null,
            'create_time' => $nowTs,
            'update_time' => $nowTs,
        ];
        $res = LiberumTypeModel::create($insertData);

        return $res ? ['code' => 0, 'msg' => '添加成功！'] : ['code' => -200, 'msg' => '添加失败！'];
    }

    /**
     * 编辑公海类型（仅可编辑未删除记录）
     *
     * @param int $id
     * @param array $data
     * @return array
     */
    public function edit($id, $data = [])
    {
        $id = (int)$id;
        if ($id <= 0) {
            return ['code' => -200, 'msg' => '参数错误'];
        }

        $entry = LiberumTypeModel::where('id', $id)->where('is_deleted', 0)->find();
        if (!$entry) {
            return ['code' => -200, 'msg' => '记录不存在或已删除'];
        }

        $data = is_array($data) ? $data : [];
        $typeName = trim((string)($data['type_name'] ?? ''));
        if ($typeName === '') {
            return ['code' => -200, 'msg' => '公海类型名称不能为空'];
        }

        $exists = LiberumTypeModel::where('type_name', $typeName)
            ->where('is_deleted', 0)
            ->where('id', '<>', $id)
            ->find();
        if ($exists) {
            return ['code' => -200, 'msg' => '公海类型名称已存在'];
        }

        $aff = LiberumTypeModel::where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'type_name' => $typeName,
                'update_time' => time(),
            ]);

        return $aff !== false ? ['code' => 0, 'msg' => '编辑成功！'] : ['code' => -200, 'msg' => '编辑失败！'];
    }

    /**
     * 单条软删除
     *
     * @param int $id
     * @return array
     */
    public function softDelete($id)
    {
        $id = (int)$id;
        if ($id <= 0) {
            return ['code' => -200, 'msg' => '参数错误'];
        }

        $aff = LiberumTypeModel::where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'is_deleted' => 1,
                'deleted_time' => date('Y-m-d H:i:s'),
                'deleted_by' => (int)Session::get('aid'),
                'update_time' => time(),
            ]);

        return $aff ? ['code' => 0, 'msg' => '删除成功！'] : ['code' => -200, 'msg' => '删除失败！'];
    }

    /**
     * 批量软删除
     *
     * @param array|string $ids
     * @return array
     */
    public function batchSoftDelete($ids)
    {
        if (!is_array($ids)) {
            $ids = explode(',', (string)$ids);
        }
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids))));
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '未选择任何记录'];
        }

        Db::startTrans();
        try {
            $delCount = LiberumTypeModel::whereIn('id', $ids)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => (int)Session::get('aid'),
                    'update_time' => time(),
                ]);

            Db::commit();
            if ($delCount > 0) {
                return ['code' => 0, 'msg' => '删除成功', 'data' => ['count' => (int)$delCount]];
            }
            return ['code' => -200, 'msg' => '删除失败或记录不存在'];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '删除异常：' . $e->getMessage()];
        }
    }

    /**
     * 导入行数据（仅处理 type_name）
     *
     * @param array $rows
     * @return array
     */
    public function importRows($rows = [])
    {
        if (!is_array($rows) || empty($rows)) {
            return ['code' => 0, 'msg' => '导入完成：新增 0 条，恢复 0 条，跳过 0 条', 'data' => ['inserted' => 0, 'revived' => 0, 'skipped' => 0]];
        }

        $creator = trim((string)Session::get('username'));
        $nowTs = time();
        $normalizedTypeNames = [];

        foreach ($rows as $row) {
            $typeName = '';
            if (is_array($row)) {
                $typeName = trim((string)($row['type_name'] ?? ''));
            } elseif (is_string($row)) {
                $typeName = trim($row);
            }
            if ($typeName === '') {
                continue;
            }
            if (!isset($normalizedTypeNames[$typeName])) {
                $normalizedTypeNames[$typeName] = true;
            }
        }

        if (empty($normalizedTypeNames)) {
            return ['code' => 0, 'msg' => '导入完成：新增 0 条，恢复 0 条，跳过 0 条', 'data' => ['inserted' => 0, 'revived' => 0, 'skipped' => 0]];
        }

        $typeNames = array_keys($normalizedTypeNames);
        $existingRows = LiberumTypeModel::whereIn('type_name', $typeNames)
            ->field('id,type_name,is_deleted')
            ->select()
            ->toArray();

        $existingMap = [];
        foreach ($existingRows as $item) {
            $existingMap[$item['type_name']] = $item;
        }

        $inserted = 0;
        $revived = 0;
        $skipped = 0;

        Db::startTrans();
        try {
            $toInsert = [];
            foreach ($typeNames as $typeName) {
                if (isset($existingMap[$typeName])) {
                    $exists = $existingMap[$typeName];
                    if ((int)$exists['is_deleted'] === 0) {
                        $skipped++;
                        continue;
                    }

                    LiberumTypeModel::where('id', (int)$exists['id'])->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'creator' => $creator,
                        'update_time' => $nowTs,
                    ]);
                    $revived++;
                    continue;
                }

                $toInsert[] = [
                    'type_name' => $typeName,
                    'creator' => $creator,
                    'is_deleted' => 0,
                    'deleted_time' => null,
                    'deleted_by' => null,
                    'create_time' => $nowTs,
                    'update_time' => $nowTs,
                ];
            }

            if (!empty($toInsert)) {
                $inserted = (int)LiberumTypeModel::insertAll($toInsert);
            }

            Db::commit();
            return [
                'code' => 0,
                'msg' => '导入完成：新增 ' . $inserted . ' 条，恢复 ' . $revived . ' 条，跳过 ' . $skipped . ' 条',
                'data' => [
                    'inserted' => $inserted,
                    'revived' => $revived,
                    'skipped' => $skipped,
                ],
            ];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '导入失败：' . $e->getMessage()];
        }
    }
}
