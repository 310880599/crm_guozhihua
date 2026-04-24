<?php

namespace app\admin\service;

use app\admin\model\PositionTitle as PositionTitleModel;
use think\Db;

class PositionTitleService
{
    /** @var array<string, array<string, bool>> */
    private static $tableColumnsCache = [];

    /**
     * 获取用于下拉框的职位身份列表（仅有效数据）
     *
     * @return array<int, array{id:int,position_title:string}>
     */
    public function getActivePositionTitleList(): array
    {
        $nameField = $this->getNameField();
        $query = PositionTitleModel::where('id', '>', 0);
        $this->applyActiveFilter($query);
        $rows = $query->field('id,' . $nameField)->order('id asc')->select();

        $result = [];
        foreach ($rows as $row) {
            $name = trim((string)($row[$nameField] ?? ''));
            if ($name === '') {
                continue;
            }
            $result[] = [
                'id' => (int)$row['id'],
                'position_title' => $name,
            ];
        }

        return $result;
    }

    /**
     * 验证职位身份ID并返回规范化数据
     *
     * @param mixed $id
     * @return array{0:bool,1:string,2:?array{id:int,name:string}}
     */
    public function validatePositionTitle($id): array
    {
        $id = (int)$id;
        if ($id <= 0) {
            return [true, '', null];
        }

        $nameField = $this->getNameField();
        $query = PositionTitleModel::where('id', $id);
        $this->applyActiveFilter($query);
        $row = $query->field('id,' . $nameField)->find();
        if (!$row) {
            return [false, '所选职位身份不存在或已禁用，请重新选择', null];
        }

        return [true, '', [
            'id' => (int)$row['id'],
            'name' => trim((string)$row[$nameField]),
        ]];
    }

    /**
     * 通过名称反查职位身份（用于旧数据兼容）
     *
     * @param string $name
     * @return array{id:int,name:string}|null
     */
    public function findByName(string $name): ?array
    {
        $name = trim($name);
        if ($name === '') {
            return null;
        }

        $nameField = $this->getNameField();
        $query = PositionTitleModel::where($nameField, $name);
        $this->applyActiveFilter($query);
        $row = $query->field('id,' . $nameField)->find();
        if (!$row) {
            return null;
        }

        return [
            'id' => (int)$row['id'],
            'name' => trim((string)$row[$nameField]),
        ];
    }

    /**
     * 从表结构中识别职位名称字段（title_name / position_title）
     */
    private function getNameField(): string
    {
        if ($this->hasColumn('title_name')) {
            return 'title_name';
        }
        if ($this->hasColumn('position_title')) {
            return 'position_title';
        }
        return 'title_name';
    }

    /**
     * 根据现有字段套用“有效数据”过滤条件
     *
     * @param \think\db\Query $query
     * @return void
     */
    private function applyActiveFilter($query): void
    {
        if ($this->hasColumn('is_deleted')) {
            $query->where('is_deleted', 0);
        } elseif ($this->hasColumn('is_delete')) {
            $query->where('is_delete', 0);
        }

        if ($this->hasColumn('status') && !$this->hasColumn('is_deleted') && !$this->hasColumn('is_delete')) {
            $query->where('status', 0);
        }
    }

    private function hasColumn(string $column): bool
    {
        if (!isset(self::$tableColumnsCache['crm_position_title'])) {
            self::$tableColumnsCache['crm_position_title'] = $this->loadTableColumns('crm_position_title');
        }
        return !empty(self::$tableColumnsCache['crm_position_title'][$column]);
    }

    /**
     * @return array<string, bool>
     */
    private function loadTableColumns(string $table): array
    {
        try {
            $rows = Db::query("SHOW COLUMNS FROM `{$table}`");
            $map = [];
            foreach ($rows as $row) {
                if (!empty($row['Field'])) {
                    $map[$row['Field']] = true;
                }
            }
            return $map;
        } catch (\Throwable $e) {
            return [];
        }
    }

    /**
     * 获取详情（仅未删除）
     *
     * @param int $id
     * @return PositionTitleModel|null
     */
    public function getDetail($id)
    {
        $id = (int)$id;
        if ($id <= 0) {
            return null;
        }
        return PositionTitleModel::where('id', $id)->where('is_deleted', 0)->find();
    }

    /**
     * 职位身份列表（分页 + 名称模糊搜索 + 仅未删除）
     *
     * @param array $params
     * @return array
     */
    public function getList($params = [])
    {
        $params = is_array($params) ? $params : [];
        $titleName = trim((string)($params['title_name'] ?? ''));
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        $query = PositionTitleModel::where('is_deleted', 0)->order('id desc');
        if ($titleName !== '') {
            $query->where('title_name', 'like', "%{$titleName}%");
        }

        $list = $query->paginate(['list_rows' => $limit, 'page' => $page])->toArray();

        return [
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'] ?? [],
            'count' => $list['total'] ?? 0,
            'rel' => 1,
        ];
    }

    /**
     * 新增职位身份（支持复活已删除同名）
     *
     * @param array $data
     * @return array
     */
    public function add($data = [])
    {
        $data = is_array($data) ? $data : [];
        $titleName = trim((string)($data['title_name'] ?? ''));
        $createdBy = trim((string)($data['created_by'] ?? ''));
        $now = date('Y-m-d H:i:s');

        if ($titleName === '') {
            return ['code' => -200, 'msg' => '职位身份名称不能为空'];
        }

        $existsActive = PositionTitleModel::where('title_name', $titleName)
            ->where('is_deleted', 0)
            ->find();
        if ($existsActive) {
            return ['code' => -200, 'msg' => '职位身份已存在'];
        }

        $existsDeleted = PositionTitleModel::where('title_name', $titleName)
            ->where('is_deleted', 1)
            ->find();
        if ($existsDeleted) {
            $existsDeleted->is_deleted = 0;
            $existsDeleted->deleted_time = null;
            $existsDeleted->deleted_by = null;
            $existsDeleted->created_by = $createdBy;
            $existsDeleted->update_time = $now;
            $res = $existsDeleted->save();
            return $res ? ['code' => 0, 'msg' => '添加成功！'] : ['code' => -200, 'msg' => '添加失败！'];
        }

        $insertData = [
            'title_name' => $titleName,
            'created_by' => $createdBy,
            'is_deleted' => 0,
            'deleted_time' => null,
            'deleted_by' => null,
            'create_time' => $now,
            'update_time' => $now,
        ];
        $res = PositionTitleModel::create($insertData);
        return $res ? ['code' => 0, 'msg' => '添加成功！'] : ['code' => -200, 'msg' => '添加失败！'];
    }

    /**
     * 编辑职位身份（改名时校验重复）
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

        $entry = PositionTitleModel::where('id', $id)->where('is_deleted', 0)->find();
        if (!$entry) {
            return ['code' => -200, 'msg' => '记录不存在或已删除'];
        }

        $data = is_array($data) ? $data : [];
        $updateData = [];
        $now = date('Y-m-d H:i:s');

        if (array_key_exists('title_name', $data)) {
            $titleName = trim((string)$data['title_name']);
            if ($titleName === '') {
                return ['code' => -200, 'msg' => '职位身份名称不能为空'];
            }

            if ($titleName !== (string)$entry->title_name) {
                $exists = PositionTitleModel::where('title_name', $titleName)
                    ->where('is_deleted', 0)
                    ->where('id', '<>', $id)
                    ->find();
                if ($exists) {
                    return ['code' => -200, 'msg' => '职位身份名称已存在'];
                }
            }
            $updateData['title_name'] = $titleName;
        }

        if (array_key_exists('created_by', $data)) {
            $updateData['created_by'] = trim((string)$data['created_by']);
        }

        if (empty($updateData)) {
            return ['code' => -200, 'msg' => '没有可修改的数据'];
        }

        $updateData['update_time'] = $now;
        $aff = PositionTitleModel::where('id', $id)->where('is_deleted', 0)->update($updateData);
        return $aff ? ['code' => 0, 'msg' => '修改成功！'] : ['code' => -200, 'msg' => '修改失败！'];
    }

    /**
     * 软删除
     *
     * @param int $id
     * @param int $deletedBy
     * @return array
     */
    public function delete($id, $deletedBy = 0)
    {
        $id = (int)$id;
        if ($id <= 0) {
            return ['code' => -200, 'msg' => '参数错误'];
        }
        $now = date('Y-m-d H:i:s');

        $aff = PositionTitleModel::where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'is_deleted' => 1,
                'deleted_time' => $now,
                'deleted_by' => (int)$deletedBy,
                'update_time' => $now,
            ]);

        return $aff ? ['code' => 0, 'msg' => '删除成功！'] : ['code' => -200, 'msg' => '删除失败！'];
    }

    /**
     * 批量软删除
     *
     * @param array|string $ids
     * @param int $deletedBy
     * @return array
     */
    public function batchDelete($ids, $deletedBy = 0)
    {
        if (!is_array($ids)) {
            $ids = explode(',', (string)$ids);
        }
        $ids = array_values(array_unique(array_filter(array_map('intval', $ids))));
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '未选择任何记录'];
        }
        $now = date('Y-m-d H:i:s');

        Db::startTrans();
        try {
            $delCount = PositionTitleModel::whereIn('id', $ids)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => $now,
                    'deleted_by' => (int)$deletedBy,
                    'update_time' => $now,
                ]);
            Db::commit();

            if ($delCount > 0) {
                return ['code' => 0, 'msg' => '删除成功', 'data' => ['count' => $delCount]];
            }
            return ['code' => -200, 'msg' => '删除失败或记录不存在'];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '删除异常：' . $e->getMessage()];
        }
    }

    /**
     * 导入数据写库（控制器负责解析，Service 负责落库）
     *
     * @param array $rows
     * @return array
     */
    public function importRows($rows = [])
    {
        if (!is_array($rows) || empty($rows)) {
            return ['code' => -200, 'msg' => '没有可导入的数据'];
        }

        $normalizedRows = [];
        foreach ($rows as $r) {
            $titleName = trim((string)($r['title_name'] ?? ''));
            $createdBy = trim((string)($r['created_by'] ?? ''));
            if ($titleName === '') {
                continue;
            }
            // 同一文件内重复名称，仅保留首次出现的一条
            if (!isset($normalizedRows[$titleName])) {
                $normalizedRows[$titleName] = [
                    'title_name' => $titleName,
                    'created_by' => $createdBy,
                ];
            }
        }

        if (empty($normalizedRows)) {
            return ['code' => -200, 'msg' => '有效数据为空（职位身份名称为空已被跳过）'];
        }

        try {
            $titleNames = array_keys($normalizedRows);
            $existing = PositionTitleModel::whereIn('title_name', $titleNames)
                ->field('id,title_name,is_deleted')
                ->select()
                ->toArray();

            $existingMap = [];
            foreach ($existing as $item) {
                $existingMap[$item['title_name']] = $item;
            }

            $now = date('Y-m-d H:i:s');
            $toInsert = [];
            $skipCount = 0;
            $reviveCount = 0;
            $insertCount = 0;

            Db::startTrans();
            foreach ($normalizedRows as $row) {
                $titleName = $row['title_name'];
                $createdBy = $row['created_by'];
                if (isset($existingMap[$titleName])) {
                    $exists = $existingMap[$titleName];
                    if ((int)$exists['is_deleted'] === 0) {
                        $skipCount++;
                        continue;
                    }

                    PositionTitleModel::where('id', (int)$exists['id'])->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'created_by' => $createdBy,
                        'update_time' => $now,
                    ]);
                    $reviveCount++;
                    continue;
                }

                $toInsert[] = [
                    'title_name' => $titleName,
                    'created_by' => $createdBy,
                    'is_deleted' => 0,
                    'deleted_time' => null,
                    'deleted_by' => null,
                    'create_time' => $now,
                    'update_time' => $now,
                ];
            }

            if (!empty($toInsert)) {
                $insertCount = (int)PositionTitleModel::insertAll($toInsert);
            }
            Db::commit();

            return [
                'code' => 0,
                'msg' => '导入完成：新增 ' . $insertCount . ' 条，复活 ' . $reviveCount . ' 条，跳过重复 ' . $skipCount . ' 条',
                'data' => [
                    'inserted' => $insertCount,
                    'revived' => $reviveCount,
                    'skipped' => $skipCount,
                ],
            ];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '写入失败：' . $e->getMessage()];
        }
    }
}
