<?php

namespace app\admin\service;

use app\admin\model\PositionTitle as PositionTitleModel;
use think\Db;

class PositionTitleService
{
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
        $account = trim((string)($params['account'] ?? ''));
        $page = max(1, (int)($params['page'] ?? 1));
        $limit = max(1, (int)($params['limit'] ?? 10));

        $query = PositionTitleModel::where('is_deleted', 0)->order('id desc');
        if ($account !== '') {
            $query->where('account', 'like', "%{$account}%");
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
        $account = trim((string)($data['account'] ?? ''));
        $receiver = trim((string)($data['receiver'] ?? ''));

        if ($account === '') {
            return ['code' => -200, 'msg' => '职位身份名称不能为空'];
        }

        $existsActive = PositionTitleModel::where('account', $account)
            ->where('is_deleted', 0)
            ->find();
        if ($existsActive) {
            return ['code' => -200, 'msg' => '职位身份已存在'];
        }

        $existsDeleted = PositionTitleModel::where('account', $account)
            ->where('is_deleted', 1)
            ->find();
        if ($existsDeleted) {
            $existsDeleted->is_deleted = 0;
            $existsDeleted->deleted_time = null;
            $existsDeleted->deleted_by = null;
            $existsDeleted->receiver = $receiver;
            $existsDeleted->update_time = time();
            $res = $existsDeleted->save();
            return $res ? ['code' => 0, 'msg' => '添加成功！'] : ['code' => -200, 'msg' => '添加失败！'];
        }

        $insertData = [
            'account' => $account,
            'receiver' => $receiver,
            'is_deleted' => 0,
            'deleted_time' => null,
            'deleted_by' => null,
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

        if (array_key_exists('account', $data)) {
            $account = trim((string)$data['account']);
            if ($account === '') {
                return ['code' => -200, 'msg' => '职位身份名称不能为空'];
            }

            if ($account !== (string)$entry->account) {
                $exists = PositionTitleModel::where('account', $account)
                    ->where('is_deleted', 0)
                    ->where('id', '<>', $id)
                    ->find();
                if ($exists) {
                    return ['code' => -200, 'msg' => '职位身份名称已存在'];
                }
            }
            $updateData['account'] = $account;
        }

        if (array_key_exists('receiver', $data)) {
            $updateData['receiver'] = trim((string)$data['receiver']);
        }

        if (empty($updateData)) {
            return ['code' => -200, 'msg' => '没有可修改的数据'];
        }

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

        $aff = PositionTitleModel::where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'is_deleted' => 1,
                'deleted_time' => date('Y-m-d H:i:s'),
                'deleted_by' => (int)$deletedBy,
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

        Db::startTrans();
        try {
            $delCount = PositionTitleModel::whereIn('id', $ids)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => (int)$deletedBy,
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
            $account = trim((string)($r['account'] ?? ''));
            $receiver = trim((string)($r['receiver'] ?? ''));
            if ($account === '') {
                continue;
            }
            // 同一文件内重复名称，仅保留首次出现的一条
            if (!isset($normalizedRows[$account])) {
                $normalizedRows[$account] = [
                    'account' => $account,
                    'receiver' => $receiver,
                ];
            }
        }

        if (empty($normalizedRows)) {
            return ['code' => -200, 'msg' => '有效数据为空（职位身份名称为空已被跳过）'];
        }

        try {
            $accounts = array_keys($normalizedRows);
            $existing = PositionTitleModel::whereIn('account', $accounts)
                ->field('id,account,is_deleted')
                ->select()
                ->toArray();

            $existingMap = [];
            foreach ($existing as $item) {
                $existingMap[$item['account']] = $item;
            }

            $now = time();
            $toInsert = [];
            $skipCount = 0;
            $reviveCount = 0;
            $insertCount = 0;

            Db::startTrans();
            foreach ($normalizedRows as $row) {
                $account = $row['account'];
                $receiver = $row['receiver'];
                if (isset($existingMap[$account])) {
                    $exists = $existingMap[$account];
                    if ((int)$exists['is_deleted'] === 0) {
                        $skipCount++;
                        continue;
                    }

                    PositionTitleModel::where('id', (int)$exists['id'])->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'receiver' => $receiver,
                        'update_time' => $now,
                    ]);
                    $reviveCount++;
                    continue;
                }

                $toInsert[] = [
                    'account' => $account,
                    'receiver' => $receiver,
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
