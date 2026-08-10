<?php

namespace app\admin\service;

use app\admin\model\ClientRowMark;
use think\Db;

/**
 * 客户行颜色标记业务（“批量添加跟进记录”弹窗行颜色）
 * 颜色仅属于当前登录员工（admin_id 维度隔离）。
 */
class ClientRowMarkService
{
    /**
     * 批量查询颜色标记（一次查询，禁止循环查库）
     *
     * @param array $leadsIds
     * @param int $adminId
     * @param int $markType
     * @return array<int, string> 形如 [1001 => '#FFE5E5', 1002 => '#FFFFCC']
     */
    public function getMarksMap(array $leadsIds, int $adminId, int $markType = 1): array
    {
        $leadsIds = array_values(array_unique(array_filter(array_map('intval', $leadsIds))));
        if (empty($leadsIds) || $adminId <= 0) {
            return [];
        }

        $rows = ClientRowMark::whereIn('leads_id', $leadsIds)
            ->where('admin_id', $adminId)
            ->where('mark_type', $markType)
            ->field('leads_id,bg_color')
            ->select();

        $map = [];
        foreach ($rows as $row) {
            $leadsId = (int)($row['leads_id'] ?? 0);
            $bgColor = trim((string)($row['bg_color'] ?? ''));
            if ($leadsId > 0 && $bgColor !== '') {
                $map[$leadsId] = $bgColor;
            }
        }

        return $map;
    }

    /**
     * 批量查询颜色标记详情（含备注），V2 颜色筛选/备注展示使用
     * 不影响 getMarksMap() 原有返回结构，供新场景单独调用
     *
     * @param array $leadsIds
     * @param int $adminId
     * @param int $markType
     * @return array<int, array{bg_color:string, remark:string}> 形如 [1001 => ['bg_color'=>'#FFE5E5','remark'=>'重点客户']]
     */
    public function getMarksMapDetail(array $leadsIds, int $adminId, int $markType = 1): array
    {
        $leadsIds = array_values(array_unique(array_filter(array_map('intval', $leadsIds))));
        if (empty($leadsIds) || $adminId <= 0) {
            return [];
        }

        $rows = ClientRowMark::whereIn('leads_id', $leadsIds)
            ->where('admin_id', $adminId)
            ->where('mark_type', $markType)
            ->field('leads_id,bg_color,remark')
            ->select();

        $map = [];
        foreach ($rows as $row) {
            $leadsId = (int)($row['leads_id'] ?? 0);
            if ($leadsId <= 0) {
                continue;
            }
            $map[$leadsId] = [
                'bg_color' => trim((string)($row['bg_color'] ?? '')),
                'remark' => trim((string)($row['remark'] ?? '')),
            ];
        }

        return $map;
    }

    /**
     * 颜色筛选选项：前端传值 => 标准颜色码
     */
    public const COLOR_FILTER_MAP = [
        'red' => '#FFC7CE',
        'yellow' => '#FFEB9C',
        'green' => '#C6EFCE',
        'blue' => '#BDD7EE',
    ];

    /**
     * 按颜色筛选行数据（在颜色标记加载完成之后的内存过滤，不涉及数据库分页查询）
     *
     * @param array $rows 每行需包含 bg_color 字段
     * @param string $colorFilter all|red|yellow|green|blue|none，为空或 all 表示不筛选
     * @return array
     */
    public function filterRowsByColor(array $rows, string $colorFilter): array
    {
        $colorFilter = trim($colorFilter);
        if ($colorFilter === '' || $colorFilter === 'all') {
            return $rows;
        }

        if ($colorFilter === 'none') {
            return array_values(array_filter($rows, function ($row) {
                return trim((string)($row['bg_color'] ?? '')) === '';
            }));
        }

        $target = self::COLOR_FILTER_MAP[$colorFilter] ?? $colorFilter;
        $target = strtoupper(trim($target));
        if ($target === '' || $target[0] !== '#') {
            return $rows;
        }

        return array_values(array_filter($rows, function ($row) use ($target) {
            return strtoupper(trim((string)($row['bg_color'] ?? ''))) === $target;
        }));
    }

    /**
     * 批量保存（新增/更新）行颜色标记，admin_id 必须来自后端调用，不信任前端
     * 一次查询已有记录 + whereIn 批量更新 + insertAll 批量插入，全程使用事务
     *
     * @param array $leadsIds
     * @param int $adminId
     * @param string $bgColor #RRGGBB 或空字符串（清除颜色）
     * @param string $remark
     * @param int $markType
     * @return array{code:int,msg:string,data?:array}
     */
    public function batchSaveMark(array $leadsIds, int $adminId, string $bgColor, string $remark = '', int $markType = 1): array
    {
        $leadsIds = array_values(array_unique(array_filter(array_map('intval', $leadsIds), function ($v) {
            return $v > 0;
        })));

        if (empty($leadsIds)) {
            return $this->fail('请先选择客户');
        }
        if ($adminId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }

        $bgColor = trim($bgColor);
        if ($bgColor !== '' && !preg_match('/^#[0-9A-Fa-f]{6}$/', $bgColor)) {
            return $this->fail('颜色格式不正确');
        }

        $remark = trim($remark);
        $now = date('Y-m-d H:i:s');

        Db::startTrans();
        try {
            // 一次查询已有记录，避免 foreach + find/save 循环查库
            $existLeadsIds = Db::table('crm_client_row_mark')
                ->whereIn('leads_id', $leadsIds)
                ->where('admin_id', $adminId)
                ->where('mark_type', $markType)
                ->column('leads_id');
            $existLeadsIds = array_values(array_unique(array_map('intval', $existLeadsIds)));

            $updatedCount = 0;
            if (!empty($existLeadsIds)) {
                $updatedCount = Db::table('crm_client_row_mark')
                    ->whereIn('leads_id', $existLeadsIds)
                    ->where('admin_id', $adminId)
                    ->where('mark_type', $markType)
                    ->update([
                        'bg_color' => $bgColor,
                        'remark' => $remark,
                        'update_time' => $now,
                    ]);
            }

            $insertLeadsIds = array_values(array_diff($leadsIds, $existLeadsIds));
            $insertedCount = 0;
            if (!empty($insertLeadsIds)) {
                $insertData = [];
                foreach ($insertLeadsIds as $leadsId) {
                    $insertData[] = [
                        'leads_id' => $leadsId,
                        'admin_id' => $adminId,
                        'mark_type' => $markType,
                        'bg_color' => $bgColor,
                        'remark' => $remark,
                        'create_time' => $now,
                        'update_time' => $now,
                    ];
                }
                Db::table('crm_client_row_mark')->insertAll($insertData);
                $insertedCount = count($insertLeadsIds);
            }

            Db::commit();
        } catch (\Throwable $e) {
            Db::rollback();
            return $this->fail('批量设置失败：' . $e->getMessage());
        }

        return [
            'code' => 0,
            'msg' => '批量设置成功，共处理 ' . count($leadsIds) . ' 个客户',
            'data' => [
                'total' => count($leadsIds),
                'updated' => (int)$updatedCount,
                'inserted' => (int)$insertedCount,
            ],
        ];
    }

    /**
     * 保存（新增/更新）行颜色标记，admin_id 必须来自后端调用，不信任前端
     *
     * @param int $leadsId
     * @param int $adminId
     * @param string $bgColor  #RRGGBB 或空字符串（清除颜色）
     * @param int $markType
     * @param string $remark
     * @return array{code:int,msg:string}
     */
    public function saveMark(int $leadsId, int $adminId, string $bgColor, int $markType = 1, string $remark = ''): array
    {
        if ($leadsId <= 0) {
            return $this->fail('缺少客户ID');
        }
        if ($adminId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }

        $bgColor = trim($bgColor);
        if ($bgColor !== '' && !preg_match('/^#[0-9A-Fa-f]{6}$/', $bgColor)) {
            return $this->fail('颜色格式不正确');
        }

        $now = date('Y-m-d H:i:s');
        $remark = trim($remark);

        $exists = ClientRowMark::where('leads_id', $leadsId)
            ->where('admin_id', $adminId)
            ->find();

        if ($exists) {
            $exists->bg_color = $bgColor;
            $exists->mark_type = $markType;
            $exists->remark = $remark;
            $exists->update_time = $now;
            $ok = $exists->save();
        } else {
            $ok = ClientRowMark::create([
                'leads_id' => $leadsId,
                'admin_id' => $adminId,
                'mark_type' => $markType,
                'bg_color' => $bgColor,
                'remark' => $remark,
                'create_time' => $now,
                'update_time' => $now,
            ]);
        }

        if (!$ok) {
            return $this->fail('保存失败');
        }

        return ['code' => 0, 'msg' => '保存成功'];
    }

    private function fail(string $msg): array
    {
        return ['code' => 1, 'msg' => $msg];
    }
}
