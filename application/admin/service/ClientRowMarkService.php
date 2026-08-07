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
