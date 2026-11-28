<?php

namespace app\admin\service;

use think\Db;
use think\facade\Session;

/**
 * 询盘来源 / 运营端口 受控停用与启用
 * status: 0=启用, 1=停用（复用软删除字段，不物理删除）
 */
class InquiryChannelStatusService
{
    const STATUS_ENABLED = 0;
    const STATUS_DISABLED = 1;

    /**
     * 是否超级管理员（与现有 Inquiry/InquiryPort 删除权限一致）
     */
    public function isSuperAdmin(array $currentAdmin): bool
    {
        return ((int)session('aid') === 1) || (($currentAdmin['username'] ?? '') === 'admin');
    }

    /**
     * 严格解析单个正整数 ID（拒绝 "12abc"、浮点、负数、数组、对象等）
     *
     * @param mixed $value
     * @return int|null 合法时返回正整数，否则 null
     */
    public function parseStrictPositiveInt($value): ?int
    {
        if (is_int($value)) {
            return $value > 0 ? $value : null;
        }
        if (is_string($value)) {
            $value = trim($value);
            if ($value === '' || !preg_match('/^[1-9]\d*$/', $value)) {
                return null;
            }
            return (int)$value;
        }
        // 浮点 / bool / array / object / null / resource 一律拒绝
        return null;
    }

    /**
     * 规范化 ID 列表：仅严格正整数、去重、剔除非法（不把 "12abc" 误转为 12）
     *
     * @param mixed $ids
     * @return int[]
     */
    public function normalizeIds($ids): array
    {
        if (!is_array($ids)) {
            if ($ids === null || $ids === '') {
                return [];
            }
            if (is_int($ids)) {
                $ids = [$ids];
            } elseif (is_string($ids)) {
                $ids = explode(',', $ids);
            } else {
                // 浮点/对象等整包拒绝，避免 is_numeric 把异常值带进转换
                return [];
            }
        }
        $out = [];
        foreach ($ids as $id) {
            $n = $this->parseStrictPositiveInt($id);
            if ($n !== null) {
                $out[$n] = $n;
            }
        }
        return array_values($out);
    }

    /**
     * 导入状态严格解析：空→0；仅允许明确的 0/1；其余返回 null（调用方拒绝/跳过）
     *
     * @param mixed $raw 通常已是 trim 后的字符串
     * @return int|null
     */
    public function parseImportStatus($raw): ?int
    {
        if ($raw === null) {
            return self::STATUS_ENABLED;
        }
        if (is_int($raw)) {
            if ($raw === self::STATUS_ENABLED || $raw === self::STATUS_DISABLED) {
                return $raw;
            }
            return null;
        }
        if (!is_string($raw)) {
            return null;
        }
        $raw = trim($raw);
        if ($raw === '') {
            return self::STATUS_ENABLED;
        }
        if ($raw === '0') {
            return self::STATUS_ENABLED;
        }
        if ($raw === '1') {
            return self::STATUS_DISABLED;
        }
        return null;
    }

    /**
     * 写入渠道状态变更审计（复用 crm_operation_log；leads_id=0，字段同 Client::addOperLog）
     */
    public function writeStatusLog(string $operType, array $payload): void
    {
        $payload['operator_aid'] = (int)Session::get('aid');
        $payload['operator'] = (string)Session::get('username');
        $payload['at'] = date('Y-m-d H:i:s');
        $description = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        Db::table('crm_operation_log')->insert([
            'user_id' => Session::get('aid'),
            'leads_id' => 0,
            'oper_type' => $operType,
            'description' => $description,
            'oper_user' => Session::get('username'),
            'created_at' => date('Y-m-d H:i:s'),
        ]);
    }

    /**
     * 单条停用询盘来源
     * 锁序：先父(crm_inquiry)后子(crm_inquiry_port)，与 enablePort / assertParentInquiryEnabledForWrite 一致
     *
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function disableInquiry(int $id, array $currentAdmin): array
    {
        if ($id <= 0) {
            return $this->fail(500, '参数错误');
        }
        $isSuper = $this->isSuperAdmin($currentAdmin);

        Db::startTrans();
        try {
            $rowQuery = Db::name('crm_inquiry')->where('id', $id)->lock(true);
            if (!$isSuper) {
                $rowQuery->where('submit_person', $currentAdmin['username'] ?? '');
            }
            $row = $rowQuery->find();
            if (!$row) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }

            $oldStatus = (int)($row['status'] ?? 0);
            if ($oldStatus === self::STATUS_DISABLED) {
                Db::commit();
                return $this->ok(200, '已停用（无需重复操作）', [
                    'id' => $id,
                    'already_disabled' => 1,
                ]);
            }

            $activeChild = Db::name('crm_inquiry_port')
                ->where('inquiry_id', $id)
                ->where('status', self::STATUS_ENABLED)
                ->lock(true)
                ->limit(1)
                ->value('id');
            if ($activeChild) {
                Db::rollback();
                $name = $row['inquiry_name'] ?: ('ID#' . $id);
                return $this->fail(500, '该询盘来源下仍有启用中的运营端口，请先停用子端口后再停用来源：' . $name, [
                    'blocked_ids' => [$id],
                    'blocked_names' => [$name],
                ]);
            }

            $now = time();
            $aff = Db::name('crm_inquiry')
                ->where('id', $id)
                ->where('status', self::STATUS_ENABLED)
                ->update([
                    'status' => self::STATUS_DISABLED,
                    'edit_time' => $now,
                ]);
            if ($aff <= 0) {
                Db::rollback();
                return $this->fail(500, '停用失败，请重试');
            }

            $this->writeStatusLog('询盘来源停用', [
                'target' => 'crm_inquiry',
                'target_id' => $id,
                'inquiry_name' => $row['inquiry_name'] ?? '',
                'before_status' => $oldStatus,
                'after_status' => self::STATUS_DISABLED,
            ]);

            Db::commit();
            return $this->ok(200, '停用成功', ['id' => $id]);
        } catch (\Throwable $e) {
            Db::rollback();
            return $this->fail(500, '停用失败，请稍后重试');
        }
    }

    /**
     * 批量停用询盘来源
     *
     * @param int[] $ids
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function batchDisableInquiry(array $ids, array $currentAdmin): array
    {
        $ids = $this->normalizeIds($ids);
        if (empty($ids)) {
            return $this->fail(-200, '未选择任何记录');
        }

        $success = 0;
        $skippedAlready = 0;
        $skippedPermission = 0;
        $skippedActiveChild = 0;
        $failed = 0;
        $blockedNames = [];

        foreach ($ids as $id) {
            $ret = $this->disableInquiry($id, $currentAdmin);
            if (!empty($ret['ok'])) {
                if (!empty($ret['data']['already_disabled'])) {
                    $skippedAlready++;
                } else {
                    $success++;
                }
                continue;
            }
            $msg = (string)($ret['msg'] ?? '');
            if (strpos($msg, '无权限') !== false || strpos($msg, '不存在') !== false) {
                $skippedPermission++;
            } elseif (strpos($msg, '启用中的运营端口') !== false) {
                $skippedActiveChild++;
                if (!empty($ret['data']['blocked_names'])) {
                    foreach ($ret['data']['blocked_names'] as $n) {
                        $blockedNames[] = $n;
                    }
                }
            } else {
                $failed++;
            }
        }

        $parts = [];
        $parts[] = '停用成功：' . $success . ' 条';
        if ($skippedAlready > 0) {
            $parts[] = '已停用跳过：' . $skippedAlready . ' 条';
        }
        if ($skippedActiveChild > 0) {
            $preview = implode('、', array_slice($blockedNames, 0, 30));
            $p = '跳过(仍有启用子端口)：' . $skippedActiveChild . ' 条';
            if ($preview !== '') {
                $p .= '（' . $preview . (count($blockedNames) > 30 ? ' 等' : '') . '）';
            }
            $parts[] = $p;
        }
        if ($skippedPermission > 0) {
            $parts[] = '跳过(无权限/不存在)：' . $skippedPermission . ' 条';
        }
        if ($failed > 0) {
            $parts[] = '失败：' . $failed . ' 条';
        }

        return $this->ok(0, implode('，', $parts), [
            'success' => $success,
            'skipped_already' => $skippedAlready,
            'skipped_used' => $skippedActiveChild,
            'skipped_permission' => $skippedPermission,
            'failed' => $failed,
            'blocked_names' => $blockedNames,
        ]);
    }

    /**
     * 单条启用询盘来源
     *
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function enableInquiry(int $id, array $currentAdmin): array
    {
        if ($id <= 0) {
            return $this->fail(500, '参数错误');
        }
        $isSuper = $this->isSuperAdmin($currentAdmin);

        Db::startTrans();
        try {
            $rowQuery = Db::name('crm_inquiry')->where('id', $id)->lock(true);
            if (!$isSuper) {
                $rowQuery->where('submit_person', $currentAdmin['username'] ?? '');
            }
            $row = $rowQuery->find();
            if (!$row) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }

            $oldStatus = (int)($row['status'] ?? 0);
            if ($oldStatus === self::STATUS_ENABLED) {
                Db::commit();
                return $this->ok(200, '已启用（无需重复操作）', [
                    'id' => $id,
                    'already_enabled' => 1,
                ]);
            }

            $now = time();
            $aff = Db::name('crm_inquiry')
                ->where('id', $id)
                ->where('status', self::STATUS_DISABLED)
                ->update([
                    'status' => self::STATUS_ENABLED,
                    'edit_time' => $now,
                ]);
            if ($aff <= 0) {
                Db::rollback();
                return $this->fail(500, '启用失败，请重试');
            }

            $this->writeStatusLog('询盘来源启用', [
                'target' => 'crm_inquiry',
                'target_id' => $id,
                'inquiry_name' => $row['inquiry_name'] ?? '',
                'before_status' => $oldStatus,
                'after_status' => self::STATUS_ENABLED,
            ]);

            Db::commit();
            return $this->ok(200, '启用成功', ['id' => $id]);
        } catch (\Throwable $e) {
            Db::rollback();
            return $this->fail(500, '启用失败，请稍后重试');
        }
    }

    /**
     * 批量启用询盘来源
     *
     * @param int[] $ids
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function batchEnableInquiry(array $ids, array $currentAdmin): array
    {
        $ids = $this->normalizeIds($ids);
        if (empty($ids)) {
            return $this->fail(-200, '未选择任何记录');
        }

        $success = 0;
        $skippedAlready = 0;
        $skippedPermission = 0;
        $failed = 0;

        foreach ($ids as $id) {
            $ret = $this->enableInquiry($id, $currentAdmin);
            if (!empty($ret['ok'])) {
                if (!empty($ret['data']['already_enabled'])) {
                    $skippedAlready++;
                } else {
                    $success++;
                }
                continue;
            }
            $msg = (string)($ret['msg'] ?? '');
            if (strpos($msg, '无权限') !== false || strpos($msg, '不存在') !== false) {
                $skippedPermission++;
            } else {
                $failed++;
            }
        }

        $parts = ['启用成功：' . $success . ' 条'];
        if ($skippedAlready > 0) {
            $parts[] = '已启用跳过：' . $skippedAlready . ' 条';
        }
        if ($skippedPermission > 0) {
            $parts[] = '跳过(无权限/不存在)：' . $skippedPermission . ' 条';
        }
        if ($failed > 0) {
            $parts[] = '失败：' . $failed . ' 条';
        }

        return $this->ok(0, implode('，', $parts), [
            'success' => $success,
            'skipped_already' => $skippedAlready,
            'skipped_permission' => $skippedPermission,
            'failed' => $failed,
        ]);
    }

    /**
     * 查找仍绑定该端口的启用运营账号用户名列表
     *
     * @return string[]
     */
    public function findActiveAdminsBoundToPort(int $portId): array
    {
        if ($portId <= 0) {
            return [];
        }
        $admins = Db::name('admin')
            ->where('is_open', 1)
            ->where('port_id', '<>', '')
            ->whereNotNull('port_id')
            ->field('admin_id, username, port_id')
            ->select();
        $names = [];
        $needle = (string)$portId;
        foreach ($admins as $admin) {
            $ids = array_filter(array_map('trim', explode(',', (string)$admin['port_id'])));
            if (in_array($needle, $ids, true)) {
                $names[] = (string)$admin['username'];
            }
        }
        return $names;
    }

    /**
     * 单条停用运营端口（历史客户引用不阻止；启用运营账号绑定则拒绝）
     *
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function disablePort(int $id, array $currentAdmin): array
    {
        if ($id <= 0) {
            return $this->fail(500, '参数错误');
        }
        $isSuper = $this->isSuperAdmin($currentAdmin);

        Db::startTrans();
        try {
            $rowQuery = Db::name('crm_inquiry_port')->where('id', $id)->lock(true);
            if (!$isSuper) {
                $rowQuery->where('submit_person', $currentAdmin['username'] ?? '');
            }
            $row = $rowQuery->find();
            if (!$row) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }

            $oldStatus = (int)($row['status'] ?? 0);
            if ($oldStatus === self::STATUS_DISABLED) {
                Db::commit();
                return $this->ok(200, '已停用（无需重复操作）', [
                    'id' => $id,
                    'already_disabled' => 1,
                ]);
            }

            $boundNames = $this->findActiveAdminsBoundToPort($id);
            if (!empty($boundNames)) {
                Db::rollback();
                $name = $row['port_name'] ?: ('ID#' . $id);
                $preview = implode('、', array_slice($boundNames, 0, 10));
                return $this->fail(500, '该运营端口仍被启用中的运营账号绑定，请先解绑后再停用：' . $name . '（' . $preview . '）', [
                    'blocked_ids' => [$id],
                    'blocked_names' => [$name],
                    'bound_admins' => $boundNames,
                ]);
            }

            $now = time();
            $aff = Db::name('crm_inquiry_port')
                ->where('id', $id)
                ->where('status', self::STATUS_ENABLED)
                ->update([
                    'status' => self::STATUS_DISABLED,
                    'edit_time' => $now,
                ]);
            if ($aff <= 0) {
                Db::rollback();
                return $this->fail(500, '停用失败，请重试');
            }

            // 条件更新后再次核对绑定，缩小「校验通过后、提交前」被运营账号写入绑定的窗口
            $boundNamesAfter = $this->findActiveAdminsBoundToPort($id);
            if (!empty($boundNamesAfter)) {
                Db::rollback();
                $name = $row['port_name'] ?: ('ID#' . $id);
                $preview = implode('、', array_slice($boundNamesAfter, 0, 10));
                return $this->fail(500, '该运营端口仍被启用中的运营账号绑定，请先解绑后再停用：' . $name . '（' . $preview . '）', [
                    'blocked_ids' => [$id],
                    'blocked_names' => [$name],
                    'bound_admins' => $boundNamesAfter,
                ]);
            }

            $this->writeStatusLog('运营端口停用', [
                'target' => 'crm_inquiry_port',
                'target_id' => $id,
                'port_name' => $row['port_name'] ?? '',
                'inquiry_id' => (int)($row['inquiry_id'] ?? 0),
                'before_status' => $oldStatus,
                'after_status' => self::STATUS_DISABLED,
            ]);

            Db::commit();
            return $this->ok(200, '停用成功', ['id' => $id]);
        } catch (\Throwable $e) {
            Db::rollback();
            return $this->fail(500, '停用失败，请稍后重试');
        }
    }

    /**
     * 批量停用运营端口
     *
     * @param int[] $ids
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function batchDisablePort(array $ids, array $currentAdmin): array
    {
        $ids = $this->normalizeIds($ids);
        if (empty($ids)) {
            return $this->fail(-200, '未选择任何记录');
        }

        $success = 0;
        $skippedAlready = 0;
        $skippedPermission = 0;
        $skippedBound = 0;
        $failed = 0;
        $blockedNames = [];

        foreach ($ids as $id) {
            $ret = $this->disablePort($id, $currentAdmin);
            if (!empty($ret['ok'])) {
                if (!empty($ret['data']['already_disabled'])) {
                    $skippedAlready++;
                } else {
                    $success++;
                }
                continue;
            }
            $msg = (string)($ret['msg'] ?? '');
            if (strpos($msg, '无权限') !== false || strpos($msg, '不存在') !== false) {
                $skippedPermission++;
            } elseif (strpos($msg, '运营账号绑定') !== false) {
                $skippedBound++;
                if (!empty($ret['data']['blocked_names'])) {
                    foreach ($ret['data']['blocked_names'] as $n) {
                        $blockedNames[] = $n;
                    }
                }
            } else {
                $failed++;
            }
        }

        $parts = ['停用成功：' . $success . ' 条'];
        if ($skippedAlready > 0) {
            $parts[] = '已停用跳过：' . $skippedAlready . ' 条';
        }
        if ($skippedBound > 0) {
            $preview = implode('、', array_slice($blockedNames, 0, 30));
            $p = '跳过(仍绑定启用运营账号)：' . $skippedBound . ' 条';
            if ($preview !== '') {
                $p .= '（' . $preview . (count($blockedNames) > 30 ? ' 等' : '') . '）';
            }
            $parts[] = $p;
        }
        if ($skippedPermission > 0) {
            $parts[] = '跳过(无权限/不存在)：' . $skippedPermission . ' 条';
        }
        if ($failed > 0) {
            $parts[] = '失败：' . $failed . ' 条';
        }

        return $this->ok(0, implode('，', $parts), [
            'success' => $success,
            'skipped_already' => $skippedAlready,
            'skipped_bound' => $skippedBound,
            'skipped_permission' => $skippedPermission,
            'failed' => $failed,
            'blocked_names' => $blockedNames,
        ]);
    }

    /**
     * 单条启用运营端口（父来源必须存在且启用）
     *
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function enablePort(int $id, array $currentAdmin): array
    {
        if ($id <= 0) {
            return $this->fail(500, '参数错误');
        }
        $isSuper = $this->isSuperAdmin($currentAdmin);

        Db::startTrans();
        try {
            // 统一锁序：先父(crm_inquiry)后子(crm_inquiry_port)，避免与 disableInquiry 死锁
            $preview = Db::name('crm_inquiry_port')->where('id', $id)->field('id,inquiry_id,submit_person')->find();
            if (!$preview) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }
            if (!$isSuper && (($preview['submit_person'] ?? '') !== ($currentAdmin['username'] ?? ''))) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }

            $inquiryId = (int)($preview['inquiry_id'] ?? 0);
            if ($inquiryId <= 0) {
                Db::rollback();
                return $this->fail(500, '所属询盘来源不存在，无法启用该端口');
            }

            $parent = Db::name('crm_inquiry')->where('id', $inquiryId)->lock(true)->find();
            if (!$parent) {
                Db::rollback();
                return $this->fail(500, '所属询盘来源不存在，无法启用该端口');
            }
            if ((int)($parent['status'] ?? 0) !== self::STATUS_ENABLED) {
                Db::rollback();
                $pname = $parent['inquiry_name'] ?: ('ID#' . $inquiryId);
                return $this->fail(500, '所属询盘来源已停用，请先启用来源后再启用端口：' . $pname);
            }

            $rowQuery = Db::name('crm_inquiry_port')->where('id', $id)->lock(true);
            if (!$isSuper) {
                $rowQuery->where('submit_person', $currentAdmin['username'] ?? '');
            }
            $row = $rowQuery->find();
            if (!$row) {
                Db::rollback();
                return $this->fail(500, '无权限或记录不存在');
            }
            if ((int)($row['inquiry_id'] ?? 0) !== $inquiryId) {
                Db::rollback();
                return $this->fail(500, '端口所属来源已变更，请重试');
            }

            $oldStatus = (int)($row['status'] ?? 0);
            if ($oldStatus === self::STATUS_ENABLED) {
                Db::commit();
                return $this->ok(200, '已启用（无需重复操作）', [
                    'id' => $id,
                    'already_enabled' => 1,
                ]);
            }

            $now = time();
            $aff = Db::name('crm_inquiry_port')
                ->where('id', $id)
                ->where('status', self::STATUS_DISABLED)
                ->update([
                    'status' => self::STATUS_ENABLED,
                    'edit_time' => $now,
                ]);
            if ($aff <= 0) {
                Db::rollback();
                return $this->fail(500, '启用失败，请重试');
            }

            $this->writeStatusLog('运营端口启用', [
                'target' => 'crm_inquiry_port',
                'target_id' => $id,
                'port_name' => $row['port_name'] ?? '',
                'inquiry_id' => $inquiryId,
                'before_status' => $oldStatus,
                'after_status' => self::STATUS_ENABLED,
            ]);

            Db::commit();
            return $this->ok(200, '启用成功', ['id' => $id]);
        } catch (\Throwable $e) {
            Db::rollback();
            return $this->fail(500, '启用失败，请稍后重试');
        }
    }

    /**
     * 批量启用运营端口
     *
     * @param int[] $ids
     * @return array{ok:bool,code:int,msg:string,data:array}
     */
    public function batchEnablePort(array $ids, array $currentAdmin): array
    {
        $ids = $this->normalizeIds($ids);
        if (empty($ids)) {
            return $this->fail(-200, '未选择任何记录');
        }

        $success = 0;
        $skippedAlready = 0;
        $skippedPermission = 0;
        $skippedParent = 0;
        $failed = 0;

        foreach ($ids as $id) {
            $ret = $this->enablePort($id, $currentAdmin);
            if (!empty($ret['ok'])) {
                if (!empty($ret['data']['already_enabled'])) {
                    $skippedAlready++;
                } else {
                    $success++;
                }
                continue;
            }
            $msg = (string)($ret['msg'] ?? '');
            if (strpos($msg, '无权限') !== false || strpos($msg, '不存在') !== false) {
                $skippedPermission++;
            } elseif (strpos($msg, '所属询盘来源') !== false) {
                $skippedParent++;
            } else {
                $failed++;
            }
        }

        $parts = ['启用成功：' . $success . ' 条'];
        if ($skippedAlready > 0) {
            $parts[] = '已启用跳过：' . $skippedAlready . ' 条';
        }
        if ($skippedParent > 0) {
            $parts[] = '跳过(父来源未启用/不存在)：' . $skippedParent . ' 条';
        }
        if ($skippedPermission > 0) {
            $parts[] = '跳过(无权限/不存在)：' . $skippedPermission . ' 条';
        }
        if ($failed > 0) {
            $parts[] = '失败：' . $failed . ' 条';
        }

        return $this->ok(0, implode('，', $parts), [
            'success' => $success,
            'skipped_already' => $skippedAlready,
            'skipped_parent' => $skippedParent,
            'skipped_permission' => $skippedPermission,
            'failed' => $failed,
        ]);
    }

    /**
     * 新增/改挂/导入端口时校验父来源必须启用（带行锁防并发停用）
     * 锁序约定：调用方事务内先对本方法加父行锁，再写子端口，禁止先锁子再锁父
     *
     * @return array{ok:bool,msg:string}
     */
    public function assertParentInquiryEnabledForWrite(int $inquiryId): array
    {
        if ($inquiryId <= 0) {
            return ['ok' => false, 'msg' => '请选择询盘来源'];
        }
        $parent = Db::name('crm_inquiry')->where('id', $inquiryId)->lock(true)->find();
        if (!$parent) {
            return ['ok' => false, 'msg' => '询盘来源不存在'];
        }
        if ((int)($parent['status'] ?? 0) !== self::STATUS_ENABLED) {
            return ['ok' => false, 'msg' => '询盘来源已停用，不能挂靠新端口或改归属到该来源'];
        }
        return ['ok' => true, 'msg' => ''];
    }

    /**
     * 合并启用列表与历史已选（用于编辑/详情回显）
     *
     * @param array $enabledRows
     * @param array|null $extraRow
     * @param string $nameField
     * @return array
     */
    public function mergeDisabledCurrent(array $enabledRows, $extraRow, string $nameField = 'inquiry_name'): array
    {
        if (empty($extraRow) || !isset($extraRow['id'])) {
            return $enabledRows;
        }
        $extraId = (int)$extraRow['id'];
        foreach ($enabledRows as $row) {
            if ((int)$row['id'] === $extraId) {
                return $enabledRows;
            }
        }
        if ((int)($extraRow['status'] ?? 0) !== self::STATUS_ENABLED) {
            $label = (string)($extraRow[$nameField] ?? '');
            if ($label !== '' && mb_strpos($label, '已停用') === false) {
                $extraRow[$nameField] = $label . '（已停用）';
            }
        }
        $enabledRows[] = $extraRow;
        return $enabledRows;
    }

    /**
     * 合并多条历史端口到启用列表
     *
     * @param array $enabledRows
     * @param int[] $includeIds
     * @return array
     */
    public function mergeDisabledPortsByIds(array $enabledRows, array $includeIds): array
    {
        $includeIds = $this->normalizeIds($includeIds);
        if (empty($includeIds)) {
            return $enabledRows;
        }
        $have = [];
        foreach ($enabledRows as $row) {
            $have[(int)$row['id']] = true;
        }
        $missing = [];
        foreach ($includeIds as $pid) {
            if (!isset($have[$pid])) {
                $missing[] = $pid;
            }
        }
        if (empty($missing)) {
            return $enabledRows;
        }
        $extras = Db::name('crm_inquiry_port')->whereIn('id', $missing)->select();
        foreach ($extras as $extra) {
            $enabledRows = $this->mergeDisabledCurrent($enabledRows, $extra, 'port_name');
        }
        return $enabledRows;
    }

    private function ok(int $code, string $msg, array $data = []): array
    {
        return ['ok' => true, 'code' => $code, 'msg' => $msg, 'data' => $data];
    }

    private function fail(int $code, string $msg, array $data = []): array
    {
        return ['ok' => false, 'code' => $code, 'msg' => $msg, 'data' => $data];
    }
}
