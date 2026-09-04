<?php

namespace app\admin\service;

use app\admin\model\ClientConfig;
use think\Db;
use think\facade\Session;

/**
 * 客户管理配置（负责人白名单等）
 *
 * 说明：本 Service 负责配置读写与配置页数据。
 * 客户负责人转移 / 首次超时分配通过 ClientOwnerCandidateService
 * 单向读取 getLatestConfig()，本类禁止反向调用 OwnerCandidate。
 */
class ClientConfigService
{
    /** 配置页候选人员禁止的岗位：超级管理员、运营人员 */
    const EXCLUDED_GROUP_IDS = [1, 12];

    /** 首次无配置时，页面默认预选的旧规则岗位（仅展示，不写库） */
    const DEFAULT_SUGGEST_GROUP_IDS = [10, 11, 14, 17, 18, 19, 21, 22];

    /**
     * 获取最新一行配置（无记录返回 null）
     *
     * @return array|null
     */
    public function getLatestConfig()
    {
        $row = ClientConfig::order('id', 'desc')->find();
        if (!$row) {
            return null;
        }
        if (is_object($row) && method_exists($row, 'toArray')) {
            $row = $row->toArray();
        }
        return is_array($row) ? $row : null;
    }

    /**
     * 是否已有正式配置
     *
     * @return bool
     */
    public function hasConfig()
    {
        return $this->getLatestConfig() !== null;
    }

    /**
     * 解析 allowed_owner_user_ids（数组或逗号字符串）为正整数 ID 列表
     *
     * @param mixed $raw
     * @return int[]
     */
    public function parseAllowedOwnerUserIds($raw)
    {
        if (is_array($raw)) {
            $parts = $raw;
        } else {
            $str = trim((string)$raw);
            if ($str === '') {
                return [];
            }
            $parts = explode(',', $str);
        }

        $ids = [];
        $seen = [];
        foreach ($parts as $part) {
            $id = (int)$part;
            if ($id <= 0 || isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;
            $ids[] = $id;
        }

        return $ids;
    }

    /**
     * 配置页候选人员（排除超管/运营，含停用账号）
     *
     * @return array<int,array{admin_id:int,username:string,group_id:int,is_open:int,group_title:string}>
     */
    public function getCandidateUsers()
    {
        $rows = Db::name('admin')
            ->alias('a')
            ->leftJoin('auth_group g', 'a.group_id = g.group_id')
            ->where('a.username', '<>', '')
            ->where('a.group_id', 'not in', self::EXCLUDED_GROUP_IDS)
            ->field('a.admin_id,a.username,a.group_id,a.is_open,g.title as group_title')
            ->order('a.group_id', 'asc')
            ->order('a.admin_id', 'asc')
            ->select();

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        }
        if (!is_array($rows)) {
            $rows = [];
        }

        $list = [];
        foreach ($rows as $row) {
            $adminId = (int)($row['admin_id'] ?? 0);
            $username = trim((string)($row['username'] ?? ''));
            if ($adminId <= 0 || $username === '') {
                continue;
            }
            $list[] = [
                'admin_id' => $adminId,
                'username' => $username,
                'group_id' => (int)($row['group_id'] ?? 0),
                'is_open' => (int)($row['is_open'] ?? 0),
                'group_title' => trim((string)($row['group_title'] ?? '')),
            ];
        }

        return $list;
    }

    /**
     * 组装配置页展示数据（含 xmSelect 数据与默认预选）
     *
     * @return array
     */
    public function getConfigPageData()
    {
        $candidates = $this->getCandidateUsers();
        $config = $this->getLatestConfig();
        $hasConfig = $config !== null;

        if ($hasConfig) {
            $selectedIds = $this->parseAllowedOwnerUserIds($config['allowed_owner_user_ids'] ?? '');
            $excludeDisabled = (int)($config['exclude_disabled_users'] ?? 1) === 1 ? 1 : 0;
            $isSuggestedDefault = false;
        } else {
            $selectedIds = $this->getSuggestedDefaultOwnerIds($candidates);
            $excludeDisabled = 1;
            $isSuggestedDefault = true;
        }

        $selectedMap = array_fill_keys($selectedIds, true);
        $xmSelectData = [];
        foreach ($candidates as $item) {
            $groupTitle = $item['group_title'] !== '' ? $item['group_title'] : ('岗位' . $item['group_id']);
            if ((int)$item['is_open'] === 0) {
                $name = $item['username'] . '（' . $groupTitle . ' / 已停用）';
            } else {
                $name = $item['username'] . '（' . $groupTitle . '）';
            }
            $xmSelectData[] = [
                'name' => $name,
                'value' => (int)$item['admin_id'],
                'selected' => isset($selectedMap[(int)$item['admin_id']]),
            ];
        }

        return [
            'has_config' => $hasConfig,
            'is_suggested_default' => $isSuggestedDefault,
            'exclude_disabled_users' => $excludeDisabled,
            'selected_owner_ids' => $selectedIds,
            'candidates' => $candidates,
            'xm_select_data' => $xmSelectData,
            'update_user' => $hasConfig ? (string)($config['update_user'] ?? '') : '',
            'update_time' => $hasConfig ? (string)($config['update_time'] ?? '') : '',
        ];
    }

    /**
     * 无配置时的页面默认预选：旧规则岗位 + 启用中
     *
     * @param array $candidates
     * @return int[]
     */
    protected function getSuggestedDefaultOwnerIds(array $candidates)
    {
        $allowGroups = array_fill_keys(self::DEFAULT_SUGGEST_GROUP_IDS, true);
        $ids = [];
        foreach ($candidates as $item) {
            $adminId = (int)($item['admin_id'] ?? 0);
            $groupId = (int)($item['group_id'] ?? 0);
            $isOpen = (int)($item['is_open'] ?? 0);
            if ($adminId <= 0 || $isOpen !== 1 || !isset($allowGroups[$groupId])) {
                continue;
            }
            $ids[] = $adminId;
        }
        return $ids;
    }

    /**
     * 保存配置（首次 INSERT，后续 UPDATE 最新行）
     *
     * @param array $params
     * @return array{code:int,msg:string,data:array}
     */
    public function saveConfig(array $params = [])
    {
        $allowedIds = $this->parseAllowedOwnerUserIds($params['allowed_owner_user_ids'] ?? null);
        if (empty($allowedIds)) {
            return ['code' => -200, 'msg' => '请至少选择 1 个允许接收客户的人员', 'data' => []];
        }

        $excludeRaw = $params['exclude_disabled_users'] ?? null;
        if (is_bool($excludeRaw)) {
            $excludeDisabled = $excludeRaw ? 1 : 0;
        } else {
            $excludeStr = trim((string)$excludeRaw);
            if ($excludeStr === '' || !in_array($excludeStr, ['0', '1'], true)) {
                return ['code' => -200, 'msg' => '排除禁用账号参数无效，只允许 0 或 1', 'data' => []];
            }
            $excludeDisabled = (int)$excludeStr;
        }

        $validate = $this->validateAllowedOwnerUserIds($allowedIds);
        if ((int)($validate['code'] ?? -200) !== 0) {
            return $validate;
        }

        $now = date('Y-m-d H:i:s');
        $payload = [
            'allowed_owner_user_ids' => implode(',', $allowedIds),
            'exclude_disabled_users' => $excludeDisabled,
            'update_user_id' => (int)Session::get('aid'),
            'update_user' => trim((string)Session::get('username')),
            'update_time' => $now,
        ];

        try {
            $latest = $this->getLatestConfig();
            if ($latest && (int)($latest['id'] ?? 0) > 0) {
                ClientConfig::where('id', (int)$latest['id'])->update($payload);
            } else {
                $payload['create_time'] = $now;
                ClientConfig::create($payload);
            }

            return [
                'code' => 0,
                'msg' => '保存成功',
                'data' => [
                    'allowed_owner_user_ids' => $allowedIds,
                    'exclude_disabled_users' => $excludeDisabled,
                ],
            ];
        } catch (\Throwable $e) {
            return ['code' => -200, 'msg' => '保存失败：' . $e->getMessage(), 'data' => []];
        }
    }

    /**
     * 强校验：存在、用户名非空、非超管、非运营；停用允许
     *
     * @param int[] $ids
     * @return array{code:int,msg:string,data:array}
     */
    public function validateAllowedOwnerUserIds(array $ids)
    {
        $ids = $this->parseAllowedOwnerUserIds($ids);
        if (empty($ids)) {
            return ['code' => -200, 'msg' => '请至少选择 1 个允许接收客户的人员', 'data' => []];
        }

        $rows = Db::name('admin')
            ->where('admin_id', 'in', $ids)
            ->field('admin_id,username,group_id,is_open')
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        }
        if (!is_array($rows)) {
            $rows = [];
        }

        $map = [];
        foreach ($rows as $row) {
            $map[(int)($row['admin_id'] ?? 0)] = $row;
        }

        foreach ($ids as $id) {
            if (!isset($map[$id])) {
                return [
                    'code' => -200,
                    'msg' => '人员ID ' . $id . ' 不存在，配置保存失败',
                    'data' => [],
                ];
            }

            $username = trim((string)($map[$id]['username'] ?? ''));
            if ($username === '') {
                return [
                    'code' => -200,
                    'msg' => '人员ID ' . $id . ' 用户名为空，配置保存失败',
                    'data' => [],
                ];
            }

            $groupId = (int)($map[$id]['group_id'] ?? 0);
            if ($groupId === 1) {
                return [
                    'code' => -200,
                    'msg' => '超级管理员 ' . $username . ' 不能加入客户负责人名单',
                    'data' => [],
                ];
            }
            if ($groupId === 12) {
                return [
                    'code' => -200,
                    'msg' => '运营人员 ' . $username . ' 不能加入客户负责人名单',
                    'data' => [],
                ];
            }
        }

        return ['code' => 0, 'msg' => 'ok', 'data' => []];
    }
}
