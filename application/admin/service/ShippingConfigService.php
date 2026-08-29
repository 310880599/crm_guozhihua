<?php

namespace app\admin\service;

use app\admin\model\ShippingConfig;
use think\Db;

class ShippingConfigService
{
    /**
     * 可读写的配置白名单
     *
     * @var string[]
     */
    protected $allowedKeys = [
        'approval_threshold',
        'amount_basis',
        'product_director_id',
        'finance_cc_user_id',
        'cc_super_admin',
        'cc_team_leader',
    ];

    /**
     * 缺失配置时的默认元信息（仅白名单）
     *
     * @var array
     */
    protected $configMeta = [
        'approval_threshold' => [
            'config_name' => '产品总监审批金额线',
            'config_type' => 'number',
            'remark' => '原订单总金额达到该金额及以上，需要产品总监审批',
            'sort' => 10,
            'default' => '10000',
        ],
        'amount_basis' => [
            'config_name' => '审批金额判断依据',
            'config_type' => 'select',
            'remark' => '固定按原订单总金额判断',
            'sort' => 20,
            'default' => 'order_total',
        ],
        'product_director_id' => [
            'config_name' => '产品总监',
            'config_type' => 'user',
            'remark' => 'config_value保存管理员ID',
            'sort' => 30,
            'default' => '',
        ],
        'finance_cc_user_id' => [
            'config_name' => '财务抄送人',
            'config_type' => 'user',
            'remark' => 'config_value保存管理员ID',
            'sort' => 40,
            'default' => '',
        ],
        'cc_super_admin' => [
            'config_name' => '抄送超级管理员',
            'config_type' => 'bool',
            'remark' => '1=抄送，0=不抄送',
            'sort' => 50,
            'default' => '1',
        ],
        'cc_team_leader' => [
            'config_name' => '抄送销售所属主管',
            'config_type' => 'bool',
            'remark' => '1=抄送，0=不抄送',
            'sort' => 60,
            'default' => '1',
        ],
    ];

    /**
     * 是否具备发货管理配置权限（超级管理员）
     * admin_id==1 OR group_id==1 OR username==admin
     *
     * @param int $adminId
     * @return bool
     */
    public function isShippingConfigAdmin($adminId = 0)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            $adminId = (int)session('aid');
        }
        if ($adminId <= 0) {
            return false;
        }

        if ($adminId === 1) {
            return true;
        }

        $admin = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,group_id,username')
            ->find();
        if (empty($admin)) {
            return false;
        }

        if ((int)($admin['group_id'] ?? 0) === 1) {
            return true;
        }

        if (strtolower(trim((string)($admin['username'] ?? ''))) === 'admin') {
            return true;
        }

        return false;
    }

    /**
     * 无权限时返回统一错误结构；有权限返回 null
     *
     * @return array|null
     */
    public function assertShippingConfigAdmin()
    {
        if ($this->isShippingConfigAdmin()) {
            return null;
        }

        return ['code' => -200, 'msg' => '您无权限访问发货管理配置', 'data' => []];
    }

    /**
     * 读取配置 Map（按 sort ASC, id ASC）
     *
     * @return array
     */
    public function getConfigMap()
    {
        $map = [];
        foreach ($this->allowedKeys as $key) {
            $meta = $this->configMeta[$key] ?? [];
            $map[$key] = (string)($meta['default'] ?? '');
        }

        try {
            $rows = ShippingConfig::whereIn('config_key', $this->allowedKeys)
                ->order('sort', 'asc')
                ->order('id', 'asc')
                ->select();
            foreach ($rows as $row) {
                $key = (string)($row['config_key'] ?? '');
                if ($key === '' || !in_array($key, $this->allowedKeys, true)) {
                    continue;
                }
                $map[$key] = (string)($row['config_value'] ?? '');
            }
        } catch (\Throwable $e) {
            // 保持默认值，避免页面报错
        }

        // 业务强制：审批金额判断依据永远是原订单总金额
        $map['amount_basis'] = 'order_total';

        return $map;
    }

    /**
     * 获取管理员信息
     *
     * @param int $adminId
     * @return array|null
     */
    public function getAdminInfo($adminId)
    {
        $adminId = (int)$adminId;
        if ($adminId <= 0) {
            return null;
        }

        $row = Db::name('admin')
            ->where('admin_id', $adminId)
            ->field('admin_id,username,is_open,group_id,parent_id')
            ->find();
        if (empty($row)) {
            return null;
        }

        return [
            'admin_id' => (int)($row['admin_id'] ?? 0),
            'username' => (string)($row['username'] ?? ''),
            'is_open' => (int)($row['is_open'] ?? 0),
            'group_id' => (int)($row['group_id'] ?? 0),
            'parent_id' => (int)($row['parent_id'] ?? 0),
        ];
    }

    /**
     * 管理员下拉选项
     * 正常：username 非空且 is_open=1
     * 额外：强制纳入当前已配置 ID（即使已禁用），避免静默丢失
     *
     * @param int[] $includeIds
     * @return array
     */
    public function getAdminOptions(array $includeIds = [])
    {
        $includeIds = array_values(array_unique(array_filter(array_map('intval', $includeIds), function ($id) {
            return $id > 0;
        })));

        $list = Db::name('admin')
            ->where('username', '<>', '')
            ->where('is_open', 1)
            ->field('admin_id,username,is_open')
            ->order('admin_id', 'asc')
            ->select();

        $rows = [];
        $seen = [];
        foreach ((array)$list as $item) {
            $id = (int)($item['admin_id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $username = trim((string)($item['username'] ?? ''));
            if ($username === '') {
                continue;
            }
            $seen[$id] = true;
            $rows[] = [
                'admin_id' => $id,
                'username' => $username,
                'is_open' => 1,
                'label' => $username,
            ];
        }

        foreach ($includeIds as $id) {
            if (isset($seen[$id])) {
                continue;
            }
            $info = $this->getAdminInfo($id);
            if (empty($info) || $info['username'] === '') {
                // 配置指向已删除/无用户名的账号：仍展示，避免静默丢失当前值
                $rows[] = [
                    'admin_id' => $id,
                    'username' => '未知管理员#' . $id,
                    'is_open' => 0,
                    'label' => '未知管理员#' . $id . '（禁用）',
                ];
                $seen[$id] = true;
                continue;
            }
            $isOpen = (int)$info['is_open'] === 1 ? 1 : 0;
            $label = $info['username'];
            if ($isOpen !== 1) {
                $label .= '（禁用）';
            }
            $rows[] = [
                'admin_id' => $id,
                'username' => $info['username'],
                'is_open' => $isOpen,
                'label' => $label,
            ];
            $seen[$id] = true;
        }

        usort($rows, function ($a, $b) {
            return ((int)$a['admin_id']) - ((int)$b['admin_id']);
        });

        return $rows;
    }

    /**
     * 组装页面展示用数据（含禁用提示）
     *
     * @return array
     */
    public function getPageData()
    {
        $config = $this->getConfigMap();
        $directorId = (int)($config['product_director_id'] ?? 0);
        $financeId = (int)($config['finance_cc_user_id'] ?? 0);

        $director = $this->getAdminInfo($directorId);
        $finance = $this->getAdminInfo($financeId);

        $directorDisabled = false;
        $financeDisabled = false;
        $directorName = '';
        $financeName = '';

        if ($directorId > 0) {
            if (empty($director) || $director['username'] === '') {
                $directorDisabled = true;
                $directorName = '未知管理员#' . $directorId;
            } else {
                $directorName = $director['username'];
                if ((int)$director['is_open'] !== 1) {
                    $directorDisabled = true;
                }
            }
        }

        if ($financeId > 0) {
            if (empty($finance) || $finance['username'] === '') {
                $financeDisabled = true;
                $financeName = '未知管理员#' . $financeId;
            } else {
                $financeName = $finance['username'];
                if ((int)$finance['is_open'] !== 1) {
                    $financeDisabled = true;
                }
            }
        }

        return [
            'config' => $config,
            'admin_options' => $this->getAdminOptions([$directorId, $financeId]),
            'product_director' => [
                'admin_id' => $directorId,
                'username' => $directorName,
                'is_open' => $directorDisabled ? 0 : 1,
                'disabled' => $directorDisabled,
            ],
            'finance_cc_user' => [
                'admin_id' => $financeId,
                'username' => $financeName,
                'is_open' => $financeDisabled ? 0 : 1,
                'disabled' => $financeDisabled,
            ],
            'director_disabled_tip' => $directorDisabled
                ? '当前配置人员已被禁用，请重新选择有效管理员。'
                : '',
            'finance_disabled_tip' => $financeDisabled
                ? '当前配置人员已被禁用，请重新选择有效管理员。'
                : '',
        ];
    }

    /**
     * 保存配置（事务 + 白名单）
     *
     * @param array $data
     * @return array
     */
    public function saveConfig($data = [])
    {
        $denied = $this->assertShippingConfigAdmin();
        if ($denied !== null) {
            return $denied;
        }

        $data = is_array($data) ? $data : [];

        $approvalThresholdRaw = trim((string)($data['approval_threshold'] ?? ''));
        // checkbox 未勾选时前端可能不传字段，后端按 0 处理
        $ccSuperAdminRaw = trim((string)($data['cc_super_admin'] ?? '0'));
        $ccTeamLeaderRaw = trim((string)($data['cc_team_leader'] ?? '0'));
        $productDirectorId = (int)($data['product_director_id'] ?? 0);
        $financeCcUserId = (int)($data['finance_cc_user_id'] ?? 0);

        if ($approvalThresholdRaw === '' || !preg_match('/^\d+(\.\d+)?$/', $approvalThresholdRaw)) {
            return ['code' => -200, 'msg' => '产品总监审批金额线必须是大于等于0的数字', 'data' => []];
        }
        if ((float)$approvalThresholdRaw < 0) {
            return ['code' => -200, 'msg' => '产品总监审批金额线必须大于等于0', 'data' => []];
        }

        if ($ccSuperAdminRaw === '' || $ccSuperAdminRaw === 'on') {
            $ccSuperAdminRaw = ($ccSuperAdminRaw === 'on') ? '1' : '0';
        }
        if ($ccTeamLeaderRaw === '' || $ccTeamLeaderRaw === 'on') {
            $ccTeamLeaderRaw = ($ccTeamLeaderRaw === 'on') ? '1' : '0';
        }
        if (!in_array($ccSuperAdminRaw, ['0', '1'], true)) {
            return ['code' => -200, 'msg' => '抄送超级管理员只能是0或1', 'data' => []];
        }
        if (!in_array($ccTeamLeaderRaw, ['0', '1'], true)) {
            return ['code' => -200, 'msg' => '抄送销售所属主管只能是0或1', 'data' => []];
        }

        $directorCheck = $this->validateSelectableAdmin($productDirectorId, '产品总监');
        if ($directorCheck !== null) {
            return $directorCheck;
        }
        $financeCheck = $this->validateSelectableAdmin($financeCcUserId, '财务抄送人');
        if ($financeCheck !== null) {
            return $financeCheck;
        }

        // 白名单写入；amount_basis 强制 order_total，不信任前端
        $saveMap = [
            'approval_threshold' => $approvalThresholdRaw,
            'amount_basis' => 'order_total',
            'product_director_id' => (string)$productDirectorId,
            'finance_cc_user_id' => (string)$financeCcUserId,
            'cc_super_admin' => $ccSuperAdminRaw,
            'cc_team_leader' => $ccTeamLeaderRaw,
        ];

        $updatedBy = (int)session('aid');
        $now = time();

        Db::startTrans();
        try {
            foreach ($saveMap as $configKey => $configValue) {
                if (!in_array($configKey, $this->allowedKeys, true)) {
                    continue;
                }
                $row = ShippingConfig::where('config_key', $configKey)->find();
                if ($row) {
                    $row->config_value = $configValue;
                    $row->updated_by = $updatedBy;
                    $row->update_time = $now;
                    $row->save();
                } else {
                    $meta = $this->configMeta[$configKey] ?? [];
                    ShippingConfig::create([
                        'config_key' => $configKey,
                        'config_value' => $configValue,
                        'config_name' => (string)($meta['config_name'] ?? $configKey),
                        'config_type' => (string)($meta['config_type'] ?? 'text'),
                        'remark' => (string)($meta['remark'] ?? ''),
                        'sort' => (int)($meta['sort'] ?? 0),
                        'updated_by' => $updatedBy,
                        'create_time' => $now,
                        'update_time' => $now,
                    ]);
                }
            }

            Db::commit();
            return ['code' => 0, 'msg' => '配置保存成功', 'data' => $this->getConfigMap()];
        } catch (\Throwable $e) {
            Db::rollback();
            return ['code' => -200, 'msg' => '配置保存失败', 'data' => []];
        }
    }

    /**
     * 校验可选管理员：存在、username 非空、is_open=1
     *
     * @param int $adminId
     * @param string $label
     * @return array|null
     */
    protected function validateSelectableAdmin($adminId, $label)
    {
        $adminId = (int)$adminId;
        $label = (string)$label;
        if ($adminId <= 0) {
            return ['code' => -200, 'msg' => '请选择有效的' . $label, 'data' => []];
        }

        $info = $this->getAdminInfo($adminId);
        if (empty($info) || $info['username'] === '') {
            return ['code' => -200, 'msg' => $label . '管理员不存在或用户名为空', 'data' => []];
        }
        if ((int)$info['is_open'] !== 1) {
            return ['code' => -200, 'msg' => $label . '所选项已被禁用，请重新选择有效管理员', 'data' => []];
        }

        return null;
    }
}
