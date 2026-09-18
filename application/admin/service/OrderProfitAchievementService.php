<?php

namespace app\admin\service;

use think\Db;

/**
 * 订单利润业绩聚合（旺春 / 金秋共用口径）
 *
 * 口径说明：
 * - 表：crm_client_order
 * - 已审核：check_status = 2
 * - 金额字段：profit
 * - 负责人：pr_user_id / pr_user
 * - 协同：joint_person + owner_profit_rate / collaborator_profit_rate
 * - 舍入：两位小数；比例合计不足补给负责人；多人协同最后一人吃差额
 */
class OrderProfitAchievementService
{
    /**
     * 聚合时间范围内订单利润到 admin_id / 姓名。
     *
     * @param string $startTime 起始时间（含），建议 Y-m-d H:i:s
     * @param string $endTime 结束时间边界
     * @param string $orderTimeField order_time | create_time
     * @param string $endOperator '<=' 含结束时刻；'<' 半开区间（推荐金秋）
     * @return array{byUserId:array<int,float>,byName:array<string,float>}
     */
    public function aggregateProfitByUser($startTime, $endTime, $orderTimeField = 'order_time', $endOperator = '<=')
    {
        $timeField = $this->normalizeOrderTimeField($orderTimeField);
        $endOp = $endOperator === '<' ? '<' : '<=';

        $orderRows = Db::table('crm_client_order')
            ->alias('o')
            ->field('o.id, o.order_no, o.pr_user_id, o.pr_user, o.joint_person, o.profit, o.owner_profit_rate, o.collaborator_profit_rate')
            ->where('o.check_status', 2)
            ->where('o.' . $timeField, '>=', $startTime)
            ->where('o.' . $timeField, $endOp, $endTime)
            ->select();

        $achievementByUserId = [];
        $achievementByName   = [];
        if (!is_array($orderRows) || empty($orderRows)) {
            return [
                'byUserId' => $achievementByUserId,
                'byName'   => $achievementByName,
            ];
        }

        $allJointIds = [];
        foreach ($orderRows as $row) {
            foreach ($this->parseJointPersonIds(isset($row['joint_person']) ? $row['joint_person'] : '') as $jointId) {
                $allJointIds[$jointId] = $jointId;
            }
        }
        $jointNameMap = $this->getAdminNameMapByIds(array_values($allJointIds));

        foreach ($orderRows as $row) {
            $uid    = isset($row['pr_user_id']) ? (int)$row['pr_user_id'] : 0;
            $name   = isset($row['pr_user']) ? trim((string)$row['pr_user']) : '';
            $profit = $this->toFloatOrNull(isset($row['profit']) ? $row['profit'] : null);
            if ($profit === null) {
                $profit = 0.0;
            }
            $profit = round($profit, 2);

            $ownerRate = $this->normalizeProfitRate(isset($row['owner_profit_rate']) ? $row['owner_profit_rate'] : null, 100);
            $collaboratorRate = $this->normalizeProfitRate(isset($row['collaborator_profit_rate']) ? $row['collaborator_profit_rate'] : null, 0);

            $jointIds = $this->parseJointPersonIds(isset($row['joint_person']) ? $row['joint_person'] : '');
            $hasCollaborator = $collaboratorRate > 0 && !empty($jointIds);

            if (!$hasCollaborator) {
                $this->addAchievementAmount($achievementByUserId, $achievementByName, $uid, $name, $profit);
                continue;
            }

            $ownerAmount = round($profit * $ownerRate / 100, 2);
            $collaboratorAmount = round($profit * $collaboratorRate / 100, 2);
            $allocatedAmount = round($ownerAmount + $collaboratorAmount, 2);
            if ($profit > 0 && $allocatedAmount < $profit) {
                $ownerAmount = round($ownerAmount + ($profit - $allocatedAmount), 2);
            }
            $this->addAchievementAmount($achievementByUserId, $achievementByName, $uid, $name, $ownerAmount);

            $jointCount = count($jointIds);
            if ($jointCount <= 0) {
                continue;
            }
            if ($jointCount === 1) {
                $jointId = (int)$jointIds[0];
                $jointName = isset($jointNameMap[$jointId]) ? (string)$jointNameMap[$jointId] : '';
                $this->addAchievementAmount($achievementByUserId, $achievementByName, $jointId, $jointName, $collaboratorAmount);
                continue;
            }

            $avgAmount = round($collaboratorAmount / $jointCount, 2);
            $allocated = 0.0;
            foreach ($jointIds as $idx => $jointId) {
                $jointId = (int)$jointId;
                $isLast = $idx === $jointCount - 1;
                $jointShare = $isLast ? round($collaboratorAmount - $allocated, 2) : $avgAmount;
                $allocated = round($allocated + $jointShare, 2);
                $jointName = isset($jointNameMap[$jointId]) ? (string)$jointNameMap[$jointId] : '';
                $this->addAchievementAmount($achievementByUserId, $achievementByName, $jointId, $jointName, $jointShare);
            }
        }

        return [
            'byUserId' => $achievementByUserId,
            'byName'   => $achievementByName,
        ];
    }

    /**
     * @param string $field
     * @return string
     */
    public function normalizeOrderTimeField($field)
    {
        $allowedTimeFields = ['order_time', 'create_time'];

        return in_array($field, $allowedTimeFields, true) ? $field : 'order_time';
    }

    /**
     * @param mixed $jointPerson
     * @return int[]
     */
    public function parseJointPersonIds($jointPerson)
    {
        if ($jointPerson === null) {
            return [];
        }

        $rawItems = [];
        if (is_array($jointPerson)) {
            $rawItems = $jointPerson;
        } else {
            $raw = trim((string)$jointPerson);
            if ($raw === '') {
                return [];
            }

            if ($raw[0] === '[' || $raw[0] === '{') {
                $decoded = json_decode($raw, true);
                if (is_array($decoded)) {
                    $rawItems = $decoded;
                }
            }
            if (empty($rawItems)) {
                $normalized = str_replace(['，', '、', ';', '|'], ',', $raw);
                $rawItems = explode(',', $normalized);
            }
        }

        $ids = [];
        foreach ($rawItems as $item) {
            if (is_array($item)) {
                foreach (['admin_id', 'id', 'user_id', 'uid'] as $key) {
                    if (!isset($item[$key])) {
                        continue;
                    }
                    $candidate = trim((string)$item[$key]);
                    if ($candidate !== '' && preg_match('/^\d+$/', $candidate)) {
                        $id = (int)$candidate;
                        if ($id > 0) {
                            $ids[$id] = $id;
                        }
                        break;
                    }
                }
                continue;
            }

            $candidate = trim((string)$item);
            if ($candidate === '' || !preg_match('/^\d+$/', $candidate)) {
                continue;
            }
            $id = (int)$candidate;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }

        return array_values($ids);
    }

    /**
     * @param int[] $userIds
     * @return array<int,string>
     */
    public function getAdminNameMapByIds(array $userIds)
    {
        $ids = [];
        foreach ($userIds as $userId) {
            $id = (int)$userId;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }
        if (empty($ids)) {
            return [];
        }

        $rows = Db::name('admin')
            ->field('admin_id, username')
            ->where('admin_id', 'in', array_values($ids))
            ->select();

        $map = [];
        if (!is_array($rows)) {
            return $map;
        }
        foreach ($rows as $row) {
            $id = isset($row['admin_id']) ? (int)$row['admin_id'] : 0;
            if ($id <= 0) {
                continue;
            }
            $name = isset($row['username']) ? trim((string)$row['username']) : '';
            if ($name !== '') {
                $map[$id] = $name;
            }
        }

        return $map;
    }

    /**
     * @param array<int,float> $achievementByUserId
     * @param array<string,float> $achievementByName
     * @param int $userId
     * @param string $userName
     * @param float $amount
     * @return void
     */
    public function addAchievementAmount(array &$achievementByUserId, array &$achievementByName, $userId, $userName, $amount)
    {
        $uid = (int)$userId;
        $name = trim((string)$userName);
        $amount = round((float)$amount, 2);
        if ($amount <= 0) {
            return;
        }

        if ($uid > 0) {
            if (!isset($achievementByUserId[$uid])) {
                $achievementByUserId[$uid] = 0.0;
            }
            $achievementByUserId[$uid] = round((float)$achievementByUserId[$uid] + $amount, 2);
        }

        if ($name !== '') {
            if (!isset($achievementByName[$name])) {
                $achievementByName[$name] = 0.0;
            }
            $achievementByName[$name] = round((float)$achievementByName[$name] + $amount, 2);
        }
    }

    /**
     * @param mixed $value
     * @param mixed $default
     * @return float
     */
    public function normalizeProfitRate($value, $default)
    {
        $defaultRate = $this->toFloatOrNull($default);
        if ($defaultRate === null || $defaultRate < 0) {
            $defaultRate = 0.0;
        }
        if ($defaultRate > 100) {
            $defaultRate = 100.0;
        }

        $rate = $this->toFloatOrNull($value);
        if ($rate === null || $rate < 0) {
            $rate = $defaultRate;
        }
        if ($rate > 100) {
            $rate = 100.0;
        }

        return (float)$rate;
    }

    /**
     * @param mixed $value
     * @return float|null
     */
    public function toFloatOrNull($value)
    {
        if ($value === null) {
            return null;
        }
        if (is_int($value) || is_float($value)) {
            return (float)$value;
        }
        $raw = trim((string)$value);
        if ($raw === '' || !is_numeric($raw)) {
            return null;
        }

        return (float)$raw;
    }
}
