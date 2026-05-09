<?php

namespace app\admin\service;

class BusinessPerformanceSplitService
{
    public function parseJointPersonIds($jointPerson): array
    {
        if ($jointPerson === null) {
            return [];
        }
        if (is_array($jointPerson)) {
            return $this->normalizeIdList($jointPerson);
        }
        $raw = trim((string)$jointPerson);
        if ($raw === '') {
            return [];
        }
        if ($raw[0] === '[') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                return $this->normalizeIdList($decoded);
            }
        }
        $normalized = str_replace(['，', ';', '|'], ',', $raw);
        $normalized = preg_replace('/\s+/', '', $normalized);
        if ($normalized === null || $normalized === '') {
            return [];
        }
        if (preg_match('/^\d+$/', $normalized)) {
            return [(int)$normalized];
        }
        return $this->normalizeIdList(explode(',', $normalized));
    }

    public function normalizeProfitRates($ownerRate, $collaboratorRate): array
    {
        $owner = $this->toFloatOrNull($ownerRate);
        if ($owner === null || $owner <= 0) {
            $owner = 100.0;
        }
        $collaborator = $this->toFloatOrNull($collaboratorRate);
        if ($collaborator === null || $collaborator < 0) {
            $collaborator = 0.0;
        }
        return ['owner_rate' => $owner, 'collaborator_rate' => $collaborator];
    }

    public function splitOrderProfit(array $order, array $adminById = []): array
    {
        $profit = (float)($order['profit'] ?? 0);
        $money = (float)($order['money'] ?? 0);
        $ownerName = trim((string)($order['pr_user'] ?? ''));
        if ($ownerName === '') {
            $ownerName = '未知业务员';
        }
        $ownerId = (int)($order['pr_user_id'] ?? 0);
        $ownerTeam = trim((string)($order['team_name'] ?? ''));
        if ($ownerTeam === '' && $ownerId > 0 && isset($adminById[$ownerId])) {
            $ownerTeam = trim((string)($adminById[$ownerId]['team_name'] ?? ''));
        }
        if ($ownerTeam === '') {
            $ownerTeam = '未分组';
        }

        $rates = $this->normalizeProfitRates($order['owner_profit_rate'] ?? null, $order['collaborator_profit_rate'] ?? null);
        $jointIds = $this->parseJointPersonIds($order['joint_person'] ?? '');
        $hasCollaborator = !empty($jointIds) && (float)$rates['collaborator_rate'] > 0;
        $ownerProfit = $hasCollaborator ? round($profit * (float)$rates['owner_rate'] / 100, 2) : round($profit, 2);
        $rows = [[
            'admin_id' => $ownerId,
            'username' => $ownerName,
            'team_name' => $ownerTeam,
            'role' => 'owner',
            'order_count_weight' => 1,
            'money' => round($money, 2),
            'profit' => $ownerProfit,
        ]];
        if (!$hasCollaborator) {
            return $rows;
        }

        $collaboratorTotal = round($profit * (float)$rates['collaborator_rate'] / 100, 2);
        $count = count($jointIds);
        $avg = $count > 0 ? round($collaboratorTotal / $count, 2) : 0.0;
        $allocated = 0.0;
        foreach ($jointIds as $idx => $jointId) {
            $admin = $adminById[$jointId] ?? [];
            $name = trim((string)($admin['username'] ?? ''));
            if ($name === '') {
                $name = '协同人#' . $jointId;
            }
            $team = trim((string)($admin['team_name'] ?? ''));
            if ($team === '') {
                $team = '未分组';
            }
            $jointProfit = ($idx === $count - 1) ? round($collaboratorTotal - $allocated, 2) : $avg;
            $allocated = round($allocated + $jointProfit, 2);
            $rows[] = [
                'admin_id' => (int)$jointId,
                'username' => $name,
                'team_name' => $team,
                'role' => 'collaborator',
                'order_count_weight' => 0,
                'money' => 0.0,
                'profit' => $jointProfit,
            ];
        }
        return $rows;
    }

    public function aggregateOrderPerformance(array $orders, array $adminById = []): array
    {
        $aggMap = [];
        $totalProfit = 0.0;
        $totalMoney = 0.0;
        foreach ($orders as $order) {
            if (!is_array($order)) {
                continue;
            }
            $orderTime = trim((string)($order['order_time'] ?? ''));
            foreach ($this->splitOrderProfit($order, $adminById) as $splitRow) {
                $username = trim((string)($splitRow['username'] ?? ''));
                if ($username === '') {
                    continue;
                }
                if (!isset($aggMap[$username])) {
                    $aggMap[$username] = ['username' => $username, 'order_count' => 0, 'total_profit' => 0.0, 'total_money' => 0.0, 'snap_team_name' => '', 'latest_order_time' => ''];
                }
                $aggMap[$username]['order_count'] += (int)($splitRow['order_count_weight'] ?? 0);
                $aggMap[$username]['total_profit'] = round((float)$aggMap[$username]['total_profit'] + (float)($splitRow['profit'] ?? 0), 2);
                $aggMap[$username]['total_money'] = round((float)$aggMap[$username]['total_money'] + (float)($splitRow['money'] ?? 0), 2);
                $teamName = trim((string)($splitRow['team_name'] ?? ''));
                if ($teamName !== '' && ($aggMap[$username]['latest_order_time'] === '' || strcmp($orderTime, (string)$aggMap[$username]['latest_order_time']) >= 0)) {
                    $aggMap[$username]['snap_team_name'] = $teamName;
                    $aggMap[$username]['latest_order_time'] = $orderTime;
                }
                $totalProfit = round($totalProfit + (float)($splitRow['profit'] ?? 0), 2);
                $totalMoney = round($totalMoney + (float)($splitRow['money'] ?? 0), 2);
            }
        }
        return ['agg_map' => $aggMap, 'total_profit' => round($totalProfit, 2), 'total_money' => round($totalMoney, 2)];
    }

    private function normalizeIdList(array $items): array
    {
        $ids = [];
        foreach ($items as $item) {
            $raw = trim((string)$item);
            if ($raw === '' || !preg_match('/^\d+$/', $raw)) {
                continue;
            }
            $id = (int)$raw;
            if ($id > 0) {
                $ids[$id] = $id;
            }
        }
        return array_values($ids);
    }

    private function toFloatOrNull($value): ?float
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
