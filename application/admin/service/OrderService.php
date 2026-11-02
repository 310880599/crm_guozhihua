<?php

namespace app\admin\service;

use think\Db;
use think\facade\Log;

class OrderService
{
    /** 审核失败未处理达到或超过该秒数，视为超时。 */
    const FAILED_ORDER_OVERDUE_SECONDS = 86400;

    /** 提醒接口返回的订单摘要条数上限。事件标识仍按全部待处理订单计算。 */
    const FAILED_ORDER_REMINDER_LIMIT = 20;

    /**
     * 是否具备订单全量管理权限（查看/编辑/删除/跳过归属拦截）
     *
     * 规则：
     * - 超级管理员 aid=1
     * - admin 用户
     * - group_id in [13, 15]
     *
     * @param array|null $adminInfo
     * @return bool
     */
    public static function canManageAllOrders($adminInfo = null)
    {
        if ($adminInfo === null) {
            $adminInfo = \app\admin\model\Admin::getMyInfo();
        }

        $aid = (int)($adminInfo['admin_id'] ?? session('aid'));
        $gid = (int)($adminInfo['group_id'] ?? session('gid'));
        $username = (string)($adminInfo['username'] ?? session('username') ?? '');

        return $aid === 1
            || $username === 'admin'
            || in_array($gid, [13, 15], true);
    }

    /**
     * 校验客户主/辅电话职位身份是否完整
     *
     * 规则：
     * - 必须存在主/辅电话记录（contact_type in 1,3）
     * - 只要有电话行，则该行 position_title 必须非空
     *
     * @param int $clientId crm_leads.id
     * @return array
     * [
     *   'ok' => bool,
     *   'missing_count' => int,
     *   'message' => string
     * ]
     */
    public static function checkClientPhonePositionTitleComplete($clientId)
    {
        $clientId = (int)$clientId;
        if ($clientId <= 0) {
            return [
                'ok' => false,
                'missing_count' => 0,
                'message' => '客户ID不能为空',
            ];
        }

        $contacts = Db::name('crm_contacts')
            ->where('leads_id', $clientId)
            ->where('is_delete', 0)
            ->whereIn('contact_type', [1, 3])
            ->field('contact_value, position_title')
            ->select();

        if (empty($contacts)) {
            return [
                'ok' => false,
                'missing_count' => 0,
                'message' => '客户暂无可校验的电话记录',
            ];
        }

        $phoneCount = 0;
        $missingCount = 0;
        foreach ($contacts as $item) {
            $phone = trim((string)($item['contact_value'] ?? ''));
            if ($phone === '') {
                continue;
            }
            $phoneCount++;
            $title = trim((string)($item['position_title'] ?? ''));
            if ($title === '') {
                $missingCount++;
            }
        }

        if ($phoneCount <= 0) {
            return [
                'ok' => false,
                'missing_count' => 0,
                'message' => '客户暂无可校验的电话记录',
            ];
        }

        if ($missingCount > 0) {
            return [
                'ok' => false,
                'missing_count' => $missingCount,
                'message' => '客户电话职位身份未完善',
            ];
        }

        return [
            'ok' => true,
            'missing_count' => 0,
            'message' => '校验通过',
        ];
    }

    /**
     * 标准化联系方式（统一清洗手机号）
     *
     * @param string $contact
     * @return string
     */
    public static function normalizeContact($contact)
    {
        $contact = trim((string)$contact);
        if ($contact === '') {
            return '';
        }

        $contact = html_entity_decode($contact, ENT_QUOTES | ENT_HTML5, 'UTF-8');
        $contact = str_replace("\xC2\xA0", ' ', $contact);
        $contact = str_replace('　', ' ', $contact);
        $contact = preg_replace('/[+\-\s]/u', '', $contact);

        return trim((string)$contact);
    }

    /**
     * 按联系方式精确解析唯一客户 leads_id（跟进场景专用）
     *
     * 规则：
     * - 复用 normalizeContact，不另起清洗规则
     * - 仅匹配 is_delete=0 的联系人
     * - 收集 distinct leads_id：0 拒绝 / 1 通过 / >1 拒绝
     * - 禁止 find() 取第一条、禁止名称模糊兜底
     *
     * @param string $contact
     * @return array{ok:bool,leads_id:int,msg:string,match_count:int}
     */
    public static function resolveUniqueLeadsIdByContact($contact)
    {
        $raw = trim((string)$contact);
        $normalized = self::normalizeContact($raw);
        if ($normalized === '') {
            return [
                'ok' => false,
                'leads_id' => 0,
                'msg' => '订单联系方式为空，无法定位客户',
                'match_count' => 0,
            ];
        }

        $candidateValues = array_values(array_unique(array_filter([$raw, $normalized])));
        $rows = Db::name('crm_contacts')
            ->where('is_delete', 0)
            ->where(function ($query) use ($candidateValues, $normalized) {
                $query->whereIn('contact_value', $candidateValues);
                $query->whereOrRaw(
                    "REPLACE(REPLACE(REPLACE(IFNULL(contact_value,''), '+', ''), '-', ''), ' ', '') = '"
                    . addslashes($normalized) . "'"
                );
            })
            ->field('leads_id, contact_value')
            ->select();

        $leadsIds = [];
        foreach ($rows as $row) {
            $lid = (int)($row['leads_id'] ?? 0);
            if ($lid > 0) {
                $leadsIds[$lid] = true;
            }
        }
        $distinctIds = array_keys($leadsIds);
        $matchCount = count($distinctIds);

        if ($matchCount === 0) {
            return [
                'ok' => false,
                'leads_id' => 0,
                'msg' => '无法定位订单对应客户，请先核实客户关联',
                'match_count' => 0,
            ];
        }
        if ($matchCount > 1) {
            return [
                'ok' => false,
                'leads_id' => 0,
                'msg' => '订单联系方式匹配到多个客户，无法安全打开跟进',
                'match_count' => $matchCount,
            ];
        }

        return [
            'ok' => true,
            'leads_id' => (int)$distinctIds[0],
            'msg' => '',
            'match_count' => 1,
        ];
    }

    /**
     * 按联系方式批量精确解析唯一客户 leads_id（口径与 resolveUniqueLeadsIdByContact 一致）
     *
     * - 复用 normalizeContact，不另起清洗规则
     * - 仅匹配 is_delete=0 的联系人
     * - 每个联系方式独立统计 distinct leads_id：0 拒绝 / 1 通过 / >1 拒绝
     * - 禁止 find() 取第一条、禁止名称模糊兜底
     *
     * @param string[] $contacts 原始联系方式列表（可含重复）
     * @return array<string, array{ok:bool,leads_id:int,msg:string,match_count:int}> 以 trim 后的原始联系方式为键
     */
    public static function batchResolveUniqueLeadsIdByContacts(array $contacts)
    {
        $resultMap = [];
        $uniqueRaws = [];
        $normalizedByRaw = [];
        $allCandidateValues = [];
        $uniqueNormalized = [];

        foreach ($contacts as $contact) {
            $raw = trim((string)$contact);
            if ($raw === '') {
                continue;
            }
            if (isset($normalizedByRaw[$raw])) {
                continue;
            }
            $normalized = self::normalizeContact($raw);
            $normalizedByRaw[$raw] = $normalized;
            $uniqueRaws[] = $raw;
            if ($normalized === '') {
                $resultMap[$raw] = [
                    'ok' => false,
                    'leads_id' => 0,
                    'msg' => '订单联系方式为空，无法定位客户',
                    'match_count' => 0,
                ];
                continue;
            }
            $allCandidateValues[] = $raw;
            $allCandidateValues[] = $normalized;
            $uniqueNormalized[$normalized] = true;
        }

        $allCandidateValues = array_values(array_unique(array_filter($allCandidateValues, function ($v) {
            return trim((string)$v) !== '';
        })));
        $uniqueNormalizedList = array_keys($uniqueNormalized);

        $rows = [];
        if (!empty($allCandidateValues) || !empty($uniqueNormalizedList)) {
            $query = Db::name('crm_contacts')->where('is_delete', 0);
            $query->where(function ($q) use ($allCandidateValues, $uniqueNormalizedList) {
                $hasCondition = false;
                if (!empty($allCandidateValues)) {
                    $q->whereIn('contact_value', $allCandidateValues);
                    $hasCondition = true;
                }
                foreach ($uniqueNormalizedList as $norm) {
                    $rawSql = "REPLACE(REPLACE(REPLACE(IFNULL(contact_value,''), '+', ''), '-', ''), ' ', '') = '"
                        . addslashes($norm) . "'";
                    if ($hasCondition) {
                        $q->whereOrRaw($rawSql);
                    } else {
                        $q->whereRaw($rawSql);
                        $hasCondition = true;
                    }
                }
            });
            $rows = $query->field('leads_id, contact_value')->select();
            if (!is_array($rows)) {
                $rows = [];
            }
        }

        foreach ($uniqueRaws as $raw) {
            if (isset($resultMap[$raw])) {
                continue;
            }
            $normalized = $normalizedByRaw[$raw] ?? '';
            if ($normalized === '') {
                $resultMap[$raw] = [
                    'ok' => false,
                    'leads_id' => 0,
                    'msg' => '订单联系方式为空，无法定位客户',
                    'match_count' => 0,
                ];
                continue;
            }

            $candidateValues = array_values(array_unique(array_filter([$raw, $normalized])));
            $leadsIds = [];
            foreach ($rows as $row) {
                $rowValue = trim((string)($row['contact_value'] ?? ''));
                $rowNorm = self::normalizeContact($rowValue);
                $matched = in_array($rowValue, $candidateValues, true)
                    || ($rowNorm !== '' && $rowNorm === $normalized);
                if (!$matched) {
                    continue;
                }
                $lid = (int)($row['leads_id'] ?? 0);
                if ($lid > 0) {
                    $leadsIds[$lid] = true;
                }
            }
            $distinctIds = array_keys($leadsIds);
            $matchCount = count($distinctIds);

            if ($matchCount === 0) {
                $resultMap[$raw] = [
                    'ok' => false,
                    'leads_id' => 0,
                    'msg' => '无法定位订单对应客户，请先核实客户关联',
                    'match_count' => 0,
                ];
            } elseif ($matchCount > 1) {
                $resultMap[$raw] = [
                    'ok' => false,
                    'leads_id' => 0,
                    'msg' => '订单联系方式匹配到多个客户，无法安全打开跟进',
                    'match_count' => $matchCount,
                ];
            } else {
                $resultMap[$raw] = [
                    'ok' => true,
                    'leads_id' => (int)$distinctIds[0],
                    'msg' => '',
                    'match_count' => 1,
                ];
            }
        }

        return $resultMap;
    }

    /**
     * 我的订单入口：按 order_id 解析可跟进的唯一客户
     *
     * @param int $orderId
     * @param string $username 当前登录用户名（与 personClientSearch 口径一致）
     * @param array $operatorInfo ['admin_id'=>int,'username'=>string,'group_id'=>int,'team_name'=>string]
     * @return array{code:int,msg:string,data:array}
     */
    public static function resolveOrderClientForFollow($orderId, $username, array $operatorInfo = [])
    {
        $orderId = (int)$orderId;
        $username = trim((string)$username);
        if ($orderId <= 0) {
            return ['code' => 1, 'msg' => '缺少订单ID', 'data' => []];
        }
        if ($username === '') {
            return ['code' => 1, 'msg' => '登录状态已失效，请重新登录', 'data' => []];
        }

        $order = Db::table('crm_client_order')
            ->where('id', $orderId)
            ->where('check_status', 2)
            ->where(function ($query) use ($username) {
                $query->where('at_user', '=', $username)
                    ->whereOr('pr_user', '=', $username);
            })
            ->find();

        if (empty($order)) {
            return ['code' => 1, 'msg' => '订单不存在或无权操作', 'data' => []];
        }

        $contactMatch = self::resolveUniqueLeadsIdByContact($order['contact'] ?? '');
        if (empty($contactMatch['ok'])) {
            return [
                'code' => 1,
                'msg' => (string)($contactMatch['msg'] ?? '无法定位订单对应客户'),
                'data' => [],
            ];
        }

        $leadsId = (int)$contactMatch['leads_id'];
        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (empty($client)) {
            return ['code' => 1, 'msg' => '无法定位订单对应客户', 'data' => []];
        }

        $followService = new ClientFollowService();
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? $username));
        $adminContext = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($operatorInfo['group_id'] ?? 0),
            'team_name' => (string)($operatorInfo['team_name'] ?? ''),
        ];

        if (!$followService->canReadClientFollow($client, $operatorId, $operatorName, $adminContext)) {
            return ['code' => 1, 'msg' => '您没有权限查看该客户跟进', 'data' => []];
        }

        $writeRole = $followService->resolveFollowRole($client, $operatorId, $operatorName);

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'order_id' => $orderId,
                'leads_id' => $leadsId,
                'kh_name' => (string)($client['kh_name'] ?? ''),
                'pr_user' => (string)($client['pr_user'] ?? ''),
                'can_read_follow' => true,
                'can_write_follow' => $writeRole !== '',
                'current_role' => $writeRole,
                'current_role_text' => $writeRole !== '' ? $followService->getFollowRoleText($writeRole) : '',
            ],
        ];
    }

    /**
     * 待审核订单入口：按 order_id 解析可跟进的唯一客户（独立于我的订单）
     *
     * 订单层规则对齐 pendingClientSearch：
     * - 仅 check_status = 1
     * - canManageAllOrders 可解析全部待审订单
     * - 普通用户仅限 at_user / pr_user = 当前用户
     * 客户跟进读写权限仍由 ClientFollowService 判定，不在此放宽。
     *
     * @param int $orderId
     * @param string $username
     * @param array $operatorInfo ['admin_id'=>int,'username'=>string,'group_id'=>int,'team_name'=>string]
     * @return array{code:int,msg:string,data:array}
     */
    public static function resolvePendingOrderClientForFollow($orderId, $username, array $operatorInfo = [])
    {
        $orderId = (int)$orderId;
        $username = trim((string)$username);
        if ($orderId <= 0) {
            return ['code' => 1, 'msg' => '缺少订单ID', 'data' => []];
        }
        if ($username === '') {
            return ['code' => 1, 'msg' => '登录状态已失效，请重新登录', 'data' => []];
        }

        $canManageAll = self::canManageAllOrders($operatorInfo);

        $query = Db::table('crm_client_order')
            ->where('id', $orderId)
            ->where('check_status', 1);

        if (!$canManageAll) {
            $query->where(function ($q) use ($username) {
                $q->where('at_user', '=', $username)
                    ->whereOr('pr_user', '=', $username);
            });
        }

        $order = $query->find();
        if (empty($order)) {
            return ['code' => 1, 'msg' => '订单不存在或无权操作', 'data' => []];
        }

        $contactMatch = self::resolveUniqueLeadsIdByContact($order['contact'] ?? '');
        if (empty($contactMatch['ok'])) {
            return [
                'code' => 1,
                'msg' => (string)($contactMatch['msg'] ?? '无法定位订单对应客户'),
                'data' => [],
            ];
        }

        $leadsId = (int)$contactMatch['leads_id'];
        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (empty($client)) {
            return ['code' => 1, 'msg' => '无法定位订单对应客户', 'data' => []];
        }

        $followService = new ClientFollowService();
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? $username));
        $adminContext = [
            'admin_id' => $operatorId,
            'username' => $operatorName,
            'group_id' => (int)($operatorInfo['group_id'] ?? 0),
            'team_name' => (string)($operatorInfo['team_name'] ?? ''),
        ];

        if (!$followService->canReadClientFollow($client, $operatorId, $operatorName, $adminContext)) {
            return ['code' => 1, 'msg' => '您没有权限查看该客户跟进', 'data' => []];
        }

        $writeRole = $followService->resolveFollowRole($client, $operatorId, $operatorName);

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'order_id' => $orderId,
                'leads_id' => $leadsId,
                'kh_name' => (string)($client['kh_name'] ?? ''),
                'pr_user' => (string)($client['pr_user'] ?? ''),
                'can_read_follow' => true,
                'can_write_follow' => $writeRole !== '',
                'current_role' => $writeRole,
                'current_role_text' => $writeRole !== '' ? $followService->getFollowRoleText($writeRole) : '',
            ],
        ];
    }

    /**
     * 根据联系方式匹配客户ID（leads_id）
     *
     * 规则：
     * - 仅基于 contact_value 匹配
     * - 匹配前统一走 normalizeContact
     *
     * @param string $contact
     * @return int
     */
    public static function getClientIdByContact($contact)
    {
        $match = self::matchContactRecord($contact);
        return (int)($match['leads_id'] ?? 0);
    }

    /**
     * 批量根据联系方式匹配客户ID（leads_id）
     *
     * 规则：
     * - 仅基于 contact_value 匹配
     * - 匹配前统一走 normalizeContact
     * - 仅匹配未删除联系人（is_delete = 0）
     *
     * @param array $contacts
     * @return array
     * [
     *   '原始联系方式' => leads_id,
     *   '归一化联系方式' => leads_id
     * ]
     */
    public static function getClientIdMapByContacts(array $contacts)
    {
        $contactMap = [];
        $rawContacts = [];
        foreach ($contacts as $contact) {
            $raw = trim((string)$contact);
            if ($raw === '') {
                continue;
            }
            if (!isset($contactMap[$raw])) {
                $contactMap[$raw] = self::normalizeContact($raw);
                $rawContacts[] = $raw;
            }
        }

        if (empty($rawContacts)) {
            return [];
        }

        $normalizedContacts = [];
        foreach ($contactMap as $raw => $normalized) {
            if ($normalized !== '') {
                $normalizedContacts[] = $normalized;
            }
        }
        $normalizedContacts = array_values(array_unique($normalizedContacts));

        $candidateValues = array_values(array_unique(array_merge($rawContacts, $normalizedContacts)));
        if (empty($candidateValues)) {
            return [];
        }

        $records = Db::name('crm_contacts')
            ->where('is_delete', 0)
            ->whereIn('contact_value', $candidateValues)
            ->field('leads_id, contact_value')
            ->select();

        if (empty($records)) {
            return [];
        }

        $result = [];
        foreach ($contactMap as $rawContact => $normalizedContact) {
            $bestScore = 999;
            $bestLeadsId = 0;

            foreach ($records as $row) {
                $rowLeadsId = (int)($row['leads_id'] ?? 0);
                if ($rowLeadsId <= 0) {
                    continue;
                }

                $rowValue = trim((string)($row['contact_value'] ?? ''));
                $rowValueNormalized = self::normalizeContact($rowValue);

                $score = 999;
                if ($rowValue !== '' && $rowValue === $rawContact) {
                    $score = 1;
                } elseif ($normalizedContact !== '' && $rowValue !== '' && $rowValue === $normalizedContact) {
                    $score = 2;
                } elseif ($normalizedContact !== '' && $rowValueNormalized !== '' && $rowValueNormalized === $normalizedContact) {
                    $score = 3;
                }

                if ($score < $bestScore) {
                    $bestScore = $score;
                    $bestLeadsId = $rowLeadsId;
                    if ($score === 1) {
                        break;
                    }
                }
            }

            if ($bestLeadsId > 0) {
                if (!isset($result[$rawContact])) {
                    $result[$rawContact] = $bestLeadsId;
                }
                if ($normalizedContact !== '' && !isset($result[$normalizedContact])) {
                    $result[$normalizedContact] = $bestLeadsId;
                }
            }
        }

        return $result;
    }

    /**
     * 获取客户全部未删除主/辅电话（contact_type in 1,3）
     *
     * @param int $leadsId
     * @return array
     */
    public static function getClientAllPhones($leadsId)
    {
        $leadsId = (int)$leadsId;
        if ($leadsId <= 0) {
            return [];
        }

        $rows = Db::name('crm_contacts')
            ->where('leads_id', $leadsId)
            ->where('is_delete', 0)
            ->whereIn('contact_type', [1, 3])
            ->field('contact_value')
            ->select();

        if (empty($rows)) {
            return [];
        }

        $phones = [];
        $seen = [];
        foreach ($rows as $row) {
            $raw = trim((string)($row['contact_value'] ?? ''));
            if ($raw !== '' && !isset($seen[$raw])) {
                $phones[] = $raw;
                $seen[$raw] = 1;
            }
            $normalized = self::normalizeContact($raw);
            if ($normalized !== '' && !isset($seen[$normalized])) {
                $phones[] = $normalized;
                $seen[$normalized] = 1;
            }
        }

        return $phones;
    }

    /**
     * 判断客户名下任意电话是否存在审核通过订单
     *
     * @param int $leadsId
     * @param int $excludeOrderId 编辑场景排除当前订单
     * @return bool
     */
    public static function checkClientHasApprovedOrderByAnyPhone($leadsId, $excludeOrderId = 0)
    {
        $leadsId = (int)$leadsId;
        if ($leadsId <= 0) {
            return false;
        }

        $phones = self::getClientAllPhones($leadsId);
        if (empty($phones)) {
            return false;
        }

        $normalizedPhones = [];
        foreach ($phones as $phone) {
            $normalized = self::normalizeContact($phone);
            if ($normalized !== '') {
                $normalizedPhones[] = $normalized;
            }
        }
        $normalizedPhones = array_values(array_unique($normalizedPhones));
        $normalizedSql = "'" . implode("','", array_map('addslashes', $normalizedPhones)) . "'";

        $query = Db::name('crm_client_order')
            ->where('check_status', 2)
            ->where(function ($q) use ($phones, $normalizedSql) {
                $q->whereIn('contact', $phones);
                if (!empty($normalizedSql) && $normalizedSql !== "''") {
                    $q->whereOrRaw("REPLACE(REPLACE(REPLACE(IFNULL(contact, ''), '+', ''), '-', ''), ' ', '') IN (" . $normalizedSql . ")");
                }
            });

        $excludeOrderId = (int)$excludeOrderId;
        if ($excludeOrderId > 0) {
            $query->where('id', '<>', $excludeOrderId);
        }

        return !empty($query->field('id')->find());
    }

    /**
     * 根据联系方式返回“是否必须返单”的完整规则
     *
     * @param string $contact
     * @param int $excludeOrderId 编辑场景排除当前订单
     * @return array
     */
    public static function getReturnOrderRuleByContact($contact, $excludeOrderId = 0)
    {
        $result = [
            'must_return' => false,
            'leads_id' => 0,
            'phones' => [],
            'matched_contact' => '',
            'suggest_source_name' => '返单',
            'suggest_port_name' => '',
            'suggest_source_id' => 0,
            'suggest_port_id' => 0,
            'message' => '',
        ];

        $contact = trim((string)$contact);
        if ($contact === '') {
            $result['message'] = '联系方式为空，无法判定返单规则';
            return $result;
        }

        $match = self::matchContactRecord($contact);
        $leadsId = (int)($match['leads_id'] ?? 0);
        if ($leadsId <= 0) {
            $result['message'] = '未匹配到客户信息，不触发返单锁定';
            return $result;
        }

        $result['leads_id'] = $leadsId;
        $result['matched_contact'] = (string)($match['matched_contact'] ?? '');
        $result['phones'] = self::getClientAllPhones($leadsId);

        $hasApproved = self::checkClientHasApprovedOrderByAnyPhone($leadsId, $excludeOrderId);
        if (!$hasApproved) {
            $result['message'] = '该客户暂无审核通过订单，不强制返单';
            return $result;
        }

        $result['must_return'] = true;
        $result['message'] = '该客户已有审核通过订单，本次订单必须选择返单来源和对应返单运营端口，请勿选择非返单来源。';

        $returnSource = Db::name('crm_inquiry')
            ->where('inquiry_name', '返单')
            ->field('id, inquiry_name')
            ->find();
        if (!empty($returnSource['id'])) {
            $result['suggest_source_id'] = (int)$returnSource['id'];
        } else {
            $result['message'] = '检测到必须返单，但系统未配置“返单”询盘来源，请先在字典中补充后再提交。';
        }

        $leads = Db::name('crm_leads')
            ->where('id', $leadsId)
            ->field('id, inquiry_id, port_id, kh_status, source_port')
            ->find();

        $guessedPortName = self::guessReturnPortNameByLeads($leads);
        if ($guessedPortName !== '') {
            $result['suggest_port_name'] = $guessedPortName;
        }

        $portInfo = self::resolveReturnPortInfo($result['suggest_source_id'], $result['suggest_port_name']);
        if (!empty($portInfo['id'])) {
            $result['suggest_port_id'] = (int)$portInfo['id'];
            $result['suggest_port_name'] = (string)$portInfo['port_name'];
        } elseif (!empty($portInfo['port_name']) && $result['suggest_port_name'] === '') {
            $result['suggest_port_name'] = (string)$portInfo['port_name'];
        }

        if ($result['suggest_port_id'] <= 0 && $result['suggest_port_name'] === '') {
            $result['message'] = '检测到必须返单，但系统未找到返单运营端口（如“竞价返单/C端返单/抖音返单/B2B返单”），请先补充端口字典后再提交。';
        }

        return $result;
    }

    /**
     * 标准化来源名称（支持来源名称或来源ID）
     *
     * @param mixed $sourceInput
     * @return string
     */
    public static function normalizeSourceName($sourceInput)
    {
        $sourceInput = trim((string)$sourceInput);
        if ($sourceInput === '') {
            return '';
        }

        if ($sourceInput === '返单') {
            return '返单';
        }

        $isNumericId = ctype_digit($sourceInput) || (is_numeric($sourceInput) && (int)$sourceInput > 0);
        if ($isNumericId) {
            $inquiry = Db::name('crm_inquiry')
                ->where('id', (int)$sourceInput)
                ->field('inquiry_name')
                ->find();
            if (!empty($inquiry['inquiry_name'])) {
                return trim((string)$inquiry['inquiry_name']);
            }
        }

        return $sourceInput;
    }

    /**
     * 解析端口名称（支持端口名称或端口ID）
     *
     * @param mixed $sourcePortInput
     * @return string
     */
    public static function resolvePortName($sourcePortInput)
    {
        $sourcePortInput = trim((string)$sourcePortInput);
        if ($sourcePortInput === '') {
            return '';
        }

        $isNumericId = ctype_digit($sourcePortInput) || (is_numeric($sourcePortInput) && (int)$sourcePortInput > 0);
        if ($isNumericId) {
            $portInfo = Db::name('crm_inquiry_port')
                ->where('id', (int)$sourcePortInput)
                ->field('port_name')
                ->find();
            if ($portInfo && !empty($portInfo['port_name'])) {
                return trim((string)$portInfo['port_name']);
            }
            return '';
        }

        return $sourcePortInput;
    }

    /**
     * 返单端口匹配校验（支持 ID/名称/已保存名称）
     *
     * @param mixed $sourcePortInput
     * @param array $rule
     * @param string $sourcePortName
     * @return bool
     */
    public static function isReturnPortMatched($sourcePortInput, array $rule = [], $sourcePortName = '')
    {
        $sourcePortRaw = trim((string)$sourcePortInput);
        $sourcePortName = trim((string)$sourcePortName);
        $resolvedPortName = self::resolvePortName($sourcePortRaw);
        if ($sourcePortName === '' && $resolvedPortName !== '') {
            $sourcePortName = $resolvedPortName;
        }
        if ($sourcePortName === '' && $sourcePortRaw !== '' && !ctype_digit($sourcePortRaw)) {
            $sourcePortName = $sourcePortRaw;
        }

        $suggestPortId = (int)($rule['suggest_port_id'] ?? 0);
        $suggestPortName = trim((string)($rule['suggest_port_name'] ?? ''));

        $sourcePortId = 0;
        $isNumericId = ctype_digit($sourcePortRaw) || (is_numeric($sourcePortRaw) && (int)$sourcePortRaw > 0);
        if ($isNumericId) {
            $sourcePortId = (int)$sourcePortRaw;
        }

        if ($sourcePortId > 0 && $suggestPortId > 0 && $sourcePortId === $suggestPortId) {
            return true;
        }

        if ($sourcePortName !== '' && $suggestPortName !== '' && $sourcePortName === $suggestPortName) {
            return true;
        }

        if ($sourcePortName !== '' && mb_stripos($sourcePortName, '返单') !== false) {
            return true;
        }

        return false;
    }

    /**
     * 校验提交的来源/端口是否符合返单强规则
     *
     * @param string $contact
     * @param mixed $sourceInput
     * @param mixed $sourcePortInput
     * @param int $excludeOrderId
     * @param string $sourcePortName
     * @return array ['ok'=>bool,'message'=>string,'rule'=>array]
     */
    public static function validateReturnOrderSelection($contact, $sourceInput, $sourcePortInput, $excludeOrderId = 0, $sourcePortName = '')
    {
        $rule = self::getReturnOrderRuleByContact($contact, $excludeOrderId);
        if (empty($rule['must_return'])) {
            return ['ok' => true, 'message' => '', 'rule' => $rule];
        }

        $normalizedSourceName = self::normalizeSourceName($sourceInput);
        if ($normalizedSourceName !== '返单') {
            return [
                'ok' => false,
                'message' => '该客户已有审核通过订单，本次订单必须选择返单来源和对应返单运营端口，请勿选择非返单来源。',
                'rule' => $rule,
            ];
        }

        if (!self::isReturnPortMatched($sourcePortInput, $rule, $sourcePortName)) {
            return [
                'ok' => false,
                'message' => '该客户已有审核通过订单，本次订单必须选择返单来源和对应返单运营端口，请勿选择非返单来源。',
                'rule' => $rule,
            ];
        }

        return ['ok' => true, 'message' => '', 'rule' => $rule];
    }

    /**
     * 按返单规则统一修正 source / order_type（以后端为准）
     *
     * 规则：
     * - 命中客户 + 客户任意主/辅电话存在审核通过订单 => 强制返单（order_type=2）
     * - 其他情况 => 新单（order_type=1）
     *
     * @param array $data
     * @param int $excludeOrderId 编辑场景排除当前订单
     * @return array
     */
    public static function applyOrderTypeByReturnRule(array $data, $excludeOrderId = 0)
    {
        $contact = trim((string)($data['contact'] ?? ''));
        $leadsId = self::getClientIdByContact($contact);
        if ($leadsId <= 0) {
            $data['order_type'] = 1;
            return $data;
        }

        $hasApproved = self::checkClientHasApprovedOrderByAnyPhone($leadsId, $excludeOrderId);
        if ($hasApproved) {
            $data['source'] = '返单';
            $data['order_type'] = 2;
            return $data;
        }

        $data['order_type'] = 1;
        return $data;
    }

    /**
     * 匹配联系方式并返回客户、命中电话
     *
     * @param string $contact
     * @return array
     */
    private static function matchContactRecord($contact)
    {
        $contact = trim((string)$contact);
        $normalized = self::normalizeContact($contact);
        if ($normalized === '') {
            return ['leads_id' => 0, 'matched_contact' => ''];
        }

        $record = Db::name('crm_contacts')
            ->where('is_delete', 0)
            ->where(function ($query) use ($contact, $normalized) {
                $query->where('contact_value', $normalized);
                if ($contact !== '' && $contact !== $normalized) {
                    $query->whereOr('contact_value', $contact);
                }
                $query->whereOrRaw("REPLACE(REPLACE(REPLACE(IFNULL(contact_value,''), '+', ''), '-', ''), ' ', '') = '" . addslashes($normalized) . "'");
            })
            ->field('leads_id, contact_value')
            ->find();

        if (empty($record)) {
            return ['leads_id' => 0, 'matched_contact' => ''];
        }

        $matched = trim((string)($record['contact_value'] ?? ''));

        return [
            'leads_id' => (int)($record['leads_id'] ?? 0),
            'matched_contact' => $matched,
        ];
    }

    /**
     * 根据联系方式解析客户（crm_leads）当前登记的“询盘来源渠道”名称
     *
     * 解析优先级（与 Order::changeyewu 中的渠道解析逻辑保持一致）：
     * 1. port_id 反查端口所属渠道（crm_inquiry_port.inquiry_id -> crm_inquiry.inquiry_name）
     * 2. crm_leads.inquiry_id 直接关联的渠道
     * 3. crm_leads.kh_status 兼容旧数据（数字ID或名称）
     *
     * @param string $contact
     * @return string 未匹配到客户或渠道时返回空字符串
     */
    public static function resolveClientSourceNameByContact($contact)
    {
        $match = self::matchContactRecord($contact);
        $leadsId = (int)($match['leads_id'] ?? 0);
        if ($leadsId <= 0) {
            return '';
        }

        $custinfo = Db::name('crm_leads')->where('id', $leadsId)->find();
        if (empty($custinfo)) {
            return '';
        }

        $sourceName = '';

        if (!empty($custinfo['port_id'])) {
            $portIds = array_filter(explode(',', $custinfo['port_id']));
            if (!empty($portIds)) {
                $firstPortId = trim((string)reset($portIds));
                if ($firstPortId !== '') {
                    $portInfo = Db::name('crm_inquiry_port')
                        ->where('id', $firstPortId)
                        ->field('inquiry_id')
                        ->find();
                    if (!empty($portInfo['inquiry_id'])) {
                        $inquiryInfo = Db::name('crm_inquiry')
                            ->where('id', $portInfo['inquiry_id'])
                            ->field('inquiry_name')
                            ->find();
                        if (!empty($inquiryInfo['inquiry_name'])) {
                            $sourceName = $inquiryInfo['inquiry_name'];
                        }
                    }
                }
            }
        }

        if ($sourceName === '' && !empty($custinfo['inquiry_id'])) {
            $inquiryInfo = Db::name('crm_inquiry')
                ->where('id', $custinfo['inquiry_id'])
                ->field('inquiry_name')
                ->find();
            if (!empty($inquiryInfo['inquiry_name'])) {
                $sourceName = $inquiryInfo['inquiry_name'];
            }
        }

        if ($sourceName === '') {
            $khStatusValue = $custinfo['kh_status'] ?? '';
            if (!empty($khStatusValue)) {
                if (is_numeric($khStatusValue)) {
                    $inquiryInfo = Db::name('crm_inquiry')->where('id', $khStatusValue)->find();
                    if ($inquiryInfo) {
                        $sourceName = $inquiryInfo['inquiry_name'];
                    } else {
                        $statusInfo = Db::name('crm_client_status')->where('id', $khStatusValue)->find();
                        if ($statusInfo) {
                            $sourceName = $statusInfo['status_name'];
                        }
                    }
                } else {
                    $inquiryInfo = Db::name('crm_inquiry')->where('inquiry_name', $khStatusValue)->find();
                    $sourceName = $inquiryInfo ? $inquiryInfo['inquiry_name'] : $khStatusValue;
                }
            }
        }

        return trim((string)$sourceName);
    }

    /**
     * 判断客户（crm_leads）原始登记的询盘来源渠道是否为“返单”
     *
     * @param string $contact
     * @return bool
     */
    public static function isClientOriginalSourceReturn($contact)
    {
        return self::resolveClientSourceNameByContact($contact) === '返单';
    }

    /**
     * 根据客户历史渠道信息推断返单端口名称
     *
     * @param array $leads
     * @return string
     */
    private static function guessReturnPortNameByLeads(array $leads = [])
    {
        if (empty($leads)) {
            return '';
        }

        $candidates = [];
        if (!empty($leads['source_port'])) {
            $candidates[] = (string)$leads['source_port'];
        }
        if (!empty($leads['kh_status'])) {
            $candidates[] = (string)$leads['kh_status'];
        }

        $inquiryId = (int)($leads['inquiry_id'] ?? 0);
        if ($inquiryId > 0) {
            $inquiry = Db::name('crm_inquiry')->where('id', $inquiryId)->field('inquiry_name')->find();
            if (!empty($inquiry['inquiry_name'])) {
                $candidates[] = (string)$inquiry['inquiry_name'];
            }
        }

        $portIdsRaw = trim((string)($leads['port_id'] ?? ''));
        if ($portIdsRaw !== '') {
            $portIds = array_values(array_filter(array_map('trim', explode(',', $portIdsRaw)), function ($v) {
                return $v !== '';
            }));
            if (!empty($portIds)) {
                $ports = Db::name('crm_inquiry_port')
                    ->where('id', 'in', $portIds)
                    ->field('port_name')
                    ->select();
                foreach ($ports as $p) {
                    if (!empty($p['port_name'])) {
                        $candidates[] = (string)$p['port_name'];
                    }
                }
            }
        }

        $joinedText = mb_strtolower(implode(' ', $candidates), 'UTF-8');
        if ($joinedText === '') {
            return '';
        }

        if (mb_strpos($joinedText, '竞价') !== false) {
            return '竞价返单';
        }
        if (mb_strpos($joinedText, '抖音') !== false) {
            return '抖音返单';
        }
        if (mb_strpos($joinedText, 'c端') !== false || mb_strpos($joinedText, 'c 端') !== false) {
            return 'C端返单';
        }
        if (mb_strpos($joinedText, 'b2b') !== false) {
            return 'B2B返单';
        }

        return '';
    }

    /**
     * 查询返单端口（先精确，再模糊）
     *
     * @param int $returnSourceId
     * @param string $guessPortName
     * @return array
     */
    private static function resolveReturnPortInfo($returnSourceId, $guessPortName)
    {
        $returnSourceId = (int)$returnSourceId;
        $guessPortName = trim((string)$guessPortName);

        if ($guessPortName !== '') {
            $query = Db::name('crm_inquiry_port')->where('port_name', $guessPortName);
            if ($returnSourceId > 0) {
                $query->where('inquiry_id', $returnSourceId);
            }
            $exact = $query->field('id, port_name')->find();
            if (!empty($exact)) {
                return $exact;
            }
        }

        $likeQuery = Db::name('crm_inquiry_port')->where('port_name', 'like', '%返单%');
        if ($returnSourceId > 0) {
            $likeQuery->where('inquiry_id', $returnSourceId);
        }
        $likeRow = $likeQuery->order('id', 'asc')->field('id, port_name')->find();
        if (!empty($likeRow)) {
            return $likeRow;
        }

        return [];
    }

    /**
     * 将任意值转换为 float，空字符串/null 统一按 0 处理
     *
     * @param mixed $value
     * @return float
     */
    public static function toFloatAmount($value)
    {
        if ($value === null) {
            return 0.0;
        }

        if (is_string($value)) {
            $value = trim($value);
            if ($value === '') {
                return 0.0;
            }
        }

        return (float)$value;
    }

    /**
     * 统一金额字段归一化：所有金额字段转 float，空字符串/null 转 0
     *
     * @param array $params
     * @return array
     */
    public static function normalizeAmountFields(array $params)
    {
        $params['shipping_cost'] = self::toFloatAmount(isset($params['shipping_cost']) ? $params['shipping_cost'] : 0);
        $params['tax_amount'] = self::toFloatAmount(isset($params['tax_amount']) ? $params['tax_amount'] : 0);
        $params['debugging_cost'] = self::toFloatAmount(isset($params['debugging_cost']) ? $params['debugging_cost'] : 0);
        $params['sales_commission'] = self::toFloatAmount(isset($params['sales_commission']) ? $params['sales_commission'] : 0);

        $params['qty'] = self::normalizeAmountArray(isset($params['qty']) ? $params['qty'] : []);
        $params['unit_price'] = self::normalizeAmountArray(isset($params['unit_price']) ? $params['unit_price'] : []);
        $params['purchase_price'] = self::normalizeAmountArray(isset($params['purchase_price']) ? $params['purchase_price'] : []);

        return $params;
    }

    /**
     * 将数组中的数值全部转换为 float
     *
     * @param mixed $value
     * @return array
     */
    public static function normalizeAmountArray($value)
    {
        if (!is_array($value)) {
            if ($value === null || $value === '') {
                return [];
            }
            $value = [$value];
        }

        $result = [];
        foreach ($value as $item) {
            $result[] = self::toFloatAmount($item);
        }

        return $result;
    }

    /**
     * 统一利润重算（不信任前端传入的 profit）
     *
     * 【本项目唯一口径（与前端列头"销售价合计 / 进价合计"完全一致）】
     *  - unit_price    : 销售单价
     *  - qty           : 数量
     *  - total_price   : 销售价合计 = qty × unit_price
     *  - purchase_price: 进价合计（整行成本，非单价！！！）
     *  - sub_profit    : 行利润   = total_price - purchase_price
     *  - sales_total   : Σ total_price
     *  - purchase_total: Σ purchase_price            （★ 不再 × qty，历史 bug 根因）
     *  - profit        : sales_total - purchase_total - 运费 - 税费 - 调试费 - 佣金
     *  - profit_rate   : profit / sales_total × 100（sales_total<=0 时为 0）
     *
     * 返回：
     * - sales_total    销售总额
     * - purchase_total 进价总额
     * - extra_cost     附加费用（运费+税费+调试费+佣金）
     * - cost_total     成本总额（purchase_total + extra_cost）
     * - profit         利润
     * - profit_rate    利润率（百分比）
     *
     * @param array $params
     * @return array
     */
    public static function recalculateOrderProfit(array $params)
    {
        $params = self::normalizeAmountFields($params);

        $qtyList = isset($params['qty']) ? $params['qty'] : [];
        $unitPriceList = isset($params['unit_price']) ? $params['unit_price'] : [];
        $purchasePriceList = isset($params['purchase_price']) ? $params['purchase_price'] : [];

        $lineCount = max(count($qtyList), count($unitPriceList), count($purchasePriceList));

        $salesTotal = 0.0;
        $purchaseTotal = 0.0;

        for ($i = 0; $i < $lineCount; $i++) {
            $qty = isset($qtyList[$i]) ? self::toFloatAmount($qtyList[$i]) : 0.0;
            $unitPrice = isset($unitPriceList[$i]) ? self::toFloatAmount($unitPriceList[$i]) : 0.0;
            // purchase_price 语义：整行"进价合计"，绝不再 × qty
            $purchaseLineTotal = isset($purchasePriceList[$i]) ? self::toFloatAmount($purchasePriceList[$i]) : 0.0;

            $salesTotal += ($qty * $unitPrice);
            $purchaseTotal += $purchaseLineTotal;
        }

        $extraCost = $params['shipping_cost']
            + $params['tax_amount']
            + $params['debugging_cost']
            + $params['sales_commission'];

        $costTotal = $purchaseTotal + $extraCost;
        $profit = $salesTotal - $costTotal;
        $profitRate = $salesTotal > 0 ? (($profit / $salesTotal) * 100) : 0.0;

        return [
            'sales_total'    => round($salesTotal, 2),
            'purchase_total' => round($purchaseTotal, 2),
            'extra_cost'     => round($extraCost, 2),
            'cost_total'     => round($costTotal, 2),
            'profit'         => round($profit, 2),
            'profit_rate'    => round($profitRate, 2),
        ];
    }

    /**
     * 统一构造订单保存所需的"明细行 + 主表金额"数据包
     *
     * 所有订单保存入口（新增/编辑/详情保存/草稿自动保存）都应走此方法，
     * 禁止在 Controller 中各自重复一份利润公式。
     *
     * 口径完全等同于 recalculateOrderProfit（purchase_price = 进价合计，不再 × qty）。
     *
     * @param array $params   表单原始输入，支持的 key：
     *   - product_ids (array)  产品ID数组（对应 product_name[]）
     *   - manager_ids (array)  产品经理ID数组
     *   - spec_models (array)  规格型号数组
     *   - units       (array)  单位数组
     *   - qty         (array)  数量数组
     *   - unit_price  (array)  销售单价数组
     *   - purchase_price (array) 进价合计数组（★整行，非单价）
     *   - item_remarks (array) 行备注数组
     *   - shipping_cost / tax_amount / debugging_cost / sales_commission（标量）
     * @param array $prodMap    [pid => product_name] （产品名称快照）
     * @param array $supIdMap   [pid => category_id]
     * @param array $supNameMap [pid => category_name]
     * @param int   $orderId    写入 items.order_id；新增时可先传 0，主表 insert 后再回填
     * @return array {
     *   items                : array  待 insertAll 的明细行
     *   money                : float  主表 money = sales_total
     *   profit               : float  主表 profit
     *   margin_rate          : float  主表 margin_rate
     *   sales_total / purchase_total / extra_cost / cost_total,
     *   product_name_summary : string 主表 product_name 摘要（"首个产品 等"）
     * }
     */
    public static function buildOrderSaveData(
        array $params,
        array $prodMap = [],
        array $supIdMap = [],
        array $supNameMap = [],
        $orderId = 0
    ) {
        $getArray = function ($val) {
            if (is_array($val)) {
                return $val;
            }
            if ($val === null || $val === '') {
                return [];
            }
            return [$val];
        };

        $productIds = $getArray(isset($params['product_ids']) ? $params['product_ids'] : []);
        $managerIds = $getArray(isset($params['manager_ids']) ? $params['manager_ids'] : []);
        $specModels = $getArray(isset($params['spec_models']) ? $params['spec_models'] : []);
        $units      = $getArray(isset($params['units']) ? $params['units'] : []);
        $itemRemarks = $getArray(isset($params['item_remarks']) ? $params['item_remarks'] : []);

        $qtys           = self::normalizeAmountArray(isset($params['qty']) ? $params['qty'] : []);
        $unitPrices     = self::normalizeAmountArray(isset($params['unit_price']) ? $params['unit_price'] : []);
        $purchasePrices = self::normalizeAmountArray(isset($params['purchase_price']) ? $params['purchase_price'] : []);

        $shippingCost    = self::toFloatAmount(isset($params['shipping_cost']) ? $params['shipping_cost'] : 0);
        $taxAmount       = self::toFloatAmount(isset($params['tax_amount']) ? $params['tax_amount'] : 0);
        $debuggingCost   = self::toFloatAmount(isset($params['debugging_cost']) ? $params['debugging_cost'] : 0);
        $salesCommission = self::toFloatAmount(isset($params['sales_commission']) ? $params['sales_commission'] : 0);

        $salesTotal    = 0.0;
        $purchaseTotal = 0.0;
        $items         = [];
        $firstName     = '';
        $validCount    = 0;
        $orderId       = (int)$orderId;

        foreach ($productIds as $index => $pid) {
            $pid = (int)$pid;
            if ($pid <= 0) {
                // 跳过空行，保持与原 Controller 逻辑一致
                continue;
            }

            $pnameText    = isset($prodMap[$pid]) ? $prodMap[$pid] : '';
            $supplierId   = isset($supIdMap[$pid]) ? $supIdMap[$pid] : 0;
            $supplierName = isset($supNameMap[$pid]) ? $supNameMap[$pid] : '';

            $qty       = isset($qtys[$index]) ? self::toFloatAmount($qtys[$index]) : 0.0;
            $unitPrice = isset($unitPrices[$index]) ? self::toFloatAmount($unitPrices[$index]) : 0.0;
            // ★ 进价合计：整行成本，不再 × qty
            $purchaseLineTotal = isset($purchasePrices[$index]) ? self::toFloatAmount($purchasePrices[$index]) : 0.0;

            $lineTotal  = round($qty * $unitPrice, 2);
            $lineProfit = round($lineTotal - $purchaseLineTotal, 2);

            $salesTotal    += $lineTotal;
            $purchaseTotal += $purchaseLineTotal;

            $managerId = 0;
            if (isset($managerIds[$index]) && $managerIds[$index] !== '' && $managerIds[$index] !== null) {
                $managerId = (int)$managerIds[$index];
            }

            $items[] = [
                'order_id'       => $orderId,
                'line_no'        => $index + 1,
                'product_id'     => (string)$pid,
                'product_name'   => $pnameText,
                'supplier_id'    => (string)$supplierId,
                'supplier_name'  => $supplierName,
                'spec_model'     => isset($specModels[$index]) ? $specModels[$index] : '',
                'unit'           => isset($units[$index]) ? $units[$index] : '',
                'qty'            => (int)$qty,
                'unit_price'     => number_format($unitPrice, 2, '.', ''),
                'total_price'    => number_format($lineTotal, 2, '.', ''),
                'purchase_price' => number_format($purchaseLineTotal, 2, '.', ''),
                'sub_profit'     => number_format($lineProfit, 2, '.', ''),
                'remark'         => isset($itemRemarks[$index]) ? $itemRemarks[$index] : '',
                'manager_id'     => $managerId,
            ];

            if ($firstName === '' && $pnameText !== '') {
                $firstName = $pnameText;
            }
            $validCount++;
        }

        $extraCost  = $shippingCost + $taxAmount + $debuggingCost + $salesCommission;
        $costTotal  = $purchaseTotal + $extraCost;
        $profit     = $salesTotal - $costTotal;
        $profitRate = $salesTotal > 0 ? ($profit / $salesTotal * 100) : 0.0;

        $productNameSummary = '';
        if ($firstName !== '') {
            $productNameSummary = $firstName . ($validCount > 1 ? ' 等' : '');
        }

        return [
            'items'                => $items,
            'money'                => round($salesTotal, 2),
            'profit'               => round($profit, 2),
            'margin_rate'          => round($profitRate, 2),
            'sales_total'          => round($salesTotal, 2),
            'purchase_total'       => round($purchaseTotal, 2),
            'extra_cost'           => round($extraCost, 2),
            'cost_total'           => round($costTotal, 2),
            'product_name_summary' => $productNameSummary,
        ];
    }

    /**
     * 利润达到阈值时，是否必须上传两类凭证
     *
     * @param mixed $profit
     * @return bool
     */
    public static function isVoucherRequiredByProfit($profit)
    {
        return floatval($profit) >= 2000;
    }

    /**
     * 统一校验凭证是否满足规则（兼容新增/编辑/旧图/删除旧图/新上传）
     *
     * @param array $params 请求参数（新提交数据）
     * @param array $existing 旧数据（编辑时传入，建议包含 wechat_receipt_image / inquiry_assign_image）
     * @param bool $mustReturn 是否为必须返单的老客户订单（返单时询盘来源凭证不再强制要求）
     * @return array ['ok' => bool, 'message' => string]
     */
    public static function validateVoucherRequirement(array $params, array $existing = [], $mustReturn = false)
    {
        $calc = self::recalculateOrderProfit($params);
        $profit = isset($calc['profit']) ? $calc['profit'] : 0.0;

        if (!self::isVoucherRequiredByProfit($profit)) {
            return ['ok' => true, 'message' => ''];
        }

        $wechatImages = self::resolveFinalVoucherImages(
            'wechat_receipt_image',
            'clear_wechat_receipt_image',
            $params,
            $existing
        );

        // 返单老客户：不再要求询盘来源凭证，只校验微信沟通及付款凭证
        if ($mustReturn) {
            if (count($wechatImages) < 1) {
                return ['ok' => false, 'message' => '利润达到2000元及以上，必须上传微信沟通及付款凭证'];
            }
            return ['ok' => true, 'message' => ''];
        }

        $inquiryImages = self::resolveFinalVoucherImages(
            'inquiry_assign_image',
            'clear_inquiry_assign_image',
            $params,
            $existing
        );

        if (count($wechatImages) < 1 && count($inquiryImages) < 1) {
            return ['ok' => false, 'message' => '利润达到2000元及以上，必须上传微信沟通及付款凭证和询盘来源凭证'];
        }

        if (count($wechatImages) < 1) {
            return ['ok' => false, 'message' => '利润达到2000元及以上，必须上传微信沟通及付款凭证'];
        }

        if (count($inquiryImages) < 1) {
            return ['ok' => false, 'message' => '利润达到2000元及以上，必须上传询盘来源凭证'];
        }

        return ['ok' => true, 'message' => ''];
    }

    /**
     * 解析编辑后最终生效的凭证图片列表
     *
     * @param string $field 图片字段名
     * @param string $clearField 清空标记字段名
     * @param array $params 新提交参数
     * @param array $existing 旧数据
     * @return array
     */
    public static function resolveFinalVoucherImages($field, $clearField, array $params, array $existing = [])
    {
        $clearFlag = isset($params[$clearField]) ? (int)$params[$clearField] : 0;
        if ($clearFlag === 1) {
            return [];
        }

        $existingImages = self::normalizeVoucherImages(isset($existing[$field]) ? $existing[$field] : []);
        $hasSubmittedField = array_key_exists($field, $params);

        if (!$hasSubmittedField) {
            return $existingImages;
        }

        $submittedImages = self::normalizeVoucherImages($params[$field]);
        // 字段已提交时，以本次提交结果为准（允许为空数组，表示删除后未保留任何图片）
        return $submittedImages;
    }

    /**
     * 历史单图兼容 + 新多图方案：统一解析为图片 URL 数组
     * 兼容输入：
     * - 数组
     * - JSON 数组字符串
     * - 单字符串
     *
     * @param mixed $raw
     * @return array
     */
    public static function parseImageList($raw)
    {
        $list = [];
        if (is_array($raw)) {
            $list = $raw;
        } elseif (is_string($raw)) {
            $raw = trim($raw);
            if ($raw !== '') {
                if (preg_match('/^\s*\[.*\]\s*$/', $raw)) {
                    $decoded = json_decode($raw, true);
                    if (is_array($decoded)) {
                        $list = $decoded;
                    } else {
                        $list = [$raw];
                    }
                } elseif (strpos($raw, ',') !== false) {
                    // 兼容历史逗号分隔存储
                    $list = explode(',', $raw);
                } else {
                    $list = [$raw];
                }
            }
        }

        $result = [];
        foreach ($list as $item) {
            $url = '';
            if (is_string($item)) {
                $url = trim($item);
            } elseif (is_array($item)) {
                // 兼容对象结构数组（如 {url/full/path/src}）
                if (!empty($item['full'])) {
                    $url = trim((string)$item['full']);
                } elseif (!empty($item['url'])) {
                    $url = trim((string)$item['url']);
                } elseif (!empty($item['path'])) {
                    $url = trim((string)$item['path']);
                } elseif (!empty($item['src'])) {
                    $url = trim((string)$item['src']);
                }
            } elseif (is_object($item)) {
                $item = (array)$item;
                if (!empty($item['full'])) {
                    $url = trim((string)$item['full']);
                } elseif (!empty($item['url'])) {
                    $url = trim((string)$item['url']);
                } elseif (!empty($item['path'])) {
                    $url = trim((string)$item['path']);
                } elseif (!empty($item['src'])) {
                    $url = trim((string)$item['src']);
                }
            }

            if ($url !== '') {
                $result[] = $url;
            }
        }

        // 去空、去重、重建索引
        $result = array_values(array_unique($result));
        return $result;
    }

    /**
     * 统一凭证图片数据：基于 parseImageList，最多 10 张，返回标准 URL 数组
     *
     * @param mixed $raw
     * @param int $max
     * @return array
     */
    public static function normalizeVoucherImages($raw, $max = 10)
    {
        $max = min(10, (int)$max);
        if ($max <= 0) {
            return [];
        }

        $images = self::parseImageList($raw);
        if (count($images) > $max) {
            $images = array_slice($images, 0, $max);
        }

        return $images;
    }

    /**
     * 构建前端预览组件所需图片项结构
     *
     * @param mixed $raw
     * @return array
     */
    public static function buildPreviewImageItems($raw)
    {
        $images = self::normalizeVoucherImages($raw);
        $items = [];

        foreach ($images as $url) {
            $items[] = [
                'full' => $url,
                'thumb' => $url,
            ];
        }

        return $items;
    }

    /**
     * 根据图片数量计算图片列宽
     *
     * 规则：
     * - 0~1 张：180
     * - 2 张：260
     * - 3 张：340
     * - 4 张：420
     * - >=5 张：按每张 +80 递增，最大 780
     *
     * @param mixed $images
     * @return int
     */
    public static function calcImageColumnWidth($images)
    {
        $count = count(self::parseImageList($images));

        if ($count <= 1) {
            return 180;
        }

        $width = 180 + (($count - 1) * 80);
        return (int)min($width, 780);
    }

    /**
     * 处理询盘图片并返回可入库的 JSON 字符串
     *
     * @param mixed $raw
     * @return string
     */
    public static function handleInquiryImages($raw)
    {
        // 先统一解析，兼容历史单图/对象数组等输入
        $parsed = self::parseImageList($raw);
        // 再执行凭证图片规范化（去重、去空、数量限制等）
        $images = self::normalizeVoucherImages($parsed);

        $json = json_encode($images, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        return $json === false ? '[]' : $json;
    }

    // ===== 防重复提交：BEGIN =====
    /**
     * 获取订单正式提交幂等锁（Redis 可用时启用；异常时降级放行）
     *
     * @param array $data
     * @param int $ttl
     * @return array
     */
    public static function acquireSubmitLock(array $data, $ttl = 8)
    {
        $ttl = (int)$ttl;
        if ($ttl <= 0) {
            $ttl = 8;
        }

        $fingerprintData = self::buildSubmitFingerprintData($data);
        $fingerprintJson = json_encode($fingerprintData, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $lockKey = 'crm:order:submit_lock:' . md5($fingerprintJson === false ? serialize($fingerprintData) : $fingerprintJson);

        $redis = self::getRedisHandlerFromCache();
        if (!$redis) {
            self::logSubmitLock('[OrderSubmitLock] redis handler unavailable, skip lock. key=' . $lockKey);
            return [
                'ok' => true,
                'key' => $lockKey,
                'msg' => '',
            ];
        }

        try {
            $acquired = false;

            if ($redis instanceof \Redis) {
                $setResult = $redis->set($lockKey, '1', ['nx', 'ex' => $ttl]);
                if ($setResult === true || $setResult === 'OK') {
                    $acquired = true;
                } else {
                    $acquired = (bool)$redis->setnx($lockKey, '1');
                    if ($acquired) {
                        $redis->expire($lockKey, $ttl);
                    }
                }
            } else {
                if (method_exists($redis, 'set')) {
                    $setResult = $redis->set($lockKey, '1', 'EX', $ttl, 'NX');
                    if ($setResult === true || $setResult === 'OK') {
                        $acquired = true;
                    }
                }
                if (!$acquired && method_exists($redis, 'setnx')) {
                    $acquired = (bool)$redis->setnx($lockKey, '1');
                    if ($acquired && method_exists($redis, 'expire')) {
                        $redis->expire($lockKey, $ttl);
                    }
                }
            }

            if (!$acquired) {
                return [
                    'ok' => false,
                    'key' => $lockKey,
                    'msg' => '请勿重复提交，订单正在处理中',
                ];
            }

            return [
                'ok' => true,
                'key' => $lockKey,
                'msg' => '',
            ];
        } catch (\Throwable $e) {
            self::logSubmitLock('[OrderSubmitLock] acquire failed, skip lock. key=' . $lockKey . ' err=' . $e->getMessage());
            return [
                'ok' => true,
                'key' => $lockKey,
                'msg' => '',
            ];
        }
    }

    /**
     * 释放订单正式提交幂等锁
     *
     * @param string $lockKey
     * @return void
     */
    public static function releaseSubmitLock($lockKey)
    {
        $lockKey = trim((string)$lockKey);
        if ($lockKey === '') {
            return;
        }

        $redis = self::getRedisHandlerFromCache();
        if (!$redis) {
            return;
        }

        try {
            if (method_exists($redis, 'del')) {
                $redis->del($lockKey);
            } elseif (method_exists($redis, 'delete')) {
                $redis->delete($lockKey);
            }
        } catch (\Throwable $e) {
            self::logSubmitLock('[OrderSubmitLock] release failed. key=' . $lockKey . ' err=' . $e->getMessage());
        }
    }

    /**
     * 构建正式提交指纹数据（稳定顺序，保证同次提交 key 一致）
     *
     * @param array $data
     * @return array
     */
    private static function buildSubmitFingerprintData(array $data)
    {
        $aid = (int)($data['aid'] ?? session('aid') ?? 0);
        $username = trim((string)($data['username'] ?? session('username') ?? ''));
        $contact = self::normalizeContact((string)($data['contact'] ?? ''));
        $money = self::normalizeFingerprintNumber($data['money'] ?? 0);
        $profit = self::normalizeFingerprintNumber($data['profit'] ?? 0);
        $source = trim((string)($data['source'] ?? ''));
        $sourcePort = trim((string)($data['source_port'] ?? ''));
        $orderTime = trim((string)($data['order_time'] ?? ''));
        $orderId = (int)($data['order_id'] ?? 0);
        $products = self::buildSubmitFingerprintProducts($data);

        return [
            'aid' => $aid,
            'username' => $username,
            'contact' => $contact,
            'money' => $money,
            'profit' => $profit,
            'source' => $source,
            'source_port' => $sourcePort,
            'order_time' => $orderTime,
            'order_id' => $orderId,
            'products' => $products,
        ];
    }

    /**
     * 归一化产品明细指纹（排序后返回）
     *
     * @param array $data
     * @return array
     */
    private static function buildSubmitFingerprintProducts(array $data)
    {
        $details = [];

        if (!empty($data['product_details']) && is_array($data['product_details'])) {
            foreach ($data['product_details'] as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $details[] = [
                    'product_name' => trim((string)($row['product_name'] ?? '')),
                    'qty' => self::normalizeFingerprintNumber($row['qty'] ?? 0),
                    'unit_price' => self::normalizeFingerprintNumber($row['unit_price'] ?? 0),
                    'purchase_price' => self::normalizeFingerprintNumber($row['purchase_price'] ?? 0),
                    'product_manager' => trim((string)($row['product_manager'] ?? '')),
                ];
            }
        } else {
            $productNames = is_array($data['product_name'] ?? null) ? $data['product_name'] : [];
            $qtyList = is_array($data['qty'] ?? null) ? $data['qty'] : [];
            $unitPriceList = is_array($data['unit_price'] ?? null) ? $data['unit_price'] : [];
            $purchasePriceList = is_array($data['purchase_price'] ?? null) ? $data['purchase_price'] : [];
            $managerList = is_array($data['product_manager'] ?? null) ? $data['product_manager'] : [];

            $maxCount = max(
                count($productNames),
                count($qtyList),
                count($unitPriceList),
                count($purchasePriceList),
                count($managerList)
            );

            for ($i = 0; $i < $maxCount; $i++) {
                $item = [
                    'product_name' => trim((string)($productNames[$i] ?? '')),
                    'qty' => self::normalizeFingerprintNumber($qtyList[$i] ?? 0),
                    'unit_price' => self::normalizeFingerprintNumber($unitPriceList[$i] ?? 0),
                    'purchase_price' => self::normalizeFingerprintNumber($purchasePriceList[$i] ?? 0),
                    'product_manager' => trim((string)($managerList[$i] ?? '')),
                ];

                if (
                    $item['product_name'] === ''
                    && $item['qty'] === '0'
                    && $item['unit_price'] === '0'
                    && $item['purchase_price'] === '0'
                    && $item['product_manager'] === ''
                ) {
                    continue;
                }

                $details[] = $item;
            }
        }

        usort($details, function ($a, $b) {
            $aKey = implode('|', [
                $a['product_name'] ?? '',
                $a['qty'] ?? '',
                $a['unit_price'] ?? '',
                $a['purchase_price'] ?? '',
                $a['product_manager'] ?? '',
            ]);
            $bKey = implode('|', [
                $b['product_name'] ?? '',
                $b['qty'] ?? '',
                $b['unit_price'] ?? '',
                $b['purchase_price'] ?? '',
                $b['product_manager'] ?? '',
            ]);
            return strcmp($aKey, $bKey);
        });

        return $details;
    }

    /**
     * 将数值归一化为稳定字符串
     *
     * @param mixed $value
     * @return string
     */
    private static function normalizeFingerprintNumber($value)
    {
        $num = self::toFloatAmount($value);
        $formatted = rtrim(rtrim(sprintf('%.6F', $num), '0'), '.');
        return $formatted === '' ? '0' : $formatted;
    }

    /**
     * 从缓存组件中提取 Redis handler（兼容 TP5.1）
     *
     * @return mixed|null
     */
    private static function getRedisHandlerFromCache()
    {
        try {
            if (class_exists('\\think\\facade\\Cache')) {
                $cacheStore = \think\facade\Cache::store('redis');
                if (is_object($cacheStore) && method_exists($cacheStore, 'handler')) {
                    $handler = $cacheStore->handler();
                    if (is_object($handler)) {
                        return $handler;
                    }
                }
            }
        } catch (\Throwable $e) {
            self::logSubmitLock('[OrderSubmitLock] get redis store failed: ' . $e->getMessage());
        }

        return null;
    }

    /**
     * 记录提交幂等锁日志（日志失败不影响主流程）
     *
     * @param string $message
     * @return void
     */
    private static function logSubmitLock($message)
    {
        $message = (string)$message;
        try {
            if (class_exists('\\think\\facade\\Log')) {
                \think\facade\Log::write($message, 'error');
                return;
            }
        } catch (\Throwable $e) {
        }

        try {
            Log::record($message, 'error');
        } catch (\Throwable $e) {
        }
    }
    // ===== 防重复提交：END =====

    /**
     * 是否允许审核拒绝订单，以及查看历史拒绝理由。
     * 仅超级管理员（admin_id=1）、产品总监（group_id=13）、财务专员（group_id=15）。
     * 不包含“用户名是 admin”等更宽的订单管理权限。
     *
     * @return bool
     */
    public static function canAuditRejectOrder()
    {
        $aid = (int)session('aid');
        $gid = (int)session('gid');
        return $aid === 1 || in_array($gid, [13, 15], true);
    }

    /**
     * 拒绝一条待审核订单。
     * 同一事务内更新订单、写入永久历史、写入拒绝通知。任一写入失败则整单回滚。
     *
     * @param int $orderId
     * @param string $reason 原始理由，方法内去掉首尾空格
     * @param int $auditUserId 当前登录审核人，来自服务端会话
     * @return array{success:bool,msg:string,target_user?:string}
     */
    public static function rejectPendingOrder($orderId, $reason, $auditUserId)
    {
        $orderId = (int)$orderId;
        $auditUserId = (int)$auditUserId;
        $reason = trim((string)$reason);

        if ($orderId <= 0 || $auditUserId <= 0) {
            return ['success' => false, 'msg' => '参数错误'];
        }
        if ($reason === '') {
            return ['success' => false, 'msg' => '拒绝理由不能为空'];
        }
        if (self::utf8Length($reason) > 500) {
            return ['success' => false, 'msg' => '拒绝理由不能超过500个字符'];
        }

        try {
            $order = Db::name('crm_client_order')->where('id', $orderId)->find();
        } catch (\Throwable $e) {
            self::logOrderReject('查询待拒绝订单失败：' . $e->getMessage());
            return ['success' => false, 'msg' => '审核拒绝失败，请稍后重试'];
        }
        if (empty($order)) {
            return ['success' => false, 'msg' => '订单不存在'];
        }
        if ((int)($order['check_status'] ?? 0) !== 1) {
            return ['success' => false, 'msg' => '只有待审核订单才能拒绝'];
        }

        $now = date('Y-m-d H:i:s');
        Db::startTrans();
        try {
            $affected = Db::name('crm_client_order')
                ->where('id', $orderId)
                ->where('check_status', 1)
                ->update([
                    'check_status'  => 3,
                    'audit_remark'  => $reason,
                    'audit_user_id' => $auditUserId,
                    'audit_time'    => $now,
                ]);
            if ((int)$affected !== 1) {
                try {
                    Db::rollback();
                } catch (\Throwable $rollbackError) {
                }
                return ['success' => false, 'msg' => '订单状态已变化，拒绝未生效'];
            }

            $historyId = Db::name('crm_order_reject_history')->insertGetId([
                'order_id'       => $orderId,
                'reject_reason'  => $reason,
                'audit_user_id'  => $auditUserId,
                'audit_time'     => $now,
            ]);
            if (empty($historyId)) {
                throw new \RuntimeException('审核拒绝历史写入失败');
            }

            $noticeId = Db::name('crm_order_notifications')->insertGetId([
                'order_id'     => $orderId,
                'order_no'     => $order['order_no'],
                'contact'      => $order['contact'],
                'type'         => 2,
                'message'      => '订单编号为' . $order['order_no'] . '的订单审核被拒绝',
                'target_user'  => $order['pr_user'],
                'is_read'      => 0,
                'create_time'  => $now,
            ]);
            if (empty($noticeId)) {
                throw new \RuntimeException('审核拒绝通知写入失败');
            }

            Db::commit();
        } catch (\Throwable $e) {
            try {
                Db::rollback();
            } catch (\Throwable $rollbackError) {
            }
            self::logOrderReject('审核拒绝事务失败：' . $e->getMessage());
            return ['success' => false, 'msg' => '审核拒绝失败，请稍后重试'];
        }

        return [
            'success'     => true,
            'msg'         => '已拒绝该订单',
            'target_user' => (string)($order['pr_user'] ?? ''),
        ];
    }

    /**
     * 最近 10 条不重复拒绝理由。
     * 先按去掉首尾空格后的完整文本分组，再按该理由最近使用时间倒序取 10 条。
     * 不读取 crm_client_order.audit_remark，也不把全表载入 PHP。
     *
     * @return array{success:bool,msg:string,data:array}
     */
    public static function listRecentRejectReasons()
    {
        $sql = "SELECT t.reject_reason
FROM (
    SELECT MAX(TRIM(`reject_reason`)) AS reject_reason,
           MAX(`audit_time`) AS last_audit_time,
           MAX(`id`) AS last_id
    FROM `crm_order_reject_history`
    WHERE `reject_reason` IS NOT NULL
      AND CHAR_LENGTH(TRIM(`reject_reason`)) > 0
    GROUP BY BINARY TRIM(`reject_reason`)
) t
ORDER BY t.last_audit_time DESC, t.last_id DESC
LIMIT 10";

        try {
            $rows = Db::query($sql);
        } catch (\Throwable $e) {
            self::logOrderReject('查询历史拒绝理由失败：' . $e->getMessage());
            return ['success' => false, 'msg' => '历史理由暂时无法加载', 'data' => []];
        }

        if (!is_array($rows)) {
            return ['success' => false, 'msg' => '历史理由暂时无法加载', 'data' => []];
        }

        $list = [];
        foreach ($rows as $row) {
            $text = trim((string)($row['reject_reason'] ?? ''));
            if ($text === '' || self::utf8Length($text) > 500) {
                continue;
            }
            if (in_array($text, $list, true)) {
                continue;
            }
            $list[] = $text;
            if (count($list) >= 10) {
                break;
            }
        }

        return ['success' => true, 'msg' => '获取成功', 'data' => $list];
    }

    /**
     * 当前登录用户本人的审核失败待办摘要。只读，不写通知、不改已读。
     * 身份只使用调用方从 Session 取出的用户名，不读取请求里的用户参数。
     * 管理员也不统计全公司订单，只统计 pr_user 或 at_user 与本人完全相等的订单。
     *
     * active_reject_events 与 event_token 一样覆盖全部待办，不受 orders 的 20 条上限影响。
     *
     * @param string $username
     * @return array
     */
    public static function getFailedOrderReminderSummary($username)
    {
        $username = (string)$username;
        $now = time();
        $emptyToken = self::buildFailedEventIdentity([]);
        $result = [
            'total' => 0,
            'overdue_total' => 0,
            'event_token' => $emptyToken['event_token'],
            'latest_reject_event' => $emptyToken['latest_reject_event'],
            'active_reject_events' => $emptyToken['active_reject_events'],
            'server_time' => $now,
            'orders' => [],
        ];
        if ($username === '') {
            return $result;
        }

        $rows = Db::name('crm_client_order')
            ->where('check_status', 3)
            ->where(function ($query) use ($username) {
                $query->where('pr_user', '=', $username)
                    ->whereOr('at_user', '=', $username);
            })
            ->field('id,order_no,audit_time,audit_remark')
            ->select();
        $rows = self::toArrayRows($rows);
        $snapshots = self::buildSortedRejectSnapshots($rows, $now);
        $identity = self::buildFailedEventIdentity($snapshots);

        $orderNoMap = [];
        foreach ($rows as $row) {
            $orderNoMap[(int)$row['id']] = (string)($row['order_no'] ?? '');
        }

        $overdueTotal = 0;
        foreach ($snapshots as $snapshot) {
            if (!empty($snapshot['is_overdue'])) {
                $overdueTotal++;
            }
        }

        $orders = [];
        foreach (array_slice($snapshots, 0, self::FAILED_ORDER_REMINDER_LIMIT) as $snapshot) {
            $orderId = (int)$snapshot['order_id'];
            $orders[] = [
                'order_id' => $orderId,
                'order_no' => isset($orderNoMap[$orderId]) ? $orderNoMap[$orderId] : '',
                'reject_time' => (int)$snapshot['reject_ts'],
                'reject_time_text' => $snapshot['reject_time_text'] !== '' ? $snapshot['reject_time_text'] : '未知',
                'reject_reason' => (string)$snapshot['reject_reason'],
                'is_overdue' => !empty($snapshot['is_overdue']),
                'reject_time_known' => !empty($snapshot['reject_known']),
                'event_key' => (string)$snapshot['event_key'],
            ];
        }

        $result['total'] = count($snapshots);
        $result['overdue_total'] = $overdueTotal;
        $result['event_token'] = $identity['event_token'];
        $result['latest_reject_event'] = $identity['latest_reject_event'];
        $result['active_reject_events'] = $identity['active_reject_events'];
        $result['orders'] = $orders;
        return $result;
    }

    /**
     * 是否允许查看这张订单的审核失败原因。
     * 全量订单管理员保持可查看；其他人仅限负责人或创建人与当前用户名完全一致。
     *
     * @param array $order 至少包含 pr_user、at_user
     * @param string $username
     * @return bool
     */
    public static function canViewFailedOrderAudit(array $order, $username)
    {
        if (self::canManageAllOrders()) {
            return true;
        }
        $username = (string)$username;
        if ($username === '') {
            return false;
        }
        if ((string)($order['pr_user'] ?? '') === $username) {
            return true;
        }
        return (string)($order['at_user'] ?? '') === $username;
    }

    /**
     * 按订单批量取最近一次拒绝历史。最近一次以该订单历史 id 最大值判定。
     * 查询失败时返回空映射，调用方改用订单上的审核时间，不丢弃订单。
     *
     * @param array $orderIds
     * @return array<int,array>
     */
    public static function loadLatestRejectHistoryMap(array $orderIds)
    {
        $ids = [];
        foreach ($orderIds as $orderId) {
            $orderId = (int)$orderId;
            if ($orderId > 0) {
                $ids[$orderId] = $orderId;
            }
        }
        if (!$ids) {
            return [];
        }

        $map = [];
        try {
            foreach (array_chunk(array_values($ids), 500) as $chunk) {
                $maxRows = Db::name('crm_order_reject_history')
                    ->whereIn('order_id', $chunk)
                    ->field('order_id, MAX(`id`) AS max_id')
                    ->group('order_id')
                    ->select();
                $maxIds = [];
                foreach (self::toArrayRows($maxRows) as $maxRow) {
                    $maxId = (int)($maxRow['max_id'] ?? 0);
                    if ($maxId > 0) {
                        $maxIds[] = $maxId;
                    }
                }
                if (!$maxIds) {
                    continue;
                }
                $histories = Db::name('crm_order_reject_history')
                    ->whereIn('id', $maxIds)
                    ->field('id,order_id,reject_reason,audit_time')
                    ->select();
                foreach (self::toArrayRows($histories) as $history) {
                    $orderId = (int)($history['order_id'] ?? 0);
                    if ($orderId > 0) {
                        $map[$orderId] = $history;
                    }
                }
            }
        } catch (\Throwable $e) {
            self::logOrderReject('查询最近拒绝历史失败：' . $e->getMessage());
            return [];
        }
        return $map;
    }

    /**
     * 为审核失败列表生成排序后的拒绝快照。排序在分页之前完成。
     *
     * @param array $orders 每行至少含 id，以及可选 audit_time、audit_remark
     * @param int|null $now
     * @return array
     */
    public static function buildSortedRejectSnapshots(array $orders, $now = null)
    {
        $now = $now === null ? time() : (int)$now;
        $orders = self::toArrayRows($orders);
        $historyMap = self::loadLatestRejectHistoryMap(array_column($orders, 'id'));
        $snapshots = [];
        foreach ($orders as $order) {
            $orderId = (int)($order['id'] ?? 0);
            $history = isset($historyMap[$orderId]) ? $historyMap[$orderId] : null;
            $snapshots[] = self::buildRejectSnapshot($order, $history, $now);
        }
        return self::sortRejectSnapshots($snapshots);
    }

    /**
     * 单笔订单的最近拒绝快照。
     * 有历史记录时以最大历史 id 为准；没有历史时才回退到订单 audit_time / audit_remark。
     *
     * @param array $order
     * @param array|null $history
     * @param int $now
     * @return array
     */
    public static function buildRejectSnapshot(array $order, $history, $now)
    {
        $orderId = (int)($order['id'] ?? ($order['order_id'] ?? 0));
        $historyId = 0;
        $reason = '';
        $timestamp = null;
        $useHistory = is_array($history) && (int)($history['id'] ?? 0) > 0;

        if ($useHistory) {
            $historyId = (int)$history['id'];
            $reason = (string)($history['reject_reason'] ?? '');
            $timestamp = self::parseRejectTimestamp($history['audit_time'] ?? null);
        } else {
            $timestamp = self::parseRejectTimestamp($order['audit_time'] ?? null);
            if ($timestamp !== null) {
                $reason = (string)($order['audit_remark'] ?? '');
            }
        }

        $known = $timestamp !== null;
        if ($historyId > 0) {
            $eventKey = 'h' . $historyId;
        } elseif ($known) {
            $eventKey = 't' . $timestamp;
        } else {
            $eventKey = 'u';
        }

        return [
            'order_id' => $orderId,
            'history_id' => $historyId,
            'reject_ts' => $known ? (int)$timestamp : 0,
            'reject_known' => $known,
            'is_overdue' => $known && (((int)$now - (int)$timestamp) >= self::FAILED_ORDER_OVERDUE_SECONDS),
            'reject_reason' => $reason,
            'event_key' => $eventKey,
            'reject_time_text' => $known ? date('Y-m-d H:i:s', (int)$timestamp) : '',
        ];
    }

    /**
     * 超时订单在前，其余已知时间在中，未知时间在后。同级按拒绝时间从早到晚，再按订单 id。
     *
     * @param array $snapshots
     * @return array
     */
    public static function sortRejectSnapshots(array $snapshots)
    {
        usort($snapshots, function ($left, $right) {
            $rank = function ($row) {
                if (empty($row['reject_known'])) {
                    return 2;
                }
                return !empty($row['is_overdue']) ? 0 : 1;
            };
            $leftRank = $rank($left);
            $rightRank = $rank($right);
            if ($leftRank !== $rightRank) {
                return $leftRank <=> $rightRank;
            }
            if ($leftRank !== 2 && (int)$left['reject_ts'] !== (int)$right['reject_ts']) {
                return (int)$left['reject_ts'] <=> (int)$right['reject_ts'];
            }
            return (int)$left['order_id'] <=> (int)$right['order_id'];
        });
        return $snapshots;
    }

    /**
     * 事件标识覆盖当前用户全部待处理订单，不受摘要 20 条和分页影响。
     * event_token：全部「订单 id + 事件键」排序后的 md5。含义不变。
     * latest_reject_event：当前集合中最大的拒绝历史 id；没有历史时用回退时间；都没有则为 none。含义不变。
     *
     * active_reject_events：当前用户全部待办的轻量事件快照，供下一阶段对比前后两次结果。
     * 结构固定为 JSON 对象，不是数组。键是订单 ID 的十进制字符串，值是该订单 event_key 字符串。
     * event_key 与单笔快照相同，只有三种：h{拒绝历史id}、t{可靠审核时间的 Unix 秒}、u（没有可靠时间）。
     * 键按订单 ID 数值升序。不包含金额、联系方式、客户名、拒绝原因，也不包含是否超时。
     * 不使用当前请求时间或随机数。空集合也保持对象 {}，避免 PHP 把空数组编码成 []。
     * 只由本次已经算好的快照生成，不再查询订单或审核历史。
     *
     * 前端对比规则：新增键或同一键的值变化，才是新的审核失败事件；
     * 只减少键表示订单退出待办；快照相同则没有新拒绝。是否超时不进入本快照。
     *
     * @param array $snapshots
     * @return array{event_token:string,latest_reject_event:string,active_reject_events:\stdClass}
     */
    public static function buildFailedEventIdentity(array $snapshots)
    {
        $parts = [];
        $activeRejectEvents = [];
        $latestHistoryId = 0;
        $fallback = null;
        foreach ($snapshots as $snapshot) {
            $orderId = (int)($snapshot['order_id'] ?? 0);
            $eventKey = (string)($snapshot['event_key'] ?? 'u');
            $parts[] = $orderId . ':' . $eventKey;
            if ($orderId > 0) {
                $activeRejectEvents[$orderId] = $eventKey;
            }
            $historyId = (int)($snapshot['history_id'] ?? 0);
            if ($historyId > $latestHistoryId) {
                $latestHistoryId = $historyId;
            }
            if ($historyId <= 0 && !empty($snapshot['reject_known'])) {
                $timestamp = (int)$snapshot['reject_ts'];
                if ($fallback === null
                    || $timestamp > (int)$fallback['ts']
                    || ($timestamp === (int)$fallback['ts'] && $orderId > (int)$fallback['order_id'])
                ) {
                    $fallback = ['order_id' => $orderId, 'ts' => $timestamp];
                }
            }
        }
        ksort($activeRejectEvents, SORT_NUMERIC);
        sort($parts, SORT_STRING);
        if ($latestHistoryId > 0) {
            $latestRejectEvent = 'h:' . $latestHistoryId;
        } elseif ($fallback !== null) {
            $latestRejectEvent = 'o:' . $fallback['order_id'] . ':t:' . $fallback['ts'];
        } else {
            $latestRejectEvent = 'none';
        }
        return [
            'event_token' => md5('failed-v1|' . implode('|', $parts)),
            'latest_reject_event' => $latestRejectEvent,
            'active_reject_events' => (object)$activeRejectEvents,
        ];
    }

    /**
     * 拒绝时间只接受可解析的 DATETIME 或秒级 Unix 时间戳。无法解析时返回 null。
     *
     * @param mixed $value
     * @return int|null
     */
    public static function parseRejectTimestamp($value)
    {
        if ($value === null || $value === '') {
            return null;
        }
        if (is_int($value) || (is_string($value) && ctype_digit($value))) {
            $timestamp = (int)$value;
            if ($timestamp >= 946684800 && $timestamp <= 4102444800) {
                return $timestamp;
            }
            return null;
        }
        $text = trim((string)$value);
        if ($text === '' || $text === '0000-00-00 00:00:00' || $text === '0000-00-00') {
            return null;
        }
        $timestamp = strtotime($text);
        if ($timestamp === false || $timestamp <= 0) {
            return null;
        }
        return (int)$timestamp;
    }

    /**
     * @param mixed $rows
     * @return array
     */
    private static function toArrayRows($rows)
    {
        if ($rows instanceof \think\Collection) {
            return $rows->toArray();
        }
        return is_array($rows) ? $rows : [];
    }

    /**
     * UTF-8 字符数。mbstring 不可用时按 Unicode 码点计数，不按字节截断。
     *
     * @param string $text
     * @return int
     */
    private static function utf8Length($text)
    {
        $text = (string)$text;
        if (function_exists('mb_strlen')) {
            return (int)mb_strlen($text, 'UTF-8');
        }
        if (preg_match_all('/./u', $text, $matches)) {
            return count($matches[0]);
        }
        return 0;
    }

    /**
     * @param string $message
     * @return void
     */
    private static function logOrderReject($message)
    {
        try {
            \think\facade\Log::write('[OrderReject] ' . (string)$message, 'error');
        } catch (\Throwable $e) {
        }
    }
}
