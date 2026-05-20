<?php

namespace app\admin\model;

use think\Db;
use think\Model;

class Liberum extends Model
{
    /**
     * 公海列表（支持筛选）：
     * - 固定口径：status=2, issuccess=-1
     * - 列表展示字段按内贸版补齐
     */
    public function getLiberumPageList($page, $limit, $keyword = [])
    {
        $page = max(1, (int)$page);
        $limit = max(1, (int)$limit);
        $keyword = is_array($keyword) ? $keyword : [];

        $query = Db::table('crm_leads')
            ->alias('l')
            ->where(['l.status' => 2, 'l.issuccess' => -1]);

        if (!empty($keyword['at_time'])) {
            $atTime = trim((string)$keyword['at_time']);
            if (strpos($atTime, ' - ') !== false) {
                $parts = explode(' - ', $atTime);
                $start = trim((string)($parts[0] ?? ''));
                $end = trim((string)($parts[1] ?? ''));
                if ($start !== '' && $end !== '') {
                    $query->where('l.to_gh_time', 'between time', [strtotime($start), strtotime($end . ' +1 day')]);
                }
            } else {
                $query->where('l.to_gh_time', 'between time', [strtotime($atTime), strtotime($atTime . ' +1 day')]);
            }
        }

        if (!empty($keyword['timebucket'])) {
            $query->whereTime('l.to_gh_time', $keyword['timebucket']);
        }

        if (isset($keyword['pr_gh_type']) && trim((string)$keyword['pr_gh_type']) !== '') {
            $query->where('l.pr_gh_type', trim((string)$keyword['pr_gh_type']));
        }

        if (isset($keyword['kh_name']) && trim((string)$keyword['kh_name']) !== '') {
            $query->where('l.kh_name', 'like', '%' . trim((string)$keyword['kh_name']) . '%');
        }

        if (isset($keyword['phone']) && trim((string)$keyword['phone']) !== '') {
            $phoneWhere = $this->buildPhoneSearchWhere($keyword['phone']);
            if (!empty($phoneWhere)) {
                $query->where($phoneWhere);
            }
        }

        $total = (int)(clone $query)->count('l.id');
        if ($total === 0) {
            return ['data' => [], 'total' => 0];
        }

        $rows = (clone $query)
            ->field('l.*')
            ->orderRaw("CASE WHEN l.to_gh_time IS NULL OR l.to_gh_time = '' THEN 1 ELSE 0 END ASC, l.to_gh_time DESC, l.ut_time DESC")
            ->page($page, $limit)
            ->select();

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        $this->appendLiberumDisplayFields($rows);

        return ['data' => $rows, 'total' => $total];
    }

    /**
     * 保持旧方法签名，避免影响其它调用点。
     */
    public function getLiberumSearchList($page, $limit, $keyword)
    {
        $result = $this->getLiberumPageList($page, $limit, $keyword);
        if (empty($result['total'])) {
            return null;
        }
        return $result;
    }

    private function buildPhoneSearchWhere($phone)
    {
        $phone = trim((string)$phone);
        if ($phone === '') {
            return [];
        }

        $phoneKeyword = preg_replace('/\D+/', '', $phone);
        if ($phoneKeyword === '') {
            $phoneKeyword = $phone;
        }

        $rows = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('contact_type', 'in', [1, 3])
            ->where('contact_value', 'like', "%{$phoneKeyword}%")
            ->field('leads_id')
            ->select();

        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        $leadIds = array_values(array_unique(array_filter(array_column($rows, 'leads_id'))));
        if (empty($leadIds)) {
            return [['l.id', '=', -1]];
        }

        return [['l.id', 'in', $leadIds]];
    }

    private function appendLiberumDisplayFields(&$rows)
    {
        if (empty($rows) || !is_array($rows)) {
            return;
        }

        $leadIds = array_values(array_unique(array_filter(array_column($rows, 'id'))));
        $inquiryMap = Db::table('crm_inquiry')->column('inquiry_name', 'id');
        $portMap = Db::table('crm_inquiry_port')->column('port_name', 'id');
        $rankMapRaw = Db::table('crm_client_rank')->column('rank_name', 'id');
        $rankMap = [];
        foreach ($rankMapRaw as $rankId => $rankName) {
            $rankMap[(string)$rankId] = trim((string)$rankName);
        }
        $liberumTypeMapRaw = Db::table('crm_liberum_type')
            ->where('is_deleted', 0)
            ->column('type_name', 'id');
        $liberumTypeMap = [];
        foreach ($liberumTypeMapRaw as $typeId => $typeName) {
            $liberumTypeMap[(string)$typeId] = trim((string)$typeName);
        }

        $productIds = [];
        foreach ($rows as $row) {
            $pid = trim((string)($row['product_name'] ?? ''));
            if ($pid !== '' && preg_match('/^\d+$/', $pid)) {
                $productIds[] = (int)$pid;
            }
        }
        $productIds = array_values(array_unique($productIds));

        $productNameMap = [];
        if (!empty($productIds)) {
            $productRows = Db::table('crm_products')->whereIn('id', $productIds)->select();
            if (is_object($productRows) && method_exists($productRows, 'toArray')) {
                $productRows = $productRows->toArray();
            } elseif (!is_array($productRows)) {
                $productRows = [];
            }
            $categoryIds = array_values(array_unique(array_filter(array_column($productRows, 'category_id'))));
            $categoryNameMap = !empty($categoryIds)
                ? Db::table('crm_product_category')->whereIn('id', $categoryIds)->column('category_name', 'id')
                : [];
            foreach ($productRows as $prod) {
                $supplierName = isset($categoryNameMap[$prod['category_id']]) ? (string)$categoryNameMap[$prod['category_id']] : '';
                $productNameMap[(string)$prod['id']] = (string)$prod['product_name'] . ($supplierName !== '' ? "({$supplierName})" : '');
            }
        }

        $contactMap = [];
        if (!empty($leadIds)) {
            $contacts = Db::table('crm_contacts')
                ->alias('c')
                ->leftJoin('crm_position_title pt', 'pt.id = c.position_title_id AND pt.is_deleted = 0')
                ->where('c.is_delete', 0)
                ->whereIn('c.leads_id', $leadIds)
                ->whereIn('c.contact_type', [1, 3])
                ->order('c.id asc')
                ->field('c.leads_id,c.contact_type,c.contact_value,c.position_title,pt.title_name as pt_title_name')
                ->select();

            if (is_object($contacts) && method_exists($contacts, 'toArray')) {
                $contacts = $contacts->toArray();
            } elseif (!is_array($contacts)) {
                $contacts = [];
            }

            foreach ($contacts as $contact) {
                $lid = (int)$contact['leads_id'];
                if (!isset($contactMap[$lid])) {
                    $contactMap[$lid] = [
                        'main_phones' => [],
                        'aux_phones' => [],
                        'main_titles' => [],
                        'aux_titles' => [],
                    ];
                }

                $phone = trim((string)$contact['contact_value']);
                if ($phone === '') {
                    continue;
                }

                $titleName = trim((string)($contact['pt_title_name'] ?? ''));
                if ($titleName === '') {
                    $titleName = trim((string)($contact['position_title'] ?? ''));
                }
                if ($titleName === '') {
                    $titleName = '未填写';
                }
                $phoneAndTitle = $phone . '-' . $titleName;

                if ((int)$contact['contact_type'] === 1) {
                    if (!in_array($phone, $contactMap[$lid]['main_phones'], true)) {
                        $contactMap[$lid]['main_phones'][] = $phone;
                    }
                    if (!in_array($phoneAndTitle, $contactMap[$lid]['main_titles'], true)) {
                        $contactMap[$lid]['main_titles'][] = $phoneAndTitle;
                    }
                } elseif ((int)$contact['contact_type'] === 3) {
                    if (!in_array($phone, $contactMap[$lid]['aux_phones'], true)) {
                        $contactMap[$lid]['aux_phones'][] = $phone;
                    }
                    if (!in_array($phoneAndTitle, $contactMap[$lid]['aux_titles'], true)) {
                        $contactMap[$lid]['aux_titles'][] = $phoneAndTitle;
                    }
                }
            }
        }

        foreach ($rows as &$row) {
            $lid = (int)($row['id'] ?? 0);
            $productRaw = trim((string)($row['product_name'] ?? ''));
            $inquiryId = isset($row['inquiry_id']) ? (string)$row['inquiry_id'] : '';
            $portId = isset($row['port_id']) ? (string)$row['port_id'] : '';
            $khRankRaw = isset($row['kh_rank']) ? $row['kh_rank'] : '';

            $row['kh_name'] = isset($row['kh_name']) ? (string)$row['kh_name'] : '';
            $row['product_name'] = isset($productNameMap[$productRaw]) ? $productNameMap[$productRaw] : $productRaw;
            $row['main_phone'] = isset($contactMap[$lid]) ? implode(',', $contactMap[$lid]['main_phones']) : '';
            $row['aux_phone'] = isset($contactMap[$lid]) ? implode(',', $contactMap[$lid]['aux_phones']) : '';
            $row['main_phone_position_titles'] = isset($contactMap[$lid]) ? implode(',', $contactMap[$lid]['main_titles']) : '';
            $row['aux_phone_position_titles'] = isset($contactMap[$lid]) ? implode(',', $contactMap[$lid]['aux_titles']) : '';
            $row['inquiry_name'] = isset($inquiryMap[$inquiryId]) ? (string)$inquiryMap[$inquiryId] : $inquiryId;
            $row['port_name'] = isset($portMap[$portId]) ? (string)$portMap[$portId] : $portId;
            $row['kh_rank_display'] = $this->resolveKhRankDisplayName($khRankRaw, $rankMap);
            $rawGhType = trim((string)($row['pr_gh_type'] ?? ''));
            $ghTypeName = '未设置';
            if ($rawGhType !== '' && $rawGhType !== '0' && preg_match('/^\d+$/', $rawGhType)) {
                $mappedTypeName = isset($liberumTypeMap[$rawGhType]) ? trim((string)$liberumTypeMap[$rawGhType]) : '';
                if ($mappedTypeName !== '') {
                    $ghTypeName = $mappedTypeName;
                }
            }
            $row['pr_gh_type_name'] = $ghTypeName;
            $row['current_gh_type_name'] = $ghTypeName;
            $row['remark'] = isset($row['remark']) ? (string)$row['remark'] : '';
            $row['to_gh_time'] = !empty($row['to_gh_time']) ? (string)$row['to_gh_time'] : (isset($row['ut_time']) ? (string)$row['ut_time'] : '');
            $row['at_user'] = isset($row['at_user']) ? (string)$row['at_user'] : '';
            $row['pr_user'] = isset($row['pr_user']) ? (string)$row['pr_user'] : '';
            $row['pr_user_bef'] = isset($row['pr_user_bef']) ? (string)$row['pr_user_bef'] : '';
            unset($row['creator']);
        }
        unset($row);
    }

    private function resolveKhRankDisplayName($rawKhRank, array $rankMap = [])
    {
        $raw = trim((string)$rawKhRank);
        if ($raw === '') {
            return '';
        }
        if (preg_match('/^\d+$/', $raw)) {
            return isset($rankMap[$raw]) && $rankMap[$raw] !== '' ? $rankMap[$raw] : $raw;
        }
        return $raw;
    }
}
