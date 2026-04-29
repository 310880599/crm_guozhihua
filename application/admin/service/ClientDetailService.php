<?php

namespace app\admin\service;

use think\Db;

class ClientDetailService
{
    /** @var array<string, array<string, bool>> */
    private static $tableColumnsCache = [];

    public function buildDetailViewData(array $client)
    {
        $clientId = (int)($client['id'] ?? 0);
        $positionTitleService = new PositionTitleService();
        $positionTitleList = $positionTitleService->getActivePositionTitleList();

        $mainPhoneList = [];
        $auxPhone = '';
        $auxPhonePositionTitleId = '';

        if ($clientId > 0) {
            $contactFields = ['c.contact_type', 'c.contact_value'];
            if ($this->tableHasColumn('crm_contacts', 'position_title_id')) {
                $contactFields[] = 'c.position_title_id';
            }
            if ($this->tableHasColumn('crm_contacts', 'position_title')) {
                $contactFields[] = 'c.position_title';
            }
            if ($this->tableHasColumn('crm_position_title', 'title_name')) {
                $contactFields[] = 'pt.title_name as pt_title_name';
            }
            if ($this->tableHasColumn('crm_position_title', 'position_title')) {
                $contactFields[] = 'pt.position_title as pt_position_title';
            }

            $contacts = Db::table('crm_contacts')
                ->alias('c')
                ->leftJoin('crm_position_title pt', 'pt.id = c.position_title_id AND pt.is_deleted = 0')
                ->where('c.is_delete', 0)
                ->where('c.leads_id', $clientId)
                ->whereIn('c.contact_type', [1, 3])
                ->order('c.id', 'asc')
                ->field(implode(',', $contactFields))
                ->select();

            foreach ($contacts as $contact) {
                $titleName = $this->resolvePositionTitleName($contact);
                $titleId = isset($contact['position_title_id']) ? (string)$contact['position_title_id'] : '';

                if ((int)$contact['contact_type'] === 1) {
                    $mainPhoneList[] = [
                        'phone' => (string)($contact['contact_value'] ?? ''),
                        'position_title_id' => $titleId,
                        'position_title' => $titleName,
                    ];
                } elseif ((int)$contact['contact_type'] === 3 && $auxPhone === '') {
                    $auxPhone = (string)($contact['contact_value'] ?? '');
                    $auxPhonePositionTitleId = $titleId;
                }
            }
        }

        $selectedPorts = $this->parseMixedMultiValue($client['port_id'] ?? '');
        $jointPersonInit = $this->parseMixedMultiValue($client['joint_person'] ?? '');

        $adminList = Db::name('admin')
            ->where('group_id', '<>', 1)
            ->whereIn('group_id', [10, 11, 14, 17, 18, 19, 21, 22])
            ->field('admin_id, username')
            ->select();
        $collaboratorData = [];
        foreach ($adminList as $admin) {
            $collaboratorData[] = ['name' => $admin['username'], 'value' => $admin['admin_id']];
        }

        $productList = $this->buildProductList($client);
        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        $portList = Db::table('crm_inquiry_port')->where('status', 0)->select();
        $clientRankList = $this->buildClientRankListForEdit($client['kh_rank'] ?? '');
        $khRankValue = $this->normalizeKhRankToId($client['kh_rank'] ?? '', $clientRankList);
        $shopList = $this->buildShopList();

        return [
            'result' => $client,
            'mainPhoneList' => json_encode($mainPhoneList, JSON_UNESCAPED_UNICODE),
            'auxPhone' => $auxPhone,
            'auxPhonePositionTitleId' => $auxPhonePositionTitleId,
            'selectedPorts' => $selectedPorts,
            'jointPersonInit' => json_encode($jointPersonInit, JSON_UNESCAPED_UNICODE),
            'collaboratorList' => json_encode($collaboratorData, JSON_UNESCAPED_UNICODE),
            'productList' => $productList,
            'channelList' => $channelList,
            'portList' => $portList,
            'clientRankList' => $clientRankList,
            'positionTitleList' => $positionTitleList,
            'positionTitleListJson' => json_encode($positionTitleList, JSON_UNESCAPED_UNICODE),
            'shopList' => json_encode($shopList, JSON_UNESCAPED_UNICODE),
            'khRankValue' => $khRankValue,
        ];
    }

    private function buildProductList(array $client)
    {
        $query = Db::name('crm_products')->alias('p')
            ->leftJoin('crm_product_category c', 'p.category_id = c.id');

        $productRows = $query
            ->where([
                'p.is_deleted' => 0,
                'c.is_deleted' => 0,
            ])
            ->group('p.product_name, c.category_name')
            ->field('MIN(p.id) as id, p.product_name, c.category_name')
            ->order('p.product_name', 'asc')
            ->select();

        $currentPid = (int)($client['product_name'] ?? 0);
        if ($currentPid > 0) {
            $existsInList = false;
            foreach ($productRows as $row) {
                if ((int)$row['id'] === $currentPid) {
                    $existsInList = true;
                    break;
                }
            }
            if (!$existsInList) {
                $deletedProduct = Db::name('crm_products')->alias('p')
                    ->leftJoin('crm_product_category c', 'p.category_id = c.id')
                    ->where('p.id', $currentPid)
                    ->field('p.id, p.product_name, c.category_name, p.is_deleted as p_deleted, c.is_deleted as c_deleted')
                    ->find();
                if ($deletedProduct) {
                    $deletedProduct['category_name'] = (($deletedProduct['category_name'] ?? '') ?: '无') . '【已删除】';
                    array_unshift($productRows, $deletedProduct);
                }
            }
        }

        return $productRows;
    }

    private function buildClientRankListForEdit($currentKhRank)
    {
        $activeList = Db::table('crm_client_rank')
            ->where('is_deleted', 0)
            ->field('id,rank_name,rank_code,sort,is_deleted')
            ->order('sort asc,id asc')
            ->select();

        $result = [];
        foreach ($activeList as $row) {
            $row['rank_name_display'] = $row['rank_name'];
            $result[] = $row;
        }

        $raw = trim((string)$currentKhRank);
        if ($raw !== '' && preg_match('/^\d+$/', $raw)) {
            $rankId = (int)$raw;
            $alreadyInList = false;
            foreach ($result as $item) {
                if ((int)$item['id'] === $rankId) {
                    $alreadyInList = true;
                    break;
                }
            }
            if (!$alreadyInList && $rankId > 0) {
                $deletedRow = Db::table('crm_client_rank')
                    ->where('id', $rankId)
                    ->where('is_deleted', 1)
                    ->field('id,rank_name,rank_code,sort,is_deleted')
                    ->find();
                if ($deletedRow) {
                    $deletedRow['rank_name_display'] = $deletedRow['rank_name'] . '（已删除）';
                    $result[] = $deletedRow;
                }
            }
        }

        return $result;
    }

    private function normalizeKhRankToId($rawKhRank, array $rankList = [])
    {
        $raw = trim((string)$rawKhRank);
        if ($raw === '') {
            return '';
        }

        if (empty($rankList)) {
            $rankList = Db::table('crm_client_rank')->field('id,rank_name')->select();
        }

        $idMap = [];
        $nameMap = [];
        foreach ($rankList as $rankRow) {
            $id = (string)($rankRow['id'] ?? '');
            $name = trim((string)($rankRow['rank_name'] ?? ''));
            if ($id !== '') {
                $idMap[$id] = $id;
            }
            if ($name !== '') {
                $nameMap[$name] = $id;
            }
        }

        if (preg_match('/^\d+$/', $raw) && isset($idMap[$raw])) {
            return $idMap[$raw];
        }

        return $nameMap[$raw] ?? '';
    }

    private function buildShopList()
    {
        $shopList = [];
        $hasShopNames = $this->tableHasColumn('crm_client_status', 'shop_names');
        $statusRows = $hasShopNames
            ? Db::table('crm_client_status')->field('id,status_name,shop_names')->select()
            : Db::table('crm_client_status')->field('id,status_name')->select();

        foreach ($statusRows as $status) {
            $statusName = (string)($status['status_name'] ?? '');
            if ($statusName === '') {
                continue;
            }

            $shops = [];
            $shopNamesRaw = trim((string)($status['shop_names'] ?? ''));
            if ($shopNamesRaw !== '') {
                $shopNames = array_filter(array_map('trim', explode(',', $shopNamesRaw)));
                foreach ($shopNames as $shopName) {
                    $shops[] = [
                        'id' => md5((string)$status['id'] . '_' . $shopName),
                        'name' => $shopName,
                    ];
                }
            }

            if (empty($shops)) {
                $shops = $this->queryCommonShops($statusName);
            }

            if (!empty($shops)) {
                $shopList[$statusName] = $shops;
            }
        }

        return $shopList;
    }

    private function queryCommonShops($statusName)
    {
        try {
            $query = Db::table('crm_operation_shops');
            if ($this->tableHasColumn('crm_operation_shops', 'status') && $this->tableHasColumn('crm_operation_shops', 'status_name')) {
                $rows = $query
                    ->where('status', 1)
                    ->where('status_name', $statusName)
                    ->field('id,name')
                    ->order('id', 'asc')
                    ->select();
            } elseif ($this->tableHasColumn('crm_operation_shops', 'status_name')) {
                $rows = $query
                    ->where('status_name', $statusName)
                    ->field('id,name')
                    ->order('id', 'asc')
                    ->select();
            } else {
                $rows = [];
            }
        } catch (\Throwable $e) {
            $rows = [];
        }

        $result = [];
        foreach ($rows as $row) {
            $result[] = [
                'id' => $row['id'],
                'name' => $row['name'],
            ];
        }
        return $result;
    }

    private function resolvePositionTitleName(array $contact)
    {
        $ptTitleName = trim((string)($contact['pt_title_name'] ?? ''));
        if ($ptTitleName !== '') {
            return $ptTitleName;
        }

        $ptPositionTitle = trim((string)($contact['pt_position_title'] ?? ''));
        if ($ptPositionTitle !== '') {
            return $ptPositionTitle;
        }

        $rawTitle = trim((string)($contact['position_title'] ?? ''));
        if ($rawTitle !== '') {
            return $rawTitle;
        }

        return '';
    }

    private function parseMixedMultiValue($raw)
    {
        if (is_array($raw)) {
            return $this->sanitizeArrayValues($raw);
        }

        $text = trim((string)$raw);
        if ($text === '') {
            return [];
        }

        if (preg_match('/^\s*\[.*\]\s*$/', $text)) {
            $decoded = json_decode($text, true);
            if (is_array($decoded)) {
                return $this->sanitizeArrayValues($decoded);
            }
        }

        if (strpos($text, ',') !== false) {
            return $this->sanitizeArrayValues(explode(',', $text));
        }

        return $this->sanitizeArrayValues([$text]);
    }

    private function sanitizeArrayValues(array $items)
    {
        $values = [];
        foreach ($items as $item) {
            $value = trim((string)$item);
            if ($value !== '') {
                $values[] = $value;
            }
        }
        return array_values(array_unique($values));
    }

    private function tableHasColumn($table, $column)
    {
        if (!isset(self::$tableColumnsCache[$table])) {
            self::$tableColumnsCache[$table] = $this->loadTableColumns($table);
        }
        return !empty(self::$tableColumnsCache[$table][$column]);
    }

    private function loadTableColumns($table)
    {
        try {
            $rows = Db::query("SHOW COLUMNS FROM `{$table}`");
            $map = [];
            foreach ($rows as $row) {
                if (!empty($row['Field'])) {
                    $map[$row['Field']] = true;
                }
            }
            return $map;
        } catch (\Throwable $e) {
            return [];
        }
    }
}
