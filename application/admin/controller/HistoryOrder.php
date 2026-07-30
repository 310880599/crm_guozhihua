<?php
namespace app\admin\controller;

use app\admin\model\Admin;
use app\admin\model\HistoryOrder as HistoryOrderModel;
use app\admin\service\HistoryOrderService;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Shared\Date as SpreadsheetDate;
use think\Db;
use think\facade\Env;
use think\facade\Request;

class HistoryOrder extends Common
{
    /**
     * 当前登录管理员信息
     *
     * @var array
     */
    protected $currentAdmin = [];

    public function initialize()
    {
        parent::initialize();
        $this->currentAdmin = Admin::getMyInfo();
    }

    /**
     * 判断当前登录用户是否为超级管理员
     * admin账号 或 group_id=1 均视为超级管理员，拥有全部历史订单权限
     *
     * @return bool
     */
    private function isSuperAdmin(): bool
    {
        $admin = $this->currentAdmin ?: Admin::getMyInfo();
        return ($admin['username'] ?? '') === 'admin' || (int)($admin['group_id'] ?? 0) === 1;
    }

    public function index()
    {
        if (request()->isPost()) {
            return $this->historyOrderSearch();
        }
        return $this->fetch();
    }

    private function historyOrderSearch()
    {
        $clientPhone = input('client_phone', '');
        $orderNo = input('order_no', '');
        $productName = input('product_name', '');
        $prUser = input('pr_user', '');
        $page = input('page/d', 1);
        $limit = input('limit/d', 10);

        $query = HistoryOrderModel::order('id desc')->where('is_deleted', 0);
        if (!$this->isSuperAdmin()) {
            $query->where('create_user_id', (int)session('aid'));
        }
        if ($clientPhone !== '') {
            $query->where('client_phone', 'like', "%{$clientPhone}%");
        }
        if ($orderNo !== '') {
            $query->where('order_no', 'like', "%{$orderNo}%");
        }
        if ($productName !== '') {
            $query->where('product_name', 'like', "%{$productName}%");
        }
        if ($prUser !== '') {
            $query->where('pr_user', 'like', "%{$prUser}%");
        }

        $list = $query->paginate(['list_rows' => $limit, 'page' => $page])->toArray();
        return json([
            'code' => 0,
            'msg' => '获取成功!',
            'data' => $list['data'],
            'count' => $list['total'],
            'rel' => 1,
        ]);
    }

    public function add()
    {
        if (Request::isPost()) {
            $data = Request::only([
                'client_phone',
                'order_time',
                'money',
                'profit',
                'product_id',
                'product_name',
                'pr_user_id',
                'pr_user',
                'voucher_image',
                'remark',
            ], 'post');

            $admin = Admin::getMyInfo();
            $service = new HistoryOrderService();
            $data['order_no'] = $service->generateOrderNo();
            $data['client_phone'] = trim((string)($data['client_phone'] ?? ''));
            $data['client_id'] = $this->resolveClientIdByPhone($data['client_phone']);
            $data['create_user_id'] = (int)session('aid');
            $data['create_user'] = (string)($admin['username'] ?? '');
            $data['is_deleted'] = 0;
            $data['deleted_time'] = null;
            $data['deleted_by'] = null;

            $data['create_time'] = date('Y-m-d H:i:s');
            $data['update_time'] = date('Y-m-d H:i:s');

            $res = HistoryOrderModel::create($data);
            return $res ? json(['code' => 0, 'msg' => '添加成功！']) : json(['code' => -200, 'msg' => '添加失败！']);
        }

        $currentAdmin = Admin::getMyInfo();
        $this->assign('currentAdmin', $currentAdmin);
        return $this->fetch();
    }

    public function edit()
    {
        $id = input('id/d', 0);
        if ($id <= 0) {
            return $this->error('参数错误');
        }

        $entry = HistoryOrderModel::where('id', $id)->where('is_deleted', 0)->find();
        if (empty($entry)) {
            return $this->error('记录不存在或已删除');
        }

        if (!$this->isSuperAdmin() && (int)($entry['create_user_id'] ?? 0) !== (int)session('aid')) {
            return $this->error('无权限修改该订单');
        }

        if (Request::isAjax()) {
            $data = Request::only([
                'client_phone',
                'order_time',
                'money',
                'profit',
                'product_id',
                'product_name',
                'pr_user_id',
                'pr_user',
                'voucher_image',
                'remark',
            ], 'post');
            unset($data['order_no']);

            if (array_key_exists('client_phone', $data)) {
                $newClientPhone = trim((string)$data['client_phone']);
                $oldClientPhone = trim((string)($entry['client_phone'] ?? ''));
                $data['client_phone'] = $newClientPhone;

                if ($newClientPhone !== $oldClientPhone) {
                    $data['client_id'] = $this->resolveClientIdByPhone($newClientPhone);
                }
            }

            $res = Db::name('crm_client_history_order')
                ->where('id', $id)
                ->where('is_deleted', 0)
                ->update($data);

            return $res !== false ? json(['code' => 0, 'msg' => '修改成功！']) : json(['code' => -200, 'msg' => '修改失败！']);
        }

        $this->assign('entry', $entry);
        return $this->fetch();
    }

    /**
     * 历史订单详情（只读展示，不允许编辑）
     */
    public function detail()
    {
        $id = input('id/d', 0);
        if ($id <= 0) {
            return $this->error('参数错误');
        }

        $entry = HistoryOrderModel::where('id', $id)
            ->where('is_deleted', 0)
            ->field([
                'id',
                'order_no',
                'client_phone',
                'order_time',
                'money',
                'profit',
                'product_name',
                'pr_user',
                'voucher_image',
                'remark',
                'create_user',
                'create_time',
                'update_time',
            ])
            ->find();
        if (empty($entry)) {
            return $this->error('记录不存在或已删除');
        }

        $this->assign('entry', $entry);
        return $this->fetch();
    }

    public function del()
    {
        $id = input('id/d', 0);
        if ($id <= 0) {
            return json(['code' => -200, 'msg' => '参数错误']);
        }

        $entry = Db::name('crm_client_history_order')
            ->where('id', $id)
            ->where('is_deleted', 0)
            ->find();
        if (empty($entry)) {
            return json(['code' => -200, 'msg' => '记录不存在或已删除']);
        }
        if (!$this->isSuperAdmin() && (int)($entry['create_user_id'] ?? 0) !== (int)session('aid')) {
            return json(['code' => -200, 'msg' => '无权限删除该订单']);
        }

        $aff = Db::name('crm_client_history_order')
            ->where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'is_deleted' => 1,
                'deleted_time' => date('Y-m-d H:i:s'),
                'deleted_by' => (int)session('aid'),
            ]);

        return $aff ? json(['code' => 0, 'msg' => '删除成功！']) : json(['code' => -200, 'msg' => '删除失败！']);
    }

    public function batchDel()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        if (empty($ids)) {
            return json(['code' => -200, 'msg' => '未选择任何记录']);
        }

        $query = Db::name('crm_client_history_order')
            ->whereIn('id', $ids)
            ->where('is_deleted', 0);
        if (!$this->isSuperAdmin()) {
            $query->where('create_user_id', (int)session('aid'));
        }

        Db::startTrans();
        try {
            $delCount = $query->update([
                'is_deleted' => 1,
                'deleted_time' => date('Y-m-d H:i:s'),
                'deleted_by' => (int)session('aid'),
            ]);
            Db::commit();

            if ($delCount > 0) {
                return json(['code' => 0, 'msg' => '删除成功', 'data' => ['count' => $delCount]]);
            }
            return json(['code' => -200, 'msg' => '删除失败或记录不存在']);
        } catch (\Throwable $e) {
            Db::rollback();
            return json(['code' => -200, 'msg' => '删除异常：' . $e->getMessage()]);
        }
    }

    public function import()
    {
        return $this->fetch();
    }

    public function importDo()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }

        $file = request()->file('file');
        if (!$file) {
            return json(['code' => -200, 'msg' => '请上传Excel文件']);
        }

        $saveDir = Env::get('root_path') . 'runtime' . DIRECTORY_SEPARATOR . 'upload' . DIRECTORY_SEPARATOR . 'excel';
        if (!is_dir($saveDir)) {
            @mkdir($saveDir, 0777, true);
        }
        $info = $file->validate(['size' => 10 * 1024 * 1024, 'ext' => 'xlsx,xls,csv'])->move($saveDir);
        if (!$info) {
            return json(['code' => -200, 'msg' => $file->getError() ?: '文件保存失败']);
        }
        $filePath = $info->getPathname();
        $ext = strtolower($info->getExtension());

        $rows = [];
        try {
            if ($ext === 'csv') {
                $handle = fopen($filePath, 'r');
                if (!$handle) {
                    throw new \Exception('CSV文件读取失败');
                }

                $first = fgets($handle);
                if (substr((string)$first, 0, 3) === "\xEF\xBB\xBF") {
                    $first = substr((string)$first, 3);
                }
                $buffer = (string)$first . stream_get_contents($handle);
                fclose($handle);

                $tmp = tmpfile();
                fwrite($tmp, $buffer);
                fseek($tmp, 0);

                $lineNo = 0;
                while (($line = fgetcsv($tmp)) !== false) {
                    $lineNo++;
                    if ($lineNo === 1) {
                        $headerText = implode('', $line);
                        if (mb_strpos($headerText, '客户') !== false || mb_strpos($headerText, '订单') !== false) {
                            continue;
                        }
                    }

                    $line = array_pad($line, 8, '');
                    $rows[] = [
                        'client_phone' => trim((string)$line[0]),
                        'order_no' => trim((string)$line[1]),
                        'order_time' => trim((string)$line[2]),
                        'money' => trim((string)$line[3]),
                        'profit' => trim((string)$line[4]),
                        'product_name' => trim((string)$line[5]),
                        'pr_user' => trim((string)$line[6]),
                        'remark' => trim((string)$line[7]),
                    ];
                }
                fclose($tmp);
            } else {
                if (!class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    return json([
                        'code' => -200,
                        'msg' => '服务器未安装 PhpSpreadsheet，无法解析 .xlsx/.xls，请安装 phpoffice/phpspreadsheet 或改用 CSV 导入',
                    ]);
                }

                $spreadsheet = IOFactory::load($filePath);
                $sheet = $spreadsheet->getActiveSheet();
                $highestRow = $sheet->getHighestRow();

                $firstRowText = implode('', [
                    (string)$sheet->getCell('A1')->getFormattedValue(),
                    (string)$sheet->getCell('B1')->getFormattedValue(),
                ]);
                $hasHeader = mb_strpos($firstRowText, '客户') !== false || mb_strpos($firstRowText, '订单') !== false;
                $start = $hasHeader ? 2 : 1;

                for ($r = $start; $r <= $highestRow; $r++) {
                    $rawOrderTime = $sheet->getCell("C{$r}")->getValue();
                    $orderTime = $this->normalizeImportOrderTime($rawOrderTime);

                    $row = [
                        'client_phone' => trim((string)$sheet->getCell("A{$r}")->getFormattedValue()),
                        'order_no' => trim((string)$sheet->getCell("B{$r}")->getFormattedValue()),
                        'order_time' => $orderTime,
                        'money' => trim((string)$sheet->getCell("D{$r}")->getFormattedValue()),
                        'profit' => trim((string)$sheet->getCell("E{$r}")->getFormattedValue()),
                        'product_name' => trim((string)$sheet->getCell("F{$r}")->getFormattedValue()),
                        'pr_user' => trim((string)$sheet->getCell("G{$r}")->getFormattedValue()),
                        'remark' => trim((string)$sheet->getCell("H{$r}")->getFormattedValue()),
                    ];

                    if ($row['client_phone'] === ''
                        && $row['order_no'] === ''
                        && $row['order_time'] === ''
                        && $row['money'] === ''
                        && $row['profit'] === ''
                        && $row['product_name'] === ''
                        && $row['pr_user'] === ''
                        && $row['remark'] === '') {
                        continue;
                    }
                    $rows[] = $row;
                }
            }
        } catch (\Throwable $e) {
            return json(['code' => -200, 'msg' => '解析失败：' . $e->getMessage()]);
        }

        if (empty($rows)) {
            return json(['code' => -200, 'msg' => '没有可导入的数据']);
        }

        $providedOrderNos = [];
        foreach ($rows as $idx => $row) {
            if ($row['order_no'] === '') {
                continue;
            }
            if (isset($providedOrderNos[$row['order_no']])) {
                return json(['code' => -200, 'msg' => '导入失败：第' . ($idx + 1) . '行订单编号重复']);
            }
            $providedOrderNos[$row['order_no']] = true;
        }

        if (!empty($providedOrderNos)) {
            $exists = Db::name('crm_client_history_order')
                ->whereIn('order_no', array_keys($providedOrderNos))
                ->count();
            if ((int)$exists > 0) {
                return json(['code' => -200, 'msg' => '导入失败：存在重复订单编号']);
            }
        }

        $admin = Admin::getMyInfo();
        $now = date('Y-m-d H:i:s');
        $historyOrderService = new HistoryOrderService();
        $reservedOrderNos = $providedOrderNos;
        $insertData = [];
        $phoneSet = [];
        foreach ($rows as $row) {
            if ($row['client_phone'] !== '') {
                $phoneSet[$row['client_phone']] = true;
            }
        }
        $clientIdMap = $this->buildClientIdMapByPhones(array_keys($phoneSet));

        foreach ($rows as $row) {
            $orderNo = $row['order_no'];
            if ($orderNo === '') {
                $orderNo = $historyOrderService->generateOrderNo(array_keys($reservedOrderNos));
            }
            $reservedOrderNos[$orderNo] = true;

            $insertData[] = [
                'client_id' => (int)($clientIdMap[$row['client_phone']] ?? 0),
                'client_phone' => $row['client_phone'],
                'order_no' => $orderNo,
                'order_time' => $row['order_time'],
                'money' => (float)$row['money'],
                'profit' => (float)$row['profit'],
                'product_id' => 0,
                'product_name' => $row['product_name'],
                'pr_user_id' => 0,
                'pr_user' => $row['pr_user'],
                'voucher_image' => '',
                'remark' => $row['remark'],
                'create_user_id' => (int)session('aid'),
                'create_user' => (string)($admin['username'] ?? ''),
                'create_time' => $now,
                'update_time' => $now,
                'is_deleted' => 0,
                'deleted_time' => null,
                'deleted_by' => null,
            ];
        }

        Db::startTrans();
        try {
            $inserted = Db::name('crm_client_history_order')->insertAll($insertData);
            Db::commit();
            return json(['code' => 0, 'msg' => "导入成功：{$inserted} 条"]);
        } catch (\Throwable $e) {
            Db::rollback();
            return json(['code' => -200, 'msg' => '写入失败：' . $e->getMessage()]);
        }
    }

    public function tpl()
    {
        $filename = '历史订单导入模板_' . date('Ymd_His') . '.csv';
        $csvLine = function (array $cols) {
            $safe = array_map(function ($val) {
                $val = (string)$val;
                $val = str_replace('"', '""', $val);
                return '"' . $val . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        $header = ['客户手机号', '订单编号', '成交时间', '成交金额', '利润', '产品名称', '负责人', '备注'];
        $examples = [
            ['13800138000', 'H202607280001', '2026-07-28 10:00:00', '1999.00', '300.00', 'CRM系统A套餐', '张三', '首次合作'],
            ['13900139000', '', '2026-07-28 11:00:00', '2999.00', '500.00', 'CRM系统B套餐', '李四', '续费客户'],
        ];

        if (function_exists('ob_get_level')) {
            while (ob_get_level() > 0) {
                ob_end_clean();
            }
        }

        header('Content-Type: text/csv; charset=UTF-8');
        header('Content-Disposition: attachment; filename="' . $filename . '"');
        header('Pragma: no-cache');
        header('Expires: 0');
        header('Cache-Control: must-revalidate, post-check=0, pre-check=0');

        echo "\xEF\xBB\xBF";
        echo $csvLine($header);
        foreach ($examples as $example) {
            echo $csvLine($example);
        }
        exit;
    }

    /**
     * 兼容 Excel 序列化时间和普通时间文本
     *
     * @param mixed $rawOrderTime
     * @return string
     */
    private function normalizeImportOrderTime($rawOrderTime): string
    {
        if ($rawOrderTime === null) {
            return '';
        }

        if (is_numeric($rawOrderTime) && (float)$rawOrderTime > 0) {
            try {
                $dateTime = SpreadsheetDate::excelToDateTimeObject($rawOrderTime);
                return $dateTime->format('Y-m-d H:i:s');
            } catch (\Throwable $e) {
                return trim((string)$rawOrderTime);
            }
        }

        return trim((string)$rawOrderTime);
    }

    /**
     * 根据手机号查找 CRM 客户ID（leads_id）
     *
     * @param string $clientPhone
     * @return int
     */
    private function resolveClientIdByPhone(string $clientPhone): int
    {
        if ($clientPhone === '') {
            return 0;
        }

        $leadId = Db::name('crm_contacts')
            ->where('contact_value', $clientPhone)
            ->where('is_delete', 0)
            ->order('id desc')
            ->value('leads_id');

        return (int)$leadId;
    }

    /**
     * 批量构建手机号到 CRM 客户ID（leads_id）的映射
     *
     * @param array $phones
     * @return array<string, int>
     */
    private function buildClientIdMapByPhones(array $phones): array
    {
        $phones = array_values(array_filter(array_unique(array_map('trim', $phones))));
        if (empty($phones)) {
            return [];
        }

        $contacts = Db::name('crm_contacts')
            ->field('contact_value, leads_id')
            ->whereIn('contact_value', $phones)
            ->where('is_delete', 0)
            ->order('id desc')
            ->select();

        $map = [];
        foreach ($contacts as $contact) {
            $contactValue = trim((string)($contact['contact_value'] ?? ''));
            if ($contactValue === '' || isset($map[$contactValue])) {
                continue;
            }
            $map[$contactValue] = (int)($contact['leads_id'] ?? 0);
        }

        return $map;
    }
}
