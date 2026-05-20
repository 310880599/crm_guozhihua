<?php
namespace app\admin\controller;
use think\facade\Request;
use think\Db;
use think\facade\Log;
use think\facade\Session;
use think\facade\Env;
use app\admin\behavior\ContactMap; 
use app\admin\model\LiberumType as LiberumTypeModel;
use app\admin\service\LiberumAutoCandidateService;
use app\admin\service\LiberumConfigService;
use app\admin\service\LiberumLogService;
use app\admin\service\LiberumTypeService;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;

class Liberum extends Common{

    /**
     * @var LiberumTypeService
     */
    protected $liberumTypeService;
    protected $liberumLogService;
    protected $liberumAutoCandidateService;
    protected $liberumConfigService;

    protected function getLiberumTypeService()
    {
        if (!$this->liberumTypeService) {
            $this->liberumTypeService = new LiberumTypeService();
        }
        return $this->liberumTypeService;
    }

    protected function getLiberumLogService()
    {
        if (!$this->liberumLogService) {
            $this->liberumLogService = new LiberumLogService();
        }
        return $this->liberumLogService;
    }

    protected function getLiberumAutoCandidateService()
    {
        if (!$this->liberumAutoCandidateService) {
            $this->liberumAutoCandidateService = new LiberumAutoCandidateService();
        }
        return $this->liberumAutoCandidateService;
    }

    protected function getLiberumConfigService()
    {
        if (!$this->liberumConfigService) {
            $this->liberumConfigService = new LiberumConfigService();
        }
        return $this->liberumConfigService;
    }

    // 公海列表
    public function index(){
        if(request()->isPost()){
            $page = input('page') ? input('page') : 1;
            $pageSize = input('limit') ? input('limit') : config('pageSize');
            $keyword = input('post.keyword/a', []);
            $list = model('liberum')->getLiberumPageList($page, $pageSize, $keyword);
            return ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
        }

        $ghTypeList = LiberumTypeModel::where('is_deleted', 0)->order('id desc')->select();
        $this->assign('ghTypeList', $ghTypeList);
        return $this->fetch();
    }

    // 客户提取记录页面
    public function pickLog()
    {
        $this->assign('canDangerOperate', $this->hasLiberumDangerOperationPermission() ? 1 : 0);
        $pickUserList = Db::table('crm_liberum_pick_log')
            ->where('pick_user', 'not null')
            ->where('pick_user', '<>', '')
            ->group('pick_user')
            ->order('pick_user asc')
            ->column('pick_user');
        $ghTypeList = Db::table('crm_liberum_type')
            ->where('is_deleted', 0)
            ->order('id desc')
            ->field('id,type_name')
            ->select();
        $this->assign('pickUserList', $pickUserList);
        $this->assign('ghTypeList', $ghTypeList);
        return $this->fetch('liberum/picklog');
    }

    // 客户流入公海记录页面
    public function inLog()
    {
        $this->assign('canDangerOperate', $this->hasLiberumDangerOperationPermission() ? 1 : 0);

        $beforePrUserList = Db::table('crm_liberum_in_log')
            ->where('before_pr_user', 'not null')
            ->where('before_pr_user', '<>', '')
            ->group('before_pr_user')
            ->order('before_pr_user asc')
            ->column('before_pr_user');

        $operatorNameList = Db::table('crm_liberum_in_log')
            ->where('operator_name', 'not null')
            ->where('operator_name', '<>', '')
            ->group('operator_name')
            ->order('operator_name asc')
            ->column('operator_name');

        $this->assign('beforePrUserList', $beforePrUserList);
        $this->assign('operatorNameList', $operatorNameList);

        return $this->fetch('liberum/inlog');
    }

    // 自动公海候选页面
    public function autoCandidate()
    {
        return $this->fetch('liberum/auto_candidate');
    }

    public function config()
    {
        $config = $this->getLiberumConfigService()->getConfigMap();
        $this->assign('config', $config);
        return $this->fetch('liberum/config');
    }

    public function saveConfig()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $params = Request::param();
        return $this->getLiberumConfigService()->saveConfig($params);
    }

    // 客户提取记录列表
    public function getPickLogList()
    {
        $params = Request::param();
        $list = $this->getLiberumLogService()->getPickLogList($params);
        return json([
            'code' => 0,
            'msg' => '',
            'count' => (int)($list['count'] ?? 0),
            'data' => $list['data'] ?? [],
        ]);
    }

    // 客户提取记录导出
    public function exportPickLog()
    {
        if (!class_exists('\PhpOffice\PhpSpreadsheet\Spreadsheet')) {
            return ['code' => -200, 'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出', 'data' => []];
        }

        $params = Request::param();
        $list = $this->getLiberumLogService()->exportPickLog($params);
        $rows = isset($list['data']) && is_array($list['data']) ? $list['data'] : [];

        $headers = [
            'ID',
            '客户ID',
            '客户名称',
            '客户电话',
            '提取人',
            '提取日期',
            '提取时间',
            '原负责人',
            '提取前公海类型',
            '当前负责人',
            '是否已退回',
        ];

        $dataRows = [];
        foreach ($rows as $row) {
            $isReturned = (int)($row['is_returned'] ?? 0) === 1 ? '已退回' : '未退回';
            $dataRows[] = [
                (string)($row['id'] ?? ''),
                (string)($row['client_id'] ?? ($row['leads_id'] ?? '')),
                (string)($row['client_name'] ?? ($row['kh_name'] ?? '')),
                (string)($row['client_phone'] ?? ($row['phone'] ?? '')),
                (string)($row['pick_user'] ?? ($row['operator_name'] ?? '')),
                (string)($row['pick_date'] ?? ''),
                (string)($row['pick_time'] ?? ''),
                (string)($row['before_pr_user'] ?? ''),
                (string)($row['before_gh_type_name'] ?? ($row['gh_type'] ?? '')),
                (string)($row['current_pr_user'] ?? ($row['pr_user'] ?? '')),
                $isReturned,
            ];
        }

        $this->downloadExcel('客户提取记录_' . date('Ymd_His') . '.xlsx', $headers, $dataRows);
    }

    // 客户提取记录批量退回公海
    public function batchReturnToLiberum()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }
        if (!$this->hasLiberumDangerOperationPermission()) {
            return ['code' => -200, 'msg' => '无权限执行该操作', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        $ghTypeId = input('post.gh_type_id/d', 0);
        if (empty($ids) || !is_array($ids)) {
            return ['code' => -200, 'msg' => '请选择需要退回的记录', 'data' => []];
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumLogService()->batchReturnToLiberum($ids, $operatorInfo, $ghTypeId);
        $this->recordLiberumDangerOperationLog('公海提取记录批量退回', $ids, $operatorInfo, $result);
        return $result + ['data' => []];
    }

    // 客户提取记录批量隐藏
    public function batchHidePickLog()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }
        if (!$this->hasLiberumDangerOperationPermission()) {
            return ['code' => -200, 'msg' => '无权限执行该操作', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        if (empty($ids) || !is_array($ids)) {
            return ['code' => -200, 'msg' => '请选择需要隐藏的记录', 'data' => []];
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumLogService()->batchHidePickLog($ids, $operatorInfo);
        $this->recordLiberumDangerOperationLog('公海提取记录批量隐藏', $ids, $operatorInfo, $result);
        return $result + ['data' => []];
    }

    // 客户流入公海记录列表
    public function getInLogList()
    {
        $params = Request::param();
        $list = $this->getLiberumLogService()->getInLogList($params);
        return json([
            'code' => 0,
            'msg' => '',
            'count' => (int)($list['count'] ?? 0),
            'data' => $list['data'] ?? [],
        ]);
    }

    // 自动公海候选列表
    public function getAutoCandidateList()
    {
        $params = Request::param();
        $list = $this->getLiberumAutoCandidateService()->getCandidateList($params);

        return json([
            'code' => 0,
            'msg' => '',
            'count' => (int)($list['count'] ?? 0),
            'data' => $list['data'] ?? [],
        ]);
    }

    // 自动公海候选：当前负责人下拉选项
    public function getPrUserOptions()
    {
        $rows = Db::table('admin')
            ->where('username', '<>', '')
            ->field('username')
            ->order('username asc')
            ->select();
        if (is_object($rows) && method_exists($rows, 'toArray')) {
            $rows = $rows->toArray();
        } elseif (!is_array($rows)) {
            $rows = [];
        }

        $seen = [];
        $data = [];
        foreach ($rows as $row) {
            $username = trim((string)($row['username'] ?? ''));
            if ($username === '' || isset($seen[$username])) {
                continue;
            }
            $seen[$username] = true;
            $data[] = [
                'name' => $username,
                'value' => $username,
            ];
        }

        return json([
            'code' => 0,
            'msg' => 'success',
            'data' => $data,
        ]);
    }

    // 单个确认自动流入公海
    public function confirmAutoLiberum()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $id = input('post.id/d', 0);
        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumAutoCandidateService()->confirmToLiberum($id, $operatorInfo);
        $this->recordLiberumDangerOperationLog('自动公海单个确认', [$id], $operatorInfo, $result);
        return $result + ['data' => []];
    }

    // 批量确认自动流入公海
    public function batchConfirmAutoLiberum()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumAutoCandidateService()->batchConfirmToLiberum($ids, $operatorInfo);
        $this->recordLiberumDangerOperationLog('自动公海批量确认', is_array($ids) ? $ids : [], $operatorInfo, $result);
        return $result + ['data' => []];
    }

    // 客户流入公海记录导出
    public function exportInLog()
    {
        if (!class_exists('\PhpOffice\PhpSpreadsheet\Spreadsheet')) {
            return ['code' => -200, 'msg' => '导出失败：系统缺少 PhpSpreadsheet 依赖，请先安装后再导出', 'data' => []];
        }

        $params = Request::param();
        $list = $this->getLiberumLogService()->exportInLog($params);
        $rows = isset($list['data']) && is_array($list['data']) ? $list['data'] : [];

        $headers = [
            'ID',
            '客户ID',
            '客户名称',
            '客户电话',
            '原负责人',
            '当前负责人',
            '流入原因',
            '流入时间',
            '操作人',
            '当前状态',
            '流入公海类型',
            '来源类型',
            '是否已恢复',
            '恢复操作人',
            '恢复时间',
        ];

        $dataRows = [];
        foreach ($rows as $row) {
            $sourceType = (string)($row['source_type'] ?? '');
            if ($sourceType === 'pick_return') {
                $sourceTypeText = '提取退回';
            } elseif ($sourceType === 'manual') {
                $sourceTypeText = '手动移入';
            } elseif ($sourceType === 'auto_rule') {
                $sourceTypeText = '自动规则流入';
            } elseif ($sourceType === 'pick_log_batch_return') {
                $sourceTypeText = '批量退回公海';
            } else {
                $sourceTypeText = $sourceType;
            }

            $statusText = '未知';
            if ((string)($row['current_status'] ?? '') !== '') {
                $status = (int)$row['current_status'];
                if ($status === 1) {
                    $statusText = '客户';
                } elseif ($status === 2) {
                    $statusText = '公海';
                } else {
                    $statusText = (string)$row['current_status'];
                }
            }

            $isRecovered = (int)($row['is_recovered'] ?? 0) === 1 ? '已恢复' : '未恢复';
            $currentGhTypeText = (string)($row['in_gh_type_name'] ?? '');
            if ($currentGhTypeText === '') {
                $currentGhTypeText = (string)($row['current_gh_type_name'] ?? '');
            }
            if ($currentGhTypeText === '') {
                $currentGhTypeText = (string)($row['in_gh_type'] ?? '');
            }
            if ($currentGhTypeText === '') {
                $currentGhTypeText = (string)($row['current_gh_type'] ?? '');
            }
            $dataRows[] = [
                (string)($row['id'] ?? ''),
                (string)($row['client_id'] ?? ($row['leads_id'] ?? '')),
                (string)($row['client_name'] ?? ($row['kh_name'] ?? '')),
                (string)($row['client_phone'] ?? ($row['phone'] ?? '')),
                (string)($row['before_pr_user'] ?? ''),
                (string)($row['current_pr_user'] ?? ''),
                (string)($row['reason'] ?? ''),
                (string)($row['in_time'] ?? ''),
                (string)($row['operator_name'] ?? ($row['operator_id'] ?? '')),
                $statusText,
                $currentGhTypeText,
                $sourceTypeText,
                $isRecovered,
                (string)($row['recover_operator_name'] ?? ($row['recover_operator_id'] ?? '')),
                (string)($row['recovered_time'] ?? ''),
            ];
        }

        $this->downloadExcel('客户流入公海记录_' . date('Ymd_His') . '.xlsx', $headers, $dataRows);
    }

    // 客户流入公海记录批量恢复原负责人
    public function batchRestoreOwner()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }
        if (!$this->hasLiberumDangerOperationPermission()) {
            return ['code' => -200, 'msg' => '无权限执行该操作', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        if (empty($ids) || !is_array($ids)) {
            return ['code' => -200, 'msg' => '请选择需要恢复的记录', 'data' => []];
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumLogService()->batchRestoreOwner($ids, $operatorInfo);
        $this->recordLiberumDangerOperationLog('公海流入记录批量恢复', $ids, $operatorInfo, $result);
        return $result + ['data' => []];
    }

    // 客户流入公海记录批量隐藏
    public function batchHideInLog()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }
        if (!$this->hasLiberumDangerOperationPermission()) {
            return ['code' => -200, 'msg' => '无权限执行该操作', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        if (empty($ids) || !is_array($ids)) {
            return ['code' => -200, 'msg' => '请选择需要隐藏的记录', 'data' => []];
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumLogService()->batchHideInLog($ids, $operatorInfo);
        $this->recordLiberumDangerOperationLog('公海流入记录批量隐藏', $ids, $operatorInfo, $result);
        return $result + ['data' => []];
    }

    /**
     * 记录公海高危操作日志。
     * 写日志失败时静默处理，避免影响主业务流程。
     *
     * @param string $operation
     * @param array $ids
     * @param array $operatorInfo
     * @param array $result
     */
    private function recordLiberumDangerOperationLog($operation, array $ids, array $operatorInfo, array $result)
    {
        try {
            $totalCount = count($ids);
            $safeIds = array_slice($ids, 0, 50);
            $safeIds = array_map(function ($id) {
                return (string)$id;
            }, $safeIds);
            $idsText = implode(',', $safeIds);
            if ($idsText === '') {
                $idsText = '-';
            }

            $msg = isset($result['msg']) ? (string)$result['msg'] : '';
            if (function_exists('mb_substr')) {
                $msg = mb_substr($msg, 0, 200, 'UTF-8');
            } else {
                $msg = substr($msg, 0, 200);
            }

            $ip = '';
            try {
                $ip = request()->ip();
            } catch (\Throwable $e) {
                $ip = '';
            }

            $content = '公海高危操作日志：操作=' . (string)$operation
                . '，admin_id=' . (int)($operatorInfo['admin_id'] ?? 0)
                . '，username=' . (string)($operatorInfo['username'] ?? '')
                . '，ids=' . $idsText
                . '，total_count=' . $totalCount
                . '，code=' . (string)($result['code'] ?? '')
                . '，msg=' . $msg
                . '，operate_time=' . date('Y-m-d H:i:s')
                . '，ip=' . (string)$ip;

            Log::record($content, 'info');
        } catch (\Throwable $e) {
            // 日志失败不抛出，避免影响正常业务响应
        }
    }

    // 公海类型
    public function libTypeList(){
        if(request()->isPost()){
            $page = input('page/d', 1);
            $pageSize = input('limit/d', config('pageSize'));
            $params = [
                'type_name' => input('type_name', ''),
            ];
            $list = $this->getLiberumTypeService()->search($params, $page, $pageSize);
            return ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['count'], 'rel' => 1];
        }
        return $this->fetch('liberum/lib_type_list');
    }

    // 添加公海类型
    public function libTypeAdd(){
        if(request()->isPost()){
            $data = Request::only(['type_name'], 'post');
            $data['creator'] = (string)Session::get('username');
            $result = $this->getLiberumTypeService()->add($data);
            return $result + ['data' => []];
        }
        return $this->fetch('liberum/lib_type_add');
    }

    // 编辑公海类型
    public function libTypeEdit(){
        if(Request::isAjax()){
            $id = input('post.id/d', 0);
            $data = Request::only(['type_name'], 'post');
            $result = $this->getLiberumTypeService()->edit($id, $data);
            return $result + ['data' => []];
        }

        $id = input('id/d', 0);
        $result = LiberumTypeModel::where('id', $id)->where('is_deleted', 0)->find();
        if (empty($result)) {
            return $this->error('记录不存在或已删除');
        }
        $this->assign('result', $result);
        return $this->fetch('liberum/lib_type_edit');
    }

    // 删除公海类型
    public function libTypeDel(){
        $id = input('id/d', 0);
        $result = $this->getLiberumTypeService()->softDelete($id);
        return $result + ['data' => []];
    }

    // 批量删除公海类型（软删除）
    public function libTypeBatchDel()
    {
        return $this->batchLibTypeDel();
    }

    // 批量删除公海类型（软删除）
    public function batchLibTypeDel()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        $result = $this->getLiberumTypeService()->batchSoftDelete($ids);
        return $result + ['data' => []];
    }

    // 导入弹窗页面
    public function libTypeImport()
    {
        return $this->fetch('liberum/lib_type_import');
    }

    // 执行导入
    public function libTypeImportDo()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $file = request()->file('file');
        if (!$file) {
            return ['code' => -200, 'msg' => '请上传Excel文件', 'data' => []];
        }

        $saveDir = Env::get('root_path') . 'runtime' . DIRECTORY_SEPARATOR . 'upload' . DIRECTORY_SEPARATOR . 'excel';
        if (!is_dir($saveDir)) {
            @mkdir($saveDir, 0777, true);
        }

        $info = $file->validate(['size' => 10 * 1024 * 1024, 'ext' => 'xlsx,xls,csv'])->move($saveDir);
        if (!$info) {
            return ['code' => -200, 'msg' => $file->getError() ?: '文件保存失败', 'data' => []];
        }

        $filePath = $info->getPathname();
        $ext = strtolower($info->getExtension());
        $rows = [];
        $isHeaderRow = function ($text) {
            $text = trim((string)$text);
            if ($text === '') {
                return false;
            }
            // 兼容 UTF-8 BOM
            if (strncmp($text, "\xEF\xBB\xBF", 3) === 0) {
                $text = substr($text, 3);
            }
            return mb_strpos($text, '公海类型') !== false || mb_strpos($text, '类型') !== false;
        };

        try {
            if ($ext === 'csv') {
                $handle = fopen($filePath, 'r');
                if (!$handle) {
                    throw new \Exception('CSV文件读取失败');
                }

                $lineNo = 0;
                while (($data = fgetcsv($handle)) !== false) {
                    $lineNo++;
                    $data = array_pad($data, 1, '');
                    $typeName = trim((string)$data[0]);

                    // 首行表头兼容：包含“公海类型”或“类型”则跳过
                    if ($lineNo === 1 && $isHeaderRow($typeName)) {
                        continue;
                    }

                    if ($typeName === '') {
                        continue;
                    }
                    $rows[] = ['type_name' => $typeName];
                }
                fclose($handle);
            } else {
                if (!class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    return [
                        'code' => -200,
                        'msg' => '服务器未安装 PhpSpreadsheet，无法解析 .xlsx/.xls，请安装 phpoffice/phpspreadsheet 或改用 CSV 导入',
                        'data' => [],
                    ];
                }

                $spreadsheet = IOFactory::load($filePath);
                $sheet = $spreadsheet->getActiveSheet();
                $highestRow = $sheet->getHighestRow();

                $firstCell = trim((string)$sheet->getCell('A1')->getValue());
                $hasHeader = $isHeaderRow($firstCell);
                $start = $hasHeader ? 2 : 1;

                for ($r = $start; $r <= $highestRow; $r++) {
                    $typeName = trim((string)$sheet->getCell("A{$r}")->getValue());
                    if ($typeName === '') {
                        continue;
                    }
                    $rows[] = ['type_name' => $typeName];
                }
            }
        } catch (\Throwable $e) {
            return ['code' => -200, 'msg' => '解析失败：' . $e->getMessage(), 'data' => []];
        }

        $result = $this->getLiberumTypeService()->importRows($rows);
        return $result + ['data' => []];
    }

    // 下载导入模板（CSV）
    public function libTypeTpl()
    {
        $filename = '公海类型导入模板_' . date('Ymd_His') . '.csv';

        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

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

        // UTF-8 BOM，兼容 Excel 打开中文
        echo "\xEF\xBB\xBF";

        // 仅 1 列模板：公海类型
        echo $csvLine(['公海类型']);
        echo $csvLine(['无效公海']);
        echo $csvLine(['待分配公海']);
        exit;
    }

    // 公海搜索
    public function liberumSearch(){
        $page = input('page') ?: 1;
        $limit = input('limit') ?: config('pageSize');
        $keyword = input('keyword/a', []);
        $list = model('liberum')->getLiberumPageList($page, $limit, $keyword);
        return ['code' => 0, 'msg' => '获取成功!', 'data' => $list['data'], 'count' => $list['total'], 'rel' => 1];
    }

    /**
     * 获取表格列宽
     * 路由：Liberum/getColWidths
     * 入参：page_key、table_id
     * 返回：{code:0,msg:'获取成功',data:{field:width}}
     */
    public function getColWidths()
    {
        if (!request()->isAjax()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = Session::get('aid');
        $pageKey = Request::param('page_key', '');
        $tableId = Request::param('table_id', '');

        if (empty($adminId)) {
            return json(['code' => 401, 'msg' => '未登录', 'data' => []]);
        }

        if (empty($pageKey) || empty($tableId)) {
            return json(['code' => 0, 'msg' => '获取成功', 'data' => []]);
        }

        try {
            $list = Db::table('crm_table_colwidth')
                ->where('admin_id', $adminId)
                ->where('page_key', $pageKey)
                ->where('table_id', $tableId)
                ->field('field,width')
                ->select();

            $widths = [];
            if (!empty($list)) {
                foreach ($list as $item) {
                    if (empty($item['field'])) {
                        continue;
                    }
                    $width = (int)$item['width'];
                    if ($width >= 30 && $width <= 3000) {
                        $widths[$item['field']] = $width;
                    }
                }
            }

            return json(['code' => 0, 'msg' => '获取成功', 'data' => $widths]);
        } catch (\Exception $e) {
            // 表不存在或读取失败时不影响页面
            return json(['code' => 0, 'msg' => '获取成功', 'data' => []]);
        }
    }

    /**
     * 保存表格列宽
     * 路由：Liberum/saveColWidths
     * 入参：page_key、table_id、widths(JSON字符串或数组)
     * 返回：{code:0,msg:'保存成功',data:[]}
     */
    public function saveColWidths()
    {
        if (!request()->isPost()) {
            return json(['code' => 500, 'msg' => '非法请求', 'data' => []]);
        }

        $adminId = Session::get('aid');
        $pageKey = Request::param('page_key', '');
        $tableId = Request::param('table_id', '');
        $widthsParam = Request::param('widths', '');

        if (empty($adminId) || empty($pageKey) || empty($tableId)) {
            return json(['code' => 500, 'msg' => '参数不完整', 'data' => []]);
        }

        $widths = [];
        if (is_array($widthsParam)) {
            $widths = $widthsParam;
        } elseif (!empty($widthsParam)) {
            $decoded = json_decode($widthsParam, true);
            if (is_array($decoded)) {
                $widths = $decoded;
            }
        }

        if (empty($widths) || !is_array($widths)) {
            return json(['code' => 400, 'msg' => '列宽数据格式错误或为空', 'data' => []]);
        }

        $validRows = [];
        foreach ($widths as $field => $width) {
            if (empty($field) || !is_string($field)) {
                continue;
            }
            if (!preg_match('/^[a-zA-Z0-9_]+$/', $field)) {
                continue;
            }
            $intWidth = (int)$width;
            if ($intWidth < 30 || $intWidth > 3000) {
                continue;
            }
            $validRows[$field] = $intWidth;
        }

        if (empty($validRows)) {
            return json(['code' => 400, 'msg' => '没有有效的列宽数据', 'data' => []]);
        }

        try {
            Db::startTrans();

            // 同一用户+页面+表格每次以最新快照覆盖，保证每个字段仅一条有效记录
            Db::table('crm_table_colwidth')
                ->where('admin_id', $adminId)
                ->where('page_key', $pageKey)
                ->where('table_id', $tableId)
                ->delete();

            $currentTime = time();
            foreach ($validRows as $field => $intWidth) {
                Db::table('crm_table_colwidth')->insert([
                    'admin_id'   => $adminId,
                    'page_key'   => $pageKey,
                    'table_id'   => $tableId,
                    'field'      => $field,
                    'width'      => $intWidth,
                    'updated_at' => $currentTime,
                ]);
            }

            Db::commit();
            return json(['code' => 0, 'msg' => '保存成功', 'data' => []]);
        } catch (\Exception $e) {
            Db::rollback();
            return json(['code' => 500, 'msg' => '保存失败', 'data' => []]);
        }
    }

    // 写跟进
    public function libdialog(){
        $id = Request::param('id');
        $result = Db::table('crm_leads')->where(['id' => $id])->find();
        
        $result['comment'] = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where(['leads_id' => $id])
            ->field('com.*,adm.username,adm.avatar')
            ->select();
        
        foreach ($result['comment'] as $k => $v){
            $result['comment'][$k]['reply'] = Db::table('crm_reply')->where(['comment_id' => $v['id']])->select();
        }

        $this->assign('result', $result);
        return $this->fetch('liberum/libdialog');
    }

    // 抢客户
    public function robClient(){
        $leadId = Request::param('id/d', 0);
        $curname = Session::get('username');
        $adminId = Session::get('aid');

        if ($leadId <= 0) {
            return ['code' => -200, 'msg' => '参数错误', 'data' => []];
        }

        Db::startTrans();
        try {
            $ghClient = Db::table('crm_leads')
                ->where('id', $leadId)
                ->where('status', 2)
                ->lock(true)
                ->find();

            if (empty($ghClient)) {
                Db::rollback();
                return ['code' => -200, 'msg' => '该客户已被其他人领取或已不在公海', 'data' => []];
            }

            // 保留原有系统限制：月领取次数限制 + 持有客户数量限制
            $curget = Db::table('admin')->where(['username' => $curname])->field('curgetnum')->find();
            $sysinfo = Db::table('system')->where(['id' => 1])->field('maxgetnum,custlimit')->find();
            if (!empty($curget) && !empty($sysinfo) && $curget['curgetnum'] >= $sysinfo['maxgetnum']) {
                Db::rollback();
                return ['code' => -200, 'msg' => "抱歉，您当月抢的次数已经达到上限{$sysinfo['maxgetnum']}次!", 'data' => []];
            }

            $wherecust = [
                'pr_user' => $curname,
                'status' => 1,
                'ispublic' => 2,
                'issuccess' => -1
            ];
            $maxcustnum = Db::table('crm_leads')->where($wherecust)->count('id');
            if (!empty($sysinfo) && $maxcustnum >= $sysinfo['custlimit']) {
                Db::rollback();
                return ['code' => -200, 'msg' => "抱歉，您抢得的客户数量已达上限{$sysinfo['custlimit']}!", 'data' => []];
            }

            $today = date('Y-m-d');
            $todayPickedCount = Db::table('crm_liberum_pick_log')
                ->where('pick_user', $curname)
                ->where('pick_date', $today)
                ->count('id');
            $dailyPickLimit = $this->getLiberumConfigService()->getIntValue('daily_pick_limit', 10, 1, 999);
            if ($todayPickedCount >= $dailyPickLimit) {
                Db::rollback();
                return ['code' => -200, 'msg' => "今日领取客户数量已达到上限{$dailyPickLimit}个", 'data' => []];
            }

            $now = date("Y-m-d H:i:s");
            $beforeGhTypeId = isset($ghClient['pr_gh_type']) ? (int)$ghClient['pr_gh_type'] : 0;
            $beforeGhTypeName = '';
            if ($beforeGhTypeId > 0) {
                $beforeGhTypeName = (string)Db::table('crm_liberum_type')
                    ->where('id', $beforeGhTypeId)
                    ->where('is_deleted', 0)
                    ->value('type_name');
            }
            $updateData = [
                'status' => 1,
                'pr_user_bef' => isset($ghClient['pr_user']) ? $ghClient['pr_user'] : '',
                'pr_user' => $curname,
                'pr_gh_type' => null,
                'to_kh_time' => $now,
                'ut_time' => $now,
                'ispublic' => 2
            ];
            $result = Db::table('crm_leads')
                ->where('id', $leadId)
                ->where('status', 2)
                ->update($updateData);

            if ($result !== 1) {
                Db::rollback();
                return ['code' => -200, 'msg' => '该客户已被其他人领取', 'data' => []];
            }

            $logData = [
                'leads_id' => $leadId,
                'active_leads_id' => $leadId,
                'kh_name' => isset($ghClient['kh_name']) ? $ghClient['kh_name'] : '',
                'before_pr_user' => isset($ghClient['pr_user']) ? $ghClient['pr_user'] : '',
                'pick_user' => $curname,
                'before_status' => 2,
                'after_status' => 1,
                'pick_time' => $now,
                'pick_date' => $today,
                'operator_id' => $adminId,
                'operator_name' => $curname,
                'is_returned' => 0,
                'create_time' => time()
            ];
            if ($this->tableHasColumn('crm_liberum_pick_log', 'pr_gh_type')) {
                $logData['pr_gh_type'] = $beforeGhTypeId > 0 ? $beforeGhTypeId : 0;
            }
            if ($this->tableHasColumn('crm_liberum_pick_log', 'gh_type_id')) {
                $logData['gh_type_id'] = $beforeGhTypeId > 0 ? $beforeGhTypeId : 0;
            }
            if ($this->tableHasColumn('crm_liberum_pick_log', 'gh_type')) {
                $logData['gh_type'] = $beforeGhTypeName !== '' ? $beforeGhTypeName : '';
            }
            Db::table('crm_liberum_pick_log')->insert($logData);

            Db::commit();
            return ['code' => 0, 'msg' => '提取客户成功！', 'data' => []];
        } catch (\Exception $e) {
            Db::rollback();
            $errorMsg = $e->getMessage();
            if (stripos($errorMsg, 'uk_active_leads_id') !== false || (stripos($errorMsg, 'Duplicate entry') !== false && stripos($errorMsg, 'active_leads_id') !== false)) {
                return ['code' => -200, 'msg' => '该客户已被其他人领取，请刷新列表', 'data' => []];
            }
            return ['code' => -200, 'msg' => '抢客户失败！', 'data' => []];
        }
    }

     // 获取客户详细信息接口
    public function getClientDetails() {
        // 显式定义目录分隔符常量
        defined('DS') || define('DS', DIRECTORY_SEPARATOR);
        
        // 构建日志路径并确保跨平台兼容性
        $appPath = defined('APP_PATH') ? APP_PATH : __DIR__ . '..' . DS . '..' . DS . '..' . DS . '..' . DS;
        $logPath = $appPath . 'runtime' . DS . 'log' . DS . 'admin' . DS;
        
        // 优化目录创建逻辑
        if (!is_dir($logPath)) {
            // 添加递归创建参数并增强错误处理
            if (!mkdir($logPath, 0755, true)) {
                // 记录目录创建失败日志
                error_log("Failed to create log directory: $logPath");
            }
        }
        
        // 初始化日志配置
        Log::init([
            'type' => 'File',
            'path' => $logPath,
            'level' => ['error', 'debug', 'sql']
        ]);
        
        try {
            $id = input('id');
            if(!is_numeric($id)) {
                return ['code' => 1, 'msg' => '参数错误'];
            }

            // 查询客户基本信息
            $clientInfo = db('crm_leads')
                ->alias('l')
                ->join('crm_contacts c', 'l.id = c.leads_id', 'left')
                ->where(['l.id' => $id, 'l.status' => 2])
                ->field([
                    'l.*',
                    // 获取所有联系方式
                    "SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ".ContactMap::CONTACT_MAP['whatsapp']." THEN c.contact_value END), ',', 1) AS whatsapp",
                    "SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ".ContactMap::CONTACT_MAP['email']." THEN c.contact_value END), ',', 1) AS email",
                    "SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ".ContactMap::CONTACT_MAP['phone']." THEN c.contact_value END), ',', 1) AS phone",
                    "SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ".ContactMap::CONTACT_MAP['ali_id']." THEN c.contact_value END), ',', 1) AS ali_id",
                    "SUBSTRING_INDEX(GROUP_CONCAT(CASE WHEN c.contact_type = ".ContactMap::CONTACT_MAP['wechat']." THEN c.contact_value END), ',', 1) AS wechat"
                ])
                ->group('l.id')
                ->find();

            if(!$clientInfo) {
                return ['code' => 1, 'msg' => '客户信息不存在'];
            }

            // 权限验证
            $currentUser = Session::get('username');
            if($clientInfo['at_user'] !== $currentUser && !in_array(Session::get('role'), ['admin', 'manager'])) {
                return ['code' => 1, 'msg' => '无权查看该客户信息'];
            }

            return ['code' => 0, 'msg' => '获取成功', 'data' => $clientInfo];
            
        } catch (\Exception $e) {
            // 记录详细错误日志
            Log::record('getClientDetails Error: ' . $e->getMessage());
            Log::record('Trace: ' . $e->getTraceAsString());
            
            return ['code' => 1, 'msg' => '服务器内部错误'];
        }
    }

    /**
     * 公海高危批量操作权限校验。
     * 允许：超级管理员 / 管理员 / 具备当前节点权限的后台人员。
     * 兜底：aid=1、role=admin|super_admin、用户名命中超管识别逻辑。
     */
    private function hasLiberumDangerOperationPermission(): bool
    {
        try {
            $adminId = (int)Session::get('aid');
            if ($adminId <= 0) {
                return false;
            }

            $role = strtolower(trim((string)Session::get('role')));
            if (in_array($role, ['admin', 'super_admin'], true)) {
                return true;
            }
            if ($adminId === 1) {
                return true;
            }

            $prefix = config('database.prefix');
            $adminTable = $prefix . 'admin';
            $authGroupTable = $prefix . 'auth_group';
            $authRuleTable = $prefix . 'auth_rule';

            // 仅查询稳定存在的字段，避免因历史库结构差异导致 SQL 报错。
            $adminInfo = Db::table($adminTable)
                ->where('admin_id', $adminId)
                ->field('admin_id,group_id,username')
                ->find();

            $groupId = (int)($adminInfo['group_id'] ?? Session::get('group_id'));
            $username = strtolower(trim((string)($adminInfo['username'] ?? Session::get('username'))));

            // 与项目已有超管识别习惯保持兼容（包括历史特殊管理员账号）。
            $specialAdminIds = [395, 350, 375, 387];
            if (
                $groupId === 1
                || $username === 'admin'
                || in_array($adminId, $specialAdminIds, true)
            ) {
                return true;
            }

            // 复用项目现有 auth_rule + auth_group.rules 节点权限模式，并兼容多种 href 存储格式。
            $controller = (string)request()->controller();
            $action = (string)request()->action();
            $controllerLower = strtolower($controller);
            $actionLower = strtolower($action);
            $hrefCandidates = array_values(array_unique(array_filter([
                $controllerLower . '/' . $actionLower,
                $controller . '/' . $action,
                '/admin/' . $controllerLower . '/' . $actionLower,
                '/admin/' . $controller . '/' . $action,
                $actionLower,
                $action,
            ], function ($item) {
                return $item !== '';
            })));

            $ruleIds = Db::table($authRuleTable)
                ->whereIn('href', $hrefCandidates)
                ->column('id');
            if (!empty($ruleIds)) {
                $rules = Db::table($adminTable)->alias('a')
                    ->join($authGroupTable . ' ag', 'a.group_id = ag.group_id', 'left')
                    ->where('a.admin_id', $adminId)
                    ->value('ag.rules');

                if (!empty($rules)) {
                    $ruleList = array_values(array_filter(array_map('trim', explode(',', (string)$rules)), function ($item) {
                        return $item !== '';
                    }));
                    foreach ($ruleIds as $ruleId) {
                        if (in_array((string)$ruleId, $ruleList, true)) {
                            return true;
                        }
                    }
                }
            }

            return false;
        } catch (\Throwable $e) {
            Log::record('公海高危操作权限校验异常：' . $e->getMessage(), 'error');
            return false;
        }
    }

    private function downloadExcel($filename, array $headers, array $rows)
    {
        $spreadsheet = new Spreadsheet();
        $sheet = $spreadsheet->getActiveSheet();

        $col = 1;
        foreach ($headers as $header) {
            $sheet->setCellValueByColumnAndRow($col, 1, (string)$header);
            $col++;
        }

        $rowNum = 2;
        foreach ($rows as $row) {
            $col = 1;
            foreach ($row as $value) {
                $sheet->setCellValueByColumnAndRow($col, $rowNum, (string)$value);
                $col++;
            }
            $rowNum++;
        }

        foreach (range('A', $sheet->getHighestColumn()) as $letter) {
            $sheet->getColumnDimension($letter)->setAutoSize(true);
        }

        if (function_exists('ob_get_level')) {
            while (ob_get_level() > 0) {
                ob_end_clean();
            }
        }

        $writer = new Xlsx($spreadsheet);
        $encodedFilename = rawurlencode($filename);
        header('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        header("Content-Disposition: attachment; filename=\"{$filename}\"; filename*=UTF-8''{$encodedFilename}");
        header('Cache-Control: max-age=0');
        header('Cache-Control: max-age=1');
        header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
        header('Last-Modified: ' . gmdate('D, d M Y H:i:s') . ' GMT');
        header('Cache-Control: cache, must-revalidate');
        header('Pragma: public');
        $writer->save('php://output');
        exit;
    }

    /**
     * 检测表字段是否存在，避免历史库结构差异导致 SQL 报错。
     */
    private function tableHasColumn($tableName, $columnName)
    {
        static $columnCache = [];
        $cacheKey = strtolower((string)$tableName . '.' . (string)$columnName);
        if (array_key_exists($cacheKey, $columnCache)) {
            return (bool)$columnCache[$cacheKey];
        }

        try {
            $fields = Db::getTableFields($tableName);
            $exists = is_array($fields) && in_array($columnName, $fields, true);
        } catch (\Throwable $e) {
            $exists = false;
        }
        $columnCache[$cacheKey] = $exists ? 1 : 0;
        return $exists;
    }
}