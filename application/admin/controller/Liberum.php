<?php
namespace app\admin\controller;
use think\facade\Request;
use think\Db;
use think\facade\Session;
use think\facade\Env;
use app\admin\behavior\ContactMap; 
use app\admin\model\LiberumType as LiberumTypeModel;
use app\admin\service\LiberumLogService;
use app\admin\service\LiberumTypeService;
use PhpOffice\PhpSpreadsheet\IOFactory;

class Liberum extends Common{

    /**
     * @var LiberumTypeService
     */
    protected $liberumTypeService;
    protected $liberumLogService;
    protected $dailyPickLimit = 10;

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
        return $this->fetch('liberum/picklog');
    }

    // 客户流入公海记录页面
    public function inLog()
    {
        return $this->fetch('liberum/inlog');
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

    // 客户提取记录批量退回公海
    public function batchReturnToLiberum()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
        }

        $ids = input('post.ids/a', []);
        if (empty($ids) || !is_array($ids)) {
            return ['code' => -200, 'msg' => '请选择需要退回的记录', 'data' => []];
        }

        $operatorInfo = [
            'admin_id' => (int)Session::get('aid'),
            'username' => (string)Session::get('username'),
        ];

        $result = $this->getLiberumLogService()->batchReturnToLiberum($ids, $operatorInfo);
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

    // 客户流入公海记录批量恢复原负责人
    public function batchRestoreOwner()
    {
        if (!request()->isPost()) {
            return ['code' => -200, 'msg' => '非法请求', 'data' => []];
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
        return $result + ['data' => []];
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
            if ($todayPickedCount >= $this->dailyPickLimit) {
                Db::rollback();
                return ['code' => -200, 'msg' => "今日领取客户数量已达到上限{$this->dailyPickLimit}个", 'data' => []];
            }

            $now = date("Y-m-d H:i:s");
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
        \think\Log::init([
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
            \think\Log::record('getClientDetails Error: ' . $e->getMessage());
            \think\Log::record('Trace: ' . $e->getTraceAsString());
            
            return ['code' => 1, 'msg' => '服务器内部错误'];
        }
    }
}