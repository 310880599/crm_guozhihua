<?php
namespace app\admin\controller;

use think\facade\Request;
use app\admin\model\Admin;
use app\admin\service\PositionTitleService;
use think\facade\Env;

class PositionTitle extends Common
{
    /**
     * @return PositionTitleService
     */
    protected function service()
    {
        return new PositionTitleService();
    }

    public function initialize()
    {
        parent::initialize();
        $currentAdmin = Admin::getMyInfo();
        if ($currentAdmin['group_id'] != 1
            && $currentAdmin['group_id'] != 15
            && $currentAdmin['username'] != 'admin') {
            $this->error('您无权限访问该模块');
        }
    }

    public function index()
    {
        if (request()->isPost()) {
            return $this->accountSearch();
        }
        return $this->fetch();
    }

    public function accountSearch()
    {
        $params = [
            'title_name' => input('title_name', ''),
            'page' => input('page/d', 1),
            'limit' => input('limit/d', 10),
        ];
        $result = $this->service()->getList($params);
        return json($result);
    }

    public function add()
    {
        if (Request::isPost()) {
            $data = Request::only(['title_name','created_by'], 'post');
            $result = $this->service()->add($data);
            return json($result);
        }
        $currentAdmin = Admin::getMyInfo();
        $this->assign('currentAdmin',$currentAdmin);
        return $this->fetch();
    }

    public function edit()
    {
        $id = input('id/d', 0);
        if (empty($id)) {
            return $this->error('参数错误');
        }

        if (Request::isAjax()) {
            $data = Request::only(['title_name', 'created_by'], 'post');
            $result = $this->service()->edit($id, $data);
            return json($result);
        }

        $entry = $this->service()->getDetail($id);
        if (empty($entry)) {
            return $this->error('记录不存在或已删除');
        }
        $this->assign('entry',$entry);
        return $this->fetch();
    }


    public function del()
    {
        $id = input('id/d',0);
        $result = $this->service()->delete($id, (int)session('aid'));
        return json($result);
    }
    
    
    // 批量删除
    public function batchDel()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        // ids 既可为数组也可为逗号串
        $ids = input('post.ids/a', []); // /a 过滤为数组
        $result = $this->service()->batchDelete($ids, (int)session('aid'));
        return json($result);
    }
    
    // 导入页（弹窗）
    public function import()
    {
        return $this->fetch();  // 渲染 view/inquiry_source/import.html
    }
    
    
    
    // 执行导入
    public function importDo()
    {
        if (!request()->isPost()) {
            return json(['code'=>-200,'msg'=>'非法请求']);
        }
    
        // 1) 接收上传文件
        $file = request()->file('file');
        if (!$file) {
            return json(['code'=>-200,'msg'=>'请上传Excel文件']);
        }
    
        // 2) 保存到 runtime/upload/excel
        $saveDir = Env::get('root_path') . 'runtime' . DIRECTORY_SEPARATOR . 'upload' . DIRECTORY_SEPARATOR . 'excel';
        if (!is_dir($saveDir)) {
            @mkdir($saveDir, 0777, true);
        }
        $info = $file->validate(['size'=> 10 * 1024 * 1024, 'ext'=>'xlsx,xls,csv'])->move($saveDir);
        if (!$info) {
            return json(['code'=>-200,'msg'=>$file->getError() ?: '文件保存失败']);
        }
        $filePath = $info->getPathname();
        $ext      = strtolower($info->getExtension());
    
        // 3) 解析
        $rows = [];  // 每个元素：[ 'title_name'=>..., 'created_by'=>...]
        try {
            if ($ext === 'csv') {
                // ---- 解析 CSV ----
                $handle = fopen($filePath, 'r');
                if (!$handle) throw new \Exception('CSV文件读取失败');
                // 尝试去除 UTF-8 BOM
                $first = fgets($handle);
                if (substr($first, 0, 3) === "\xEF\xBB\xBF") $first = substr($first, 3);
                // 放回第一行
                $buffer = $first . stream_get_contents($handle);
                fclose($handle);
                $tmp = tmpfile();
                fwrite($tmp, $buffer);
                fseek($tmp, 0);
    
                $lineNo = 0;
                while (($data = fgetcsv($tmp)) !== false) {
                    $lineNo++;
                    if ($lineNo === 1) {
                        $headerA = trim((string)($data[0] ?? ''));
                        if ($headerA === '职位身份名称' || $headerA === '职位身份') {
                            continue;
                        }
                    }
                    // 兼容列数量不足
                    $data = array_pad($data, 2, '');
                    $rows[] = [
                        'title_name' => trim((string)$data[0]),
                        'created_by' => trim((string)$data[1]),
                    ];
                }
                fclose($tmp);
            } else {
                // ---- 解析 Excel (xlsx/xls) ----
                if (class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    $spreadsheet = \PhpOffice\PhpSpreadsheet\IOFactory::load($filePath);
                    $sheet       = $spreadsheet->getActiveSheet();
                    $highestRow  = $sheet->getHighestRow();
    
                    // 判断首行是否表头
                    $hasHeader = false;
                    $headerA = trim((string)$sheet->getCell('A1')->getValue());
                    if ($headerA === '职位身份名称' || $headerA === '职位身份') {
                        $hasHeader = true;
                    }
    
                    $start = $hasHeader ? 2 : 1;
                    for ($r = $start; $r <= $highestRow; $r++) {
                        $titleName = trim((string)$sheet->getCell("A{$r}")->getValue());
                        $createdBy = trim((string)$sheet->getCell("B{$r}")->getValue());
                        if ($titleName === '' && $createdBy === '') {
                            continue; // 跳过空行
                        }
                        $rows[] = [
                            'title_name' => $titleName,
                            'created_by' => $createdBy,
                        ];
                    }
                } else {
                    return json([
                        'code'=>-200,
                        'msg'=>'服务器未安装 PhpSpreadsheet，无法解析 .xlsx/.xls，请安装 phpoffice/phpspreadsheet 或改用 CSV 导入',
                    ]);
                }
            }
        } catch (\Throwable $e) {
            return json(['code'=>-200,'msg'=>'解析失败：'.$e->getMessage()]);
        }
    
        if (empty($rows)) {
            return json(['code'=>-200,'msg'=>'没有可导入的数据']);
        }
    
        $result = $this->service()->importRows($rows);
        return json($result);
    }
    
    
    
    
    // 下载导入模板（CSV）
    public function tpl()
    {
        // 文件名：职位身份导入模板_YYYYMMDD_HHMMSS.csv
        $filename = '职位身份导入模板_' . date('Ymd_His') . '.csv';
    
        // 工具函数：按 CSV 规范输出一行（自动加双引号并转义内部引号）
        $csvLine = function(array $cols) {
            $safe = array_map(function($v){
                $v = (string)$v;
                $v = str_replace('"', '""', $v); // 转义内部的 "
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };
    
        // 表头（与导入逻辑字段一一对应）
        $header = ['职位身份名称', '创建人'];
    
        // 示例数据（你也可以只保留一行或自定义）
        $examples = [
            [ '百度推广', 'admin', ],
            [ '抖音短视频', '张三', ],
            [ '官网-表单',  '李四', ],
        ];
    
        // 清空缓冲区，避免输出污染
        if (function_exists('ob_get_level')) {
            while (ob_get_level() > 0) { ob_end_clean(); }
        }
    
        // 下载头
        header('Content-Type: text/csv; charset=UTF-8');
        header('Content-Disposition: attachment; filename="'.$filename.'"');
        header('Pragma: no-cache');
        header('Expires: 0');
        header('Cache-Control: must-revalidate, post-check=0, pre-check=0');
    
        // 输出 BOM（让 Excel 正确识别 UTF-8）
        echo "\xEF\xBB\xBF";
    
        // 输出表头
        echo $csvLine($header);
    
        // 输出示例数据
        foreach ($examples as $row) {
            echo $csvLine($row);
        }
        exit;
    }
}
