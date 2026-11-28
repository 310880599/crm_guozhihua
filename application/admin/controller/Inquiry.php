<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use app\admin\model\Admin;
use app\admin\service\InquiryChannelStatusService;
use PhpOffice\PhpSpreadsheet\IOFactory;

class Inquiry extends Common
{
    /** @var InquiryChannelStatusService|null */
    private $channelStatusService = null;

    private function channelStatusService(): InquiryChannelStatusService
    {
        if ($this->channelStatusService === null) {
            $this->channelStatusService = new InquiryChannelStatusService();
        }
        return $this->channelStatusService;
    }

    public function index()
    {
        if (request()->isPost()) {
            return $this->inquirySearch();
        }
        return $this->fetch();
    }

    public function inquirySearch()
    {
        $current_admin = Admin::getMyInfo();
        $inquiry_name = Request::param('inquiry_name');
        $pageSize = Request::param('limit', 10);
        $page = Request::param('page', 1);
        $query = Db::name('crm_inquiry');

        if (!empty($inquiry_name)) {
            $query->where('inquiry_name', 'like', '%' . $inquiry_name . '%');
        }

        // 状态筛选：空=全部；0=启用；1=已停用
        $status = Request::param('status', '');
        if ($status !== '' && $status !== null) {
            $query->where('status', (int)$status);
        }

        $list = $query->where([$this->getOrgWhere($current_admin['org'])])->order('id desc')->paginate([
            'list_rows' => $pageSize,
            'page' => $page
        ])->toArray();

        return json([
            'code'  => 0,
            'msg'   => '获取成功!',
            'data'  => $list['data'],
            'count' => $list['total'],
            'rel'   => 1
        ]);
    }

    public function add()
    {
        if (request()->isPost()) {
            $inquiry_name = Request::param('inquiry_name');
            $current_admin = Admin::getMyInfo();
            if (empty($inquiry_name)) {
                return $this->result([], 500, '询盘来源不能为空');
            }

            $current_admin = Admin::getMyInfo();
            $exists = Db::name('crm_inquiry')
                ->where('inquiry_name', $inquiry_name)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->find();

            if (!$exists) {
                $data = [
                    'inquiry_name' => $inquiry_name,
                    'org' => $current_admin['org'],
                    'status' => InquiryChannelStatusService::STATUS_ENABLED,
                    'add_time' => time(),
                    'edit_time' => time(),
                    'submit_person' => $current_admin['username']
                ];
                Db::name('crm_inquiry')->insert($data);
                return $this->result([], 200, '操作成功');
            } else {
                return $this->result([], 500, '询盘来源已存在');
            }
        }
        return $this->fetch();
    }

    public function edit()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return $this->result([], 500, '参数错误');
        }

        $result = Db::name('crm_inquiry')->where('id', $id)->find();
        if (empty($result)) {
            return $this->result([], 500, '参数错误');
        }


        // 权限判定
        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin');

        if (request()->isPost()) {
            $inquiry_name = Request::param('inquiry_name');
            if (empty($inquiry_name)) {
                return $this->result([], 500, '询盘来源不能为空');
            }

            $current_admin = Admin::getMyInfo();
            $exists = Db::name('crm_inquiry')
                ->where('inquiry_name', $inquiry_name)
                ->where('id', '<>', $id)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->find();

            if ($exists) {
                return $this->result([], 500, '询盘来源已存在');
            }

            $current_time = time();

            // 非超管：限制只能修改自己提交的记录
            $updateQuery = Db::name('crm_inquiry')->where('id', $id);
            if (!$isSuper) {
                $updateQuery->where('submit_person', $current_admin['username']);
            }

            $aff = $updateQuery->update([
                'inquiry_name' => $inquiry_name,
                'edit_time'    => $current_time
            ]);

            if ($aff) {
                return $this->result([], 200, '操作成功');
            } else {
                // 可能是无权限或未变更
                if (!$isSuper && $result['submit_person'] !== $current_admin['username']) {
                    return $this->result([], 500, '无权限操作他人记录');
                }
                return $this->result([], 500, '操作失败');
            }
        }

        // GET：非超管禁止打开他人记录编辑页
        if (!$isSuper && ($result['submit_person'] ?? '') !== ($current_admin['username'] ?? '')) {
            return $this->error('无权限编辑他人记录');
        }


        $this->assign('result', $result);
        return $this->fetch();
    }

    /**
     * 停用询盘来源（原删除入口，改为 status=1，保留历史关联）
     */
    public function del()
    {
        if (!request()->isPost()) {
            return $this->result([], 500, '非法请求');
        }
        $id = $this->channelStatusService()->parseStrictPositiveInt(Request::param('id'));
        if ($id === null) {
            return $this->result([], 500, '参数错误');
        }
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->disableInquiry($id, $current_admin ?: []);
        return $this->result($ret['data'] ?? [], $ret['code'], $ret['msg']);
    }

    /**
     * 批量停用询盘来源
     */
    public function batchDel()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->batchDisableInquiry($ids, $current_admin ?: []);
        return json([
            'code' => $ret['code'],
            'msg' => $ret['msg'],
            'data' => $ret['data'] ?? [],
        ]);
    }

    /**
     * 启用询盘来源
     */
    public function enable()
    {
        if (!request()->isPost()) {
            return $this->result([], 500, '非法请求');
        }
        $id = $this->channelStatusService()->parseStrictPositiveInt(Request::param('id'));
        if ($id === null) {
            return $this->result([], 500, '参数错误');
        }
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->enableInquiry($id, $current_admin ?: []);
        return $this->result($ret['data'] ?? [], $ret['code'], $ret['msg']);
    }

    /**
     * 批量启用询盘来源
     */
    public function batchEnable()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->batchEnableInquiry($ids, $current_admin ?: []);
        return json([
            'code' => $ret['code'],
            'msg' => $ret['msg'],
            'data' => $ret['data'] ?? [],
        ]);
    }



    // 导入页（弹窗）
    public function import()
    {
        // 渲染 view/inquiry/import.html
        return $this->fetch();
    }

    // 执行导入
    public function importDo()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }

        $file = request()->file('file');
        if (!$file) {
            return json(['code' => -200, 'msg' => '请上传Excel或CSV文件']);
        }

        // 上传临时信息
        $info    = $file->getInfo();           // ['name'=>..., 'tmp_name'=>...]
        $tmpPath = $info['tmp_name'];
        $ext     = strtolower(pathinfo($info['name'], PATHINFO_EXTENSION));

        // 解析为二维数组 rows: [inquiry_name, org(optional)]
        $rows = [];
        try {
            if ($ext === 'csv') {
                $fp = fopen($tmpPath, 'r');
                if (!$fp) throw new \Exception('CSV文件读取失败');

                // 去BOM
                $first = fgets($fp);
                if ($first === false) {
                    fclose($fp);
                    throw new \Exception('CSV空文件');
                }
                if (substr($first, 0, 3) === "\xEF\xBB\xBF") $first = substr($first, 3);
                $buffer = $first . stream_get_contents($fp);
                fclose($fp);

                $tmp = tmpfile();
                fwrite($tmp, $buffer);
                fseek($tmp, 0);

                $lineNo = 0;
                while (($data = fgetcsv($tmp)) !== false) {
                    $lineNo++;
                    // 首行含“询盘/所属”视作表头
                    $joined = implode('', $data);
                    if ($lineNo === 1 && (mb_strpos($joined, '询盘') !== false || mb_strpos($joined, '所属') !== false)) {
                        continue;
                    }
                    $data = array_pad($data, 2, '');
                    $rows[] = [
                        trim((string)$data[0]), // inquiry_name
                        trim((string)$data[1]), // org (optional)
                    ];
                }
                fclose($tmp);
            } else {
                // xlsx/xls 依赖 phpoffice/phpspreadsheet
                if (!class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    return json([
                        'code' => -200,
                        'msg'  => '服务器未安装 phpoffice/phpspreadsheet，暂不支持 .xlsx/.xls，请改用 CSV 或安装依赖后再试'
                    ]);
                }
                $spreadsheet = \PhpOffice\PhpSpreadsheet\IOFactory::load($tmpPath);
                $sheet      = $spreadsheet->getActiveSheet();
                $highestRow = $sheet->getHighestRow();

                $firstRow = [
                    (string)$sheet->getCell('A1')->getValue(),
                    (string)$sheet->getCell('B1')->getValue(),
                ];
                $hasHeader = (mb_strpos(implode('', $firstRow), '询盘') !== false) || (mb_strpos(implode('', $firstRow), '所属') !== false);
                $start = $hasHeader ? 2 : 1;

                for ($r = $start; $r <= $highestRow; $r++) {
                    $a = trim((string)$sheet->getCell("A{$r}")->getValue());
                    $b = trim((string)$sheet->getCell("B{$r}")->getValue());
                    if ($a === '' && $b === '') continue;
                    $rows[] = [$a, $b];
                }
            }
        } catch (\Throwable $e) {
            return json(['code' => -200, 'msg' => '解析失败：' . $e->getMessage()]);
        }

        if (empty($rows)) {
            return json(['code' => -200, 'msg' => '没有可导入的数据']);
        }

        // 入库
        $now  = time();
        $inserted     = 0;
        $skippedExist = 0;
        $skippedEmpty = 0;

        $current_admin = \app\admin\model\Admin::getMyInfo();
        $loginOrg      = $current_admin['org'] ?? '';
        $user          = $current_admin['username'] ?? '';

        // 简单归一化 org：若非 admin 且不含分隔符，则包裹分隔符
        $normalizeOrg = function ($org) {
            $org = trim((string)$org);
            if ($org === '') return '';
            if ($org === 'admin') return 'admin';
            // 使用 Common::$org_fgx = ',' 的约定
            if (strpos($org, ',') === false) return ',' . $org . ',';
            return $org;
        };

        foreach ($rows as $row) {
            list($inquiry_name, $orgFromFile) = $row;

            if ($inquiry_name === '') {
                $skippedEmpty++;
                continue;
            }

            $orgToUse = $orgFromFile !== '' ? $normalizeOrg($orgFromFile) : $loginOrg;
            if ($orgToUse === '') $orgToUse = $loginOrg;

            // 去重：同组织 + 同供应商名称
            $exists = \think\Db::name('crm_inquiry')
                ->where('inquiry_name', $inquiry_name)
                ->where([$this->getOrgWhere($orgToUse)])
                ->find();
            if ($exists) {
                $skippedExist++;
                continue;
            }

            $ok = \think\Db::name('crm_inquiry')->insert([
                'inquiry_name' => $inquiry_name,
                'org'           => $orgToUse,
                'status'        => InquiryChannelStatusService::STATUS_ENABLED,
                'add_time'      => $now,
                'edit_time'     => $now,
                'submit_person' => $user,
            ]);
            if ($ok) $inserted++;
        }

        return json([
            'code' => 0,
            'msg'  => '导入完成',
            'data' => [
                'inserted'      => $inserted,
                'skipped_exist' => $skippedExist,
                'skipped_empty' => $skippedEmpty,
            ],
        ]);
    }




    // 下载导入模板（CSV）
    public function tpl()
    {
        $filename = '询盘来源导入模板_' . date('Ymd_His') . '.csv';

        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        // A=询盘来源名称（必填），B=所属组织（可选；为空则按当前登录人的组织）
        $header   = ['询盘来源名称', '所属组织(可选)'];
        $examples = [
            ['喷播机供应商', '豫工'],
            ['A厂家',        '豫工'],
            ['B厂家',        ''],     // 留空=使用当前登录人组织
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

        // UTF-8 BOM
        echo "\xEF\xBB\xBF";
        echo $csvLine($header);
        foreach ($examples as $row) echo $csvLine($row);
        exit;
    }
}
