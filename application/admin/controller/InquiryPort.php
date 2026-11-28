<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\Queue; 
use think\facade\Session;
use think\facade\Log;
use think\exception\HttpResponseException;
use app\admin\model\Admin;
use app\admin\service\InquiryChannelStatusService;
use PhpOffice\PhpSpreadsheet\IOFactory;

class InquiryPort extends Common
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
            return $this->inquiryPortSearch();
        }
        // 管理页筛选保留全部来源（含停用），便于按历史父来源过滤
        $inquiry_list = $this->getInquiryList(false);
        $this->assign('inquiry_list', $inquiry_list);
        return $this->fetch();
    }
    public function inquiryPortSearch()
    {
        $current_admin = Admin::getMyInfo();
        $port_name = Request::param('port_name');
        $pageSize = Request::param('limit', 10);
        $page = Request::param('page', 1);
        $query = Db::name('crm_inquiry_port p')->leftJoin('crm_inquiry c', 'p.inquiry_id = c.id');
        if (!empty($port_name)) {
            $query->where('p.port_name', 'like', '%' . $port_name . '%');
        }
        $inquiry_id = Request::param('inquiry_id');
        if (!empty($inquiry_id)) {
            $query->where('p.inquiry_id', $inquiry_id);
        }
        $status = Request::param('status', '');
        if ($status !== '' && $status !== null) {
            $query->where('p.status', (int)$status);
        }
        $list = $query->field('p.*, c.inquiry_name')->where([$this->getOrgWhere($current_admin['org'], 'p')])->order('p.id desc')->paginate([
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
            //新增运营端口
            $port_name = Request::param('port_name');
            $inquiry_id = $this->channelStatusService()->parseStrictPositiveInt(Request::param('inquiry_id'));

            if (empty($port_name)) {
                return $this->result([], 500, '运营端口不能为空');
            }
            if ($inquiry_id === null) {
                return $this->result([], 500, '请选择询盘来源');
            }

            Db::startTrans();
            $stage = 'init';
            try {
                $stage = 'parent_check';
                $parentCheck = $this->channelStatusService()->assertParentInquiryEnabledForWrite($inquiry_id);
                if (!$parentCheck['ok']) {
                    Db::rollback();
                    return $this->result([], 500, $parentCheck['msg']);
                }
                $stage = 'dup_check';
                $port = $this->checkPortInquiry($port_name, $inquiry_id);
                if ($port) {
                    Db::rollback();
                    return $this->result([], 500, '运营端口已存在');
                }
                $current_admin = Admin::getMyInfo();
                $data = [
                    'org' => $current_admin['org'],
                    'port_name' => $port_name,
                    'inquiry_id' => $inquiry_id,
                    'status' => InquiryChannelStatusService::STATUS_ENABLED,
                    'add_time' => time(),
                    'edit_time' => time(),
                    'submit_person' => $current_admin['username'],
                ];
                $stage = 'insert';
                Db::name('crm_inquiry_port')->insert($data);
                $stage = 'commit';
                Db::commit();
                return $this->result([], 200, '操作成功');
            } catch (HttpResponseException $e) {
                // result()/success() 通过抛 HttpResponseException 结束请求，不得当作业务失败吞掉
                throw $e;
            } catch (\Throwable $e) {
                Db::rollback();
                Log::error(
                    '[inquiry_port_add] ' .
                    json_encode([
                        'stage' => $stage ?? 'unknown',
                        'exception' => get_class($e),
                        'message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                    ], JSON_UNESCAPED_UNICODE)
                );
                return $this->result([], 500, '操作失败，请稍后重试');
            }
        }
        $inquiry_rows = $this->getInquiryList(true);
        $inquiry_list = array_map(function ($row) {
            return [
                'id'   => (int)$row['id'],
                'name' => (string)$row['inquiry_name'],
            ];
        }, $inquiry_rows);
        $inquiry_list = json_encode($inquiry_list, JSON_UNESCAPED_UNICODE);
        $this->assign('inquiry_list', $inquiry_list);
        return $this->fetch();
    }


    public function edit()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return $this->result([], 500, '参数错误');
        }
        $result = Db::name('crm_inquiry_port')->where('id', $id)->find();
        if (empty($result)) {
            return $this->result([], 500, '参数错误');
        }

        // 权限判定
        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin');

        if (request()->isPost()) {
            $port_name = Request::param('port_name');
            $inquiry_id = $this->channelStatusService()->parseStrictPositiveInt(Request::param('inquiry_id'));

            if (empty($port_name)) {
                return $this->result([], 500, '运营端口名称不能为空');
            }
            if ($inquiry_id === null) {
                return $this->result([], 500, '请选择询盘来源');
            }

            Db::startTrans();
            try {
                // 仅当改挂其他父来源时，目标父来源必须启用；同来源改名允许（即使父已停用）
                if ($inquiry_id !== (int)($result['inquiry_id'] ?? 0)) {
                    $parentCheck = $this->channelStatusService()->assertParentInquiryEnabledForWrite($inquiry_id);
                    if (!$parentCheck['ok']) {
                        Db::rollback();
                        return $this->result([], 500, $parentCheck['msg']);
                    }
                }

                // 同组织 + 同询盘来源 下运营端口名不可重复（排除当前ID）
                $exists = Db::name('crm_inquiry_port')
                    ->where('port_name', $port_name)
                    ->where('inquiry_id', '=', $inquiry_id)
                    ->where([$this->getOrgWhere($current_admin['org'])])
                    ->where('id', '<>', $id)
                    ->find();
                if ($exists) {
                    Db::rollback();
                    return $this->result([], 500, '运营端口已存在');
                }

                $current_time = time();

                // 非超管：限制只能修改自己提交的记录
                $updateQuery = Db::name('crm_inquiry_port')->where('id', $id);
                if (!$isSuper) {
                    $updateQuery->where('submit_person', $current_admin['username']);
                }

                $aff = $updateQuery->update([
                    'port_name' => $port_name,
                    'inquiry_id'  => $inquiry_id,
                    'edit_time'    => $current_time
                ]);

                if ($aff) {
                    Db::commit();
                    return $this->result([], 200, '操作成功');
                }
                Db::rollback();
                if (!$isSuper && $result['submit_person'] !== $current_admin['username']) {
                    return $this->result([], 500, '无权限操作他人记录');
                }
                return $this->result([], 500, '操作失败');
            } catch (HttpResponseException $e) {
                // result() 正常结束请求，不得当作业务失败吞掉
                throw $e;
            } catch (\Throwable $e) {
                Db::rollback();
                Log::error(
                    '[inquiry_port_edit] ' .
                    json_encode([
                        'exception' => get_class($e),
                        'message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                    ], JSON_UNESCAPED_UNICODE)
                );
                return $this->result([], 500, '操作失败，请稍后重试');
            }
        }

        // GET：非超管禁止打开他人记录编辑页
        if (!$isSuper && ($result['submit_person'] ?? '') !== ($current_admin['username'] ?? '')) {
            return $this->error('无权限编辑他人记录');
        }

        // 非提交：准备下拉数据为 {id,name} + 默认ID（仅启用父来源 + 当前已选兜底）
        $defaultInquiryId = (int)$result['inquiry_id'];
        $svc = $this->channelStatusService();
        $inquiry_rows = $this->getInquiryList(true);
        $currentInquiry = Db::name('crm_inquiry')->where('id', $defaultInquiryId)->find();
        $inquiry_rows = $svc->mergeDisabledCurrent($inquiry_rows ?: [], $currentInquiry, 'inquiry_name');
        $inquiry_list = array_map(function ($row) {
            return [
                'id'   => (int)$row['id'],
                'name' => (string)$row['inquiry_name'],
            ];
        }, $inquiry_rows);
        $inquiry_list = json_encode($inquiry_list, JSON_UNESCAPED_UNICODE);
        $this->assign('inquiry_list', $inquiry_list);
        $this->assign('default_inquiry_id', $defaultInquiryId);
        $this->assign('result', $result);
        return $this->fetch();
    }

    /**
     * 停用运营端口（原删除入口，改为 status=1）
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
        $ret = $this->channelStatusService()->disablePort($id, $current_admin ?: []);
        return $this->result($ret['data'] ?? [], $ret['code'], $ret['msg']);
    }

    /**
     * 批量停用运营端口
     */
    public function batchDel()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->batchDisablePort($ids, $current_admin ?: []);
        return json([
            'code' => $ret['code'],
            'msg' => $ret['msg'],
            'data' => $ret['data'] ?? [],
        ]);
    }

    /**
     * 启用运营端口
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
        $ret = $this->channelStatusService()->enablePort($id, $current_admin ?: []);
        return $this->result($ret['data'] ?? [], $ret['code'], $ret['msg']);
    }

    /**
     * 批量启用运营端口
     */
    public function batchEnable()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        $current_admin = Admin::getMyInfo();
        $ret = $this->channelStatusService()->batchEnablePort($ids, $current_admin ?: []);
        return json([
            'code' => $ret['code'],
            'msg' => $ret['msg'],
            'data' => $ret['data'] ?? [],
        ]);
    }



    // 导入页（弹窗）
    public function import()
    {
        return $this->fetch();  // 渲染 view/inquiry_port/import.html
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

        // 获取上传临时信息
        $info = $file->getInfo(); // ['name'=>..., 'tmp_name'=>...]
        $tmpPath = $info['tmp_name'];
        $ext = strtolower(pathinfo($info['name'], PATHINFO_EXTENSION));

        // 解析为二维数组 rows：每行是 [运营端口, 询盘来源, 询盘来源ID(可选), 状态(可选)]
        $rows = [];
        try {
            if ($ext === 'csv') {
                $fp = fopen($tmpPath, 'r');
                if (!$fp) throw new \Exception('CSV文件读取失败');

                // 尝试去除UTF-8 BOM
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
                    // 识别并跳过表头（含“运营”或“询盘”字样即视为表头）
                    $joined = implode('', $data);
                    if ($lineNo === 1 && (mb_strpos($joined, '运营') !== false || mb_strpos($joined, '询盘') !== false)) {
                        continue;
                    }
                    $data = array_pad($data, 4, '');
                    $rows[] = [
                        trim((string)$data[0]), // port_name
                        trim((string)$data[1]), // inquiry_name
                        trim((string)$data[2]), // inquiry_id (optional)
                        trim((string)$data[3]), // status (optional)
                    ];
                }
                fclose($tmp);
            } else {
                // xlsx/xls：存在 PhpSpreadsheet 时使用；否则提示改用 CSV
                if (!class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    return json([
                        'code' => -200,
                        'msg' => '服务器未安装 phpoffice/phpspreadsheet，暂不支持 .xlsx/.xls，请使用 CSV 导入或安装依赖后再试'
                    ]);
                }
                $spreadsheet = \PhpOffice\PhpSpreadsheet\IOFactory::load($tmpPath);
                $sheet      = $spreadsheet->getActiveSheet();
                $highestRow = $sheet->getHighestRow();

                // 判断首行是否表头（含“运营”或“询盘”）
                $firstRow = [
                    (string)$sheet->getCell('A1')->getValue(),
                    (string)$sheet->getCell('B1')->getValue(),
                    (string)$sheet->getCell('C1')->getValue(),
                    (string)$sheet->getCell('D1')->getValue(),
                ];
                $hasHeader = (mb_strpos(implode('', $firstRow), '运营') !== false) || (mb_strpos(implode('', $firstRow), '询盘') !== false);
                $start = $hasHeader ? 2 : 1;

                for ($r = $start; $r <= $highestRow; $r++) {
                    $a = trim((string)$sheet->getCell("A{$r}")->getValue());
                    $b = trim((string)$sheet->getCell("B{$r}")->getValue());
                    $c = trim((string)$sheet->getCell("C{$r}")->getValue());
                    $d = trim((string)$sheet->getCell("D{$r}")->getValue());
                    if ($a === '' && $b === '' && $c === '' && $d === '') continue;
                    $rows[] = [$a, $b, $c, $d];
                }
            }
        } catch (\Throwable $e) {
            return json(['code' => -200, 'msg' => '解析失败：' . $e->getMessage()]);
        }

        if (empty($rows)) {
            return json(['code' => -200, 'msg' => '没有可导入的数据']);
        }

        // 入库
        $now = time();
        $inserted = 0;
        $skippedExist = 0;
        $skippedEmpty = 0;
        $skippedInvalidStatus = 0;
        $skippedParentDisabled = 0;
        $failed = 0;
        $createdCats = 0;
        $seenKeys = []; // 防止同一批次文件内重复
        $svc = $this->channelStatusService();

        $current_admin = \app\admin\model\Admin::getMyInfo();
        $org = $current_admin['org'] ?? '';
        $user = $current_admin['username'] ?? '';

        foreach ($rows as $row) {
            list($port_name, $inquiry_name, $inquiry_id_raw, $status_raw) = $row;

            if ($port_name === '' && $inquiry_name === '' && $inquiry_id_raw === '') {
                $skippedEmpty++;
                continue;
            }

            // 解析 status（可选，空→0；仅允许明确 0/1）
            $status = $svc->parseImportStatus($status_raw);
            if ($status === null) {
                $skippedInvalidStatus++;
                continue;
            }

            // 运营端口必填
            if ($port_name === '') {
                $skippedEmpty++;
                continue;
            }

            Db::startTrans();
            try {
                // 解析 inquiry_id：优先C列严格正整数ID，否则用B列名称在当前组织下查找/创建
                // 统一锁序：写入前锁父来源并校验启用，禁止挂靠停用父来源，也禁止与父停用并发绕过
                $inquiry_id = 0;
                $createdThisRow = false;

                $strictInquiryId = $svc->parseStrictPositiveInt($inquiry_id_raw);
                if ($strictInquiryId !== null) {
                    $parentCheck = $svc->assertParentInquiryEnabledForWrite($strictInquiryId);
                    if (!$parentCheck['ok']) {
                        Db::rollback();
                        if (strpos($parentCheck['msg'], '已停用') !== false) {
                            $skippedParentDisabled++;
                        } else {
                            $skippedEmpty++;
                        }
                        continue;
                    }
                    $catById = Db::name('crm_inquiry')
                        ->where('id', $strictInquiryId)
                        ->where([$this->getOrgWhere($org)])
                        ->find();
                    if (!$catById) {
                        Db::rollback();
                        $skippedEmpty++;
                        continue;
                    }
                    $inquiry_id = $strictInquiryId;
                } elseif ($inquiry_name !== '') {
                    $cat = Db::name('crm_inquiry')
                        ->where('inquiry_name', $inquiry_name)
                        ->where([$this->getOrgWhere($org)])
                        ->find();

                    if (!$cat) {
                        // 自动创建该询盘来源（默认启用），再加行锁与启用校验
                        $cid = Db::name('crm_inquiry')->insertGetId([
                            'inquiry_name' => $inquiry_name,
                            'org'           => $org,
                            'status'        => InquiryChannelStatusService::STATUS_ENABLED,
                            'add_time'      => $now,
                            'edit_time'     => $now,
                            'submit_person' => $user,
                        ]);
                        if (!$cid) {
                            Db::rollback();
                            $failed++;
                            continue;
                        }
                        $inquiry_id = (int)$cid;
                        $createdThisRow = true;
                        $parentCheck = $svc->assertParentInquiryEnabledForWrite($inquiry_id);
                        if (!$parentCheck['ok']) {
                            Db::rollback();
                            $failed++;
                            continue;
                        }
                    } else {
                        $inquiry_id = (int)$cat['id'];
                        $parentCheck = $svc->assertParentInquiryEnabledForWrite($inquiry_id);
                        if (!$parentCheck['ok']) {
                            Db::rollback();
                            if (strpos($parentCheck['msg'], '已停用') !== false) {
                                $skippedParentDisabled++;
                            } else {
                                $skippedEmpty++;
                            }
                            continue;
                        }
                    }
                }

                if ($inquiry_id <= 0) {
                    Db::rollback();
                    $skippedEmpty++;
                    continue;
                }

                // 组合唯一键（同组织+同询盘来源+同运营端口 视为同一条）
                $key = md5($org . '|' . $inquiry_id . '|' . $port_name);
                if (isset($seenKeys[$key])) {
                    Db::rollback();
                    $skippedExist++;
                    continue;
                }

                // DB去重：同 org 范围 + inquiry_id + port_name
                $exists = Db::name('crm_inquiry_port')
                    ->where('port_name', $port_name)
                    ->where('inquiry_id', $inquiry_id)
                    ->where([$this->getOrgWhere($org)])
                    ->find();
                if ($exists) {
                    Db::rollback();
                    $skippedExist++;
                    continue;
                }

                $ok = Db::name('crm_inquiry_port')->insert([
                    'port_name'     => $port_name,
                    'org'           => $org,
                    'inquiry_id'    => $inquiry_id,
                    'status'        => $status,
                    'add_time'      => $now,
                    'edit_time'     => $now,
                    'submit_person' => $user,
                ]);
                if ($ok) {
                    Db::commit();
                    $inserted++;
                    $seenKeys[$key] = 1;
                    if ($createdThisRow) {
                        $createdCats++;
                    }
                } else {
                    Db::rollback();
                    $failed++;
                }
            } catch (\Throwable $e) {
                Db::rollback();
                $failed++;
            }
        }

        return json([
            'code' => 0,
            'msg' => '导入完成',
            'data' => [
                'inserted' => $inserted,
                'skipped_exist' => $skippedExist,
                'skipped_empty' => $skippedEmpty,
                'skipped_invalid_status' => $skippedInvalidStatus,
                'skipped_parent_disabled' => $skippedParentDisabled,
                'failed' => $failed,
                'created_cats' => $createdCats,
            ],
        ]);
    }







    // 下载导入模板（CSV）
    public function tpl()
    {
        $filename = '运营端口导入模板_' . date('Ymd_His') . '.csv';

        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        // 表头：A=运营端口 B=询盘来源 C=询盘来源ID(可选) D=状态(0/1，可选)
        $header = ['运营端口', '询盘来源', '询盘来源ID(可选)', '状态(0或1，可选)'];
        $examples = [
            ['喷播机7', '喷播机供应商', '', '0'],
            ['A产品', 'A厂家', '', '0'],
            // 也可直接给出ID，名称留空（推荐更精准）
            ['B产品', '', '23', '1'],
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

        echo "\xEF\xBB\xBF"; // BOM
        echo $csvLine($header);
        foreach ($examples as $row) echo $csvLine($row);
        exit;
    }




}
