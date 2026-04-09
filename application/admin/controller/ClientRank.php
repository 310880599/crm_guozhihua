<?php
namespace app\admin\controller;

use app\admin\model\Admin;
use app\admin\model\ClientRank as ClientRankModel;
use think\Db;
use think\facade\Env;
use think\facade\Request;

class ClientRank extends Common
{
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
            return $this->rankSearch();
        }
        return $this->fetch();
    }

    public function rankSearch()
    {
        $rankName = trim((string)input('rank_name', ''));
        $rankCode = trim((string)input('rank_code', ''));
        $page = input('page/d', 1);
        $limit = input('limit/d', 10);

        $query = ClientRankModel::where('is_deleted', 0);
        if ($rankName !== '') {
            $query->where('rank_name', 'like', "%{$rankName}%");
        }
        if ($rankCode !== '') {
            $query->where('rank_code', 'like', "%{$rankCode}%");
        }

        $list = $query
            ->order('sort asc')
            ->order('id asc')
            ->paginate(['list_rows' => $limit, 'page' => $page])
            ->toArray();

        return json([
            'code'  => 0,
            'msg'   => '获取成功!',
            'data'  => $list['data'],
            'count' => $list['total'],
            'rel'   => 1,
        ]);
    }

    public function add()
    {
        if (Request::isPost()) {
            $post = Request::only(['rank_name', 'rank_code', 'description', 'sort'], 'post');
            $rankName = trim((string)($post['rank_name'] ?? ''));
            $rankCode = trim((string)($post['rank_code'] ?? ''));
            $description = trim((string)($post['description'] ?? ''));
            $sort = is_numeric($post['sort'] ?? null) ? (int)$post['sort'] : 0;

            if ($rankName === '') {
                return json(['code' => -200, 'msg' => '客户级别名称不能为空']);
            }
            if ($rankCode === '') {
                return json(['code' => -200, 'msg' => '客户级别编码不能为空']);
            }

            $existsActive = ClientRankModel::where('is_deleted', 0)
                ->where(function ($query) use ($rankName, $rankCode) {
                    $query->whereOr([
                        ['rank_name', '=', $rankName],
                        ['rank_code', '=', $rankCode],
                    ]);
                })
                ->find();
            if ($existsActive) {
                if ((string)$existsActive['rank_name'] === $rankName) {
                    return json(['code' => -200, 'msg' => '客户级别名称已存在']);
                }
                return json(['code' => -200, 'msg' => '客户级别编码已存在']);
            }

            $existsDeleted = ClientRankModel::where('is_deleted', 1)
                ->where(function ($query) use ($rankName, $rankCode) {
                    $query->whereOr([
                        ['rank_name', '=', $rankName],
                        ['rank_code', '=', $rankCode],
                    ]);
                })
                ->find();

            $currentAdmin = Admin::getMyInfo();
            $adminUsername = trim((string)($currentAdmin['username'] ?? ''));
            $now = time();

            if ($existsDeleted) {
                $res = Db::name('crm_client_rank')
                    ->where('id', $existsDeleted['id'])
                    ->update([
                        'rank_name'    => $rankName,
                        'rank_code'    => $rankCode,
                        'description'  => $description,
                        'sort'         => $sort,
                        'creator'      => $adminUsername,
                        'is_deleted'   => 0,
                        'deleted_time' => null,
                        'deleted_by'   => null,
                        'update_time'  => $now,
                    ]);
                return $res ? json(['code' => 0, 'msg' => '添加成功！']) : json(['code' => -200, 'msg' => '添加失败！']);
            }

            $res = ClientRankModel::create([
                'rank_name'    => $rankName,
                'rank_code'    => $rankCode,
                'description'  => $description,
                'sort'         => $sort,
                'creator'      => $adminUsername,
                'is_deleted'   => 0,
                'deleted_time' => null,
                'deleted_by'   => null,
                // crm_client_rank.add_time 为 NOT NULL，新增时必须显式写入
                'add_time'     => $now,
                'create_time'  => $now,
                'update_time'  => $now,
            ]);
            return $res ? json(['code' => 0, 'msg' => '添加成功！']) : json(['code' => -200, 'msg' => '添加失败！']);
        }

        $currentAdmin = Admin::getMyInfo();
        $this->assign('currentAdmin', $currentAdmin);
        return $this->fetch();
    }

    public function edit()
    {
        $id = input('id/d', 0);
        if (empty($id)) {
            return $this->error('参数错误');
        }

        $entry = ClientRankModel::where('id', $id)->where('is_deleted', 0)->find();
        if (empty($entry)) {
            return $this->error('记录不存在或已删除');
        }

        if (Request::isPost()) {
            $post = Request::only(['rank_name', 'rank_code', 'description', 'sort'], 'post');
            $rankName = trim((string)($post['rank_name'] ?? ''));
            $rankCode = trim((string)($post['rank_code'] ?? ''));
            $description = trim((string)($post['description'] ?? ''));
            $sort = is_numeric($post['sort'] ?? null) ? (int)$post['sort'] : 0;

            if ($rankName === '') {
                return json(['code' => -200, 'msg' => '客户级别名称不能为空']);
            }
            if ($rankCode === '') {
                return json(['code' => -200, 'msg' => '客户级别编码不能为空']);
            }

            $exists = ClientRankModel::where('is_deleted', 0)
                ->where('id', '<>', $id)
                ->where(function ($query) use ($rankName, $rankCode) {
                    $query->whereOr([
                        ['rank_name', '=', $rankName],
                        ['rank_code', '=', $rankCode],
                    ]);
                })
                ->find();
            if ($exists) {
                if ((string)$exists['rank_name'] === $rankName) {
                    return json(['code' => -200, 'msg' => '客户级别名称已存在']);
                }
                return json(['code' => -200, 'msg' => '客户级别编码已存在']);
            }

            $res = Db::name('crm_client_rank')
                ->where('id', $id)
                ->where('is_deleted', 0)
                ->update([
                    'rank_name'   => $rankName,
                    'rank_code'   => $rankCode,
                    'description' => $description,
                    'sort'        => $sort,
                    'update_time' => time(),
                ]);
            if ($res === false) {
                return json(['code' => -200, 'msg' => '修改失败！']);
            }
            return json(['code' => 0, 'msg' => '修改成功！']);
        }

        $this->assign('entry', $entry);
        return $this->fetch();
    }

    public function del()
    {
        $id = input('id/d', 0);
        if (empty($id)) {
            return json(['code' => -200, 'msg' => '参数错误']);
        }

        $now = time();
        $aff = Db::name('crm_client_rank')
            ->where('id', $id)
            ->where('is_deleted', 0)
            ->update([
                'is_deleted'   => 1,
                'deleted_time' => date('Y-m-d H:i:s'),
                'deleted_by'   => session('aid'),
                'update_time'  => $now,
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
            $idStr = trim((string)input('post.ids', ''));
            if ($idStr !== '') {
                $ids = explode(',', $idStr);
            }
        }
        $ids = array_values(array_unique(array_filter(array_map('intval', (array)$ids))));
        if (empty($ids)) {
            return json(['code' => -200, 'msg' => '未选择任何记录']);
        }

        $now = time();
        Db::startTrans();
        try {
            $delCount = Db::name('crm_client_rank')
                ->whereIn('id', $ids)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted'   => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by'   => session('aid'),
                    'update_time'  => $now,
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
                if (substr($first, 0, 3) === "\xEF\xBB\xBF") {
                    $first = substr($first, 3);
                }
                $buffer = $first . stream_get_contents($handle);
                fclose($handle);

                $tmp = tmpfile();
                fwrite($tmp, $buffer);
                fseek($tmp, 0);

                $lineNo = 0;
                while (($data = fgetcsv($tmp)) !== false) {
                    $lineNo++;
                    if ($lineNo === 1) {
                        $headerText = trim(implode('', array_map(function ($v) {
                            return (string)$v;
                        }, $data)));
                        if ($headerText !== '' && (mb_strpos($headerText, '客户级别') !== false || mb_strpos($headerText, '级别名称') !== false)) {
                            continue;
                        }
                    }
                    $data = array_pad($data, 4, '');
                    $rows[] = [
                        'rank_name'   => trim((string)$data[0]),
                        'rank_code'   => trim((string)$data[1]),
                        'description' => trim((string)$data[2]),
                        'sort'        => trim((string)$data[3]),
                    ];
                }
                fclose($tmp);
            } else {
                if (!class_exists('\PhpOffice\PhpSpreadsheet\IOFactory')) {
                    return json([
                        'code' => -200,
                        'msg'  => '服务器未安装 PhpSpreadsheet，无法解析 .xlsx/.xls，请安装 phpoffice/phpspreadsheet 或改用 CSV 导入',
                    ]);
                }

                $spreadsheet = \PhpOffice\PhpSpreadsheet\IOFactory::load($filePath);
                $sheet = $spreadsheet->getActiveSheet();
                $highestRow = $sheet->getHighestRow();

                $firstRow = [
                    trim((string)$sheet->getCell('A1')->getFormattedValue()),
                    trim((string)$sheet->getCell('B1')->getFormattedValue()),
                    trim((string)$sheet->getCell('C1')->getFormattedValue()),
                    trim((string)$sheet->getCell('D1')->getFormattedValue()),
                ];
                $firstRowText = implode('', $firstRow);
                $hasHeader = $firstRowText !== '' && (mb_strpos($firstRowText, '客户级别') !== false || mb_strpos($firstRowText, '级别名称') !== false);

                $start = $hasHeader ? 2 : 1;
                for ($r = $start; $r <= $highestRow; $r++) {
                    $rankName = trim((string)$sheet->getCell("A{$r}")->getFormattedValue());
                    $rankCode = trim((string)$sheet->getCell("B{$r}")->getFormattedValue());
                    $description = trim((string)$sheet->getCell("C{$r}")->getFormattedValue());
                    $sort = trim((string)$sheet->getCell("D{$r}")->getFormattedValue());
                    if ($rankName === '' && $rankCode === '' && $description === '' && $sort === '') {
                        continue;
                    }
                    $rows[] = [
                        'rank_name'   => $rankName,
                        'rank_code'   => $rankCode,
                        'description' => $description,
                        'sort'        => $sort,
                    ];
                }
            }
        } catch (\Throwable $e) {
            return json(['code' => -200, 'msg' => '解析失败：' . $e->getMessage()]);
        }

        if (empty($rows)) {
            return json(['code' => -200, 'msg' => '没有可导入的数据']);
        }

        $currentAdmin = Admin::getMyInfo();
        $adminUsername = trim((string)($currentAdmin['username'] ?? ''));
        $now = time();
        $inserted = 0;
        $updated = 0;
        $restored = 0;
        $skipped = 0;
        $errors = [];

        Db::startTrans();
        try {
            foreach ($rows as $idx => $row) {
                $lineNo = $idx + 1;
                $rankName = trim((string)($row['rank_name'] ?? ''));
                $rankCode = trim((string)($row['rank_code'] ?? ''));
                $description = trim((string)($row['description'] ?? ''));
                $sort = is_numeric($row['sort'] ?? null) ? (int)$row['sort'] : 0;

                if ($rankName === '' && $rankCode === '' && $description === '' && ($row['sort'] ?? '') === '') {
                    $skipped++;
                    continue;
                }
                if ($rankName === '') {
                    $skipped++;
                    continue;
                }
                if ($rankCode === '') {
                    $errors[] = "第{$lineNo}行客户级别编码不能为空";
                    continue;
                }

                $existsByCode = Db::name('crm_client_rank')->where('rank_code', $rankCode)->find();
                $existsByName = Db::name('crm_client_rank')->where('rank_name', $rankName)->find();
                $target = $existsByCode ?: $existsByName;

                if ($target) {
                    $aff = Db::name('crm_client_rank')
                        ->where('id', $target['id'])
                        ->update([
                            'rank_name'    => $rankName,
                            'rank_code'    => $rankCode,
                            'description'  => $description,
                            'sort'         => $sort,
                            'creator'      => $adminUsername,
                            'is_deleted'   => 0,
                            'deleted_time' => null,
                            'deleted_by'   => null,
                            'update_time'  => $now,
                        ]);
                    if ((int)$target['is_deleted'] === 1) {
                        $restored += $aff ? 1 : 0;
                    } else {
                        $updated += $aff ? 1 : 0;
                    }
                } else {
                    Db::name('crm_client_rank')->insert([
                        'rank_name'    => $rankName,
                        'rank_code'    => $rankCode,
                        'description'  => $description,
                        'sort'         => $sort,
                        'creator'      => $adminUsername,
                        'is_deleted'   => 0,
                        'deleted_time' => null,
                        'deleted_by'   => null,
                        // 导入新增同样需要补齐 add_time，避免数据库 1364 错误
                        'add_time'     => $now,
                        'create_time'  => $now,
                        'update_time'  => $now,
                    ]);
                    $inserted++;
                }
            }

            if (!empty($errors)) {
                Db::rollback();
                return json(['code' => -200, 'msg' => '导入失败：' . implode('；', $errors)]);
            }
            Db::commit();
        } catch (\Throwable $e) {
            Db::rollback();
            return json(['code' => -200, 'msg' => '写入失败：' . $e->getMessage()]);
        }

        return json([
            'code' => 0,
            'msg'  => "导入完成：新增{$inserted}条，更新{$updated}条，恢复{$restored}条，跳过{$skipped}条",
        ]);
    }

    public function tpl()
    {
        $filename = '客户级别导入模板_' . date('Ymd_His') . '.csv';
        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        $header = ['客户级别名称', '客户级别编码', '级别说明', '排序'];
        $examples = [
            ['A类客户', 'A', '近期高成交概率客户', '1'],
            ['B类客户', 'B', '需长期持续跟进客户', '2'],
            ['C类客户', 'C', '其他所有情况', '3'],
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
        foreach ($examples as $row) {
            echo $csvLine($row);
        }
        exit;
    }
}
