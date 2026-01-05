<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use app\admin\model\Admin;
use PhpOffice\PhpSpreadsheet\IOFactory;

class ProductCategory extends Common
{
    public function index()
    {
        if (request()->isPost()) {
            return $this->categorySearch();
        }
        return $this->fetch();
    }

    public function categorySearch()
    {
        // ★软删除改造 start
        $current_admin = Admin::getMyInfo();
        $category_name = Request::param('category_name');
        $pageSize = Request::param('limit', 10);
        $page = Request::param('page', 1);
        $query = Db::name('crm_product_category');

        // 只查询未删除的记录
        $query->where('is_deleted', 0);

        if (!empty($category_name)) {
            $query->where('category_name', 'like', '%' . $category_name . '%');
        }

        $list = $query->where([$this->getOrgWhere($current_admin['org'])])->order('id desc')->paginate([
            'list_rows' => $pageSize,
            'page' => $page
        ])->toArray();
        // ★软删除改造 end

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
            // ★软删除改造 start
            $category_name = Request::param('category_name');
            $current_admin = Admin::getMyInfo();
            if (empty($category_name)) {
                return $this->result([], 500, '供应商不能为空');
            }

            // 唯一性校验：只在 is_deleted=0 范围内判重
            $exists = Db::name('crm_product_category')
                ->where('category_name', $category_name)
                ->where('is_deleted', 0)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->find();

            if ($exists) {
                return $this->result([], 500, '供应商已存在');
            }

            // 检查是否存在同名但已删除的记录（is_deleted=1）
            $deletedExists = Db::name('crm_product_category')
                ->where('category_name', $category_name)
                ->where('is_deleted', 1)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->find();

            if ($deletedExists) {
                // 恢复已删除的记录
                $aff = Db::name('crm_product_category')
                    ->where('id', $deletedExists['id'])
                    ->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'edit_time' => time(),
                        'submit_person' => $current_admin['username']
                    ]);
                if ($aff) {
                    return $this->result([], 200, '操作成功（已恢复）');
                } else {
                    return $this->result([], 500, '恢复失败');
                }
            }

            // 插入新记录
            $data = [
                'category_name' => $category_name,
                'org' => $current_admin['org'],
                'add_time' => time(),
                'edit_time' => time(),
                'submit_person' => $current_admin['username'],
                'is_deleted' => 0
            ];
            Db::name('crm_product_category')->insert($data);
            return $this->result([], 200, '操作成功');
            // ★软删除改造 end
        }
        return $this->fetch();
    }

    public function edit()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return $this->result([], 500, '参数错误');
        }

        $result = Db::name('crm_product_category')->where('id', $id)->find();
        if (empty($result)) {
            return $this->result([], 500, '参数错误');
        }

        // ★软删除改造 start
        // 禁止编辑已删除的数据
        if (isset($result['is_deleted']) && $result['is_deleted'] == 1) {
            return $this->result([], 500, '记录已删除，无法编辑');
        }
        // ★软删除改造 end

        // 权限判定
        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin') || ($current_admin['group_id'] == 13);

        if (request()->isPost()) {
            // ★软删除改造 start
            $category_name = Request::param('category_name');
            if (empty($category_name)) {
                return $this->result([], 500, '供应商不能为空');
            }

            // 禁止编辑已删除的数据
            if (isset($result['is_deleted']) && $result['is_deleted'] == 1) {
                return $this->result([], 500, '记录已删除，无法编辑');
            }

            $current_admin = Admin::getMyInfo();
            // 唯一性校验：只在 is_deleted=0 范围内判重
            $exists = Db::name('crm_product_category')
                ->where('category_name', $category_name)
                ->where('id', '<>', $id)
                ->where('is_deleted', 0)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->find();

            if ($exists) {
                return $this->result([], 500, '供应商已存在');
            }
            // ★软删除改造 end

            $current_time = time();

            // 非超管：限制只能修改自己提交的记录
            $updateQuery = Db::name('crm_product_category')->where('id', $id);
            if (!$isSuper) {
                $updateQuery->where('submit_person', $current_admin['username']);
            }

            $aff = $updateQuery->update([
                'category_name' => $category_name,
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

    public function del()
    {
        // ★软删除改造 start
        $id = (int)\think\facade\Request::param('id');
        if ($id <= 0) {
            return json(['code' => 0, 'msg' => '参数错误']);
        }

        $current_admin = \app\admin\model\Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || (($current_admin['username'] ?? '') === 'admin') || ($current_admin['group_id'] == 13);

        // 查记录 + 按权限限制
        $rowQuery = \think\Db::name('crm_product_category')->where('id', $id);
        if (!$isSuper) {
            $rowQuery->where('submit_person', $current_admin['username']);
        }
        $row = $rowQuery->find();
        if (!$row) {
            return json(['code' => 0, 'msg' => '无权限或记录不存在']);
        }

        // 使用事务进行软删除（供应商 + 级联产品）
        \think\Db::startTrans();
        try {
            $current_time = time();
            $current_aid = session('aid') ?: 0;

            // 1. 软删除供应商
            $aff1 = \think\Db::name('crm_product_category')
                ->where('id', $id)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => $current_aid,
                    'edit_time' => $current_time
                ]);

            if ($aff1 === false) {
                throw new \Exception('软删除供应商失败');
            }

            // 2. 级联软删除该供应商下的所有产品（只软删除 is_deleted=0 的产品）
            $aff2 = \think\Db::name('crm_products')
                ->where('category_id', $id)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => $current_aid,
                    'edit_time' => $current_time
                ]);

            // 提交事务
            \think\Db::commit();
            return json(['code' => 1, 'msg' => '删除成功']);
        } catch (\Throwable $e) {
            // 回滚事务
            \think\Db::rollback();
            return json(['code' => 0, 'msg' => '删除失败：' . $e->getMessage()]);
        }
        // ★软删除改造 end
    }




    // 批量删除
    public function batchDel()
    {
        // ★软删除改造 start
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        if (empty($ids)) {
            return json(['code' => -200, 'msg' => '未选择任何记录']);
        }

        // 去重+转整型
        $ids = array_values(array_unique(array_map('intval', $ids)));

        $current_admin = \app\admin\model\Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || (($current_admin['username'] ?? '') === 'admin') || ($current_admin['group_id'] == 13);

        try {
            // 1) 按权限过滤：只保留"存在且自己提交（或超管）"的ID
            $base = \think\Db::name('crm_product_category')->whereIn('id', $ids)->where('is_deleted', 0);
            if (!$isSuper) {
                $base->where('submit_person', $current_admin['username']);
            }
            // id => category_name
            $allowedMap = $base->column('category_name', 'id');
            $allowedIds = array_map('intval', array_keys($allowedMap));
            if (empty($allowedIds)) {
                return json(['code' => -200, 'msg' => '无可删除的记录（仅能删除本人提交的记录）']);
            }

            // 开启事务
            \think\Db::startTrans();

            $current_time = time();
            $current_aid = session('aid') ?: 0;

            // 2) 一次性软删除供应商
            $deletedSuppliers = \think\Db::name('crm_product_category')
                ->whereIn('id', $allowedIds)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => $current_aid,
                    'edit_time' => $current_time
                ]);

            if ($deletedSuppliers === false) {
                throw new \Exception('软删除供应商失败');
            }

            // 3) 一次性级联软删除产品（只软删除 is_deleted=0 的产品）
            $deletedProducts = \think\Db::name('crm_products')
                ->whereIn('category_id', $allowedIds)
                ->where('is_deleted', 0)
                ->update([
                    'is_deleted' => 1,
                    'deleted_time' => date('Y-m-d H:i:s'),
                    'deleted_by' => $current_aid,
                    'edit_time' => $current_time
                ]);

            // 提交事务
            \think\Db::commit();

            // 4) 统计与消息
            $skippedPermission = count($ids) - count($allowedIds);

            $parts = [];
            $parts[] = '删除成功：供应商' . $deletedSuppliers . '条，产品' . $deletedProducts . '条';
            if ($skippedPermission > 0) {
                $parts[] = '跳过(无权限/不存在)：' . $skippedPermission . ' 条';
            }

            return json([
                'code' => 0,
                'msg'  => implode('，', $parts),
                'data' => [
                    'deleted_suppliers' => (int)$deletedSuppliers,
                    'deleted_products'  => (int)$deletedProducts,
                    'skipped_permission' => $skippedPermission,
                ],
            ]);
        } catch (\Throwable $e) {
            // 回滚事务
            \think\Db::rollback();
            return json(['code' => -200, 'msg' => '删除异常：' . $e->getMessage()]);
        }
        // ★软删除改造 end
    }



    // 导入页（弹窗）
    public function import()
    {
        // 渲染 view/product_category/import.html
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

        // 解析为二维数组 rows: [category_name, org(optional)]
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
                    // 首行含“供应/所属”视作表头
                    $joined = implode('', $data);
                    if ($lineNo === 1 && (mb_strpos($joined, '供应') !== false || mb_strpos($joined, '所属') !== false)) {
                        continue;
                    }
                    $data = array_pad($data, 2, '');
                    $rows[] = [
                        trim((string)$data[0]), // category_name
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
                $hasHeader = (mb_strpos(implode('', $firstRow), '供应') !== false) || (mb_strpos(implode('', $firstRow), '所属') !== false);
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
            // ★软删除改造 start
            list($category_name, $orgFromFile) = $row;

            if ($category_name === '') {
                $skippedEmpty++;
                continue;
            }

            $orgToUse = $orgFromFile !== '' ? $normalizeOrg($orgFromFile) : $loginOrg;
            if ($orgToUse === '') $orgToUse = $loginOrg;

            // 去重：只在 is_deleted=0 范围内判重
            $exists = \think\Db::name('crm_product_category')
                ->where('category_name', $category_name)
                ->where('is_deleted', 0)
                ->where([$this->getOrgWhere($orgToUse)])
                ->find();
            if ($exists) {
                $skippedExist++;
                continue;
            }

            // 检查是否存在同名但已删除的记录（is_deleted=1）
            $deletedExists = \think\Db::name('crm_product_category')
                ->where('category_name', $category_name)
                ->where('is_deleted', 1)
                ->where([$this->getOrgWhere($orgToUse)])
                ->find();

            if ($deletedExists) {
                // 恢复已删除的记录
                $ok = \think\Db::name('crm_product_category')
                    ->where('id', $deletedExists['id'])
                    ->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'edit_time' => $now,
                        'submit_person' => $user
                    ]);
                if ($ok) $inserted++;
            } else {
                // 插入新记录
                $ok = \think\Db::name('crm_product_category')->insert([
                    'category_name' => $category_name,
                    'org'           => $orgToUse,
                    'add_time'      => $now,
                    'edit_time'     => $now,
                    'submit_person' => $user,
                    'is_deleted' => 0
                ]);
                if ($ok) $inserted++;
            }
            // ★软删除改造 end
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
        $filename = '供应商导入模板_' . date('Ymd_His') . '.csv';

        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        // A=供应商名称（必填），B=所属组织（可选；为空则按当前登录人的组织）
        $header   = ['供应商名称', '所属组织(可选)'];
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
