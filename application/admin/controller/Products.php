<?php

namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\Queue; 
use think\facade\Session;
use app\admin\model\Admin;
use PhpOffice\PhpSpreadsheet\IOFactory;

class Products extends Common
{
    public function index()
    {
        if (request()->isPost()) {
            return $this->productSearch();
        }
        $category_list = $this->getCategoryList();
        $this->assign('category_list', $category_list);
        return $this->fetch();
    }
    public function productSearch()
    {
        $current_admin = Admin::getMyInfo();
        $product_name = Request::param('product_name');
        $pageSize = Request::param('limit', 10);
        $page = Request::param('page', 1);
        $query = Db::name('crm_products p')->leftJoin('crm_product_category c', 'p.category_id = c.id');
        if (!empty($product_name)) {
            $query->where('p.product_name', 'like', '%' . $product_name . '%');
        }
        $category_id = Request::param('category_id');
        if (!empty($category_id)) {
            $query->where('p.category_id', $category_id);
        }
        // soft delete by is_deleted: 只显示正常状态的产品（is_deleted = 0）
        $list = $query->field('p.*, c.category_name')
            ->where([$this->getOrgWhere($current_admin['org'], 'p')])
            ->where('p.is_deleted', '=', 0)
            ->order('p.id desc')
            ->paginate([
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
            //新增商品
            $product_name = Request::param('product_name');
            $category_id = (int)Request::param('category_id');

            if (empty($product_name)) {
                return $this->result([], 500, '商品名称不能为空');
            }
            
            $current_admin = Admin::getMyInfo();
            
            // soft delete by is_deleted: 检查是否存在相同名称且已删除的产品（is_deleted = 1）
            $deletedProduct = Db::name('crm_products')
                ->where('product_name', $product_name)
                ->where('category_id', $category_id)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->where('is_deleted', 1)
                ->find();
            
            if ($deletedProduct) {
                // 如果存在已删除的相同产品，恢复它
                Db::name('crm_products')
                    ->where('id', $deletedProduct['id'])
                    ->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'edit_time' => time(),
                        'submit_person' => $current_admin['username']
                    ]);
                return $this->result([], 200, '操作成功（已恢复已删除的产品）');
            }
            
            // soft delete by is_deleted: 检查是否存在相同名称且正常的产品（is_deleted = 0）
            $product = Db::name('crm_products')
                ->where('product_name', $product_name)
                ->where('category_id', $category_id)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->where('is_deleted', 0)
                ->find();
            if (!$product) {
                $data['org'] = $current_admin['org'];
                $data['product_name'] = $product_name;
                $data['category_id'] = $category_id;
                $data['is_deleted'] = 0; // 明确设置为正常状态
                $data['add_time'] = time();
                $data['edit_time'] = time();
                $data['submit_person'] = $current_admin['username'];
                $res = Db::name('crm_products')->insert($data);
                return $this->result([], 200, '操作成功');
            } else {
                return $this->result([], 500, '商品已存在');
            }
        }
        $category_rows = $this->getCategoryList();
        $category_list = array_map(function ($row) {
            return [
                'id'   => (int)$row['id'],
                'name' => (string)$row['category_name'],
            ];
        }, $category_rows);
        $category_list = json_encode($category_list, JSON_UNESCAPED_UNICODE);
        $this->assign('category_list', $category_list);
        return $this->fetch();
    }


    public function edit()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return $this->result([], 500, '参数错误');
        }
        $result = Db::name('crm_products')->where('id', $id)->find();
        if (empty($result)) {
            return $this->result([], 500, '参数错误');
        }

        // soft delete by is_deleted: 如果该产品已删除，不允许编辑
        if (isset($result['is_deleted']) && $result['is_deleted'] == 1) {
            return $this->result([], 500, '产品已删除/不可编辑');
        }

        // 权限判定
        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin') || ($current_admin['group_id'] == 13);

        if (request()->isPost()) {
            $product_name = Request::param('product_name');
            $category_id  = (int)Request::param('category_id');

            if (empty($product_name)) {
                return $this->result([], 500, '商品名称不能为空');
            }

            // soft delete by is_deleted: 同组织 + 同供应商 下产品名不可重复（排除当前ID和已删除的记录）
            $exists = Db::name('crm_products')
                ->where('product_name', $product_name)
                ->where('category_id', '=', $category_id)
                ->where([$this->getOrgWhere($current_admin['org'])])
                ->where('id', '<>', $id)
                ->where('is_deleted', '=', 0) // 只检查正常状态的产品
                ->find();
            if ($exists) {
                return $this->result([], 500, '商品已存在');
            }

            $current_time = time();

            // 非超管：限制只能修改自己提交的记录
            $updateQuery = Db::name('crm_products')->where('id', $id);
            if (!$isSuper) {
                $updateQuery->where('submit_person', $current_admin['username']);
            }

            $aff = $updateQuery->update([
                'product_name' => $product_name,
                'category_id'  => $category_id,
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

        // 非提交：准备下拉数据为 {id,name} + 默认ID
        $defaultCategoryId = (int)$result['category_id'];
        $category_rows = $this->getCategoryList();
        $category_list = array_map(function ($row) {
            return [
                'id'   => (int)$row['id'],
                'name' => (string)$row['category_name'],
            ];
        }, $category_rows);
        $category_list = json_encode($category_list, JSON_UNESCAPED_UNICODE);
        $this->assign('category_list', $category_list);
        $this->assign('default_category_id', $defaultCategoryId);
        $this->assign('result', $result);
        return $this->fetch();
    }

    public function del()
    {
        $id = Request::param('id');
        if (empty($id)) {
            return $this->result([], 500, '参数错误');
        }

        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin');

        $query = Db::name('crm_products')->where('id', $id);
        // soft delete by is_deleted: 只允许删除正常状态的产品，避免重复删除
        $query->where('is_deleted', 0);
        if (!$isSuper) {
            $query->where('submit_person', $current_admin['username']);
        }

        // soft delete by is_deleted: 软删除，更新 is_deleted 字段
        $aff = $query->update([
            'is_deleted' => 1,
            'deleted_time' => date('Y-m-d H:i:s'),
            'deleted_by' => session('aid'),
            'edit_time' => time()
        ]);
        if ($aff) {
            return $this->result([], 200, '删除成功');
        } else {
            // 可能是记录不存在或无权限
            return $this->result([], 500, '无权限或记录不存在');
        }
    }


    // 批量删除
    public function batchDel()
    {
        if (!request()->isPost()) {
            return json(['code' => -200, 'msg' => '非法请求']);
        }
        $ids = input('post.ids/a', []);
        if (empty($ids)) {
            return json(['code' => -200, 'msg' => '未选择任何记录']);
        }

        $current_admin = Admin::getMyInfo();
        $isSuper = (session('aid') == 1) || ($current_admin['username'] === 'admin');

        // soft delete by is_deleted: 使用事务保证数据一致性
        Db::startTrans();
        try {
            if ($isSuper) {
                // soft delete by is_deleted: 超管批量软删除（只更新 is_deleted=0 的）
                $delCount = Db::name('crm_products')
                    ->whereIn('id', $ids)
                    ->where('is_deleted', 0)
                    ->update([
                        'is_deleted' => 1,
                        'deleted_time' => date('Y-m-d H:i:s'),
                        'deleted_by' => session('aid'),
                        'edit_time' => time()
                    ]);
                Db::commit();
                if ($delCount > 0) {
                    return json(['code' => 0, 'msg' => '删除成功', 'data' => ['count' => $delCount]]);
                }
                return json(['code' => -200, 'msg' => '删除失败或记录不存在']);
            } else {
                // 仅允许删除本人提交的记录
                $ownIds = Db::name('crm_products')
                    ->whereIn('id', $ids)
                    ->where('submit_person', $current_admin['username'])
                    ->where('is_deleted', 0)
                    ->column('id');

                if (empty($ownIds)) {
                    Db::rollback();
                    return json(['code' => -200, 'msg' => '无可删除的记录（仅能删除本人提交的记录）']);
                }

                // soft delete by is_deleted: 批量软删除
                $delCount = Db::name('crm_products')
                    ->whereIn('id', $ownIds)
                    ->update([
                        'is_deleted' => 1,
                        'deleted_time' => date('Y-m-d H:i:s'),
                        'deleted_by' => session('aid'),
                        'edit_time' => time()
                    ]);
                $skipped  = count($ids) - $delCount;
                Db::commit();

                if ($delCount > 0) {
                    $msg = '删除成功：' . $delCount . ' 条';
                    if ($skipped > 0) $msg .= '，跳过(非本人记录)：' . $skipped . ' 条';
                    return json(['code' => 0, 'msg' => $msg, 'data' => ['count' => $delCount, 'skipped' => $skipped]]);
                }
                return json(['code' => -200, 'msg' => '删除失败或记录不存在（或无权限）']);
            }
        } catch (\Throwable $e) {
            Db::rollback();
            return json(['code' => -200, 'msg' => '删除异常：' . $e->getMessage()]);
        }
    }



    // 导入页（弹窗）
    public function import()
    {
        return $this->fetch();  // 渲染 view/products/import.html
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

        // 解析为二维数组 rows：每行是 [产品名称, 供应商名称, 供应商ID(可选), 状态(可选)]
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
                    // 识别并跳过表头（含“产品”或“供应”字样即视为表头）
                    $joined = implode('', $data);
                    if ($lineNo === 1 && (mb_strpos($joined, '产品') !== false || mb_strpos($joined, '供应') !== false)) {
                        continue;
                    }
                    $data = array_pad($data, 4, '');
                    $rows[] = [
                        trim((string)$data[0]), // product_name
                        trim((string)$data[1]), // category_name
                        trim((string)$data[2]), // category_id (optional)
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

                // 判断首行是否表头（含“产品”或“供应”）
                $firstRow = [
                    (string)$sheet->getCell('A1')->getValue(),
                    (string)$sheet->getCell('B1')->getValue(),
                    (string)$sheet->getCell('C1')->getValue(),
                    (string)$sheet->getCell('D1')->getValue(),
                ];
                $hasHeader = (mb_strpos(implode('', $firstRow), '产品') !== false) || (mb_strpos(implode('', $firstRow), '供应') !== false);
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
        $createdCats = 0;
        $seenKeys = []; // 防止同一批次文件内重复

        $current_admin = \app\admin\model\Admin::getMyInfo();
        $org = $current_admin['org'] ?? '';
        $user = $current_admin['username'] ?? '';

        foreach ($rows as $row) {
            list($product_name, $category_name, $category_id_raw, $status_raw) = $row;

            if ($product_name === '' && $category_name === '' && $category_id_raw === '') {
                $skippedEmpty++;
                continue;
            }

            // 解析 category_id：优先C列ID，否则用B列名称在当前组织下查找/创建
            $category_id = 0;
            if ($category_id_raw !== '' && is_numeric($category_id_raw)) {
                $category_id = (int)$category_id_raw;
            } elseif ($category_name !== '') {
                $cat = \think\Db::name('crm_product_category')
                    ->where('category_name', $category_name)
                    ->where([$this->getOrgWhere($org)])
                    ->where('is_deleted', 0)
                    ->find();

                if (!$cat) {
                    // 自动创建该供应商（分类）
                    $cid = \think\Db::name('crm_product_category')->insertGetId([
                        'category_name' => $category_name,
                        'org'           => $org,
                        'is_deleted'    => 0,
                        'add_time'      => $now,
                        'edit_time'     => $now,
                        'submit_person' => $user,
                    ]);
                    if ($cid) {
                        $category_id = (int)$cid;
                        $createdCats++;
                    }
                } else {
                    $category_id = (int)$cat['id'];
                }
            }

            // 产品名必填
            if ($product_name === '') {
                $skippedEmpty++;
                continue;
            }
            // 仍未拿到供应商ID，跳过
            if ($category_id <= 0) {
                $skippedEmpty++;
                continue;
            }

            // 组合唯一键（同组织+同供应商+同产品名 视为同一条）
            $key = md5($org . '|' . $category_id . '|' . $product_name);
            if (isset($seenKeys[$key])) {
                $skippedExist++;
                continue;
            }

            // soft delete by is_deleted: DB去重：同 org 范围 + category_id + product_name
            // 先检查是否有已删除的相同产品（is_deleted = 1）
            $deletedProduct = \think\Db::name('crm_products')
                ->where('product_name', $product_name)
                ->where('category_id', $category_id)
                ->where([$this->getOrgWhere($org)])
                ->where('is_deleted', 1)
                ->find();
            
            if ($deletedProduct) {
                // 如果存在已删除的相同产品，恢复它
                \think\Db::name('crm_products')
                    ->where('id', $deletedProduct['id'])
                    ->update([
                        'is_deleted' => 0,
                        'deleted_time' => null,
                        'deleted_by' => null,
                        'edit_time' => $now,
                        'submit_person' => $user
                    ]);
                $inserted++;
                $seenKeys[$key] = 1;
                continue;
            }
            
            // soft delete by is_deleted: 检查是否存在正常的相同产品（is_deleted = 0）
            $exists = \think\Db::name('crm_products')
                ->where('product_name', $product_name)
                ->where('category_id', $category_id)
                ->where([$this->getOrgWhere($org)])
                ->where('is_deleted', 0)
                ->find();
            if ($exists) {
                $skippedExist++;
                continue;
            }

            // soft delete by is_deleted: 插入新记录，显式写 is_deleted=0
            $ok = \think\Db::name('crm_products')->insert([
                'product_name'  => $product_name,
                'org'           => $org,
                'category_id'   => $category_id,
                'is_deleted'    => 0,
                'add_time'      => $now,
                'edit_time'     => $now,
                'submit_person' => $user,
            ]);
            if ($ok) {
                $inserted++;
                $seenKeys[$key] = 1;
            }
        }

        return json([
            'code' => 0,
            'msg' => '导入完成',
            'data' => [
                'inserted' => $inserted,
                'skipped_exist' => $skippedExist,
                'skipped_empty' => $skippedEmpty,
                'created_cats' => $createdCats,
            ],
        ]);
    }





    // 下载导入模板（CSV）
    public function tpl()
    {
        $filename = '产品导入模板_' . date('Ymd_His') . '.csv';

        $csvLine = function (array $cols) {
            $safe = array_map(function ($v) {
                $v = (string)$v;
                $v = str_replace('"', '""', $v);
                return '"' . $v . '"';
            }, $cols);
            return implode(',', $safe) . "\r\n";
        };

        // 表头：A=产品名称 B=供应商(分类名称) C=供应商ID(可选) D=状态(0/1，可选)
        $header = ['产品名称', '供应商(分类名称)', '供应商ID(可选)', '状态(0或1，可选)'];
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



    public function main()
    {

        $current_admin = Admin::getMyInfo();
        $data['org'] = trim($current_admin['org'], $this->org_fgx);
        if (request()->isPost()) {
            $keyword  = Request::param('keyword') ?? [];
            $timebucket = !empty($keyword['timebucket']) ? $keyword['timebucket'] : ($keyword['at_time'] ?? '');
            
            // 询盘产品排行 - 通过 inquiry_id 和 port_id 匹配运营人员
            $oper_prod = $this->getOperProdData($current_admin['org'], $timebucket);
            
            // 销售产品排行 - 通过 source 和 source_port 匹配运营人员
            $order_prod = $this->getOrderProdData($current_admin['org'], $timebucket);
            
            $data['product_data']['oper_prod'] = $oper_prod;
            $data['product_data']['order_prod'] = $order_prod;
            $data['product_data'] = array_merge($data['product_data'], $this->productCategoryCount($current_admin['org'], $timebucket));
            $data['product_data'] = array_merge($data['product_data'], $this->productCountryCount($current_admin['org'], $timebucket));
            $this->assign('data', $data);
            return $this->fetch('main_content');
        }
        $this->assign('data', $data);
        return $this->fetch();
    }
    
    // 获取询盘产品排行数据
    private function getOperProdData($org, $timebucket)
    {
        // 构建查询条件
        $l_where = [['l.status', '=', 1]];
        if (!empty($timebucket)) {
            $l_where[] = $this->getClientimeWhere($timebucket, 'l');
        }
        
        // 获取所有运营人员的 inquiry_id 和 port_id
        $yy_admins = Db::table('admin')
            ->where($this->getOrgWhere($org))
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('inquiry_id,port_id')
            ->select();
        
        // 构建查询
        $query = Db::table('crm_leads')
            ->alias('l')
            ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
            ->where($l_where)
            ->where('l.product_name', '<>', '')
            ->where('l.product_name', '<>', null)
            ->where('p.is_deleted', 0); // soft delete by is_deleted: 排除已删除产品
        
        // 如果有运营人员，添加运营人员匹配条件；否则查询所有数据
        if (!empty($yy_admins)) {
            $yy_conditions = [];
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                $port_conditions = [];
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                    }
                }
                
                if (!empty($port_conditions)) {
                    $yy_conditions[] = "(l.inquiry_id = '{$admin_inquiry_id}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
            
            if (!empty($yy_conditions)) {
                $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
                $query->whereRaw($yy_where_raw);
            }
        }
        
        // 查询询盘产品数据，通过 JOIN crm_products 获取产品名称
        $result = $query->group('IFNULL(p.product_name, l.product_name)')
            ->field('IFNULL(p.product_name, l.product_name) as product_name,count(l.id) as count')
            ->order('count', 'desc')
            ->select();
        
        return $result ?: [];
    }
    
    // 获取销售产品排行数据
    private function getOrderProdData($org, $timebucket)
    {
        // 构建查询条件
        $o_where = [];
        if (!empty($timebucket)) {
            $o_where[] = $this->buildTimeWhere($timebucket, 'order_time');
        }
        
        // 获取所有运营人员的 inquiry_id 和 port_id
        $yy_admins = Db::table('admin')
            ->where($this->getOrgWhere($org))
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('inquiry_id,port_id')
            ->select();
        
        // 构建运营人员匹配条件（如果有运营人员）
        $yy_where_raw = null;
        if (!empty($yy_admins)) {
            $yy_conditions = [];
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                // 获取该渠道下所有端口的名称列表
                $port_names = [];
                $inquiry_name = '';
                $inquiry_info = Db::table('crm_inquiry')
                    ->where('id', $admin_inquiry_id)
                    ->field('inquiry_name')
                    ->find();
                if ($inquiry_info) {
                    $inquiry_name = addslashes($inquiry_info['inquiry_name']);
                }
                
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_info = Db::table('crm_inquiry_port')
                            ->where('id', $port_id)
                            ->where('inquiry_id', $admin_inquiry_id)
                            ->field('port_name')
                            ->find();
                        if ($port_info && !empty($port_info['port_name'])) {
                            $port_names[] = addslashes($port_info['port_name']);
                        }
                    }
                }
                
                if (!empty($inquiry_name) && !empty($port_names)) {
                    $port_conditions = [];
                    foreach ($port_names as $port_name) {
                        $port_conditions[] = "o.source_port = '{$port_name}'";
                    }
                    $yy_conditions[] = "(o.source = '{$inquiry_name}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
            
            if (!empty($yy_conditions)) {
                $yy_where_raw = '(' . implode(' OR ', $yy_conditions) . ')';
            }
        }
        
        // 查询销售产品数据
        // 先查询 crm_client_order 中 product_name 不为空的记录
        $query1 = Db::table('crm_client_order')
            ->alias('o')
            ->where($o_where)
            ->where('o.product_name', '<>', '')
            ->where('o.product_name', '<>', null);
        
        if ($yy_where_raw) {
            $query1->whereRaw($yy_where_raw);
        }
        
        $result1 = $query1->group('o.product_name')
            ->field('o.product_name,count(o.id) as count')
            ->select();
        
        // 再查询 crm_order_item 中 product_name 不为空的记录（当 crm_client_order.product_name 为空时）
        $query2 = Db::table('crm_client_order')
            ->alias('o')
            ->join('crm_order_item oi', 'o.id = oi.order_id', 'LEFT')
            ->where($o_where)
            ->where(function($query) {
                $query->where('o.product_name', '=', '')
                    ->whereOr('o.product_name', '=', null);
            })
            ->where('oi.product_name', '<>', '')
            ->where('oi.product_name', '<>', null);
        
        if ($yy_where_raw) {
            $query2->whereRaw($yy_where_raw);
        }
        
        $result2 = $query2->group('oi.product_name')
            ->field('oi.product_name,count(o.id) as count')
            ->select();
        
        // 合并结果
        $merged = [];
        foreach ($result1 as $item) {
            $product_name = $item['product_name'];
            if (!isset($merged[$product_name])) {
                $merged[$product_name] = ['product_name' => $product_name, 'count' => 0];
            }
            $merged[$product_name]['count'] += $item['count'];
        }
        
        foreach ($result2 as $item) {
            $product_name = $item['product_name'];
            if (!isset($merged[$product_name])) {
                $merged[$product_name] = ['product_name' => $product_name, 'count' => 0];
            }
            $merged[$product_name]['count'] += $item['count'];
        }
        
        // 转换为数组并按数量排序
        $result = array_values($merged);
        usort($result, function($a, $b) {
            return $b['count'] - $a['count'];
        });
        
        return $result;
    }


    //统计数据时数据库同一组织下不能存在相同名称的产品名称或者分类名称，否则统计数据会不准
    //产品按分类统计
    public function productCategoryCount($org, $timebucket)
    {
        // 构建查询条件
        $l_where = [['l.status', '=', 1]];
        if (!empty($timebucket)) {
            $l_where[] = $this->getClientimeWhere($timebucket, 'l');
        }
        
        $o_where = [];
        if (!empty($timebucket)) {
            $o_where[] = $this->buildTimeWhere($timebucket, 'order_time');
        }
        
        // 获取所有运营人员
        $yy_admins = Db::table('admin')
            ->where($this->getOrgWhere($org))
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('inquiry_id,port_id')
            ->select();
        
        // 构建询盘产品匹配条件
        $yy_conditions_oper = [];
        if (!empty($yy_admins)) {
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                $port_conditions = [];
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                    }
                }
                
                if (!empty($port_conditions)) {
                    $yy_conditions_oper[] = "(l.inquiry_id = '{$admin_inquiry_id}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
        }
        
        // 构建订单产品匹配条件
        $yy_conditions_order = [];
        if (!empty($yy_admins)) {
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                $port_names = [];
                $inquiry_name = '';
                $inquiry_info = Db::table('crm_inquiry')
                    ->where('id', $admin_inquiry_id)
                    ->field('inquiry_name')
                    ->find();
                if ($inquiry_info) {
                    $inquiry_name = addslashes($inquiry_info['inquiry_name']);
                }
                
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_info = Db::table('crm_inquiry_port')
                            ->where('id', $port_id)
                            ->where('inquiry_id', $admin_inquiry_id)
                            ->field('port_name')
                            ->find();
                        if ($port_info && !empty($port_info['port_name'])) {
                            $port_names[] = addslashes($port_info['port_name']);
                        }
                    }
                }
                
                if (!empty($inquiry_name) && !empty($port_names)) {
                    $port_conditions = [];
                    foreach ($port_names as $port_name) {
                        $port_conditions[] = "o.source_port = '{$port_name}'";
                    }
                    $yy_conditions_order[] = "(o.source = '{$inquiry_name}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
        }
        
        //询盘产品按分类统计
        $oper_prod_category = [];
        $query_oper = Db::table('crm_leads l')
            ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($l_where)
            ->where('l.product_name', '<>', '')
            ->where('l.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if (!empty($yy_conditions_oper)) {
            $yy_where_raw = '(' . implode(' OR ', $yy_conditions_oper) . ')';
            $query_oper->whereRaw($yy_where_raw);
        }
        
        $oper_prod_category = $query_oper->group('c.category_name')
            ->field('c.category_name,count(*) as count')
            ->order('c.category_name', 'asc')
            ->order('count', 'desc')
            ->select();
        
        //订单产品按分类统计
        $order_prod_category = [];
        $yy_where_raw_order = null;
        if (!empty($yy_conditions_order)) {
            $yy_where_raw_order = '(' . implode(' OR ', $yy_conditions_order) . ')';
        }
        
        // 先查询 crm_client_order 中 product_name 不为空的记录
        $query1 = Db::table('crm_client_order o')
            ->join('crm_products p', 'o.product_name = p.product_name', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($o_where)
            ->where('o.product_name', '<>', '')
            ->where('o.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if ($yy_where_raw_order) {
            $query1->whereRaw($yy_where_raw_order);
        }
        
        $result1 = $query1->group('c.category_name')
            ->field('c.category_name,count(*) as count')
            ->select();
        
        // 再查询 crm_order_item 中的记录
        $query2 = Db::table('crm_client_order o')
            ->join('crm_order_item oi', 'o.id = oi.order_id', 'LEFT')
            ->join('crm_products p', 'oi.product_name = p.product_name', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($o_where)
            ->where(function($query) {
                $query->where('o.product_name', '=', '')
                    ->whereOr('o.product_name', '=', null);
            })
            ->where('oi.product_name', '<>', '')
            ->where('oi.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if ($yy_where_raw_order) {
            $query2->whereRaw($yy_where_raw_order);
        }
        
        $result2 = $query2->group('c.category_name')
            ->field('c.category_name,count(*) as count')
            ->select();
        
        // 合并结果
        $merged = [];
        foreach ($result1 as $item) {
            $category_name = $item['category_name'];
            if (!isset($merged[$category_name])) {
                $merged[$category_name] = ['category_name' => $category_name, 'count' => 0];
            }
            $merged[$category_name]['count'] += $item['count'];
        }
        
        foreach ($result2 as $item) {
            $category_name = $item['category_name'];
            if (!isset($merged[$category_name])) {
                $merged[$category_name] = ['category_name' => $category_name, 'count' => 0];
            }
            $merged[$category_name]['count'] += $item['count'];
        }
        
        // 转换为数组并按数量排序
        $order_prod_category = array_values($merged);
        usort($order_prod_category, function($a, $b) {
            if ($a['category_name'] == $b['category_name']) {
                return $b['count'] - $a['count'];
            }
            return strcmp($a['category_name'], $b['category_name']);
        });
        
        return ['oper_prod_category' => $oper_prod_category ?: [], 'order_prod_category' => $order_prod_category ?: []];
    }

    //产品按国家统计
    public function productCountryCount($org, $timebucket)
    {
        // 构建查询条件
        $l_where = [['l.status', '=', 1]];
        if (!empty($timebucket)) {
            $l_where[] = $this->getClientimeWhere($timebucket, 'l');
        }
        
        $o_where = [];
        if (!empty($timebucket)) {
            $o_where[] = $this->buildTimeWhere($timebucket, 'order_time');
        }
        
        // 获取所有运营人员
        $yy_admins = Db::table('admin')
            ->where($this->getOrgWhere($org))
            ->where('is_open', '=', 1)
            ->where('inquiry_id', '<>', '')
            ->where('inquiry_id', '<>', null)
            ->where('port_id', '<>', '')
            ->where('port_id', '<>', null)
            ->field('inquiry_id,port_id')
            ->select();
        
        // 构建询盘产品匹配条件
        $yy_conditions_oper = [];
        if (!empty($yy_admins)) {
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                $port_conditions = [];
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_conditions[] = "FIND_IN_SET('{$port_id}', l.port_id) > 0";
                    }
                }
                
                if (!empty($port_conditions)) {
                    $yy_conditions_oper[] = "(l.inquiry_id = '{$admin_inquiry_id}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
        }
        
        // 构建订单产品匹配条件
        $yy_conditions_order = [];
        if (!empty($yy_admins)) {
            foreach ($yy_admins as $admin) {
                $admin_inquiry_id = $admin['inquiry_id'];
                $admin_port_ids = !empty($admin['port_id']) ? array_filter(explode(',', $admin['port_id'])) : [];
                
                if (empty($admin_port_ids)) continue;
                
                $port_names = [];
                $inquiry_name = '';
                $inquiry_info = Db::table('crm_inquiry')
                    ->where('id', $admin_inquiry_id)
                    ->field('inquiry_name')
                    ->find();
                if ($inquiry_info) {
                    $inquiry_name = addslashes($inquiry_info['inquiry_name']);
                }
                
                foreach ($admin_port_ids as $port_id) {
                    $port_id = trim($port_id);
                    if ($port_id) {
                        $port_info = Db::table('crm_inquiry_port')
                            ->where('id', $port_id)
                            ->where('inquiry_id', $admin_inquiry_id)
                            ->field('port_name')
                            ->find();
                        if ($port_info && !empty($port_info['port_name'])) {
                            $port_names[] = addslashes($port_info['port_name']);
                        }
                    }
                }
                
                if (!empty($inquiry_name) && !empty($port_names)) {
                    $port_conditions = [];
                    foreach ($port_names as $port_name) {
                        $port_conditions[] = "o.source_port = '{$port_name}'";
                    }
                    $yy_conditions_order[] = "(o.source = '{$inquiry_name}' AND (" . implode(' OR ', $port_conditions) . "))";
                }
            }
        }
        
        //询盘产品按国家统计
        $oper_prod_country = [];
        $query_oper = Db::table('crm_leads l')
            ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($l_where)
            ->where('l.product_name', '<>', '')
            ->where('l.product_name', '<>', null)
            ->where('l.xs_area', '<>', '')
            ->where('p.is_deleted', 0); // soft delete by is_deleted: 排除已删除产品
        
        if (!empty($yy_conditions_oper)) {
            $yy_where_raw = '(' . implode(' OR ', $yy_conditions_oper) . ')';
            $query_oper->whereRaw($yy_where_raw);
        }
        
        $oper_prod_country = $query_oper->group('IFNULL(p.product_name, l.product_name),l.xs_area')
            ->field('IFNULL(p.product_name, l.product_name) as product_name,l.xs_area as country,count(*) as count')
            ->order('product_name', 'asc')
            ->order('count', 'desc')
            ->select();

        //询盘产品分类按国家统计
        $oper_prod_category_country = [];
        $query_oper_cat = Db::table('crm_leads l')
            ->join('crm_products p', 'l.product_name = p.id', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($l_where)
            ->where('l.product_name', '<>', '')
            ->where('l.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('l.xs_area', '<>', '')
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if (!empty($yy_conditions_oper)) {
            $yy_where_raw = '(' . implode(' OR ', $yy_conditions_oper) . ')';
            $query_oper_cat->whereRaw($yy_where_raw);
        }
        
        $oper_prod_category_country = $query_oper_cat->group('c.category_name,l.xs_area')
            ->field('c.category_name,l.xs_area as country,count(*) as count')
            ->order('c.category_name', 'asc')
            ->order('count', 'desc')
            ->select();

        //订单产品按国家统计（使用省市二级联动数据）
        $order_prod_country = [];
        $yy_where_raw_order = null;
        if (!empty($yy_conditions_order)) {
            $yy_where_raw_order = '(' . implode(' OR ', $yy_conditions_order) . ')';
        }
        
        // 先查询 crm_client_order 中 product_name 不为空的记录
        // 查询province、city和country字段，在PHP层面组合
        $query1 = Db::table('crm_client_order o')
            ->where($o_where)
            ->where('o.product_name', '<>', '')
            ->where('o.product_name', '<>', null)
            ->where(function($query) {
                $query->where(function($q) {
                    $q->where('o.province', '<>', '')
                      ->where('o.province', '<>', null)
                      ->where('o.city', '<>', '')
                      ->where('o.city', '<>', null);
                })->whereOr(function($q) {
                    $q->where('o.country', '<>', '')
                      ->where('o.country', '<>', null);
                });
            });
        
        if ($yy_where_raw_order) {
            $query1->whereRaw($yy_where_raw_order);
        }
        
        $result1 = $query1->field('o.product_name, o.province, o.city, o.country, count(*) as count')
            ->group('o.product_name, o.province, o.city, o.country')
            ->select();
        
        // 再查询 crm_order_item 中的记录
        $query2 = Db::table('crm_client_order o')
            ->join('crm_order_item oi', 'o.id = oi.order_id', 'LEFT')
            ->where($o_where)
            ->where(function($query) {
                $query->where('o.product_name', '=', '')
                    ->whereOr('o.product_name', '=', null);
            })
            ->where('oi.product_name', '<>', '')
            ->where('oi.product_name', '<>', null)
            ->where(function($query) {
                $query->where(function($q) {
                    $q->where('o.province', '<>', '')
                      ->where('o.province', '<>', null)
                      ->where('o.city', '<>', '')
                      ->where('o.city', '<>', null);
                })->whereOr(function($q) {
                    $q->where('o.country', '<>', '')
                      ->where('o.country', '<>', null);
                });
            });
        
        if ($yy_where_raw_order) {
            $query2->whereRaw($yy_where_raw_order);
        }
        
        $result2 = $query2->field('oi.product_name, o.province, o.city, o.country, count(*) as count')
            ->group('oi.product_name, o.province, o.city, o.country')
            ->select();
        
        // 合并结果，在PHP层面组合省市
        $merged = [];
        foreach ($result1 as $item) {
            // 组合省市：如果有province和city，则组合为"省份 城市"，否则使用country
            $region = '';
            if (!empty($item['province']) && !empty($item['city'])) {
                $region = trim($item['province']) . ' ' . trim($item['city']);
            } elseif (!empty($item['country'])) {
                $region = $item['country'];
            } else {
                continue; // 跳过没有地区信息的记录
            }
            
            $key = $item['product_name'] . '|' . $region;
            if (!isset($merged[$key])) {
                $merged[$key] = [
                    'product_name' => $item['product_name'],
                    'country' => $region,
                    'count' => 0
                ];
            }
            $merged[$key]['count'] += $item['count'];
        }
        
        foreach ($result2 as $item) {
            // 组合省市：如果有province和city，则组合为"省份 城市"，否则使用country
            $region = '';
            if (!empty($item['province']) && !empty($item['city'])) {
                $region = trim($item['province']) . ' ' . trim($item['city']);
            } elseif (!empty($item['country'])) {
                $region = $item['country'];
            } else {
                continue; // 跳过没有地区信息的记录
            }
            
            $key = $item['product_name'] . '|' . $region;
            if (!isset($merged[$key])) {
                $merged[$key] = [
                    'product_name' => $item['product_name'],
                    'country' => $region,
                    'count' => 0
                ];
            }
            $merged[$key]['count'] += $item['count'];
        }
        
        // 转换为数组并按产品名称和数量排序
        $order_prod_country = array_values($merged);
        usort($order_prod_country, function($a, $b) {
            if ($a['product_name'] == $b['product_name']) {
                return $b['count'] - $a['count'];
            }
            return strcmp($a['product_name'], $b['product_name']);
        });
        
        //订单产品分类按国家统计
        $order_prod_category_country = [];
        
        // 先查询 crm_client_order 中 product_name 不为空的记录
        $query_cat1 = Db::table('crm_client_order o')
            ->join('crm_products p', 'o.product_name = p.product_name', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($o_where)
            ->where('o.product_name', '<>', '')
            ->where('o.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('o.country', '<>', '')
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if ($yy_where_raw_order) {
            $query_cat1->whereRaw($yy_where_raw_order);
        }
        
        $result_cat1 = $query_cat1->group('c.category_name,o.country')
            ->field('c.category_name,o.country,count(*) as count')
            ->select();
        
        // 再查询 crm_order_item 中的记录
        $query_cat2 = Db::table('crm_client_order o')
            ->join('crm_order_item oi', 'o.id = oi.order_id', 'LEFT')
            ->join('crm_products p', 'oi.product_name = p.product_name', 'LEFT')
            ->join('crm_product_category c', 'p.category_id = c.id', 'LEFT')
            ->where([$this->getOrgWhere($org, 'p')])
            ->where($o_where)
            ->where(function($query) {
                $query->where('o.product_name', '=', '')
                    ->whereOr('o.product_name', '=', null);
            })
            ->where('oi.product_name', '<>', '')
            ->where('oi.product_name', '<>', null)
            ->where('c.category_name', '<>', '')
            ->where('c.category_name', '<>', null)
            ->where('o.country', '<>', '')
            ->where('p.is_deleted', 0) // soft delete by is_deleted: 排除已删除产品
            ->where('c.is_deleted', 0); // soft delete by is_deleted: 排除已删除分类
        
        if ($yy_where_raw_order) {
            $query_cat2->whereRaw($yy_where_raw_order);
        }
        
        $result_cat2 = $query_cat2->group('c.category_name,o.country')
            ->field('c.category_name,o.country,count(*) as count')
            ->select();
            
        // 合并结果
        $merged_cat = [];
        foreach ($result_cat1 as $item) {
            $key = $item['category_name'] . '|' . $item['country'];
            if (!isset($merged_cat[$key])) {
                $merged_cat[$key] = [
                    'category_name' => $item['category_name'],
                    'country' => $item['country'],
                    'count' => 0
                ];
            }
            $merged_cat[$key]['count'] += $item['count'];
        }
        
        foreach ($result_cat2 as $item) {
            $key = $item['category_name'] . '|' . $item['country'];
            if (!isset($merged_cat[$key])) {
                $merged_cat[$key] = [
                    'category_name' => $item['category_name'],
                    'country' => $item['country'],
                    'count' => 0
                ];
            }
            $merged_cat[$key]['count'] += $item['count'];
        }
        
        // 转换为数组并按分类名称和数量排序
        $order_prod_category_country = array_values($merged_cat);
        usort($order_prod_category_country, function($a, $b) {
            if ($a['category_name'] == $b['category_name']) {
                return $b['count'] - $a['count'];
            }
            return strcmp($a['category_name'], $b['category_name']);
        });
        
        return [
            'oper_prod_country' => $oper_prod_country ?: [],
            'oper_prod_category_country' => $oper_prod_category_country ?: [],
            'order_prod_country' => $order_prod_country ?: [],
            'order_prod_category_country' => $order_prod_category_country ?: [],
        ];
    }
}
