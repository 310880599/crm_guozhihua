<?php
namespace app\admin\controller;

use think\Db;
use think\facade\Request;
use think\facade\Session;
use app\admin\model\ClientFollow as ClientFollowModel;
use app\admin\model\Admin;
use think\facade\Env;
use PhpOffice\PhpSpreadsheet\IOFactory;

class ClientFollow extends Common
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
            return $this->followSearch();
        }
        return $this->fetch();
    }

    public function followSearch()
    {
        $keyword = input('keyword', '');
        $page = input('page/d', 1);
        $limit = input('limit/d', 10);

        $query = ClientFollowModel::order('id desc');
        $list = $query->paginate(['list_rows'=>$limit,'page'=>$page])->toArray();
        return $list;
    }

}
