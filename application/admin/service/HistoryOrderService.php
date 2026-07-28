<?php

namespace app\admin\service;

use think\Db;

class HistoryOrderService
{
    /**
     * 生成订单编号：H + YYYYMMDD + 4位流水号
     * 查询范围覆盖 is_deleted=0/1，软删除数据不释放编号
     *
     * @param array $reservedOrderNos 本次操作中已占用但尚未入库的编号
     * @return string
     */
    public function generateOrderNo(array $reservedOrderNos = []): string
    {
        $datePart = date('Ymd');
        $prefix = 'H' . $datePart;

        $maxOrderNo = Db::name('crm_client_history_order')
            ->whereLike('order_no', $prefix . '%')
            ->order('order_no', 'desc')
            ->value('order_no');

        $seq = 1;
        if (!empty($maxOrderNo) && preg_match('/^H\d{8}(\d{4})$/', (string)$maxOrderNo, $matches)) {
            $seq = ((int)$matches[1]) + 1;
        }

        $reservedSet = [];
        foreach ($reservedOrderNos as $orderNo) {
            $reservedSet[(string)$orderNo] = true;
        }

        do {
            $generated = $prefix . str_pad((string)$seq, 4, '0', STR_PAD_LEFT);
            $seq++;
        } while (isset($reservedSet[$generated]));

        return $generated;
    }
}
