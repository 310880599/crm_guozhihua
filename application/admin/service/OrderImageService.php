<?php

namespace app\admin\service;

/**
 * 订单列表行补充 wechat_receipt_images / inquiry_assign_images（供检查订单等接口）
 */
class OrderImageService
{
    /**
     * 与 {@see VoucherImageParseService::parseList} 相同，保留旧调用名
     *
     * @param mixed $raw
     * @return array<int, array{full: string, thumb: string}>
     */
    public static function parseImageField($raw)
    {
        return VoucherImageParseService::parseList($raw);
    }

    public static function appendOrderImageFields($rows)
    {
        if (!is_array($rows) || empty($rows)) {
            return is_array($rows) ? $rows : [];
        }

        foreach ($rows as $idx => $row) {
            if (!is_array($row)) {
                continue;
            }

            $rows[$idx]['wechat_receipt_images'] = VoucherImageParseService::parseList($row['wechat_receipt_image'] ?? null);
            $rows[$idx]['inquiry_assign_images'] = VoucherImageParseService::parseList($row['inquiry_assign_image'] ?? null);
        }

        return $rows;
    }
}
