<?php

namespace app\admin\service;

/**
 * 订单列表行补充 wechat_receipt_images / inquiry_assign_images（供检查订单等接口）
 * 结构统一为索引数组，元素为 {full, thumb}，见 {@see VoucherImageParseService::parseList}
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

    /**
     * 订单编辑页 GET：补充 wechat_receipt_images、wechat_receipt_full_urls；
     * 并将 inquiry_assign_image 替换为解析后的 [{full,thumb},...]（与历史模板约定一致）
     *
     * @param array<string, mixed> $order
     * @return array<string, mixed>
     */
    public static function enrichOrderForEditView(array $order): array
    {
        $wechatReceiptImages = VoucherImageParseService::parseList($order['wechat_receipt_image'] ?? null);
        $order['wechat_receipt_images'] = $wechatReceiptImages;
        $order['wechat_receipt_full_urls'] = array_column($wechatReceiptImages, 'full');

        $inquiryAssignImages = VoucherImageParseService::parseList($order['inquiry_assign_image'] ?? null);
        $order['inquiry_assign_image'] = $inquiryAssignImages;

        return $order;
    }

    /**
     * 为订单列表每一行补充 wechat_receipt_images、inquiry_assign_images（必有键，无图则为 []）
     *
     * @param mixed $rows
     * @return array<int, array<string, mixed>>
     */
    public static function appendOrderImageFields($rows)
    {
        if (!is_array($rows)) {
            return [];
        }
        if ($rows === []) {
            return [];
        }

        foreach ($rows as $idx => $row) {
            $row = self::normalizeRowToArray($row);
            if ($row === null) {
                continue;
            }

            $row['wechat_receipt_images'] = VoucherImageParseService::parseList($row['wechat_receipt_image'] ?? null);
            $row['inquiry_assign_images'] = VoucherImageParseService::parseList($row['inquiry_assign_image'] ?? null);

            if (!is_array($row['wechat_receipt_images'])) {
                $row['wechat_receipt_images'] = [];
            }
            if (!is_array($row['inquiry_assign_images'])) {
                $row['inquiry_assign_images'] = [];
            }

            $rows[$idx] = $row;
        }

        return $rows;
    }

    /**
     * @param mixed $row
     * @return array<string, mixed>|null
     */
    private static function normalizeRowToArray($row)
    {
        if (is_array($row)) {
            return $row;
        }
        if (is_object($row)) {
            if (method_exists($row, 'toArray')) {
                $arr = $row->toArray();
                return is_array($arr) ? $arr : null;
            }
            $json = json_encode($row, JSON_UNESCAPED_UNICODE);
            if ($json === false) {
                return null;
            }
            $arr = json_decode($json, true);
            return is_array($arr) ? $arr : null;
        }
        return null;
    }
}
