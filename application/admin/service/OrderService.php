<?php

namespace app\admin\service;

class OrderService
{
    /**
     * 将任意值转换为 float，空字符串/null 统一按 0 处理
     *
     * @param mixed $value
     * @return float
     */
    public static function toFloatAmount($value)
    {
        if ($value === null) {
            return 0.0;
        }

        if (is_string($value)) {
            $value = trim($value);
            if ($value === '') {
                return 0.0;
            }
        }

        return (float)$value;
    }

    /**
     * 统一金额字段归一化：所有金额字段转 float，空字符串/null 转 0
     *
     * @param array $params
     * @return array
     */
    public static function normalizeAmountFields(array $params)
    {
        $params['shipping_cost'] = self::toFloatAmount(isset($params['shipping_cost']) ? $params['shipping_cost'] : 0);
        $params['tax_amount'] = self::toFloatAmount(isset($params['tax_amount']) ? $params['tax_amount'] : 0);
        $params['debugging_cost'] = self::toFloatAmount(isset($params['debugging_cost']) ? $params['debugging_cost'] : 0);
        $params['sales_commission'] = self::toFloatAmount(isset($params['sales_commission']) ? $params['sales_commission'] : 0);

        $params['qty'] = self::normalizeAmountArray(isset($params['qty']) ? $params['qty'] : []);
        $params['unit_price'] = self::normalizeAmountArray(isset($params['unit_price']) ? $params['unit_price'] : []);
        $params['purchase_price'] = self::normalizeAmountArray(isset($params['purchase_price']) ? $params['purchase_price'] : []);

        return $params;
    }

    /**
     * 将数组中的数值全部转换为 float
     *
     * @param mixed $value
     * @return array
     */
    public static function normalizeAmountArray($value)
    {
        if (!is_array($value)) {
            if ($value === null || $value === '') {
                return [];
            }
            $value = [$value];
        }

        $result = [];
        foreach ($value as $item) {
            $result[] = self::toFloatAmount($item);
        }

        return $result;
    }

    /**
     * 统一利润重算（不信任前端传入的 profit）
     * 返回：
     * - sales_total 销售总额
     * - cost_total 成本总额
     * - profit 利润
     * - profit_rate 利润率（百分比）
     *
     * @param array $params
     * @return array
     */
    public static function recalculateOrderProfit(array $params)
    {
        $params = self::normalizeAmountFields($params);

        $qtyList = isset($params['qty']) ? $params['qty'] : [];
        $unitPriceList = isset($params['unit_price']) ? $params['unit_price'] : [];
        $purchasePriceList = isset($params['purchase_price']) ? $params['purchase_price'] : [];

        $lineCount = max(count($qtyList), count($unitPriceList), count($purchasePriceList));

        $salesTotal = 0.0;
        $purchaseTotal = 0.0;

        for ($i = 0; $i < $lineCount; $i++) {
            $qty = isset($qtyList[$i]) ? self::toFloatAmount($qtyList[$i]) : 0.0;
            $unitPrice = isset($unitPriceList[$i]) ? self::toFloatAmount($unitPriceList[$i]) : 0.0;
            $purchasePrice = isset($purchasePriceList[$i]) ? self::toFloatAmount($purchasePriceList[$i]) : 0.0;

            $salesTotal += ($qty * $unitPrice);
            $purchaseTotal += ($qty * $purchasePrice);
        }

        $extraCost = $params['shipping_cost']
            + $params['tax_amount']
            + $params['debugging_cost']
            + $params['sales_commission'];

        $costTotal = $purchaseTotal + $extraCost;
        $profit = $salesTotal - $costTotal;
        $profitRate = $salesTotal > 0 ? (($profit / $salesTotal) * 100) : 0.0;

        return [
            'sales_total' => round($salesTotal, 2),
            'cost_total' => round($costTotal, 2),
            'profit' => round($profit, 2),
            'profit_rate' => round($profitRate, 2),
        ];
    }

    /**
     * 利润达到阈值时，是否必须上传两类凭证
     *
     * @param mixed $profit
     * @return bool
     */
    public static function isVoucherRequiredByProfit($profit)
    {
        return floatval($profit) >= 2000;
    }

    /**
     * 统一校验凭证是否满足规则（兼容新增/编辑/旧图/删除旧图/新上传）
     *
     * @param array $params 请求参数（新提交数据）
     * @param array $existing 旧数据（编辑时传入，建议包含 wechat_receipt_image / inquiry_assign_image）
     * @return array ['ok' => bool, 'message' => string]
     */
    public static function validateVoucherRequirement(array $params, array $existing = [])
    {
        $calc = self::recalculateOrderProfit($params);
        $profit = isset($calc['profit']) ? $calc['profit'] : 0.0;

        if (!self::isVoucherRequiredByProfit($profit)) {
            return ['ok' => true, 'message' => ''];
        }

        $wechatImages = self::resolveFinalVoucherImages(
            'wechat_receipt_image',
            'clear_wechat_receipt_image',
            $params,
            $existing
        );
        $inquiryImages = self::resolveFinalVoucherImages(
            'inquiry_assign_image',
            'clear_inquiry_assign_image',
            $params,
            $existing
        );

        if (count($wechatImages) < 1 && count($inquiryImages) < 1) {
            return ['ok' => false, 'message' => '利润达到 2000 及以上时，必须上传微信沟通及付款凭证和询盘来源凭证'];
        }

        if (count($wechatImages) < 1) {
            return ['ok' => false, 'message' => '利润达到 2000 及以上时，必须上传微信沟通及付款凭证'];
        }

        if (count($inquiryImages) < 1) {
            return ['ok' => false, 'message' => '利润达到 2000 及以上时，必须上传询盘来源凭证'];
        }

        return ['ok' => true, 'message' => ''];
    }

    /**
     * 解析编辑后最终生效的凭证图片列表
     *
     * @param string $field 图片字段名
     * @param string $clearField 清空标记字段名
     * @param array $params 新提交参数
     * @param array $existing 旧数据
     * @return array
     */
    public static function resolveFinalVoucherImages($field, $clearField, array $params, array $existing = [])
    {
        $clearFlag = isset($params[$clearField]) ? (int)$params[$clearField] : 0;
        if ($clearFlag === 1) {
            return [];
        }

        $existingImages = self::normalizeVoucherImages(isset($existing[$field]) ? $existing[$field] : []);
        $hasSubmittedField = array_key_exists($field, $params);

        if (!$hasSubmittedField) {
            return $existingImages;
        }

        $submittedImages = self::normalizeVoucherImages($params[$field]);
        if (!empty($submittedImages)) {
            return $submittedImages;
        }

        // 编辑场景下：未显式清空且本次未上传新图时，保留旧图
        return $existingImages;
    }

    /**
     * 历史单图兼容 + 新多图方案：统一解析为图片 URL 数组
     * 兼容输入：
     * - 数组
     * - JSON 数组字符串
     * - 单字符串
     *
     * @param mixed $raw
     * @return array
     */
    public static function parseImageList($raw)
    {
        $list = [];
        if (is_array($raw)) {
            $list = $raw;
        } elseif (is_string($raw)) {
            $raw = trim($raw);
            if ($raw !== '') {
                if (preg_match('/^\s*\[.*\]\s*$/', $raw)) {
                    $decoded = json_decode($raw, true);
                    if (is_array($decoded)) {
                        $list = $decoded;
                    } else {
                        $list = [$raw];
                    }
                } else {
                    $list = [$raw];
                }
            }
        }

        $result = [];
        foreach ($list as $item) {
            $url = '';
            if (is_string($item)) {
                $url = trim($item);
            } elseif (is_array($item)) {
                // 兼容对象结构数组（如 {url/full/path/src}）
                if (!empty($item['full'])) {
                    $url = trim((string)$item['full']);
                } elseif (!empty($item['url'])) {
                    $url = trim((string)$item['url']);
                } elseif (!empty($item['path'])) {
                    $url = trim((string)$item['path']);
                } elseif (!empty($item['src'])) {
                    $url = trim((string)$item['src']);
                }
            } elseif (is_object($item)) {
                $item = (array)$item;
                if (!empty($item['full'])) {
                    $url = trim((string)$item['full']);
                } elseif (!empty($item['url'])) {
                    $url = trim((string)$item['url']);
                } elseif (!empty($item['path'])) {
                    $url = trim((string)$item['path']);
                } elseif (!empty($item['src'])) {
                    $url = trim((string)$item['src']);
                }
            }

            if ($url !== '') {
                $result[] = $url;
            }
        }

        // 去空、去重、重建索引
        $result = array_values(array_unique($result));
        return $result;
    }

    /**
     * 统一凭证图片数据：基于 parseImageList，最多 10 张，返回标准 URL 数组
     *
     * @param mixed $raw
     * @param int $max
     * @return array
     */
    public static function normalizeVoucherImages($raw, $max = 10)
    {
        $max = min(10, (int)$max);
        if ($max <= 0) {
            return [];
        }

        $images = self::parseImageList($raw);
        if (count($images) > $max) {
            $images = array_slice($images, 0, $max);
        }

        return $images;
    }

    /**
     * 构建前端预览组件所需图片项结构
     *
     * @param mixed $raw
     * @return array
     */
    public static function buildPreviewImageItems($raw)
    {
        $images = self::normalizeVoucherImages($raw);
        $items = [];

        foreach ($images as $url) {
            $items[] = [
                'full' => $url,
                'thumb' => $url,
            ];
        }

        return $items;
    }

    /**
     * 根据图片数量计算图片列宽
     *
     * 规则：
     * - 0~1 张：180
     * - 2 张：260
     * - 3 张：340
     * - 4 张：420
     * - >=5 张：按每张 +80 递增，最大 780
     *
     * @param mixed $images
     * @return int
     */
    public static function calcImageColumnWidth($images)
    {
        $count = count(self::parseImageList($images));

        if ($count <= 1) {
            return 180;
        }

        $width = 180 + (($count - 1) * 80);
        return (int)min($width, 780);
    }

    /**
     * 处理询盘图片并返回可入库的 JSON 字符串
     *
     * @param mixed $raw
     * @return string
     */
    public static function handleInquiryImages($raw)
    {
        // 先统一解析，兼容历史单图/对象数组等输入
        $parsed = self::parseImageList($raw);
        // 再执行凭证图片规范化（去重、去空、数量限制等）
        $images = self::normalizeVoucherImages($parsed);

        $json = json_encode($images, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        return $json === false ? '[]' : $json;
    }
}
