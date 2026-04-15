<?php

namespace app\admin\service;

class OrderService
{
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
