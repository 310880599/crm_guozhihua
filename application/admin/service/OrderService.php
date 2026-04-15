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
     * 统一凭证图片数据：去空、去重、限制数量并返回数组
     *
     * @param mixed $raw
     * @param int $max
     * @return array
     */
    public static function normalizeVoucherImages($raw, $max = 10)
    {
        $max = (int)$max;
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
