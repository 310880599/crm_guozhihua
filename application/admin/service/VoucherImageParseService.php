<?php

namespace app\admin\service;

/**
 * 订单凭证图片字段解析（微信沟通/付款凭证、询盘来源凭证等）
 *
 * 支持输入形态：
 * 1. 单图字符串：单个 URL
 * 2. JSON 数组：字符串形如 ["url1","url2"] 或已 decode 的索引数组
 * 3. 对象数组：数组元素为对象/关联数组，含 full、thumb（或 path/src/url、thumbnail 等）
 *
 * 统一输出：
 *   [ ['full' => string, 'thumb' => string], ... ]
 */
class VoucherImageParseService
{
    /**
     * @param mixed $raw 库字段或接口入参原始值
     * @return array<int, array{full: string, thumb: string}>
     */
    public static function parseList($raw)
    {
        try {
            return self::normalizeItemList(self::coerceToItemArray($raw));
        } catch (\Throwable $e) {
            return [];
        }
    }

    /**
     * 将各种输入统一成「待归一化的条目数组」（字符串 URL 或含图字段的关联数组）
     *
     * @param mixed $raw
     * @return array<int, mixed>
     */
    protected static function coerceToItemArray($raw)
    {
        if ($raw === null) {
            return [];
        }

        if (is_object($raw)) {
            return [(array)$raw];
        }

        if (is_string($raw)) {
            $s = trim($raw);
            if ($s === '' || $s === 'null' || $s === '[]') {
                return [];
            }

            // JSON 对象：{"full":"...","thumb":"..."}
            if (isset($s[0]) && $s[0] === '{') {
                $decoded = json_decode($s, true);
                if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                    if ($decoded === []) {
                        return [];
                    }
                    return [$decoded];
                }

                return [];
            }

            // JSON 数组：["url1","url2"] 或 [{...},{...}]
            if ($s[0] === '[') {
                $decoded = json_decode($s, true);
                if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                    return $decoded;
                }

                // 与待审核页 parseImages 一致：JSON 解析失败时按逗号拆（旧数据）
                $parts = array_values(array_filter(array_map('trim', explode(',', $s)), function ($p) {
                    return $p !== '';
                }));
                return !empty($parts) ? $parts : [$s];
            }

            // 逗号分隔多 URL（旧数据，非 JSON）
            if (strpos($s, ',') !== false) {
                $parts = array_values(array_filter(array_map('trim', explode(',', $s)), function ($p) {
                    return $p !== '';
                }));
                if (count($parts) > 1) {
                    return $parts;
                }
            }

            // 单图 URL 字符串
            return [$s];
        }

        if (is_array($raw)) {
            if ($raw === []) {
                return [];
            }
            // 已是 PHP 数组：索引数组（JSON 数组）或需再判断
            if (self::isAssociativeArray($raw)) {
                return [$raw];
            }

            return $raw;
        }

        return [];
    }

    /**
     * @param array<int, mixed> $items
     * @return array<int, array{full: string, thumb: string}>
     */
    protected static function normalizeItemList(array $items)
    {
        $result = [];
        $seenFulls = [];

        foreach ($items as $item) {
            $pair = self::itemToFullThumb($item);
            if ($pair === null) {
                continue;
            }
            $full = $pair['full'];
            if (isset($seenFulls[$full])) {
                continue;
            }
            $seenFulls[$full] = true;
            $result[] = $pair;
        }

        return $result;
    }

    /**
     * @param mixed $item 字符串 URL 或对象/关联数组
     * @return array{full: string, thumb: string}|null
     */
    protected static function itemToFullThumb($item)
    {
        if (is_string($item)) {
            $full = trim($item);
            if ($full === '') {
                return null;
            }

            return ['full' => $full, 'thumb' => $full];
        }

        if (is_object($item)) {
            $item = (array)$item;
        }

        if (!is_array($item)) {
            return null;
        }

        $full = '';
        foreach (['full', 'path', 'src', 'url'] as $key) {
            if (!empty($item[$key])) {
                $full = trim((string)$item[$key]);
                break;
            }
        }

        if ($full === '') {
            return null;
        }

        $thumb = $full;
        foreach (['thumb', 'thumbnail', 'small'] as $key) {
            if (!empty($item[$key])) {
                $thumb = trim((string)$item[$key]);
                break;
            }
        }

        return ['full' => $full, 'thumb' => $thumb !== '' ? $thumb : $full];
    }

    /**
     * 区分「单条对象的关联数组」与「JSON 数组解码后的索引数组」
     */
    protected static function isAssociativeArray(array $arr)
    {
        if ($arr === []) {
            return false;
        }

        return array_keys($arr) !== range(0, count($arr) - 1);
    }
}
