<?php

namespace app\admin\service;

/**
 * 订单凭证图片字段解析（微信沟通/付款凭证、询盘来源凭证等）
 *
 * 兼容输入（统一转成「条目数组」再规范为 full/thumb）：
 * 1. 单图字符串：单个 URL（含逗号分隔多 URL、非 JSON 的旧数据）
 * 2. JSON 数组：字符串 '["url1","url2"]' / '[{...}]'，或已 json_decode 的索引数组
 * 3. 数组对象：PHP 数组，元素为 string | array | object（stdClass 等），含 full/thumb 或 path/src/url
 *
 * 统一输出（索引数组；每项仅含 full、thumb，JSON 即 [{"full":"...","thumb":"..."},...]）：
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
            $items = self::coerceToItemArray($raw);
            if (!is_array($items)) {
                return [];
            }
            $normalized = self::normalizeItemList($items);
            return self::finalizeUniformList($normalized);
        } catch (\Throwable $e) {
            return [];
        }
    }

    /**
     * 将已解析条目再压成统一结构（仅保留 full/thumb，去空、去杂键）
     *
     * @param array<int, mixed> $rows
     * @return array<int, array{full: string, thumb: string}>
     */
    public static function finalizeUniformList($rows): array
    {
        if (!is_array($rows)) {
            return [];
        }
        $out = [];
        foreach ($rows as $row) {
            if (!is_array($row)) {
                continue;
            }
            $full = isset($row['full']) ? trim((string) $row['full']) : '';
            if ($full === '') {
                continue;
            }
            $thumb = isset($row['thumb']) ? trim((string) $row['thumb']) : '';
            if ($thumb === '') {
                $thumb = $full;
            }
            $out[] = ['full' => $full, 'thumb' => $thumb];
        }
        return $out;
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

        // 单对象 / ArrayAccess：统一成「数组里一条」，再走 normalize
        if (is_object($raw)) {
            if ($raw instanceof \Traversable) {
                try {
                    $raw = iterator_to_array($raw, false);
                } catch (\Throwable $e) {
                    return [];
                }
                if (!is_array($raw)) {
                    return [];
                }
            } else {
                return [self::objectToArray($raw)];
            }
        }

        if (is_string($raw)) {
            $s = self::normalizeJsonLikeString($raw);
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
            // 已是 PHP 数组：索引数组（多图）或单条关联数组（单对象）
            if (self::isAssociativeArray($raw)) {
                return [$raw];
            }

            // 索引数组：元素可能为 string / array / object
            $out = [];
            foreach ($raw as $item) {
                if (is_object($item)) {
                    $out[] = self::objectToArray($item);
                } else {
                    $out[] = $item;
                }
            }
            return $out;
        }

        return [];
    }

    /**
     * 去掉 BOM、首尾空白，便于 JSON 解析
     */
    private static function normalizeJsonLikeString($s)
    {
        $s = (string)$s;
        $s = preg_replace('/^\xEF\xBB\xBF/', '', $s);
        return trim($s);
    }

    /**
     * 对象转关联数组（便于 itemToFullThumb 读 full/url 等键）
     *
     * @param object $obj
     * @return array<string, mixed>
     */
    private static function objectToArray($obj)
    {
        if ($obj instanceof \JsonSerializable) {
            $v = $obj->jsonSerialize();
            if (is_array($v)) {
                return $v;
            }
            if (is_object($v)) {
                return (array)$v;
            }
        }
        return json_decode(json_encode($obj, JSON_UNESCAPED_UNICODE), true) ?: (array)$obj;
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
            $item = self::objectToArray($item);
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
