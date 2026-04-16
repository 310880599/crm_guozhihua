<?php

namespace app\admin\service;

class OrderImageService
{
    public static function parseImageField($raw)
    {
        try {
            // 1) 空值安全处理
            if ($raw === null) {
                return [];
            }

            if (is_string($raw)) {
                $raw = trim($raw);
                if ($raw === '' || $raw === 'null' || $raw === '[]') {
                    return [];
                }

                // 3) JSON 数组字符串
                if (substr($raw, 0, 1) === '[') {
                    $decoded = json_decode($raw, true);
                    if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                        $raw = $decoded;
                    } else {
                        // 2) 单字符串（JSON 解析失败时按普通字符串处理）
                        return [
                            ['full' => $raw, 'thumb' => $raw],
                        ];
                    }
                } else {
                    // 2) 单字符串
                    return [
                        ['full' => $raw, 'thumb' => $raw],
                    ];
                }
            }

            // 标准化为可遍历数组
            if (is_object($raw)) {
                $raw = (array)$raw;
            }
            if (!is_array($raw)) {
                return [];
            }

            $result = [];
            $seen = [];

            foreach ($raw as $item) {
                $full = '';
                $thumb = '';

                if (is_string($item)) {
                    $full = trim($item);
                    $thumb = $full;
                } elseif (is_object($item)) {
                    // 4) 对象数组
                    $item = (array)$item;
                }

                if (is_array($item)) {
                    // 优先 full/path/src/url 作为 full
                    foreach (['full', 'path', 'src', 'url'] as $key) {
                        if (!empty($item[$key])) {
                            $full = trim((string)$item[$key]);
                            break;
                        }
                    }

                    if ($full === '') {
                        continue;
                    }

                    // thumb 优先 thumb/thumbnail/small，没有则回落 full
                    $thumb = $full;
                    foreach (['thumb', 'thumbnail', 'small'] as $key) {
                        if (!empty($item[$key])) {
                            $thumb = trim((string)$item[$key]);
                            break;
                        }
                    }
                }

                if ($full === '') {
                    continue;
                }

                // 6) 自动去重（按 full 去重）
                if (isset($seen[$full])) {
                    continue;
                }
                $seen[$full] = true;

                // 5) 统一输出结构
                $result[] = [
                    'full' => $full,
                    'thumb' => ($thumb !== '' ? $thumb : $full),
                ];
            }

            return $result;
        } catch (\Throwable $e) {
            // 7) 不报错：兜底返回空数组
            return [];
        }
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

            $rows[$idx]['wechat_receipt_images'] = self::parseImageField($row['wechat_receipt_image'] ?? null);
            $rows[$idx]['inquiry_assign_images'] = self::parseImageField($row['inquiry_assign_image'] ?? null);
        }

        return $rows;
    }
}
