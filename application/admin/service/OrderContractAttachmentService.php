<?php

namespace app\admin\service;

use think\Db;
use think\facade\Env;
use think\facade\Session;

/**
 * 订单合同附件：解析 / 规范化 / 必填校验 / 安全存储与访问
 *
 * 与微信沟通凭证、询盘来源凭证完全隔离（阈值 3000，独立白名单与私有目录）。
 */
class OrderContractAttachmentService
{
    const PROFIT_THRESHOLD = 3000;
    const MAX_COUNT = 10;
    const MAX_SIZE_BYTES = 20971520; // 20MB
    const SESSION_KEY = 'contract_attachment_uploads';

    /** @var string[] */
    const ALLOWED_EXT = ['jpg', 'jpeg', 'png', 'pdf', 'doc', 'docx'];

    /** @var array<string, string[]> */
    const ALLOWED_MIME = [
        'jpg'  => ['image/jpeg'],
        'jpeg' => ['image/jpeg'],
        'png'  => ['image/png'],
        'pdf'  => ['application/pdf'],
        'doc'  => ['application/msword', 'application/octet-stream'],
        'docx' => [
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/zip',
            'application/octet-stream',
        ],
    ];

    /**
     * 利润是否要求合同附件（>=3000，经 round 后判断）
     *
     * @param mixed $profit
     * @return bool
     */
    public static function isRequiredByProfit($profit)
    {
        return round(floatval($profit), 2) >= self::PROFIT_THRESHOLD;
    }

    /**
     * 基于服务端重算利润判断是否必填
     *
     * @param array $params 含 qty/unit_price/purchase_price 与费用字段
     * @return array{required:bool,profit:float}
     */
    public static function evaluateRequirement(array $params)
    {
        $calc = OrderService::recalculateOrderProfit($params);
        $profit = isset($calc['profit']) ? (float)$calc['profit'] : 0.0;
        return [
            'required' => self::isRequiredByProfit($profit),
            'profit'   => $profit,
        ];
    }

    /**
     * 解析库值 / 提交值为对象数组（兼容 NULL、空串、非法 JSON）
     *
     * @param mixed $raw
     * @return array<int, array<string, mixed>>
     */
    public static function parseList($raw)
    {
        if ($raw === null) {
            return [];
        }
        if (is_array($raw)) {
            return self::normalizeList($raw);
        }
        if (!is_string($raw)) {
            return [];
        }
        $raw = trim($raw);
        if ($raw === '' || strcasecmp($raw, 'null') === 0) {
            return [];
        }
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            return [];
        }
        return self::normalizeList($decoded);
    }

    /**
     * @param array $list
     * @return array<int, array<string, mixed>>
     */
    public static function normalizeList(array $list)
    {
        $out = [];
        foreach ($list as $item) {
            $normalized = self::normalizeItem($item);
            if ($normalized !== null) {
                $out[] = $normalized;
            }
            if (count($out) >= self::MAX_COUNT) {
                break;
            }
        }
        return $out;
    }

    /**
     * @param mixed $item
     * @return array<string, mixed>|null
     */
    public static function normalizeItem($item)
    {
        if (!is_array($item)) {
            return null;
        }
        $id = preg_replace('/[^a-f0-9]/i', '', (string)($item['id'] ?? ''));
        if ($id === '' || strlen($id) < 8) {
            return null;
        }
        $storageKey = self::sanitizeStorageKey($item['storage_key'] ?? '');
        if ($storageKey === '') {
            return null;
        }
        $ext = strtolower(pathinfo($storageKey, PATHINFO_EXTENSION));
        if (!in_array($ext, self::ALLOWED_EXT, true)) {
            return null;
        }
        $name = trim((string)($item['name'] ?? ('合同附件.' . $ext)));
        $name = mb_substr(preg_replace('/[\r\n\t]+/', ' ', $name), 0, 180, 'UTF-8');
        if ($name === '') {
            $name = '合同附件.' . $ext;
        }
        $size = (int)($item['size'] ?? 0);
        if ($size < 0) {
            $size = 0;
        }
        $mime = trim((string)($item['mime'] ?? ''));
        $uploadedAt = trim((string)($item['uploaded_at'] ?? ''));
        if ($uploadedAt === '') {
            $uploadedAt = date('Y-m-d H:i:s');
        }

        return [
            'id'          => strtolower($id),
            'storage_key' => $storageKey,
            'name'        => $name,
            'ext'         => $ext,
            'size'        => $size,
            'mime'        => $mime,
            'uploaded_at' => $uploadedAt,
            'uploader_id' => (int)($item['uploader_id'] ?? 0),
            'order_id'    => (int)($item['order_id'] ?? 0),
        ];
    }

    /**
     * @param mixed $key
     * @return string
     */
    public static function sanitizeStorageKey($key)
    {
        $key = str_replace('\\', '/', trim((string)$key));
        $key = ltrim($key, '/');
        if ($key === '' || strpos($key, '..') !== false) {
            return '';
        }
        if (!preg_match('#^(tmp/\d+/[a-f0-9]+/[a-f0-9]+\.(?:jpg|jpeg|png|pdf|doc|docx)|\d{8}/[a-f0-9]+\.(?:jpg|jpeg|png|pdf|doc|docx))$#i', $key)) {
            return '';
        }
        return $key;
    }

    /**
     * @param array $list
     * @return string|null JSON 或 null（表示空）
     */
    public static function encodeList(array $list)
    {
        $normalized = self::normalizeList($list);
        if (empty($normalized)) {
            return null;
        }
        return json_encode($normalized, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /**
     * JSON 结构合法的附件数量（不检查磁盘文件是否存在；用于展示/兼容历史）
     *
     * @param mixed $raw
     * @return int
     */
    public static function countValid($raw)
    {
        return count(self::parseList($raw));
    }

    /**
     * 结构合法且磁盘文件真实存在的附件数量（正式必填校验用）
     *
     * @param mixed $raw
     * @return int
     */
    public static function countExistingValid($raw)
    {
        $n = 0;
        foreach (self::parseList($raw) as $item) {
            if (self::fileExistsForItem($item)) {
                $n++;
            }
        }
        return $n;
    }

    /**
     * @param array $item
     * @return bool
     */
    public static function fileExistsForItem(array $item)
    {
        $path = self::getAbsolutePath($item['storage_key'] ?? '');
        return $path !== '' && is_file($path);
    }

    /**
     * 正式提交：列表中每一项都必须真实存在于磁盘
     *
     * @param array $list
     * @return array{ok:bool,message:string}
     */
    public static function assertFilesExist(array $list)
    {
        if (empty($list)) {
            return ['ok' => false, 'message' => '利润达到3000元及以上，必须上传合同附件'];
        }
        foreach ($list as $item) {
            $normalized = self::normalizeItem($item);
            if ($normalized === null || !self::fileExistsForItem($normalized)) {
                return ['ok' => false, 'message' => '合同附件文件不存在或无效，请重新上传'];
            }
        }
        return ['ok' => true, 'message' => ''];
    }

    /**
     * 存储根目录（位于 runtime，非 public）
     *
     * @return string
     */
    public static function getStorageRoot()
    {
        $root = rtrim(str_replace('\\', '/', Env::get('root_path')), '/');
        return $root . '/runtime/storage/contract_attachments';
    }

    /**
     * @param string $storageKey
     * @return string
     */
    public static function getAbsolutePath($storageKey)
    {
        $key = self::sanitizeStorageKey($storageKey);
        if ($key === '') {
            return '';
        }
        return self::getStorageRoot() . '/' . $key;
    }

    /**
     * @param string $dir
     * @return array{ok:bool,message:string}
     */
    public static function ensureDirectory($dir)
    {
        if (is_dir($dir)) {
            if (!is_writable($dir)) {
                return ['ok' => false, 'message' => '合同附件目录不可写：' . $dir];
            }
            return ['ok' => true, 'message' => ''];
        }
        if (!@mkdir($dir, 0755, true) && !is_dir($dir)) {
            return ['ok' => false, 'message' => '合同附件目录创建失败：' . $dir];
        }
        return ['ok' => true, 'message' => ''];
    }

    /**
     * @return string
     */
    public static function generateId()
    {
        try {
            return bin2hex(random_bytes(8));
        } catch (\Exception $e) {
            return md5(uniqid((string)mt_rand(), true));
        }
    }

    /**
     * 将上传文件落入私有目录，并登记到当前会话（防伪造复用）
     *
     * @param \think\File $file
     * @param int $uploaderId
     * @param int $orderId
     * @return array{ok:bool,message:string,item?:array,preview_url?:string}
     */
    public static function storeUploadedFile($file, $uploaderId, $orderId = 0)
    {
        $uploaderId = (int)$uploaderId;
        $orderId = (int)$orderId;
        if ($uploaderId <= 0) {
            return ['ok' => false, 'message' => '未登录，无法上传合同附件'];
        }
        if (!$file) {
            return ['ok' => false, 'message' => '未检测到上传文件'];
        }

        $origName = (string)$file->getInfo('name');
        $tmpPath = (string)$file->getInfo('tmp_name');
        $size = (int)$file->getInfo('size');
        if ($size <= 0) {
            return ['ok' => false, 'message' => '空文件不能上传'];
        }
        if ($size > self::MAX_SIZE_BYTES) {
            return ['ok' => false, 'message' => '单个合同附件不能超过 20MB'];
        }

        $ext = strtolower(pathinfo($origName, PATHINFO_EXTENSION));
        if (!in_array($ext, self::ALLOWED_EXT, true)) {
            return ['ok' => false, 'message' => '仅支持 JPG、JPEG、PNG、PDF、DOC、DOCX'];
        }
        if (preg_match('/\.(php|phtml|phar|html|htm|js|exe|sh|bat|cmd|asp|aspx|jsp)(\.|$)/i', $origName)) {
            return ['ok' => false, 'message' => '禁止上传可执行或脚本类文件'];
        }
        if (!is_uploaded_file($tmpPath)) {
            return ['ok' => false, 'message' => '非法上传来源'];
        }

        $mime = self::detectMime($tmpPath, $ext);
        if (!self::isMimeAllowed($ext, $mime)) {
            return ['ok' => false, 'message' => '文件内容与扩展名不匹配，已拒绝'];
        }
        if (!self::passesContentSafety($tmpPath, $ext)) {
            return ['ok' => false, 'message' => '文件内容校验失败，已拒绝'];
        }

        $id = self::generateId();
        $safeName = self::safeOriginalName($origName, $ext);

        if ($orderId > 0) {
            $dateDir = date('Ymd');
            $relDir = $dateDir;
            $storageKey = $dateDir . '/' . $id . '.' . $ext;
        } else {
            $token = self::generateId();
            $relDir = 'tmp/' . $uploaderId . '/' . $token;
            $storageKey = $relDir . '/' . $id . '.' . $ext;
        }

        $absDir = self::getStorageRoot() . '/' . $relDir;
        $dirCheck = self::ensureDirectory($absDir);
        if (!$dirCheck['ok']) {
            return $dirCheck;
        }

        $absPath = self::getAbsolutePath($storageKey);
        if ($absPath === '') {
            return ['ok' => false, 'message' => '合同附件存储路径非法'];
        }
        $moved = false;
        if (is_uploaded_file($tmpPath) && @move_uploaded_file($tmpPath, $absPath)) {
            $moved = true;
        } elseif (@rename($tmpPath, $absPath)) {
            $moved = true;
        } elseif (@copy($tmpPath, $absPath)) {
            @unlink($tmpPath);
            $moved = true;
        } else {
            // 兼容 ThinkPHP File::move
            try {
                $info = $file->move($absDir, $id . '.' . $ext, true);
                if ($info) {
                    $moved = true;
                    $absPath = str_replace('\\', '/', $info->getPathname());
                }
            } catch (\Exception $e) {
                $moved = false;
            }
        }
        if (!$moved || !is_file($absPath)) {
            return ['ok' => false, 'message' => '合同附件保存失败，请检查目录权限'];
        }
        @chmod($absPath, 0644);

        $item = [
            'id'          => $id,
            'storage_key' => $storageKey,
            'name'        => $safeName,
            'ext'         => $ext,
            'size'        => $size,
            'mime'        => $mime,
            'uploaded_at' => date('Y-m-d H:i:s'),
            'uploader_id' => $uploaderId,
            'order_id'    => $orderId > 0 ? $orderId : 0,
        ];
        self::rememberSessionUpload($item);

        return [
            'ok'          => true,
            'message'     => '上传成功',
            'item'        => $item,
            'preview_url' => self::buildServeUrl($item['id'], $orderId, 'inline'),
            'download_url'=> self::buildServeUrl($item['id'], $orderId, 'attachment'),
        ];
    }

    /**
     * @param string $path
     * @param string $ext
     * @return string
     */
    public static function detectMime($path, $ext)
    {
        $mime = '';
        if (function_exists('finfo_open')) {
            $finfo = @finfo_open(FILEINFO_MIME_TYPE);
            if ($finfo) {
                $mime = (string)@finfo_file($finfo, $path);
                @finfo_close($finfo);
            }
        }
        if ($mime === '' && function_exists('mime_content_type')) {
            $mime = (string)@mime_content_type($path);
        }
        if ($mime === '' || $mime === 'application/octet-stream') {
            $map = [
                'jpg' => 'image/jpeg', 'jpeg' => 'image/jpeg', 'png' => 'image/png',
                'pdf' => 'application/pdf', 'doc' => 'application/msword',
                'docx' => 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            ];
            if ($mime === '' && isset($map[$ext])) {
                $mime = $map[$ext];
            }
        }
        return strtolower(trim($mime));
    }

    /**
     * @param string $ext
     * @param string $mime
     * @return bool
     */
    public static function isMimeAllowed($ext, $mime)
    {
        $ext = strtolower($ext);
        $mime = strtolower(trim($mime));
        if (!isset(self::ALLOWED_MIME[$ext])) {
            return false;
        }
        if ($mime === '') {
            return false;
        }
        return in_array($mime, self::ALLOWED_MIME[$ext], true);
    }

    /**
     * 基础内容安全：图片 getimagesize；PDF 头；DOC/DOCX 魔数；拒绝 PHP 标签伪装
     *
     * @param string $path
     * @param string $ext
     * @return bool
     */
    public static function passesContentSafety($path, $ext)
    {
        $ext = strtolower($ext);
        $head = (string)@file_get_contents($path, false, null, 0, 512);
        if ($head === '') {
            return false;
        }
        if (preg_match('/<\?php|<\?=|<script[\s>]/i', $head)) {
            return false;
        }
        if (in_array($ext, ['jpg', 'jpeg', 'png'], true)) {
            $info = @getimagesize($path);
            if ($info === false) {
                return false;
            }
            $type = (int)($info[2] ?? 0);
            if ($ext === 'png' && $type !== IMAGETYPE_PNG) {
                return false;
            }
            if (($ext === 'jpg' || $ext === 'jpeg') && $type !== IMAGETYPE_JPEG) {
                return false;
            }
            return true;
        }
        if ($ext === 'pdf') {
            return strncmp($head, '%PDF', 4) === 0;
        }
        if ($ext === 'doc') {
            // OLE Compound File
            return substr($head, 0, 8) === "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1";
        }
        if ($ext === 'docx') {
            // ZIP / OOXML
            return substr($head, 0, 2) === 'PK';
        }
        return false;
    }

    /**
     * @param string $origName
     * @param string $ext
     * @return string
     */
    public static function safeOriginalName($origName, $ext)
    {
        $base = pathinfo($origName, PATHINFO_FILENAME);
        $base = preg_replace('/[^\x{4e00}-\x{9fa5}A-Za-z0-9_\-\.\(\)\[\]\s]/u', '', (string)$base);
        $base = trim(preg_replace('/\s+/', ' ', $base));
        if ($base === '') {
            $base = '合同附件';
        }
        $base = mb_substr($base, 0, 120, 'UTF-8');
        return $base . '.' . $ext;
    }

    /**
     * @param array $item
     * @return void
     */
    public static function rememberSessionUpload(array $item)
    {
        $bag = Session::get(self::SESSION_KEY);
        if (!is_array($bag)) {
            $bag = [];
        }
        $bag[$item['id']] = $item;
        // 控制会话体积：最多保留最近 40 个临时上传
        if (count($bag) > 40) {
            $bag = array_slice($bag, -40, null, true);
        }
        Session::set(self::SESSION_KEY, $bag);
    }

    /**
     * @param string $id
     * @return array|null
     */
    public static function getSessionUpload($id)
    {
        $id = strtolower(preg_replace('/[^a-f0-9]/i', '', (string)$id));
        if ($id === '') {
            return null;
        }
        $bag = Session::get(self::SESSION_KEY);
        if (!is_array($bag) || !isset($bag[$id]) || !is_array($bag[$id])) {
            return null;
        }
        return self::normalizeItem($bag[$id]);
    }

    /**
     * @param string[] $ids
     * @return void
     */
    public static function forgetSessionUploads(array $ids)
    {
        $bag = Session::get(self::SESSION_KEY);
        if (!is_array($bag)) {
            return;
        }
        foreach ($ids as $id) {
            unset($bag[$id]);
        }
        Session::set(self::SESSION_KEY, $bag);
    }

    /**
     * 将临时文件提升为正式绑定路径（同目录可跳过）
     *
     * 重要：文件系统不参与 MySQL 事务。默认仅 copy、不删 tmp，
     * 以便数据库失败后 Session + tmp 仍可安全重试；提交成功后再 cleanupTempSources。
     *
     * @param array $item
     * @param int $orderId
     * @param bool $removeSource 是否删除源临时文件（仅在 DB 已成功后的清理阶段使用）
     * @return array|null
     */
    public static function promoteToOrder(array $item, $orderId, $removeSource = false)
    {
        $item = self::normalizeItem($item);
        if ($item === null) {
            return null;
        }
        $orderId = (int)$orderId;
        $src = self::getAbsolutePath($item['storage_key']);
        if ($src === '' || !is_file($src)) {
            return null;
        }

        // 已是正式目录
        if (strpos($item['storage_key'], 'tmp/') !== 0) {
            $item['order_id'] = $orderId > 0 ? $orderId : (int)$item['order_id'];
            return $item;
        }

        $dateDir = date('Ymd');
        $newKey = $dateDir . '/' . $item['id'] . '.' . $item['ext'];
        $destDir = self::getStorageRoot() . '/' . $dateDir;
        $dirCheck = self::ensureDirectory($destDir);
        if (!$dirCheck['ok']) {
            return null;
        }
        $dest = self::getAbsolutePath($newKey);
        if ($dest === '') {
            return null;
        }
        // 目标已存在（上次 DB 失败后的孤儿副本）则可复用
        if (!is_file($dest)) {
            if (!@copy($src, $dest)) {
                return null;
            }
            @chmod($dest, 0644);
        }
        if ($removeSource && is_file($src) && realpath($src) !== realpath($dest)) {
            @unlink($src);
        }
        $item['storage_key'] = $newKey;
        $item['order_id'] = $orderId;
        return $item;
    }

    /**
     * 将列表全部提升为正式路径；任一项失败则整体失败（禁止静默丢弃）
     *
     * @param array $list
     * @param int $orderId
     * @param bool $removeSource
     * @return array{ok:bool,message:string,list:array}
     */
    public static function promoteListToOrder(array $list, $orderId, $removeSource = false)
    {
        $bound = [];
        foreach ($list as $item) {
            $promoted = self::promoteToOrder(is_array($item) ? $item : [], $orderId, $removeSource);
            if ($promoted === null) {
                return [
                    'ok' => false,
                    'message' => '合同附件绑定订单失败，请重新上传后提交',
                    'list' => [],
                ];
            }
            $bound[] = $promoted;
        }
        foreach ($bound as $item) {
            if (strpos((string)($item['storage_key'] ?? ''), 'tmp/') === 0) {
                return [
                    'ok' => false,
                    'message' => '合同附件仍处于临时目录，已中止提交',
                    'list' => [],
                ];
            }
        }
        return ['ok' => true, 'message' => '', 'list' => $bound];
    }

    /**
     * 列表中是否仍引用 tmp 临时路径（历史草稿兼容判断，不访问磁盘）
     *
     * @param mixed $raw
     * @return bool
     */
    public static function listHasTempStorage($raw)
    {
        foreach (self::parseList($raw) as $item) {
            if (strpos((string)($item['storage_key'] ?? ''), 'tmp/') === 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * storage_key 是否已被其他订单 JSON 引用。用于清理 tmp 前确认，避免误删共享附件。
     *
     * @param string $storageKey
     * @param int $exceptOrderId 当前订单不计入
     * @return bool
     */
    public static function isStorageKeyReferencedByOtherOrders($storageKey, $exceptOrderId = 0)
    {
        $storageKey = self::sanitizeStorageKey($storageKey);
        if ($storageKey === '') {
            return false;
        }
        $like = '%' . addcslashes($storageKey, '%_\\') . '%';
        $query = Db::name('crm_client_order')
            ->where('contract_attachments', 'like', $like)
            ->field('id,contract_attachments');
        $exceptOrderId = (int)$exceptOrderId;
        if ($exceptOrderId > 0) {
            $query->where('id', '<>', $exceptOrderId);
        }
        $rows = $query->select();
        if (!$rows) {
            return false;
        }
        foreach ($rows as $row) {
            foreach (self::parseList($row['contract_attachments'] ?? null) as $item) {
                if ((string)($item['storage_key'] ?? '') === $storageKey) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * DB 提交成功后：删除已提升的 tmp 源文件并清理 Session 登记。
     * 已被其他订单 JSON 引用的 tmp 文件保留，避免误删共享附件。
     *
     * @param array $originalList promote 前的列表（含 tmp storage_key）
     * @param int $exceptOrderId 当前订单 ID，引用检查时排除自身
     * @return void
     */
    public static function cleanupTempSources(array $originalList, $exceptOrderId = 0)
    {
        $ids = [];
        $exceptOrderId = (int)$exceptOrderId;
        foreach ($originalList as $item) {
            $normalized = self::normalizeItem($item);
            if ($normalized === null) {
                continue;
            }
            $ids[] = $normalized['id'];
            if (strpos($normalized['storage_key'], 'tmp/') !== 0) {
                continue;
            }
            if (self::isStorageKeyReferencedByOtherOrders($normalized['storage_key'], $exceptOrderId)) {
                continue;
            }
            $src = self::getAbsolutePath($normalized['storage_key']);
            if ($src !== '' && is_file($src)) {
                @unlink($src);
            }
            // 尽量清理空的临时目录 tmp/{uid}/{token}
            $dir = $src !== '' ? dirname($src) : '';
            if ($dir !== '' && is_dir($dir)) {
                @rmdir($dir);
            }
        }
        if (!empty($ids)) {
            self::forgetSessionUploads($ids);
        }
    }

    /**
     * 解析提交结果：区分未提交 / 清空 / 合并合法附件
     *
     * @param array $params 利润计算参数
     * @param array $existingOrder 旧订单（含 contract_attachments）
     * @param mixed|null $submittedRaw null=未提交
     * @param int $clearFlag
     * @param bool $requireIfNeeded 正式提交时为 true
     * @param int $uploaderId
     * @param int $orderId
     * @return array{ok:bool,message:string,should_write:bool,list:array,encoded:?string,profit:float}
     */
    public static function resolveFromSubmission(
        array $params,
        array $existingOrder,
        $submittedRaw,
        $clearFlag,
        $requireIfNeeded,
        $uploaderId,
        $orderId = 0
    ) {
        $eval = self::evaluateRequirement($params);
        $existingList = self::parseList($existingOrder['contract_attachments'] ?? null);
        $existingMap = [];
        foreach ($existingList as $row) {
            $existingMap[$row['id']] = $row;
        }

        $shouldWrite = false;
        $finalList = $existingList;

        if ((int)$clearFlag === 1) {
            $shouldWrite = true;
            $finalList = [];
        } elseif ($submittedRaw !== null) {
            $shouldWrite = true;
            // 提交侧允许只带 id（完整元数据从旧订单或会话上传登记中还原，防伪造 storage_key）
            $candidateIds = self::extractSubmittedIds($submittedRaw);

            $finalList = [];
            $boundIds = [];
            foreach ($candidateIds as $id) {
                if ($id === '') {
                    continue;
                }
                if (isset($existingMap[$id])) {
                    $finalList[] = $existingMap[$id];
                    $boundIds[] = $id;
                    continue;
                }
                $sessionItem = self::getSessionUpload($id);
                if ($sessionItem === null) {
                    return [
                        'ok' => false,
                        'message' => '存在无效或无权使用的合同附件，请重新上传',
                        'should_write' => false,
                        'list' => $existingList,
                        'encoded' => self::encodeList($existingList),
                        'profit' => $eval['profit'],
                    ];
                }
                if ((int)$sessionItem['uploader_id'] !== (int)$uploaderId) {
                    return [
                        'ok' => false,
                        'message' => '合同附件归属校验失败',
                        'should_write' => false,
                        'list' => $existingList,
                        'encoded' => self::encodeList($existingList),
                        'profit' => $eval['profit'],
                    ];
                }
                // 会话中的 order_id>0 时必须匹配当前订单
                if ((int)$sessionItem['order_id'] > 0 && (int)$orderId > 0 && (int)$sessionItem['order_id'] !== (int)$orderId) {
                    return [
                        'ok' => false,
                        'message' => '合同附件与当前订单不匹配',
                        'should_write' => false,
                        'list' => $existingList,
                        'encoded' => self::encodeList($existingList),
                        'profit' => $eval['profit'],
                    ];
                }
                $abs = self::getAbsolutePath($sessionItem['storage_key']);
                if ($abs === '' || !is_file($abs)) {
                    return [
                        'ok' => false,
                        'message' => '合同附件文件不存在，请重新上传',
                        'should_write' => false,
                        'list' => $existingList,
                        'encoded' => self::encodeList($existingList),
                        'profit' => $eval['profit'],
                    ];
                }
                // 不在此处 promote / forgetSession：文件移动与 Session 清理必须等 DB 成功后再做
                $finalList[] = $sessionItem;
                $boundIds[] = $id;
                if (count($finalList) >= self::MAX_COUNT) {
                    break;
                }
            }
            // 保留 $boundIds 仅用于可读性；刻意不调用 forgetSessionUploads
            unset($boundIds);
        } else {
            // 未提交字段：沿用历史列表时，正式必填仍需校验磁盘真实存在
            $finalList = $existingList;
        }

        if (count($finalList) > self::MAX_COUNT) {
            return [
                'ok' => false,
                'message' => '合同附件最多允许 ' . self::MAX_COUNT . ' 个',
                'should_write' => false,
                'list' => $existingList,
                'encoded' => self::encodeList($existingList),
                'profit' => $eval['profit'],
            ];
        }

        if ($requireIfNeeded && $eval['required']) {
            $existCheck = self::assertFilesExist($finalList);
            if (empty($existCheck['ok'])) {
                return [
                    'ok' => false,
                    'message' => $existCheck['message'],
                    'should_write' => false,
                    'list' => $finalList,
                    'encoded' => self::encodeList($finalList),
                    'profit' => $eval['profit'],
                ];
            }
        }

        // 利润降至阈值以下：若本次未提交字段，保留历史；不因隐藏而清空
        return [
            'ok' => true,
            'message' => '',
            'should_write' => $shouldWrite,
            'list' => $finalList,
            'encoded' => empty($finalList) ? null : self::encodeList($finalList),
            'profit' => $eval['profit'],
        ];
    }

    /**
     * 从提交值提取附件 id 列表（忽略客户端伪造的 path/url）
     *
     * @param mixed $submittedRaw
     * @return string[]
     */
    public static function extractSubmittedIds($submittedRaw)
    {
        $ids = [];
        $rows = [];
        if (is_array($submittedRaw)) {
            $rows = $submittedRaw;
        } elseif (is_string($submittedRaw)) {
            $text = trim($submittedRaw);
            if ($text === '' || strcasecmp($text, 'null') === 0) {
                return [];
            }
            $decoded = json_decode($text, true);
            if (is_array($decoded)) {
                $rows = $decoded;
            }
        }
        foreach ($rows as $row) {
            if (is_string($row)) {
                $id = strtolower(preg_replace('/[^a-f0-9]/i', '', $row));
            } elseif (is_array($row)) {
                $id = strtolower(preg_replace('/[^a-f0-9]/i', '', (string)($row['id'] ?? '')));
            } else {
                $id = '';
            }
            if ($id !== '' && !in_array($id, $ids, true)) {
                $ids[] = $id;
            }
            if (count($ids) >= self::MAX_COUNT) {
                break;
            }
        }
        return $ids;
    }

    /**
     * 正式提交必填校验（仅看最终列表 + 重算利润）
     *
     * @param array $params
     * @param mixed $attachmentsRaw
     * @return array{ok:bool,message:string,profit:float}
     */
    public static function validateRequirement(array $params, $attachmentsRaw)
    {
        $eval = self::evaluateRequirement($params);
        if (!$eval['required']) {
            return ['ok' => true, 'message' => '', 'profit' => $eval['profit']];
        }
        $existCheck = self::assertFilesExist(self::parseList($attachmentsRaw));
        if (empty($existCheck['ok'])) {
            return [
                'ok' => false,
                'message' => $existCheck['message'],
                'profit' => $eval['profit'],
            ];
        }
        return ['ok' => true, 'message' => '', 'profit' => $eval['profit']];
    }

    /**
     * 详情/编辑展示：补充受控预览与下载 URL
     *
     * @param mixed $raw
     * @param int $orderId
     * @return array<int, array<string, mixed>>
     */
    public static function enrichForView($raw, $orderId)
    {
        $list = self::parseList($raw);
        $orderId = (int)$orderId;
        foreach ($list as &$item) {
            $item['preview_url'] = self::buildServeUrl($item['id'], $orderId, 'inline');
            $item['download_url'] = self::buildServeUrl($item['id'], $orderId, 'attachment');
            $item['is_image'] = in_array($item['ext'], ['jpg', 'jpeg', 'png'], true);
            $item['is_pdf'] = ($item['ext'] === 'pdf');
            $item['is_office'] = in_array($item['ext'], ['doc', 'docx'], true);
        }
        unset($item);
        return $list;
    }

    /**
     * @param string $fileId
     * @param int $orderId
     * @param string $disposition inline|attachment
     * @return string
     */
    public static function buildServeUrl($fileId, $orderId, $disposition = 'inline')
    {
        $fileId = strtolower(preg_replace('/[^a-f0-9]/i', '', (string)$fileId));
        $orderId = (int)$orderId;
        $disposition = $disposition === 'attachment' ? 'attachment' : 'inline';
        $query = http_build_query([
            'file_id' => $fileId,
            'order_id' => $orderId,
            'disposition' => $disposition,
        ]);
        return url('Order/serveContractAttachment', [], false) . '?' . $query;
    }

    /**
     * 解析可访问的附件元数据（订单绑定或会话临时）
     *
     * @param string $fileId
     * @param int $orderId
     * @param array|null $orderRow
     * @param int $viewerId
     * @return array{ok:bool,message:string,item?:array,path?:string}
     */
    public static function resolveAccessibleFile($fileId, $orderId, $orderRow, $viewerId)
    {
        $fileId = strtolower(preg_replace('/[^a-f0-9]/i', '', (string)$fileId));
        $orderId = (int)$orderId;
        $viewerId = (int)$viewerId;
        if ($fileId === '' || $viewerId <= 0) {
            return ['ok' => false, 'message' => '参数错误'];
        }

        if ($orderId > 0) {
            if (!$orderRow || (int)($orderRow['id'] ?? 0) !== $orderId) {
                return ['ok' => false, 'message' => '订单不存在'];
            }
            if (!self::canAccessOrder($orderRow)) {
                return ['ok' => false, 'message' => '无权限访问该合同附件'];
            }
            $list = self::parseList($orderRow['contract_attachments'] ?? null);
            foreach ($list as $item) {
                if ($item['id'] === $fileId) {
                    $path = self::getAbsolutePath($item['storage_key']);
                    if ($path === '' || !is_file($path)) {
                        return ['ok' => false, 'message' => '附件文件不存在'];
                    }
                    return ['ok' => true, 'message' => '', 'item' => $item, 'path' => $path];
                }
            }
            // 添加/编辑/草稿里刚上传、尚未写入订单的附件：只允许上传者本人预览。
            // 必须同时满足会话归属和本次 order_id，避免拿任意 file_id 读取他人文件。
            $pending = self::resolveOwnPendingUpload($fileId, $viewerId, $orderId);
            if ($pending !== null) {
                return $pending;
            }
            return ['ok' => false, 'message' => '附件不属于该订单'];
        }

        $pending = self::resolveOwnPendingUpload($fileId, $viewerId, 0);
        if ($pending === null) {
            return ['ok' => false, 'message' => '无权访问该临时附件'];
        }
        return $pending;
    }

    /**
     * 当前用户自己的未绑定上传。
     * $expectedOrderId > 0 时，会话里记录的 order_id 必须一致。
     *
     * @param string $fileId
     * @param int $viewerId
     * @param int $expectedOrderId
     * @return array{ok:bool,message:string,item?:array,path?:string}|null
     */
    private static function resolveOwnPendingUpload($fileId, $viewerId, $expectedOrderId)
    {
        $sessionItem = self::getSessionUpload($fileId);
        if ($sessionItem === null || (int)$sessionItem['uploader_id'] !== $viewerId) {
            return null;
        }
        $expectedOrderId = (int)$expectedOrderId;
        if ($expectedOrderId > 0 && (int)$sessionItem['order_id'] !== $expectedOrderId) {
            return null;
        }
        $path = self::getAbsolutePath($sessionItem['storage_key']);
        if ($path === '' || !is_file($path)) {
            return ['ok' => false, 'message' => '附件文件不存在'];
        }
        return ['ok' => true, 'message' => '', 'item' => $sessionItem, 'path' => $path];
    }

    /**
     * 订单合同附件「编辑/上传」权限：全量管理 / 负责人 / 创建人 / 协同人
     * （不含同团队只读；同队可见不等于可上传）
     *
     * @param array $order
     * @param array|null $adminInfo
     * @return bool
     */
    public static function canEditOrder(array $order, $adminInfo = null)
    {
        if (OrderService::canManageAllOrders($adminInfo)) {
            return true;
        }
        $username = (string)session('username');
        $aid = (int)session('aid');
        if ($username !== '' && (
            (string)($order['pr_user'] ?? '') === $username
            || (string)($order['at_user'] ?? '') === $username
        )) {
            return true;
        }
        $jp = $order['joint_person'] ?? '';
        if ($jp !== '' && $jp !== null) {
            $ids = [];
            if (is_array($jp)) {
                $ids = $jp;
            } elseif (preg_match('/^\s*\[.*\]\s*$/', (string)$jp)) {
                $tmp = json_decode((string)$jp, true);
                if (is_array($tmp)) {
                    $ids = $tmp;
                }
            } else {
                $ids = explode(',', (string)$jp);
            }
            foreach ($ids as $id) {
                if ((int)$id === $aid && $aid > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 订单合同附件「查看/预览」权限：编辑权限 + 同团队只读
     * （对齐订单列表按团队可见的范围；不授予上传权限）
     *
     * @param array $order
     * @param array|null $adminInfo
     * @return bool
     */
    public static function canAccessOrder(array $order, $adminInfo = null)
    {
        if (self::canEditOrder($order, $adminInfo)) {
            return true;
        }
        // 同团队可见（与订单列表团队过滤口径兼容）
        $orderTeam = trim((string)($order['team_name'] ?? ''));
        if ($orderTeam !== '') {
            $adminInfo = $adminInfo !== null ? $adminInfo : \app\admin\model\Admin::getMyInfo();
            $myTeam = trim((string)($adminInfo['team_name'] ?? session('team_name') ?? ''));
            if ($myTeam !== '' && $myTeam === $orderTeam) {
                return true;
            }
        }
        return false;
    }

    /**
     * 输出安全响应头并读取文件
     *
     * @param string $path
     * @param array $item
     * @param string $disposition
     * @return void
     */
    public static function outputFile($path, array $item, $disposition = 'inline')
    {
        $ext = strtolower((string)($item['ext'] ?? ''));
        $mime = (string)($item['mime'] ?? 'application/octet-stream');
        $name = (string)($item['name'] ?? ('file.' . $ext));
        $disposition = $disposition === 'attachment' ? 'attachment' : 'inline';

        // Word 不作为 inline 可执行页；PDF/图片可 inline
        if (in_array($ext, ['doc', 'docx'], true)) {
            $disposition = 'attachment';
            $mime = $ext === 'docx'
                ? 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                : 'application/msword';
        } elseif ($ext === 'pdf') {
            $mime = 'application/pdf';
        } elseif ($ext === 'png') {
            $mime = 'image/png';
        } elseif ($ext === 'jpg' || $ext === 'jpeg') {
            $mime = 'image/jpeg';
        }

        $fallback = preg_replace('/[^\w\.\-]+/', '_', $name);
        $utf8Name = rawurlencode($name);

        header('X-Content-Type-Options: nosniff');
        header('X-Frame-Options: SAMEORIGIN');
        header('Content-Security-Policy: default-src \'none\'; img-src \'self\'; object-src \'self\'; frame-src \'self\'; style-src \'unsafe-inline\'; sandbox');
        header('Content-Type: ' . $mime);
        header('Content-Length: ' . (string)filesize($path));
        header(
            'Content-Disposition: ' . $disposition
            . '; filename="' . $fallback . '"; filename*=UTF-8\'\'' . $utf8Name
        );
        header('Cache-Control: private, no-store');
        readfile($path);
        exit;
    }
}
