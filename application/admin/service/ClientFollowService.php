<?php

namespace app\admin\service;

use think\Db;
use Throwable;

class ClientFollowService
{
    /**
     * 保存客户跟进记录，并同步更新 crm_leads 跟进字段
     *
     * @param int|string $leadsId
     * @param string $content
     * @param string|null $nextUpTime
     * @param array $operatorInfo ['admin_id' => int, 'username' => string]
     * @return array
     */
    public function saveFollow($leadsId, $content, $nextUpTime, $operatorInfo = [])
    {
        $leadsId = (int)$leadsId;
        $content = trim((string)$content);
        $nextUpTime = trim((string)$nextUpTime);
        $operatorId = (int)($operatorInfo['admin_id'] ?? 0);
        $operatorName = trim((string)($operatorInfo['username'] ?? ''));

        if ($leadsId <= 0) {
            return $this->fail('缺少客户ID');
        }
        if ($operatorId <= 0) {
            return $this->fail('登录状态已失效，请重新登录');
        }
        if ($content === '') {
            return $this->fail('请输入跟进内容');
        }

        $client = Db::table('crm_leads')->where('id', $leadsId)->find();
        if (!$client) {
            return $this->fail('客户不存在');
        }

        if (!$this->canOperateClient($client, $operatorId, $operatorName)) {
            return $this->fail('您没有权限操作该客户');
        }

        list($ok, $nextUpValue, $nextUpErrMsg) = $this->normalizeNextUpTimeValue($nextUpTime);
        if (!$ok) {
            return $this->fail($nextUpErrMsg);
        }

        $nowTimestamp = time();
        $nowDateTime = date('Y-m-d H:i:s', $nowTimestamp);

        Db::startTrans();
        try {
            Db::table('crm_comment')->insert([
                'leads_id' => $leadsId,
                'user_id' => $operatorId,
                'reply_msg' => $content,
                'create_date' => $nowTimestamp,
            ]);

            Db::table('crm_leads')->where('id', $leadsId)->update([
                'last_up_records' => $content,
                'last_up_time' => $nowDateTime,
                'next_up_time' => $nextUpValue,
                'ut_time' => $nowDateTime,
            ]);

            Db::commit();

            return [
                'code' => 0,
                'msg' => '保存成功',
                'data' => [
                    'leads_id' => $leadsId,
                    'reply_msg' => $content,
                    'create_date' => date('Y年m月d日 H:i', $nowTimestamp),
                    'next_up_time' => $this->formatNextUpTimeForDisplay($nextUpValue),
                ],
            ];
        } catch (Throwable $e) {
            Db::rollback();
            return $this->fail('保存跟进失败：' . $e->getMessage());
        }
    }

    /**
     * 当前用户是否有权限操作客户（admin_id=1 超管放行）
     */
    private function canOperateClient(array $client, $operatorId, $operatorName)
    {
        if ((int)$operatorId === 1) {
            return true;
        }

        $ownerName = trim((string)($client['pr_user'] ?? ''));
        $isOwner = ($ownerName !== '' && $ownerName === $operatorName);
        if ($isOwner) {
            return true;
        }

        return $this->isJointPerson($client, $operatorId);
    }

    /**
     * 判断是否协同人：兼容 JSON 数组和逗号分隔两种存储格式
     */
    private function isJointPerson(array $client, $operatorId)
    {
        $jointRaw = trim((string)($client['joint_person'] ?? ''));
        if ($jointRaw === '') {
            return false;
        }

        $jointIds = [];
        if (preg_match('/^\s*\[.*\]\s*$/', $jointRaw)) {
            $decoded = json_decode($jointRaw, true);
            if (is_array($decoded)) {
                $jointIds = $decoded;
            }
        } else {
            $jointIds = array_values(array_filter(explode(',', $jointRaw)));
        }

        return in_array((string)$operatorId, array_map('strval', $jointIds), true);
    }

    /**
     * 标准化 next_up_time：
     * - 有值：校验并格式化为 Y-m-d H:i:s
     * - 无值：按字段可空性写入 NULL 或空字符串
     */
    private function normalizeNextUpTimeValue($nextUpTime)
    {
        if ($nextUpTime !== '') {
            $timestamp = strtotime($nextUpTime);
            if ($timestamp === false) {
                return [false, null, '下次跟进时间格式不正确'];
            }
            return [true, date('Y-m-d H:i:s', $timestamp), ''];
        }

        return [true, $this->emptyNextUpTimeValue(), ''];
    }

    /**
     * next_up_time 置空值的兼容处理
     */
    private function emptyNextUpTimeValue()
    {
        try {
            $column = Db::query("SHOW COLUMNS FROM `crm_leads` LIKE 'next_up_time'");
            if (!empty($column) && isset($column[0]['Null']) && strtoupper((string)$column[0]['Null']) === 'YES') {
                return null;
            }
        } catch (Throwable $e) {
            // 忽略字段探测异常，默认回退为空字符串
        }

        return '';
    }

    /**
     * next_up_time 展示值格式化（仅用于返回前端展示）。
     */
    public function formatNextUpTimeForDisplay($nextUpTime)
    {
        if ($nextUpTime === null) {
            return '';
        }

        $raw = trim((string)$nextUpTime);
        if ($raw === '' || strtolower($raw) === 'null' || strtolower($raw) === 'undefined') {
            return '';
        }

        $timestamp = strtotime($raw);
        if ($timestamp === false) {
            return $raw;
        }

        return date('Y-m-d H:i:s', $timestamp);
    }

    /**
     * 跟进弹窗客户信息组装（控制器调度，service 统一组装展示字段）。
     */
    public function buildFollowClientDetailData(array $client, $clientId)
    {
        if (!array_key_exists('next_up_time', $client)) {
            $client['next_up_time'] = '';
        }
        $client['next_up_time'] = $this->formatNextUpTimeForDisplay($client['next_up_time']);

        // 主/辅电话（1=主，3=辅）
        $mainPhone = '';
        $auxPhone = '';
        $contacts = Db::table('crm_contacts')
            ->where('is_delete', 0)
            ->where('leads_id', (int)$clientId)
            ->whereIn('contact_type', [1, 3])
            ->order('id', 'asc')
            ->field('contact_type, contact_value')
            ->select();
        foreach ($contacts as $c) {
            if ($c['contact_type'] == 1 && $mainPhone === '') {
                $mainPhone = $c['contact_value'];
            } elseif ($c['contact_type'] == 3 && $auxPhone === '') {
                $auxPhone = $c['contact_value'];
            }
        }
        $client['main_phone'] = $mainPhone;
        $client['aux_phone'] = $auxPhone;

        // 客户状态展示名
        $khStatusValue = $client['kh_status'] ?? '';
        $statusName = $khStatusValue;
        if (is_numeric($khStatusValue)) {
            $statusInfo = Db::table('crm_client_status')->where('id', $khStatusValue)->find();
            if ($statusInfo) {
                $statusName = $statusInfo['status_name'];
            }
        }
        $client['kh_status_name'] = $statusName;

        // 客户级别展示名（兼容新老数据）
        $rankMap = Db::table('crm_client_rank')->column('rank_name', 'id');
        $rankNameMap = [];
        foreach ($rankMap as $rankId => $rankName) {
            $rankNameMap[(string)$rankId] = trim((string)$rankName);
        }
        $rawKhRank = trim((string)($client['kh_rank'] ?? ''));
        if ($rawKhRank === '') {
            $client['kh_rank_name'] = '';
        } elseif (preg_match('/^\d+$/', $rawKhRank)) {
            $client['kh_rank_name'] = isset($rankNameMap[$rawKhRank]) && trim((string)$rankNameMap[$rawKhRank]) !== ''
                ? trim((string)$rankNameMap[$rawKhRank])
                : $rawKhRank;
        } else {
            $client['kh_rank_name'] = $rawKhRank;
        }

        // 所属渠道名称
        $inquiryValue = $client['inquiry_id'] ?? '';
        $inquiryName = $inquiryValue;
        if (is_numeric($inquiryValue)) {
            $inquiryInfo = Db::table('crm_inquiry')->where('id', $inquiryValue)->find();
            if ($inquiryInfo) {
                $inquiryName = $inquiryInfo['inquiry_name'];
            }
        }
        $client['inquiry_name'] = $inquiryName;

        // 运营端口名称
        $portValue = $client['port_id'] ?? '';
        $portName = $portValue;
        if (is_numeric($portValue)) {
            $portInfo = Db::table('crm_inquiry_port')->where('id', $portValue)->find();
            if ($portInfo) {
                $portName = $portInfo['port_name'];
            }
        }
        $client['port_name'] = $portName;

        // 产品名称
        $productValue = $client['product_name'] ?? '';
        $productName = $productValue;
        if (is_numeric($productValue) && !empty($productValue)) {
            $productInfo = Db::table('crm_products')->where('id', $productValue)->find();
            if ($productInfo) {
                $productName = $productInfo['product_name'];
            }
        }
        $client['product_name'] = $productName;

        // 协同人
        $jointPersonIds = [];
        $jointPersonNames = [];
        if (!empty($client['joint_person'])) {
            $jp = $client['joint_person'];
            if (preg_match('/^\s*\[.*\]\s*$/', $jp)) {
                $tmp = json_decode($jp, true);
                if (is_array($tmp)) {
                    $jointPersonIds = $tmp;
                }
            } else {
                $jointPersonIds = array_values(array_filter(explode(',', $jp)));
            }

            if (!empty($jointPersonIds)) {
                $adminMap = Db::table('admin')
                    ->whereIn('admin_id', $jointPersonIds)
                    ->column('username', 'admin_id');
                foreach ($jointPersonIds as $uid) {
                    $jointPersonNames[] = $adminMap[$uid] ?? (string)$uid;
                }
            }
        }
        $client['joint_person_ids'] = $jointPersonIds;
        $client['joint_person_names'] = implode('、', $jointPersonNames);

        return $client;
    }

    /**
     * 获取并格式化跟进历史（统一时间格式）。
     */
    public function getFollowComments($clientId)
    {
        $comments = Db::table('crm_comment')
            ->alias('com')
            ->join('admin adm', 'com.user_id = adm.admin_id')
            ->where(['leads_id' => (int)$clientId])
            ->field('com.*, adm.username, adm.avatar')
            ->order('com.create_date desc')
            ->select();

        foreach ($comments as &$comment) {
            $comment['create_date'] = date('Y年m月d日 H:i', $comment['create_date']);
        }
        unset($comment);

        return $comments;
    }

    private function fail($msg)
    {
        return ['code' => 1, 'msg' => $msg, 'data' => []];
    }
}
