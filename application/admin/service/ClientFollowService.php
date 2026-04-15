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
                    'next_up_time' => is_null($nextUpValue) ? '' : (string)$nextUpValue,
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

    private function fail($msg)
    {
        return ['code' => 1, 'msg' => $msg, 'data' => []];
    }
}
