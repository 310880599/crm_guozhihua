<?php

namespace app\admin\job;

use app\admin\controller\Client;
use app\admin\service\ClientOwnerHistoryService;
use think\Db;
use think\queue\Job;
use PhpOffice\PhpSpreadsheet\IOFactory;
use think\facade\Log;
use think\facade\Session;

class ExcelImport
{


    // public function fire(Job $job, $data)
    // {
    //     try {
    //         // 提取数据
    //         $filePath = $data['filePath'] ?? '';
    //         $pr_user = $data['pr_user'] ?? '';
    //         $chunkData = $data['chunkData'] ?? [];
    //         $headers = $data['headers'] ?? [];
    //         $current_time = date("Y-m-d H:i:s");
    //         Session::set('aid', $data['user_id']);
    //         Session::set('username', $pr_user);
    //         $success_row = 0;
    //         $error_row = 0;
    //         foreach ($chunkData as $index => $row) {
    //             // 检查当前行是否为空白行
    //             if ($this->isRowEmpty($row)) {
    //                 continue;
    //             }

    //             try {
    //                 // 准备单条数据
    //                 $rowAssoc = $this->buildAssocRow($row, $headers);
    //                 $leadsData = $this->buildLeadsRow($rowAssoc, $pr_user, $current_time);
    //                 $contactsData = [];

    //                 // 开启单条记录事务
    //                 Db::startTrans();
    //                 // 插入客户主表数据
    //                 $leadsId = $this->insertSingleLeadsData($leadsData);
    //                 if (!$leadsId) {
    //                     $error_row++;
    //                     continue;
    //                 }
    //                 // 构建并插入联系人数据
    //                 $contacts = $this->buildContacts($leadsId, $rowAssoc, $current_time);
    //                 foreach ($contacts as $contact) {
    //                     if (!empty($contact['contact_value'])) {
    //                         $contactsData[] = $contact;
    //                     }
    //                 }
    //                 $this->insertSingleContactsData($contactsData);
    //                 $success_row++;
    //                 // 提交单条记录事务
    //                 Db::commit();
    //             } catch (\Exception $e) {
    //                 // 回滚单条记录事务
    //                 Db::rollback();
    //                 $error_row++;
    //                 // 记录详细错误日志
    //                 $logData = [
    //                     'message' => "第 {$index} 条 Excel 数据导入失败",
    //                     'error_message' => $e->getMessage(),
    //                     'file' => $e->getFile(),
    //                     'line' => $e->getLine(),
    //                     'task_data' => $row,
    //                     // 'trace' => $e->getTraceAsString()
    //                 ];
    //                 Client::addOperLog(null, '数据导入', $logData);
    //                 Log::error(json_encode($logData, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
    //                 continue;
    //             }
    //         }


    //         // 记录成功和失败的条数
    //         $logData = [
    //             'message' => 'Excel 导入任务完成',
    //             'success_count' => $success_row,
    //             'fail_count' => $error_row,
    //             'task_data' => $filePath
    //         ];
    //         Client::addOperLog(null, '数据导入', $logData);
    //         Log::info(json_encode($logData, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
    //         // 删除任务
    //         $job->delete();
    //     } catch (\Exception $e) {
    //         // 记录详细错误日志
    //         $logData = [
    //             'message' => 'Excel 导入任务整体失败',
    //             'error_message' => $e->getMessage(),
    //             'file' => $e->getFile(),
    //             'line' => $e->getLine(),
    //             'task_data' => $filePath,
    //             // 'trace' => $e->getTraceAsString()
    //         ];
    //         Client::addOperLog(null, '数据导入', $logData);
    //         Log::error(json_encode($logData, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));

    //         // 重试任务
    //         if ($job->attempts() < 3) {
    //             $job->release(10);
    //         } else {
    //             $job->delete();
    //         }
    //     }
    // }

    // application/admin/job/ExcelImport.php  (只展示修改或新增部分)

    public function fire(Job $job, $data)
    {
        try {
            // 初始化基础数据
            $filePath   = $data['filePath'] ?? '';
            $pr_user    = $data['pr_user'] ?? '';
            $headers    = $data['headers'] ?? [];
            $chunkData  = $data['chunkData'] ?? [];
            $current_time = date("Y-m-d H:i:s");
            // 设置队列任务的用户上下文
            Session::set('aid', $data['user_id'] ?? 0);
            Session::set('username', $pr_user);
            $success_count = 0;
            $fail_count = 0;

            foreach ($chunkData as $index => $row) {
                // 跳过空白行
                if ($this->isRowEmpty($row)) {
                    continue;
                }
                // 将当前行转为关联数组（表头=>值）
                $rowAssoc = $this->buildAssocRow($row, $headers);
                try {
                    // **数据解析与验证** 
                    // 验证至少客户名称或联系方式不为空
                    if (empty($rowAssoc['客户名称']) && empty($rowAssoc['电话']) && empty($rowAssoc['辅助电话']) && empty($rowAssoc['联系人邮箱']) && empty($rowAssoc['联系人社交'])) {
                        $fail_count++;
                        $logData = [
                            'message'       => "第{$index}条数据导入失败",
                            'error_message' => '缺少客户名称和联系方式',
                            'task_data'     => $row
                        ];
                        Client::addOperLog(null, '数据导入', $logData);
                        continue;
                    }
                    // 验证电话号码格式（主电话11位数字）
                    $mainPhoneNum = preg_replace('/\D/', '', (string)($rowAssoc['电话'] ?? ''));
                    if (!empty($mainPhoneNum) && !preg_match('/^\d{11}$/', $mainPhoneNum)) {
                        $fail_count++;
                        $logData = [
                            'message'       => "第{$index}条数据导入失败",
                            'error_message' => '电话格式不正确',
                            'task_data'     => $row
                        ];
                        Client::addOperLog(null, '数据导入', $logData);
                        continue;
                    }
                    // 验证辅助电话格式
                    $auxPhoneNum = preg_replace('/\D/', '', (string)($rowAssoc['辅助电话'] ?? ''));
                    if (!empty($auxPhoneNum) && !preg_match('/^\d{11}$/', $auxPhoneNum)) {
                        $fail_count++;
                        $logData = [
                            'message'       => "第{$index}条数据导入失败",
                            'error_message' => '辅助电话格式不正确',
                            'task_data'     => $row
                        ];
                        Client::addOperLog(null, '数据导入', $logData);
                        continue;
                    }
                    // 处理所属渠道：名称转ID
                    $inquiry_id = 0;
                    if (!empty($rowAssoc['所属渠道'])) {
                        $channel = Db::name('crm_inquiry')->where('inquiry_name', $rowAssoc['所属渠道'])->find();
                        if (!$channel) {
                            $fail_count++;
                            $logData = [
                                'message'       => "第{$index}条数据导入失败",
                                'error_message' => '所属渠道不存在',
                                'task_data'     => $row
                            ];
                            Client::addOperLog(null, '数据导入', $logData);
                            continue;
                        }
                        $inquiry_id = $channel['id'];
                    }
                    // 处理运营端口：支持ID或名称，多个以逗号分隔
                    $port_id_str = '';
                    if (!empty($rowAssoc['运营端口'])) {
                        $portsInput = $rowAssoc['运营端口'];
                        if (preg_match('/^[\d,]+$/', $portsInput)) {
                            // 纯数字ID列表
                            $portIds = array_filter(explode(',', $portsInput));
                            // 验证端口ID是否存在
                            $validPorts = Db::name('crm_inquiry_port')->whereIn('id', $portIds)->column('id');
                            if (count($validPorts) != count($portIds)) {
                                $fail_count++;
                                $logData = [
                                    'message'       => "第{$index}条数据导入失败",
                                    'error_message' => '运营端口ID不存在',
                                    'task_data'     => $row
                                ];
                                Client::addOperLog(null, '数据导入', $logData);
                                continue;
                            }
                            $port_id_str = implode(',', $portIds);
                        } else {
                            // 按名称查找端口ID（默认限定所属渠道）
                            $portNames = array_map('trim', explode(',', $portsInput));
                            $foundIds = [];
                            foreach ($portNames as $pname) {
                                if ($pname === '') continue;
                                $portQuery = Db::name('crm_inquiry_port')->where('port_name', $pname);
                                if ($inquiry_id) {
                                    $portQuery->where('inquiry_id', $inquiry_id);
                                }
                                $port = $portQuery->find();
                                if (!$port) {
                                    $fail_count++;
                                    $logData = [
                                        'message'       => "第{$index}条数据导入失败",
                                        'error_message' => "运营端口{$pname}不存在",
                                        'task_data'     => $row
                                    ];
                                    Client::addOperLog(null, '数据导入', $logData);
                                    // 找不到端口则跳过整条数据
                                    continue 2;
                                }
                                $foundIds[] = $port['id'];
                            }
                            $port_id_str = implode(',', array_unique($foundIds));
                        }
                    }
                    // 处理协同人：支持用户ID或用户名，多个以逗号分隔
                    $joint_person_str = '';
                    if (!empty($rowAssoc['协同人'])) {
                        $jpRaw = $rowAssoc['协同人'];
                        $jpIds = [];
                        if (preg_match('/\d/', $jpRaw)) {
                            // 提取数字（假定为ID）
                            $jpRawClean = preg_replace('/[^\d,]/', '', $jpRaw);
                            $jpIds = array_filter(explode(',', $jpRawClean));
                        } else {
                            // 无数字，则按用户名处理
                            $names = array_map('trim', explode(',', $jpRaw));
                            foreach ($names as $name) {
                                if ($name === '') continue;
                                $admin = Db::name('admin')->where('username', $name)->find();
                                if (!$admin) {
                                    $fail_count++;
                                    $logData = [
                                        'message'       => "第{$index}条数据导入失败",
                                        'error_message' => "协同人{$name}不存在",
                                        'task_data'     => $row
                                    ];
                                    Client::addOperLog(null, '数据导入', $logData);
                                    continue 2;
                                }
                                $jpIds[] = $admin['id'];
                            }
                        }
                        $jpIds = array_values(array_unique(array_filter($jpIds)));
                        if (!empty($jpIds)) {
                            // 验证协同人ID有效性
                            $validAdmins = Db::name('admin')->whereIn('id', $jpIds)->column('id');
                            if (count($validAdmins) != count($jpIds)) {
                                $fail_count++;
                                $logData = [
                                    'message'       => "第{$index}条数据导入失败",
                                    'error_message' => '协同人ID不存在',
                                    'task_data'     => $row
                                ];
                                Client::addOperLog(null, '数据导入', $logData);
                                continue;
                            }
                            $joint_person_str = implode(',', $jpIds);
                            if (strlen($joint_person_str) > 30) {
                                $fail_count++;
                                $logData = [
                                    'message'       => "第{$index}条数据导入失败",
                                    'error_message' => '协同人过多，超出存储限制',
                                    'task_data'     => $row
                                ];
                                Client::addOperLog(null, '数据导入', $logData);
                                continue;
                            }
                        }
                    }
                    // 处理负责人：Excel「负责人」作为客户真实负责人；空则回退导入操作人
                    $ownerName = $pr_user;
                    $ownerId   = (int)($data['user_id'] ?? 0);
                    if (!empty($rowAssoc['负责人'])) {
                        $resp = trim((string)$rowAssoc['负责人']);
                        $admin = Db::name('admin')->where('username', $resp)->find();
                        if (!$admin) {
                            $fail_count++;
                            $logData = [
                                'message'       => "第{$index}条数据导入失败",
                                'error_message' => '负责人不存在',
                                'task_data'     => $row
                            ];
                            Client::addOperLog(null, '数据导入', $logData);
                            continue;
                        }
                        $ownerName = $admin['username'];
                        $ownerId   = (int)$admin['id'];
                    }

                    // **新增代码**：产品名称匹配产品ID
                    $productId = 0;
                    if (!empty($rowAssoc['产品名称'])) {
                        $product = Db::name('crm_products')->where('product_name', $rowAssoc['产品名称'])->find();
                        if (!$product) {
                            $fail_count++;
                            $logData = [
                                'message'       => "第{$index}条数据导入失败",
                                'error_message' => '产品名称“' . $rowAssoc['产品名称'] . '”未找到对应产品',
                                'task_data'     => $row
                            ];
                            Client::addOperLog(null, '数据导入', $logData);
                            continue;  // 跳过该条数据导入
                        }
                        $productId = $product['id'];
                    }

                    // 构建客户主表数据数组（注意：product_name存储为产品ID）
                    // oper_user = 运营人员（与负责人无关）；Excel 模板无「运营人员」列，最小风险置空，不再写 Excel 负责人
                    $leadsData = [
                        'kh_name'      => $rowAssoc['客户名称'] ?? '',
                        'kh_contact'   => $rowAssoc['联系人'] ?? '',
                        'kh_status'    => $rowAssoc['客户来源'] ?? '',
                        'kh_rank'      => $rowAssoc['客户等级'] ?? '',
                        'xs_area'      => $rowAssoc['地区'] ?? ($rowAssoc['国家'] ?? ''),
                        'product_name' => $productId,  // 使用匹配到的产品ID
                        'oper_user'    => '',
                        'inquiry_id'   => $inquiry_id,
                        'port_id'      => $port_id_str,
                        'joint_person' => $joint_person_str,
                        'remark'       => $rowAssoc['其他信息'] ?? ($rowAssoc['客户备注'] ?? ''),
                        'pr_user'      => $ownerName,
                        // pr_user_id：负责人生命周期初始化（createInitialOwnerStage）强制要求 owner.user_id > 0
                        'pr_user_id'   => $ownerId,
                        'pr_user_bef'  => $ownerName,
                        'pr_user_bef_id' => $ownerId,
                        'at_user'      => $pr_user,
                        'at_time'      => $current_time,
                        'ut_time'      => $current_time,
                        'status'       => 1,
                        'ispublic'     => 3
                    ];
                    // 设置 original_creation_time 和 original_modification_time 默认值为当前时间戳（秒）
                    $timestampNow = time();  // 新增代码
                    $leadsData['original_creation_time'] = $timestampNow;       // 新增代码
                    $leadsData['original_modification_time'] = $timestampNow;   // 新增代码
                    // **新增代码**：处理原始创建时间和修改时间字段
                    if (!empty($rowAssoc['原来创建时间'])) {
                        // 若 Excel 提供了原始创建时间，则解析为时间戳，否则保持默认当前时间
                        $leadsData['original_creation_time'] = strtotime($rowAssoc['原来创建时间']) ?: $timestampNow;  // 替换代码
                    }
                    if (!empty($rowAssoc['原来修改时间'])) {
                        // 若 Excel 提供了原始修改时间，则解析为时间戳，否则保持默认当前时间
                        $leadsData['original_modification_time'] = strtotime($rowAssoc['原来修改时间']) ?: $timestampNow;  // 替换代码
                    }

                    // 使用事务插入客户及联系人数据
                    Db::startTrans();
                    // 写入客户主表（返回自增ID）:contentReference[oaicite:5]{index=5}:contentReference[oaicite:6]{index=6}
                    $leadsId = Db::name('crm_leads')->strict(false)->insertGetId($leadsData);
                    if (!$leadsId) {
                        throw new \Exception('客户主表数据插入失败');
                    }

                    // 负责人生命周期初始化：必须使用最终写入 crm_leads 的负责人信息（pr_user/pr_user_id）。
                    // 失败将抛出异常，交由外层 catch 触发本行事务 rollback，
                    // 避免出现 crm_leads 存在但 crm_client_owner_history 不存在的情况。
                    $ownerHistoryService = new ClientOwnerHistoryService();
                    $ownerHistoryService->createInitialOwnerStage(
                        $leadsId,
                        [
                            'user_id'   => (int)$leadsData['pr_user_id'],
                            'user_name' => trim((string)$leadsData['pr_user']),
                        ],
                        [
                            'admin_id' => (int)Session::get('aid'),
                            'username' => trim((string)Session::get('username')),
                        ],
                        0,
                        $current_time,
                        'excel_import',
                        'Excel导入初始化负责人生命周期'
                    );

                    // 构建联系人数据列表
                    $contactsData = [];
                    // 主电话（可能有多个）
                    if (!empty($mainPhoneNum)) {
                        // 若 mainPhoneNum 是用多个主电话组合（此处简单假定只有一个主电话）
                        $contactsData[] = [
                            'leads_id'      => $leadsId,
                            'contact_type'  => 1, // 联系方式类型：1=主电话
                            'contact_value' => $mainPhoneNum,
                            'contact_extra' => '', 
                            'created_at'    => $current_time
                        ];
                    }
                    // 辅助电话
                    if (!empty($auxPhoneNum)) {
                        $contactsData[] = [
                            'leads_id'      => $leadsId,
                            'contact_type'  => 3, // 3=辅助电话
                            'contact_value' => $auxPhoneNum,
                            'contact_extra' => '',
                            'created_at'    => $current_time
                        ];
                    }
                    // 其他联系人方式：邮箱和社交账号
                    $emailVal = trim($rowAssoc['联系人邮箱'] ?? '');
                    if ($emailVal !== '') {
                        $contactsData[] = [
                            'leads_id'      => $leadsId,
                            'contact_type'  => 2, // 2=邮箱
                            'contact_value' => $emailVal,
                            'contact_extra' => '',
                            'created_at'    => $current_time
                        ];
                    }
                    $socialVal = trim($rowAssoc['联系人社交'] ?? '');
                    if ($socialVal !== '') {
                        // 注意：社交联系人字段格式如 "类型:账号"
                        $socialParts = explode(':', $socialVal, 2);
                        $contactType = 0;
                        $contactValue = $socialVal;
                        if (count($socialParts) == 2) {
                            $typeKey = strtolower($socialParts[0]);
                            $contactValue = $socialParts[1];
                            // 映射社交类型字符串到CONTACT_MAP定义的整型类型
                            $contactType = Client::CONTACT_MAP[$typeKey] ?? 0;
                        }
                        if ($contactType === 0) {
                            // 若无法识别类型，可统一存为自定义类型
                            $contactType = 99;
                        }
                        $contactsData[] = [
                            'leads_id'      => $leadsId,
                            'contact_type'  => $contactType,
                            'contact_value' => trim($contactValue),
                            'contact_extra' => '',
                            'created_at'    => $current_time
                        ];
                    }
                    // 批量插入联系人数据
                    if (!empty($contactsData)) {
                        Db::name('crm_contacts')->strict(false)
                            ->field(['leads_id','contact_type','contact_value','contact_extra','created_at'])
                            ->insertAll($contactsData);
                    }
                    // 提交事务
                    Db::commit();
                    $success_count++;
                } catch (\Exception $e) {
                    // 单条数据处理异常，回滚并记录日志
                    Db::rollback();
                    $fail_count++;
                    $logData = [
                        'message'       => "第{$index}条Excel数据导入失败",
                        'error_message' => $e->getMessage(),
                        'file'          => $e->getFile(),
                        'line'          => $e->getLine(),
                        'task_data'     => $row
                    ];
                    Client::addOperLog(null, '数据导入', $logData);
                    continue;
                }
            }
            // 当前批次完成，记录成功/失败数量日志
            $summary = [
                'message'      => 'Excel导入任务完成',
                'success_count'=> $success_count,
                'fail_count'   => $fail_count,
                'task_data'    => $filePath
            ];
            Client::addOperLog(null, '数据导入', $summary);
            $job->delete();
        } catch (\Exception $e) {
            // 整体任务异常处理，记录错误并重试或删除任务
            $logData = [
                'message'       => 'Excel导入任务整体失败',
                'error_message' => $e->getMessage(),
                'file'          => $e->getFile(),
                'line'          => $e->getLine(),
                'task_data'     => $data['filePath'] ?? ''
            ];
            Client::addOperLog(null, '数据导入', $logData);
            // 重试机制：最多重试3次
            if ($job->attempts() < 3) {
                $job->release(10);
            } else {
                $job->delete();
            }
        }
    }



    /**
     * 插入单条联系人数据
     *
     * @param array $contactsData 联系人数据
     */
    private function insertSingleContactsData($contactsData)
    {
        if (!empty($contactsData)) {
            Db::name('crm_contacts')
                ->strict(false)
                ->field(['leads_id', 'contact_type', 'contact_value', 'contact_extra', 'created_at'])
                ->insertAll($contactsData);
        }
    }


    /**
     * 插入单条客户主表数据
     *
     * @param array $leadsData 单条客户主表数据
     * @return int 插入后的客户主表 ID
     * @throws \Exception 插入失败时抛出异常
     */
    private function insertSingleLeadsData($leadsData)
    {
        $id = Db::name('crm_leads')->insertGetId($leadsData);
        return $id;
    }

    /**
     * 处理 Excel 导入任务
     *
     * @param Job $job 队列任务实例
     * @param array $data 任务数据
     */
    public function fireOld(Job $job, $data)
    {
        try {
            // 提取数据
            $filePath = $data['filePath'];
            $pr_user = $data['pr_user'];
            $chunkData = $data['chunkData'];
            $headers = $data['headers'];
            $current_time = date("Y-m-d H:i:s");

            // 准备数据
            list($leadsData, $contactsData) = $this->prepareData($chunkData, $headers, $pr_user, $current_time);

            // 开启事务
            Db::startTrans();
            try {
                // 批量插入客户主表
                $leadsIds = $this->insertLeadsData($leadsData);

                // 构建并插入联系人数据
                $this->insertContactsData($chunkData, $headers, $leadsIds, $current_time, $contactsData);

                // 提交事务
                Db::commit();
                // 删除任务
                $job->delete();
            } catch (\Exception $e) {
                // 回滚事务
                Db::rollback();
                throw $e;
            }
        } catch (\Exception $e) {
            // 记录详细错误日志
            $logData = [
                'message' => 'Excel 导入失败',
                'error_message' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'task_data' => $filePath,
                // 'trace' => $e->getTraceAsString()
            ];
            Client::addOperLog(null, '数据导入', $logData);
            $logData['trace'] = $e->getTraceAsString();
            Log::error(json_encode($logData, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));

            // 重试任务
            if ($job->attempts() < 3) {
                $job->release(10);
            } else {
                $job->delete();
            }
        }
    }



    /**
     * 检查行数据是否为空
     *
     * @param array $row 行数据
     * @return bool 如果行为空返回 true，否则返回 false
     */
    private function isRowEmpty($row)
    {
        foreach ($row as $value) {
            if (!empty(trim($value))) {
                return false;
            }
        }
        return true;
    }
    /**
     * 准备客户主表和联系人表的数据
     *
     * @param array $chunkData 数据块
     * @param array $headers 表头
     * @param string $pr_user 操作人
     * @param string $current_time 当前时间
     * @return array 包含客户主表数据和联系人表数据的数组
     */
    private function prepareData($chunkData, $headers, $pr_user, $current_time)
    {
        $leadsData = [];
        $contactsData = [];

        foreach ($chunkData as $row) {
            $rowAssoc = $this->buildAssocRow($row, $headers);
            // 准备客户主表数据
            $leadsRow = $this->buildLeadsRow($rowAssoc, $pr_user, $current_time);
            $leadsData[] = $leadsRow;
        }

        return [$leadsData, $contactsData];
    }

    /**
     * 构建关联数组行
     *
     * @param array $row 原始行数据
     * @param array $headers 表头
     * @return array 关联数组行
     */
    private function buildAssocRow($row, $headers)
    {
        $rowAssoc = [];
        foreach ($headers as $key => $title) {
            $rowAssoc[$title] = $row[$key] ?? '';
        }
        return $rowAssoc;
    }

    /**
     * 构建客户主表数据行
     *
     * @param array $rowAssoc 关联数组行
     * @param string $pr_user 操作人
     * @param string $current_time 当前时间
     * @return array 客户主表数据行
     */
    private function buildLeadsRow($rowAssoc, $pr_user, $current_time)
    {
        return [
            'kh_name'      => $rowAssoc['客户名称'] ?? '',
            'kh_rank'      => $rowAssoc['客户等级'] ?? '',
            'xs_area'      => $rowAssoc['国家'] ?? '',
            'kh_contact'   => $rowAssoc['联系人'] ?? '',
            'remark'       => $rowAssoc['客户备注'] ?? '',
            'kh_status'    => $rowAssoc['客户来源'] ?? '',
            'pr_user'      => $pr_user,
            'ut_time'      => $current_time,
            'at_time'      => $current_time,
            'at_user'      => $pr_user,
            'status'       => 1,
            'ispublic'     => 3,
            'pr_user_bef'  => $pr_user,
        ];
    }

    /**
     * 插入客户主表数据
     *
     * @param array $leadsData 客户主表数据
     * @return array 插入后的客户主表 ID 数组
     */
    private function insertLeadsData($leadsData)
    {
        $leadsIds = [];
        foreach ($leadsData as $data) {
            // 单条插入数据并获取插入后的 ID
            $id = Db::name('crm_leads')->insertGetId($data);
            if (!$id) {
                // 记录错误日志
                $logData = [
                    'message' => '客户主表数据插入失败',
                    'error_message' => '数据库插入操作返回无效 ID',
                    'file' => __FILE__,
                    'line' => __LINE__,
                    'task_data' => $data,
                    // 'trace' => (new \Exception())->getTraceAsString()
                ];
                if (Db::getConfig('trans_begin')) {
                    Db::rollback();
                }
                Client::addOperLog(null, '数据导入', $logData);
                Log::error('客户主表数据插入失败，数据：' . json_encode($data, JSON_UNESCAPED_UNICODE));
                throw new \Exception('客户主表数据插入失败');
            }
            $leadsIds[] = $id;
        }
        return $leadsIds;
    }

    /**
     * 构建并插入联系人数据
     *
     * @param array $chunkData 数据块
     * @param array $headers 表头
     * @param array $leadsIds 客户主表 ID 数组
     * @param string $current_time 当前时间
     * @param array &$contactsData 联系人表数据
     */
    private function insertContactsData($chunkData, $headers, $leadsIds, $current_time, &$contactsData)
    {
        foreach ($chunkData as $index => $row) {
            $rowAssoc = $this->buildAssocRow($row, $headers);
            $leadsId = $leadsIds[$index];

            $contacts = $this->buildContacts($leadsId, $rowAssoc, $current_time);

            foreach ($contacts as $contact) {
                if (!empty($contact['contact_value'])) {
                    $contactsData[] = $contact;
                }
            }
        }

        // 批量插入联系方式表（crm_contacts）
        if (!empty($contactsData)) {
            Db::name('crm_contacts')
                ->strict(false)
                ->field(['leads_id', 'contact_type', 'contact_value', 'contact_extra', 'created_at'])
                ->insertAll($contactsData);
        }
    }

    /**
     * 构建联系人数据
     *
     * @param int $leadsId 客户主表 ID
     * @param array $rowAssoc 关联数组行
     * @param string $current_time 当前时间
     * @return array 联系人数据数组
     */
    private function buildContacts($leadsId, $rowAssoc, $current_time)
    {

        $data = [];
        //联系方式
        $map = [
            '联系人电话' => 'phone',
            '联系人邮箱' => 'email',
            '联系人社交' => 'contacts'
        ];
        foreach ($map as $key => $con_key) {
            $con_v = trim($rowAssoc[$key] ?? '');
            if ($con_v) {
                $contact_extra = '';
                $contact_type = isset(Client::CONTACT_MAP[$con_key]) ? Client::CONTACT_MAP[$con_key] : $con_key;
                $contact_values = explode(',', $con_v);
                foreach ($contact_values as $contact_value) {
                    $contact_value = $contact_value;
                    if ($con_key == 'contacts') {
                        $c = explode(':', $contact_value);
                        if (count($c) == 2) {
                            $c[0] = strtolower($c[0]);
                            $contact_type =  isset(Client::CONTACT_MAP[$c[0]]) ? Client::CONTACT_MAP[$c[0]] : $c[0];
                            $contact_value = $c[1];
                        }
                        $contact_value = trim($contact_value);
                    } elseif ($con_key == 'phone') {
                        $phone = explode('-', $contact_value);
                        if (count($phone) == 2) {
                            $contact_extra = $phone[0];
                            $contact_value = $phone[1];
                        }
                    }
                    $data[] = [
                        'leads_id'      => $leadsId,
                        'contact_type'  => $contact_type,
                        'contact_value' => $contact_value,
                        'contact_extra' => $contact_extra,
                        'created_at'    => $current_time
                    ];
                }
            }
        }
        return $data;
    }
}
