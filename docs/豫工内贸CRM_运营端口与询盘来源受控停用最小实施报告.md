# 豫工内贸 CRM 运营端口与询盘来源受控停用最小实施报告

> **文档性质**：第二步最小代码实施报告（供用户与 ChatGPT 审核）  
> **实施日期**：2026-11-27  
> **依据**：`docs/豫工内贸CRM_运营端口与询盘来源硬删除改停用专项只读分析及最小实施方案.md`  
> **本轮约束**：已改业务代码；**未** ALTER；**未** Git add/commit/push；**未**部署正式服务器  

---

## 1. 最终结论

**`READY FOR LOCAL BROWSER ACCEPTANCE`**

核心四入口已由物理删除改为受控停用（`status: 0→1`），并对称提供启用；客户/运营/导入/统计已做历史兼容。浏览器人工验收清单见第 12 节。

---

## 2. Git 与验证命令结果

| 项 | 结果 |
|---|---|
| 分支 | `master`（相对实施前 HEAD `3720201`） |
| `git status --short` | 11 个已改文件 + 1 个新增 Service + 既有第一步分析文档（untracked） |
| `git diff --check` | 无输出（无空白错误） |
| `git diff --stat` | 约 +660 / −352（不含新增 Service 行数） |
| `php -l`（全部改动 PHP） | **全部 No syntax errors** |
| 内嵌 JS | 管理页主业务脚本静态可解析；`{:url()}` 模板占位导致机械 checker 假阳性，人工核对无语法问题 |
| Git 写操作 | **未执行** add/commit/push/pull |

---

## 3. 实际修改文件与方法

### 3.1 新增

| 文件 | 职责 |
|---|---|
| `application/admin/service/InquiryChannelStatusService.php` | 停用/启用/批量、父子约束、运营绑定拒绝、事务行锁、条件更新、审计写入、列表回显合并 |

### 3.2 Controller / Service / Job

| 文件 | 方法/区域 | 变更要点 |
|---|---|---|
| `Inquiry.php` | `del` / `batchDel` | DELETE → 停用；强制 POST |
| 同上 | `enable` / `batchEnable` | 新增启用 |
| 同上 | `inquirySearch` | 支持 `status` 筛选；默认全部 |
| 同上 | `add` / `importDo` | 显式写入 `status=0` |
| `InquiryPort.php` | `del` / `batchDel` | DELETE → 停用；强制 POST；启用账号绑定则拒绝 |
| 同上 | `enable` / `batchEnable` | 新增启用（父来源须启用） |
| 同上 | `add` | 事务 + 父来源启用校验 + `status=0` |
| 同上 | `edit` | 改挂父来源时校验父启用；下拉启用+当前兜底 |
| 同上 | `inquiryPortSearch` | `status` 筛选 |
| 同上 | `importDo` | 禁止挂靠停用父来源 |
| `Common.php` | `getInquiryList($onlyEnabled=false)` | 新增可选仅启用 |
| `Client.php` | `validateInquiryAndPortData` | 新旧值区分；新值须启用；旧停用可保留 |
| 同上 | `getPortsByInquiry` | 支持 `include_ids` 兜底 |
| 同上 | `edit` GET/POST | 历史停用渠道/端口回显；保存带旧值校验 |
| `ClientDetailService.php` | `buildDetailViewData` | 详情回显合并停用当前值 |
| `Operator.php` | `getTotalInquiryStats` | 分桶含停用；名称可标「已停用」 |
| `Auth.php` | `getChannels` / `getShops` | 编辑兜底已绑停用渠道/端口 |
| 同上 | `adminAdd` / `adminEdit` | 新绑定仅启用；保留旧停用绑定 |
| `ExcelImport.php` | 客户导入渠道/端口解析 | 仅允许 `status=0` |

### 3.3 前端

| 文件 | 变更 |
|---|---|
| `view/inquiry/index.html` | 删除→停用；启用/批量启用；状态列；状态筛选；确认文案 |
| `view/inquiry_port/index.html` | 同上 |
| `view/auth/adminForm.html` | `getChannels` 传 `include_id` 回显停用渠道 |
| `view/client/edit.html` / `details.html` | **未改模板结构**；依赖后端列表名追加「（已停用）」回显 |

---

## 4. 明确未修改的文件 / 范围

| 范围 | 说明 |
|---|---|
| `Order.php` 订单删除/编辑 | 未改 |
| `ShippingApplyService.php` 及发货模块 | 未改 |
| 客户硬删除 | 未改 |
| 财务/业绩金额公式 | 未改 |
| 公共权限框架 / 菜单 auth_rule | 未放宽、未新增独立规则（与原删权限一致） |
| 数据库 ALTER | **未执行** |
| `CrmInquiry::listEnabledNames` 等已按 status=0 的候选 | 保持 |

---

## 5. 停用 / 启用状态规则

| 表 | 启用 | 停用 |
|---|---|---|
| `crm_inquiry` | `status=0` | `status=1` |
| `crm_inquiry_port` | `status=0` | `status=1` |

- 停用只 UPDATE，不物理 DELETE。  
- 重复停用 / 重复启用：返回成功语义「已停用/已启用（无需重复操作）」。  
- 更新 `edit_time`。  
- 不改客户 `inquiry_id`/`port_id`、不改管理员绑定字段、不改订单 `source`/`source_port` 快照。

---

## 6. 权限与父子关系

### 6.1 权限（与原删除对齐，未扩大）

- Session 登录（`Common::initialize`）。  
- 超管：`aid==1` 或 `username==='admin'`。  
- 非超管：仅 `submit_person` 为本人。  
- 单条 / 批量均强制 POST。  
- 批量逐条判定；无权记录不修改；返回成功/跳过/失败数量。

### 6.2 询盘来源停用

- 存在任意 `crm_inquiry_port.status=0` 子端口 → **拒绝**。  
- **不**级联停用子端口。  
- 子端口全部停用后才可停用父来源。

### 6.3 运营端口停用

- 历史客户引用 **不阻止**。  
- 仍被 **`admin.is_open=1`** 且 `port_id` CSV 包含该端口的账号引用 → **拒绝**（安全拒绝策略；不自动解绑）。  
- 不修改运营归属与历史业绩字段。

### 6.4 启用

- 来源启用：不自动恢复子端口。  
- 端口启用：父来源必须存在且 `status=0`。

---

## 7. 历史客户 / 运营账号 / 导入兼容

| 场景 | 行为 |
|---|---|
| 新建客户 | 仅启用渠道/端口 |
| 编辑客户回显 | 启用集 ∪ 当前停用值，标签「已停用」 |
| 未改渠道保存其他字段 | 允许保留停用历史值 |
| 主动改到其他停用渠道/端口 | 服务端拒绝 |
| 多端口 CSV | `normalizePortIds` + 逐端口新旧区分 |
| 运营账号编辑 | 已绑停用渠道/端口可回显；新绑定仅启用；互斥占用规则保留 |
| Excel 客户导入 | 渠道/端口必须启用，否则失败 |

---

## 8. 统计兼容

`Operator::getTotalInquiryStats`：

- 不再只遍历 `status=0` 维度。  
- 含停用渠道/端口分桶；展示名可标「已停用」。  
- **未**给订单金额/利润 SQL 加 `status=0`。  
- `DataStatistics` 侧此前按 ID 取名、JOIN 未滤 status → 本轮确认无需改。

---

## 9. 审计与并发

### 9.1 审计

- 复用 `crm_operation_log`（字段同 `Client::addOperLog`）。  
- `leads_id=0`；`oper_type` 如「询盘来源停用/启用」「运营端口停用/启用」。  
- `description` JSON：目标表、ID、名称、前后 status、操作人、时间。  
- **缺口（已披露）**：无独立停用原因字段、无专用审计表；零 ALTER 下以操作日志满足最小追责。

### 9.2 并发

- 单条停用/启用：`Db::startTrans` + 目标行 `lock(true)` + `WHERE status=?` 条件更新。  
- 父停用：锁父行后检查启用子端口。  
- 端口新增：锁父行并校验父启用，与父停用串行化。  
- **残余竞态（未宣称已关闭）**：端口停用检查运营绑定与管理员保存之间无跨表行锁；新绑定侧已拒绝停用端口，极端并发仍可能窗口极短。不得描述为“已完全解决”。

---

## 10. 测试结果（分级）

| 类别 | 结果 | 说明 |
|---|---|---|
| `STATIC PASS` | 是 | 全部改动 PHP `php -l`；管理页主 JS 人工/静态核对 |
| `MOCK PASS` | 是 | Service 常量、`normalizeIds`、`mergeDisabledCurrent` |
| `DB READ PASS` | 是 | 本地 `crmneimao`：inquiry 6 条 status=0；port 69 条 status=0；status 注释「软删除」；`crm_operation_log` 字段齐全 |
| `LOCAL WRITE`（临时行） | 是 | 仅本脚本创建的 TMP 行：子端口拦父停用、端口停用/幂等、父停用、启用、审计插入后清理 |
| `LOCAL BROWSER PASS` | **NOT TESTED** | 待人工浏览器验收 |
| 全量业务写测（真实客户/运营账号） | **NOT TESTED** | 避免污染正式业务数据 |

---

## 11. 测试矩阵覆盖标注

| ID | 场景 | 状态 |
|---|---|---|
| 1–2 | 来源/端口单批停用 | STATIC + LOCAL WRITE（核心 SQL 路径） |
| 3 | 有启用子端口拒停父 | LOCAL WRITE |
| 4 | 绑定启用运营账号拒停端口 | STATIC（代码路径）；浏览器 NOT TESTED |
| 5 | 历史客户引用可停端口 | STATIC（无客户引用检查） |
| 6 | 非法 ID / 越权 / 非 POST | STATIC |
| 7 | 重复停用/恢复 | LOCAL WRITE + STATIC |
| 8 | 恢复父/子 | LOCAL WRITE |
| 9–14 | 客户下拉/回显/保存/伪造/CSV | STATIC |
| 15–16 | 运营回显 / Excel | STATIC |
| 17 | 运营渠道历史统计 | STATIC（代码改分桶） |
| 18 | 订单快照不变 | STATIC（未改订单模块） |
| 19 | 批量权限混合 | STATIC |
| 20 | 并发 | STATIC（事务锁）；全量压力 NOT TESTED |
| 21 | 审计与失败回滚 | LOCAL WRITE（审计）+ STATIC（rollback） |
| 22 | 管理页搜索分页编辑回归 | 浏览器 NOT TESTED |

---

## 12. 浏览器人工验收清单

1. 询盘来源列表：状态列、筛选、单停用/启用、批停用/启用、确认文案。  
2. 有启用子端口的来源停用应失败并提示。  
3. 运营端口列表：同上；绑定启用运营账号的端口停用应失败。  
4. 停用后新建客户下拉不可见；编辑历史客户可见并标「已停用」；未改渠道可保存。  
5. 伪造停用渠道 ID POST 应失败。  
6. 运营账号编辑：已绑停用端口仍回显；不可新选其他停用端口。  
7. 运营总询盘统计：停用渠道历史数仍在，可带「已停用」。  
8. 抽一笔历史订单：`source` / `source_port` 不变。  
9. 原搜索、分页、编辑弹窗回归。

---

## 13. 未完成风险

1. 浏览器端尚未人工验收。  
2. 端口停用 ↔ 运营账号绑定的极端并发窗口未压测关闭。  
3. 无停用原因字段；审计依赖通用操作日志。  
4. 停用后同名仍占用（与原 add 去重不过滤 status 一致），可能影响“同名再建启用”——保持原口径，待业务确认。  
5. 菜单级无独立 `del`/`enable` auth_rule（与原删除相同既有缺口）。

---

## 14. 回滚策略

1. 回滚代码至实施前提交 `372020100e85db77453c2a9735484e03032248de`（或发布前 tag）。  
2. 若已产生 `status=1`：回滚代码后管理列表仍可见这些行；下拉仍按 `status=0` 过滤。误停用需**人工授权** SQL 改回 `0`。  
3. 因未物理删除，回滚不导致客户渠道 ID 断裂。  
4. **禁止**用物理 DELETE“清理”停用数据。

---

## 15. 阻塞条件复核

| 条件 | 结论 |
|---|---|
| status 取值冲突 | **未发现**（注释软删除；库内全 0；下拉已按 0=启用） |
| 必须 ALTER | **否** |
| 绑定运营账号端口策略 | 采用**安全拒绝**（未取得“警告后强制停用”确认） |

---

## 最终结论

**`READY FOR LOCAL BROWSER ACCEPTANCE`**

请用户与 ChatGPT 审核本报告与代码差异；完成浏览器清单后再决定是否进入发布流程。本轮到此停止：**未** commit / push / 部署。
