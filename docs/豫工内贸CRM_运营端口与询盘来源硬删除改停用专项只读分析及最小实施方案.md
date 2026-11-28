# 豫工内贸 CRM 运营端口与询盘来源硬删除改停用专项只读分析及最小实施方案

> **文档性质**：第一步只读分析 + 最小实施方案设计（供用户与 ChatGPT 审核）  
> **分析日期**：2026-11-27  
> **分析库**：本地 `crmneimao@127.0.0.1`  
> **本轮约束**：未修改业务代码、数据库、配置、菜单、Git；未执行真实删除/停用操作  

---

## 1. 执行结论

| 项 | 结论 |
|---|---|
| 物理删除是否仍存在 | **是**。`Inquiry::del/batchDel`、`InquiryPort::del/batchDel` 均执行 `DELETE`（`CODE VERIFIED`） |
| `status` 是否可复用为停用 | **可以**。两表 `status` 注释为「软删除」，默认 `0`；全库当前全部为 `0`；多处候选下拉已按 `status=0` 视为启用（`DB READ VERIFIED` + `CODE VERIFIED`） |
| 是否必须 ALTER | **否**（复用现有 `status` 即可完成停用；审计字段不足见第 17 节） |
| 停用后最大风险 | 客户编辑/详情、统计桶若只查 `status=0`，已停用渠道会“看不见”，造成回显失败或历史统计缺口 |
| 是否扩大到客户/订单删除 | **否**。本专项边界仅渠道停用 |
| 最终状态 | **`READY FOR STEP 2 IMPLEMENTATION REVIEW`** |

未发现「`status` 同时承担互相冲突的多种业务状态机」导致无法复用的硬阻塞。字段注释为「软删除」，业务目标为「停用」——二者可统一解释为：`0=启用/有效`，`1=停用/软删除`（与导入模板 `0/1`、现有下拉过滤一致）。`DESIGN INFERENCE`

---

## 2. 当前 Git 状态

| 项 | 值 | 证据 |
|---|---|---|
| 分支 | `master` | `git branch --show-current` |
| HEAD | `372020100e85db77453c2a9735484e03032248de` | `git rev-parse HEAD` |
| 工作区 | 干净（`git status --short` 无输出；`git diff --stat/--check` 无输出） | `CODE VERIFIED`（只读 Git 命令） |
| 本轮 Git 写操作 | **未执行** | — |

---

## 3. 实际代码删除链路

### 3.1 询盘来源 `Inquiry`

| 环节 | 详情 | 标注 |
|---|---|---|
| 前端单删 | `application/admin/view/inquiry/index.html` L159–179：确认框 → `$.post("{:url('Inquiry/del')}", {id})` | `CODE VERIFIED` |
| 前端批删 | 同文件 L188–214：勾选 → `$.post("{:url('Inquiry/batchDel')}", {ids})` | `CODE VERIFIED` |
| URL | `Inquiry/del`、`Inquiry/batchDel`（ThinkPHP 默认路由） | `CODE VERIFIED` |
| Controller | `application/admin/controller/Inquiry.php` | `CODE VERIFIED` |
| Service | **无独立 Service**；Controller 直连 `Db::name('crm_inquiry')` | `CODE VERIFIED` |
| Model | `CrmInquiry` 未被删除逻辑调用 | `CODE VERIFIED` |
| SQL | `DELETE FROM crm_inquiry WHERE id=...` / `WHERE id IN (...)` | `CODE VERIFIED` |
| 表 | `crm_inquiry` | `DB READ VERIFIED` |

**`Inquiry::del()`（L146–179）**

1. 取 `id`；超管判定：`session('aid')==1` 或 `username==='admin'`。  
2. 非超管仅可操作 `submit_person = 当前用户名` 的记录。  
3. **引用检查**：`crm_inquiry_port.inquiry_id = id` 存在则拒绝（**只查端口，不查客户/管理员**）。  
4. `Db::name('crm_inquiry')->where('id',$id)->delete()` 物理删除。  
5. **未强制 POST**；无事务；无审计写库。

**`Inquiry::batchDel()`（L185–284）**

1. 强制 POST。  
2. 权限过滤同单删。  
3. 有子端口的 ID 跳过，其余物理删除。  
4. 返回删除数/跳过数/`blocked_names`。  
5. try/catch，但删除本身无事务包裹多步一致性需求（单表批量 delete）。

### 3.2 运营端口 `InquiryPort`

| 环节 | 详情 | 标注 |
|---|---|---|
| 前端单删 | `application/admin/view/inquiry_port/index.html` L170–183：`$.post("{:url('InquiryPort/del')}", {id})` | `CODE VERIFIED` |
| 前端批删 | 同文件 L192–214：`$.post("{:url('InquiryPort/batchDel')}", {ids})` | `CODE VERIFIED` |
| Controller | `application/admin/controller/InquiryPort.php` L168–238 | `CODE VERIFIED` |
| Service | **无** | `CODE VERIFIED` |
| SQL | `DELETE FROM crm_inquiry_port ...` | `CODE VERIFIED` |
| 表 | `crm_inquiry_port` | `DB READ VERIFIED` |

**`InquiryPort::del()`（L168–190）**

1. 超管/本人 `submit_person` 限制后直接 `delete()`。  
2. **无客户引用检查、无管理员绑定检查**。  
3. **未强制 POST**；无事务；无审计。

**`InquiryPort::batchDel()`（L194–238）**

1. 强制 POST。  
2. 超管：`whereIn(id)->delete()`；非超管仅删本人提交记录。  
3. 同样**无业务引用检查**。

### 3.3 调用链示意

```text
[列表页删除按钮]
  → POST Inquiry/del | Inquiry/batchDel
  → Inquiry Controller
  → Db::name('crm_inquiry')->delete()
  → 表 crm_inquiry

[列表页删除按钮]
  → POST InquiryPort/del | InquiryPort/batchDel
  → InquiryPort Controller
  → Db::name('crm_inquiry_port')->delete()
  → 表 crm_inquiry_port
```

**不存在**“先物理删端口再删来源”的级联代码；来源删除仅在仍有端口行时拒绝。`CODE VERIFIED`

---

## 4. 两张表真实 `status` 语义

### 4.1 表结构（`SHOW FULL COLUMNS`）

**`crm_inquiry`**

| 字段 | 类型 | 默认 | 注释 |
|---|---|---|---|
| id | int PK AI | — | 主键 |
| inquiry_name | varchar(255) | NULL | 询盘来源名字 |
| org | varchar(255) | NULL | 所属组织 |
| add_time / edit_time | bigint | — | 时间戳 |
| submit_person | varchar(255) | NULL | 提交人 |
| **status** | **tinyint(1)** | **0** | **软删除** |

**`crm_inquiry_port`**

| 字段 | 类型 | 默认 | 注释 |
|---|---|---|---|
| id | int PK AI | — | 主键 |
| port_name | varchar(255) | NULL | 端口名称 |
| org | varchar(255) | NULL | 所属组织 |
| inquiry_id | int | 0 | 对应询盘 id |
| **status** | **tinyint(1)** | **0** | **软删除** |
| add_time / edit_time | bigint | — | 时间戳 |
| submit_person | varchar(255) | — | 提交人 |

索引：两表均**仅 PRIMARY(id)**。无外键、无触发器（`information_schema` 查询为空）。`DB READ VERIFIED`

### 4.2 代码中的取值含义

| 取值 | 实际用法 | 证据 |
|---|---|---|
| `0` | 启用 / 可出现在新建候选下拉 | `CrmInquiry::listEnabledNames`、`Client::add/edit`、`Auth::getChannels/getShops`、`Order` 下拉、`Operator` 部分统计入口等均 `where status=0`（`CODE VERIFIED`） |
| `1`（及非 0） | 导入模板允许写入；语义上对应“非启用/软删除” | `InquiryPort::tpl` 表头「状态(0或1)」；`importDo` 默认 0（`CODE VERIFIED`） |
| 当前库中非 0 | **无** | 询盘 6 条全为 0；端口 69 条全为 0（`DB READ VERIFIED`） |

**结论**：可将业务「停用」落地为 `status=1`（保留行），与注释「软删除」及现有启用过滤一致。  
**不建议**改用 `-1`（产品模块另有约定，渠道导入已是 0/1）。`DESIGN INFERENCE`

**无已有启用/停用操作入口**（列表仅有删除）。`CODE VERIFIED`  
**无已有软删除记录**（全为 0）。`DB READ VERIFIED`

---

## 5. 当前数据库数量与引用情况

| 指标 | 数量 | 标注 |
|---|---|---|
| 询盘来源总数 | 6（全 status=0） | `DB READ VERIFIED` |
| 运营端口总数 | 69（全 status=0） | `DB READ VERIFIED` |
| 来源→端口分布 | 竞价4 / 抖音8 / C端49 / 返单6 / 闲鱼1 / B2B1 | `DB READ VERIFIED` |
| `crm_leads` 有 inquiry_id | 106；孤立 inquiry 引用 **0** | `DB READ VERIFIED` |
| `crm_leads` 有 port_id | 106 | `DB READ VERIFIED` |
| 被客户引用的端口数 | 12（FIND_IN_SET） | `DB READ VERIFIED` |
| `admin` 绑定 inquiry_id | 13 人（均为运营组 group_id=12） | `DB READ VERIFIED` |
| `admin` 绑定 port_id | 12 人；涉及端口约 59 个 | `DB READ VERIFIED` |
| 被客户引用的来源数 | 6（全部来源均有客户） | `DB READ VERIFIED` |
| 被管理员引用的来源数 | 3（id 10/11/12） | `DB READ VERIFIED` |
| 订单 `source` / `source_port` 非空 | 各 180（名称快照） | `DB READ VERIFIED` |

高引用端口示例：`竞价卡1` 关联客户 52 + 管理员 1。`DB READ VERIFIED`

---

## 6. 单删与批量删除权限矩阵

| 能力 | 询盘来源单删 | 询盘来源批删 | 端口单删 | 端口批删 |
|---|---|---|---|---|
| 登录 Session | 要（Common::initialize） | 要 | 要 | 要 |
| 菜单级 auth_rule | 仅有 `Inquiry/Index`、`InquiryPort/Index`；**无** `del`/`batchDel` 独立规则。无 HrefId 时不拦 action | 同左 | 同左 | 同左 |
| 超管 | aid=1 或 username=admin，可操作全部 | 同 | 同 | 同 |
| 普通用户 | 仅 `submit_person` 本人 | 同 | 同 | 同 |
| 组织隔离 | 删除查询**未**再加 org 条件（依赖提交人/超管） | 同 | 同 | 同 |
| 业务引用拦截 | 仅子端口 | 仅子端口 | **无** | **无** |
| 客户/管理员引用 | **未检查** | **未检查** | **未检查** | **未检查** |
| HTTP 方法 | 未限 POST（前端用 POST） | 强制 POST | 未限 POST | 强制 POST |
| CSRF Token | 未见专用 token；依赖 Session | 同 | 同 | 同 |
| 事务 | 无 | 无 | 无 | 无 |
| 审计 | 无 | 无 | 无 | 无 |
| 并发 | 无行锁；重复删第二次变“不存在/失败” | 同 | 同 | 同 |

`CODE VERIFIED`（`Common.php` L34–59；`Inquiry.php`；`InquiryPort.php`；`auth_rule` 查询）

**越权风险**：任意已登录且能猜到 URL 的账号，若 action 无独立 auth_rule，可能绕过菜单；业务上仍受 `submit_person`/超管限制。属既有缺口，停用改造时应保持与单删/批删权限一致，不宜放宽。`DESIGN INFERENCE`

---

## 7. 前端删除交互现状

| 页面 | 交互 | 成功码 |
|---|---|---|
| `inquiry/index.html` | 「删除」「批量删除」；确认文案为删除 | 单删 `code===200`；批删 `code===0` |
| `inquiry_port/index.html` | 同上 | 同上 |
| 列表列 | **不展示 status** | — |
| 停用/启用按钮 | **无** | — |

`CODE VERIFIED`

---

## 8. 运营端口被使用的情况

| 使用面 | 现状 | 标注 |
|---|---|---|
| 客户归属 | `crm_leads.port_id` 可多值 CSV；12 个端口有客户 | `DB READ VERIFIED` |
| 运营账号绑定 | `admin.port_id` + `admin.inquiry_id`；同渠道端口互斥占用（`Auth::checkPortsUsed`） | `CODE VERIFIED` + `DB READ VERIFIED` |
| 自动分配/业绩归属 | `Products.php` 等用运营人员的 inquiry_id+port_id 匹配客户询盘 | `CODE VERIFIED` |
| 订单快照 | `crm_client_order.source_port` 存**名称** | `DB READ VERIFIED` |
| 唯一性 | 同 org + 同 inquiry_id + 同 port_name 去重（新增/导入） | `CODE VERIFIED` |

**历史引用是否允许停用**：从数据保全角度应**允许**（停用 ≠ 抹除历史）。`DESIGN INFERENCE`  
**业务仍在使用是否允许停用**：若端口仍绑定启用中的运营账号，建议**警告并可继续停用**或**要求先解绑**——需业务确认（见第 24 节）。停用后新客户不可选，但已绑定账号的匹配逻辑若不改，历史统计/运营过滤仍可能命中该 port_id。`DESIGN INFERENCE`

---

## 9. 询盘来源与端口父子关系

| 规则 | 现状 |
|---|---|
| 端口归属 | `crm_inquiry_port.inquiry_id` → `crm_inquiry.id`（逻辑外键，无 DB FK） |
| 父删限制 | 有任意子端口行则禁止物理删来源 |
| 父停用候选方案 | **有启用中子端口时禁止停用父来源，提示先停用子端口**（推荐，低风险） |
| 子停用后父停用 | 允许 |
| 父恢复 | 建议允许；子端口不自动恢复 |
| 父停用后子选择器 | 新建链路中父不可选则子不会被选到；`getPortsByInquiry` 已只返回 `status=0` 端口 |

`CODE VERIFIED` + `DESIGN INFERENCE`

---

## 10. 历史客户展示风险

| 场景 | 现状 | 停用后风险 |
|---|---|---|
| 列表名称映射 | `column('inquiry_name','id')` **不过滤 status** | 行保留则名称仍可显示 ✅ |
| 客户详情只读 | `ClientDetailService` 仅加载 `status=0` 的 channelList/portList 供下拉回显 | 已停用渠道/端口 **option 消失**，详情显示空白 ❌ |
| 客户编辑页 | `Client::edit` 同样只加载 `status=0` | 回显失败；保存可能被清空/校验失败 ❌ |
| 跟进详情 | `ClientFollowService` 按 id 查名称，不过滤 status | 行保留则正常 ✅ |
| 公海 Liberum | 名称映射不过滤 status；筛选项查全表 | 基本安全 ✅ |

**关键阻塞点**：详情/编辑若继续只喂 `status=0` 列表，停用后历史回显会坏。必须做「候选仅启用 + 当前值兜底」。`CODE VERIFIED`

---

## 11. 客户新增和编辑候选选择器风险

| 位置 | 过滤 | 类别 | 改造建议 |
|---|---|---|---|
| `Client::add` L3257–3258 | status=0 | A 新建候选 | 保持仅启用 |
| `Client::edit` L3750–3751 | status=0 | B 编辑候选 | 启用集 + **当前已选停用项兜底** |
| `Client::getPortsByInquiry` L196–198 | status=0 | A/B 联动 | 新建仅启用；编辑需带当前选中停用端口 |
| `Client` 各列表页 inquiryList | **无 status 过滤** | E 筛选 | 可保留全量，便于查历史 |
| `FollowTeam` / `ClientFollow` | 全量 | E 筛选 | 可保留全量或标注停用 |
| `Auth::getChannels/getShops` | status=0 | A 账号配置 | 保持仅启用；编辑管理员需兜底已绑停用端口 |
| `Order` 下拉 | status=0 | A 新建订单 | 保持；订单侧另有“缺失来源塞回列表”兜底（编辑订单） |
| `Common::getInquiryList` | **无 status** | 端口管理父下拉 | 新增端口时应改为仅启用父来源 |
| `CheckOrderService` | listEnabledNames | A | 保持 |

`CODE VERIFIED`

---

## 12. 后端渠道校验影响

`Client::validateInquiryAndPortData`（L2828–2854）：

- 只验证 inquiry 存在、port 属于该 inquiry。  
- **不检查 status**。  

停用后若前端仍提交原停用 ID：校验可通过（有利于“未改渠道保存”）。  
若前端因 option 消失提交空值：会失败「请选择所属渠道/运营端口」。  

**改造要求**：

1. 新选择：仅允许 `status=0`。  
2. 未变更的原值：即使 `status=1` 也允许保留。  
3. 主动改到其他停用渠道：拒绝。

`CODE VERIFIED` + `DESIGN INFERENCE`

导入 `ExcelImport.php` 按名称找渠道/端口时**不排除停用**——停用后仍可能导入挂到停用渠道；建议第二步改为仅允许启用。`CODE VERIFIED`

---

## 13. 运营账号分配影响

- 新增/编辑管理员：端口互斥占用检查；渠道名写入旧字段 `channel`。  
- 下拉仅启用渠道/端口。  
- 库中 13 个运营账号绑定 inquiry/port。  

停用端口后：

- 账号表绑定关系**不会自动清除**（当前也无删除级联）。  
- 运营业绩匹配仍可能按 port_id 命中历史客户（通常应保留，避免业绩消失）。  
- 管理员编辑页可能看不到已停用端口选项 → 需兜底或先解绑。

**建议规则（待确认）**：停用时若存在 `admin` 引用，返回警告列表；默认**不强制解绑**（避免误伤账号配置），或提供“停用并提示运营负责人”。`DESIGN INFERENCE`

---

## 14. 订单历史快照影响

- `crm_client_order.source` / `source_port` 为**名称快照**，不依赖渠道行存活。  
- 物理删除渠道不影响已落库快照文字；停用同样不影响。  
- 订单新建下拉已过滤 `status=0`；编辑页对缺失来源有塞回逻辑（产品类似思路）。  
- **本专项不改订单模块**，除非审核认为订单新建必须同步文案；当前非阻塞。

`CODE VERIFIED` + `DB READ VERIFIED`

**旁路风险（既有）**：渠道 `edit` 改名会改变客户侧 JOIN/映射显示名，但订单快照仍是旧名——属既有改名问题，本步可不修，但应在规则中提示慎改名。`DESIGN INFERENCE`

---

## 15. 统计与业绩影响

| 模块 | 行为 | 停用影响 |
|---|---|---|
| `DataStatistics` / `Operator` 多数汇总 | 先取 lead 的 inquiry_id/port_id，再 map 名称；LEFT JOIN 渠道表；**map 查询一般不过滤 status** | 行保留则历史统计名称仍在 ✅ |
| `Operator::getTotalInquiryStats` | **只遍历 status=0 的来源与端口再聚合** | 停用后该渠道桶从总览消失，客户数不进这些桶 ❌ |
| `Operator`/`DataStatistics` 订单筛选项 | 部分下拉 status=0 | 仅影响筛选候选，不影响已出报表金额 ✅ |
| 按名称快照的订单业绩 | 不依赖 status | 金额口径不变 ✅ |

**必须避免**：给所有统计 SQL 统一加 `status=0`。  
**必须修改**：以“启用渠道列表驱动分桶”的统计（至少 `getTotalInquiryStats` 及同类），改为「全部未物理删除的渠道」或「有数据的渠道含停用」。`CODE VERIFIED`

改造前后有效业绩金额：订单侧不改则**不应变化**。`DESIGN INFERENCE`

---

## 16. JOIN 过滤风险

- 渠道相关统计以 `LEFT JOIN crm_inquiry / crm_inquiry_port` 为主，未见对 `ci.status`/`cp.status` 的 JOIN 条件。  
- 风险点不在 JOIN，而在**先用 status=0 限定维度再统计**。  
- 另：`l.port_id = cp.id` 对多值 CSV 本就不精确（既有问题），停用不加剧物理删那种“名称全丢”。

`CODE VERIFIED`

---

## 17. 审计与并发风险

| 项 | 现状 | 最小方案 |
|---|---|---|
| 操作人 | 无专用字段；仅有 `submit_person`（创建人） | 可临时写入操作日志表（若项目已有通用 oper log）或仅更新 `edit_time` |
| 停用时间 | 无专用字段 | 复用 `edit_time=time()` |
| 停用原因 | 无字段 | **本步可不强制**（避免 ALTER）；前端确认文案即可 |
| 审计表 | Inquiry 模块无专用审计 | **缺口**：无停用前后快照表。可记为待确认；不为此新建表 unless 业务强制 |
| 重复停用 | — | 幂等：已 status=1 则返回成功或“已停用” |
| 并发 | 无锁 | 条件更新：`WHERE id=? AND status=0`；影响行数 0 则提示 |
| 恢复启用 | 无 | 建议第二步同时做「启用」对称接口，否则只能改库 |

`CODE VERIFIED` + `DESIGN INFERENCE`

**审计缺口不隐瞒**：若公司要求追责级审计，需另议是否新增字段/复用 `Client::addOperLog` 一类机制；**不阻塞**用 `status` 完成停用主路径。

---

## 18. 推荐停用业务规则（供审核）

1. **删除入口改为停用**：`status: 0 → 1`，禁止 `DELETE`。  
2. **权限**：与现单删/批删一致（超管或提交人）；批停与单停一致。  
3. **询盘来源停用**：若存在 `crm_inquiry_port.status=0` 子端口 → 拒绝，提示先停用子端口。  
4. **运营端口停用**：  
   - 不因历史客户引用而拒绝；  
   - 若 `admin.port_id` 仍引用 → **警告**（推荐）或拒绝（待确认）。  
5. **新建候选**：仅 `status=0`。  
6. **编辑/详情回显**：启用集 ∪ 当前已选（即使停用），标签可加「已停用」。  
7. **保存校验**：原值允许停用态；新值必须启用。  
8. **管理列表**：显示全部或可筛选状态；操作改为「停用/启用」。  
9. **统计**：历史分桶包含停用渠道；不得因 status 丢业绩。  
10. **订单快照**：不改。  
11. **改名/改归属**：`edit` 保留但提示高风险；本步可不改逻辑。  
12. **唯一名**：停用后同名仍占用（当前 add 去重不过滤 status）——保持，避免影子重复；或改为仅启用唯一（待确认）。

---

## 19. 预计修改文件与方法清单

### 19.1 必须修改的 Controller

| 文件 | 方法 | 必须修改 | 原因 | 风险 | 测试 |
|---|---|---|---|---|---|
| `application/admin/controller/Inquiry.php` | `del` | 是 | DELETE→status=1；保留子端口检查（改为启用子端口） | 中 | 单停/拒停/越权 |
| 同上 | `batchDel` | 是 | 批量停用；权限与引用规则对齐 | 中 | 混合勾选 |
| 同上 | （建议）`enable` 或 `del` 对称恢复 | 强烈建议 | 否则无法自助恢复 | 低 | 恢复后可选 |
| `application/admin/controller/InquiryPort.php` | `del` | 是 | DELETE→status=1；增加可选 admin 引用提示 | 高 | 有客户/有管理员 |
| 同上 | `batchDel` | 是 | 同上 | 高 | 批量 |
| 同上 | `add`/`edit` 所用父列表 | 是（或改 Common） | 父来源仅启用 | 中 | 停用父不可选 |
| `application/admin/controller/Common.php` | `getInquiryList` | 是 | 默认仅启用；管理列表另议 | 中 | 端口新增下拉 |
| `application/admin/controller/Client.php` | `edit`（装载 channel/port） | 是 | 当前值兜底 | 高 | 停用渠道客户编辑 |
| 同上 | `add` | 否（保持 status=0） | 已正确 | — | 回归 |
| 同上 | `getPortsByInquiry` | 是 | 支持 `include_ids` 或编辑态兜底 | 高 | 联动回显 |
| 同上 | `validateInquiryAndPortData` | 是 | 新值启用/旧值放行 | 高 | 未改保存/改选 |
| `application/admin/controller/Operator.php` | `getTotalInquiryStats` | 是 | 分桶勿丢停用渠道 | 中 | 停用前后桶对比 |
| `application/admin/controller/Auth.php` | `getShops`（及管理员编辑回显） | 建议 | 已绑停用端口回显 | 中 | 编辑运营账号 |

### 19.2 必须修改的前端页面

| 文件 | 方法/区域 | 必须修改 | 原因 | 风险 | 测试 |
|---|---|---|---|---|---|
| `view/inquiry/index.html` | 删除按钮/批删/文案 | 是 | 改为停用；展示状态列 | 低 | UI |
| `view/inquiry_port/index.html` | 同上 | 是 | 同上 | 低 | UI |
| `view/client/edit.html` | 渠道/端口 option | 配合后端兜底 | 显示已停用项 | 中 | 回显 |
| `view/client/details.html` | 同上 | 配合 `ClientDetailService` | 只读回显 | 中 | 回显 |

### 19.3 候选选择器与后端校验

| 文件 | 方法 | 必须修改 | 原因 |
|---|---|---|---|
| `service/ClientDetailService.php` | 装载 channel/port | 是 | 详情回显 |
| `job/ExcelImport.php` | 渠道/端口解析 | 建议 | 禁止挂到停用渠道 |
| `model/CrmInquiry.php` / `CrmInquiryPort.php` | `listEnabledNames` | 否（已正确） | — |

### 19.4 历史展示

| 文件 | 说明 |
|---|---|
| Client 列表映射（多处 `column`） | **无需改**（不过滤 status） |
| `ClientFollowService` 名称解析 | **无需改** |
| `conflict_worker.php` | **无需改**（按 id 取名） |

### 19.5 统计查询

| 文件 | 方法 | 必须修改 |
|---|---|---|
| `Operator.php` | `getTotalInquiryStats` | 是 |
| `DataStatistics.php` 同类“先取启用渠道再聚合” | 逐处核对；有则改 | 是（若存在同类） |

### 19.6 风险分级汇总

- **高**：端口停用无引用提示、客户编辑/详情回显、保存校验。  
- **中**：父来源停用与子端口、运营统计分桶、管理员端口回显。  
- **低**：文案与列表状态列。

---

## 20. 不需要修改的文件清单（已检查）

| 文件 | 原因 |
|---|---|
| `CrmInquiry::listEnabledNames` / `CrmInquiryPort::listEnabledNames` | 已按 status=0 |
| `Client::add` 装载（已 status=0） | 新建口径正确 |
| 订单快照读写主路径 | 名称快照；非本步阻塞 |
| `ShippingApplyService` 等发货模块 | 无关 |
| 客户/订单物理删除相关代码 | 超出边界 |
| `Order - 最重要的一次备份副本.php` | 备份文件，勿动 |

---

## 21. 数据库 ALTER 是否必要

| 需求 | 是否必要 |
|---|---|
| 停用主路径 | **不必要**（复用 `status`） |
| 停用原因/操作人专用列 | 非必须；缺则记审计缺口 |
| 外键/触发器 | 不需要新增 |
| 索引 | 可选：`(status)`, `(inquiry_id,status)` 提升筛选；非上线硬前置 |

**结论：可在零 ALTER 前提下完成最小停用改造。** `DB READ VERIFIED` + `DESIGN INFERENCE`

---

## 22. 本地测试矩阵（第二步实施后执行；本轮未测写操作）

| ID | 场景 | 期望 |
|---|---|---|
| T1 | 停用无子端口的来源 | status=1，行仍在 |
| T2 | 停用仍有启用子端口的来源 | 拒绝 |
| T3 | 停用有客户的端口 | 成功；客户列表仍显示名称 |
| T4 | 停用有管理员绑定的端口 | 按确认规则警告或拒绝 |
| T5 | 新建客户 | 下拉无该停用项 |
| T6 | 编辑未改渠道的停用渠道客户 | 保存成功，值不变 |
| T7 | 编辑时改选其他启用渠道 | 成功 |
| T8 | 编辑时试图改到另一停用渠道 | 拒绝 |
| T9 | 客户详情回显停用渠道 | 可见名称（可标已停用） |
| T10 | 运营总询盘统计 | 停用渠道历史数仍可见 |
| T11 | 订单历史 source/source_port | 不变 |
| T12 | 非提交人停用 | 拒绝 |
| T13 | 超管停用他人 | 允许 |
| T14 | 重复停用 | 幂等 |
| T15 | 启用恢复 | 重新出现在新建下拉 |
| T16 | 批停混合权限/引用 | 部分成功提示清晰 |
| T17 | 导入停用渠道名 | 失败（若实施了导入限制） |

---

## 23. 正式部署和回滚建议

**部署**

1. 仅发布代码；**无 DDL**。  
2. 低峰发布；先在预发/本地用拷贝库验证 T1–T17。  
3. 发布后抽查：任意停用渠道客户详情、一条运营统计、一笔历史订单来源字段。  
4. 确认菜单仍走原 Inquiry/InquiryPort 入口；权限未放宽。

**回滚**

1. 回滚代码至本 HEAD `372020100e85db77453c2a9735484e03032248de`（或发布前 tag）。  
2. 若已产生 `status=1` 数据：回滚代码后这些行会重新出现在「未过滤 status」的管理列表，但不会被 `status=0` 下拉选中——可用 SQL 只读核查后，**人工**将误停用行改回 `0`（属数据修复，需单独授权）。  
3. 因未物理删除，回滚不导致历史客户渠道 ID 断裂。

**禁止**：用物理 DELETE“清理”停用数据；禁止在未审核时扩大到客户/订单删除。

---

## 24. 未确认的业务问题

1. 端口仍绑定启用运营账号时：警告继续 vs 强制先解绑？  
2. 第二步是否必须同步「启用/恢复」按钮？  
3. 停用后同名是否允许再新建一条启用记录？  
4. 管理列表默认显示全部还是仅启用？  
5. 是否要求停用原因与独立审计表？（现结构不足）  
6. `Operator::getTotalInquiryStats` 停用渠道：计入并标「已停用」，还是单独“含停用”开关？  
7. 父来源停用是否级联停用所有子端口（高风险，**不推荐**）？  

---

## 25. 下一步最小实施边界

**纳入第二步（审核通过后）**

- 四个删除入口改为停用（`status=1`）。  
- 单/批权限对齐。  
- 父子：有启用子端口不可停用父来源。  
- 新建候选隐藏停用；编辑/详情/校验兼容历史值。  
- 修正会丢停用桶的统计入口。  
- 前端文案与状态展示。  
- 建议含启用恢复。  

**明确不纳入**

- 客户删除、订单删除、发货模块。  
- 订单表结构/业绩公式改造。  
- 强制 DDL。  
- 渠道改名级联刷新订单快照。  

---

## 附录 A：其他影响历史数据的既有接口（非删除）

| 接口 | 行为 | 历史影响 |
|---|---|---|
| `Inquiry::edit` | 改 `inquiry_name` | 客户侧显示名变；订单快照不变 |
| `InquiryPort::edit` | 改名或改 `inquiry_id` 归属 | 父子关系变化；管理员互斥可能受影响 |
| `InquiryPort::importDo` | 可写入 status | 已支持非 0，但列表当启用用 |
| `Auth` 管理员保存 | 写 inquiry_id/port_id | 运营归属 |

`CODE VERIFIED`

---

## 附录 B：引用分类速查（A–H）

| 类别 | 代表位置 | 停用口径 |
|---|---|---|
| A 新建候选 | Client add、Order 下拉、Auth getChannels | 仅 status=0 |
| B 编辑候选 | Client edit、订单编辑 | 启用 + 当前值 |
| C 历史详情显示 | 列表 map、Follow、详情页 | 按 ID 取名，勿滤掉停用 |
| D 历史统计 | DataStatistics/Operator | 含停用；禁统一 status=0 |
| E 管理/筛选列表 | Inquiry 管理、客户筛选 | 管理页看全部；筛选可含停用 |
| F 提交校验 | validateInquiryAndPortData | 新旧值区分 |
| G 导入/任务 | ExcelImport | 建议仅启用 |
| H 其他 | Products 运营匹配、conflict_worker | 保留 ID 匹配 |

---

## 最终结论

**`READY FOR STEP 2 IMPLEMENTATION REVIEW`**

请用户与 ChatGPT 审核本报告后再进入第二步实施。本轮到此停止，未改代码、数据库、配置与 Git。
