# 豫工内贸 CRM 运营端口与询盘来源 P1 边界安全最小修复报告

> **文档性质**：第二步主体完成后的 P1 边界安全复核与最小修复报告  
> **修复日期**：2026-11-27  
> **依据**：`docs/豫工内贸CRM_运营端口与询盘来源受控停用最小实施报告.md`  
> **本轮约束**：仅修明确问题；**未** ALTER；**未** Git add/commit/push/pull；**未**部署正式服务器  

---

## 最终结论

**`READY FOR LOCAL BROWSER ACCEPTANCE`**

P1-1～P1-3 已在限定文件内完成最小修复并通过静态 / Mock / 本地 TMP 写测；P1-4 运营绑定极端并发窗口无法在不重构 Auth 的前提下完全关闭，已缩小一半窗口并在下文明确残留风险。浏览器人工验收仍未做。

---

## 1. 实际修改文件和函数

| 文件 | 函数 / 区域 | 变更 |
|---|---|---|
| `application/admin/service/InquiryChannelStatusService.php` | `parseStrictPositiveInt`（新增） | 严格正整数解析 |
| 同上 | `normalizeIds` | 去掉 `(int)` 误转；仅严格正整数 + 去重 |
| 同上 | `parseImportStatus`（新增） | 空→0；仅允许明确 `0`/`1` |
| 同上 | `disableInquiry` | 文档标明统一锁序（先父后子） |
| 同上 | `disablePort` | 条件更新后二次核对运营绑定；失败则 rollback（审计未写） |
| 同上 | `enablePort` | **锁序改为先父后子**，消除与父停用的可预防死锁 |
| 同上 | `assertParentInquiryEnabledForWrite` | 明确锁序约定注释 |
| `application/admin/controller/Inquiry.php` | `del` / `enable` | `(int)` → `parseStrictPositiveInt`；非法返回「参数错误」 |
| `application/admin/controller/InquiryPort.php` | `del` / `enable` | 同上 |
| 同上 | `add` / `edit` | `inquiry_id` 严格正整数 |
| 同上 | `importDo` | 状态严格校验；逐行事务；写前 `assertParentInquiryEnabledForWrite`；严格 ID；错误计数 |

**未修改**：`Auth.php`、订单/发货/财务、客户硬删除、数据库结构、无关前端页。

---

## 2. 各问题根因及修复

### P1-1：ID 严格校验

**根因**：`normalizeIds` 与单条入口使用 `(int)`，`"12abc"`→`12`、浮点截断等会误操作合法 ID。

**修复**：

- `parseStrictPositiveInt`：仅 `int>0` 或匹配 `/^[1-9]\d*$/` 的字符串。
- 拒绝：`"12abc"`、`"012"`、`0`、负数、浮点、`"12.0"`、数组、对象、bool、null。
- `normalizeIds`：去重；非法项丢弃且**绝不**映射为其他合法 ID；批量语义不变。
- 单条 `del`/`enable`：非法 → `code=500, msg=参数错误`（保持原成功响应约定，不改前端协议）。

### P1-2：运营端口导入状态校验

**根因**：`$status = ($status_raw === '' ? 0 : (int)$status_raw)` 可将 `2`、`-1`、`"1abc"` 写成非预期状态。

**修复**：

- `parseImportStatus`：空/null→`0`；仅 `"0"`/`"1"`/int `0`/`1`；其余 `null`。
- 非法状态：跳过并计入 `skipped_invalid_status`。
- 父来源：C 列改严格正整数；写端口前事务内 `assertParentInquiryEnabledForWrite`（行锁）。
- 自动创建父来源：仅当同组织同名不存在时创建 `status=0`；若已存在且停用则跳过（`skipped_parent_disabled`），**不能**绕过停用约束另建同名启用父来源。
- Excel 模板列结构未改。

### P1-3：父子并发一致性

**根因**：`enablePort` 先锁子再锁父，`disableInquiry` 先锁父再锁子 → 可预防死锁；`importDo` 无事务/父锁，存在校验后父被停用仍写入的窗口。

**修复**：

- 统一锁序：**先 `crm_inquiry`，后 `crm_inquiry_port`**。
- `enablePort`：无无锁预读 `inquiry_id` → 锁父并校验启用 → 锁子并复核归属 → 条件更新 → 审计。
- `add`/`edit`（改挂）/`importDo`：事务内先 `assertParentInquiryEnabledForWrite`。
- **未**引入死锁重试（锁序统一后不必）；审计仍在成功提交路径上只写一次。

### P1-4：运营账号并发绑定（复核）

**已做最小收紧（Service 内）**：`disablePort` 在 `status` 条件更新后、写审计前再次 `findActiveAdminsBoundToPort`；若出现绑定则 **rollback**（不落库、不写审计）。

**未改 Auth**：按边界禁止大范围重构公共绑定框架。

---

## 3. 未修复问题及原因

| 项 | 原因 |
|---|---|
| 运营绑定 ↔ 端口停用的**另一半**竞态 | Auth `adminAdd`/`adminEdit` 校验启用后、落库前无端口行锁；关闭需改 Auth 事务/锁，超出本轮最小范围 |
| 浏览器验收 | 任务明确本轮不做人工浏览器验收 |
| 停用原因字段 / 专用审计表 | 零 ALTER；沿用 `crm_operation_log` |
| 菜单级独立 enable 权限规则 | 与原删除口径一致，本轮不扩权 |

---

## 4. 测试结果与未测试项

| 类别 | 结果 | 说明 |
|---|---|---|
| `STATIC PASS` | 是 | `php -l`：Service / Inquiry / InquiryPort 均 No syntax errors；`git diff --check` 无空白错误 |
| `MOCK PASS` | 是 | 严格 ID 14 项 + 导入状态 12 项全部通过 |
| `DB LOCAL WRITE (TMP)` | 是 | 仅脚本创建的 `P1TMP_*` 行：子拦父停、父子启停、`assertParent`、审计后清理；**passed=37 failed=0** |
| 导入 Excel 端到端 | **NOT TESTED** | 逻辑静态 + 状态解析 Mock；未上传真实文件 |
| 死锁压力并发 | **NOT TESTED** | 锁序静态确认；未做双线程压测 |
| 权限/审计回归 | `STATIC` + `DB TMP` | 审计插入路径 TMP 验证；完整权限矩阵浏览器未测 |
| `LOCAL BROWSER` | **NOT TESTED** | 待人工验收 |
| 正式业务数据破坏性测试 | **未执行** | 禁止 |

测试脚本：`runtime/p1_boundary_test.php`（`/runtime/` 已 gitignore；仅本地验证用）。

---

## 5. 是否影响原有停用、启用和历史统计

| 能力 | 影响 |
|---|---|
| 停用 / 启用业务语义 | **无改变**（仍 `0/1`、条件更新、幂等文案） |
| 历史客户引用 / 订单快照 | **未改** |
| 统计分桶 | **未改本轮** |
| 前端成功响应约定 | **保持**；单条非法 ID 仍为「参数错误」 |
| 导入返回 `data` | **向后兼容扩展**字段：`skipped_invalid_status`、`skipped_parent_disabled`、`failed`（旧字段保留） |

---

## 6. 是否仍存在并发风险

**是（有限残留），但父子死锁与导入绕过父停用已收紧。**

| 场景 | 状态 |
|---|---|
| 父停用 ↔ 子启用死锁 | **已修**（统一先父后子） |
| 导入/新增/改挂绕过父停用 | **已修**（事务 + 父行锁） |
| 端口停用过程中运营账号**先提交绑定** | **已收紧**：更新后二次检查可 rollback |
| 运营账号**先校验通过**、端口随后停用、账号再落库绑到已停用端口 | **仍残留**：触发需两端近乎同时提交；影响为「停用端口仍被启用账号绑定」，停用侧安全拒绝策略在下次停用时仍会拦截；新绑定侧日常校验已拒绝停用端口，窗口极短 |

---

## 7. 本地浏览器验收建议

在原实施报告第 12 节清单基础上，**追加**：

1. 单条停用/启用：对 `id=12abc`、负数、空 ID 应提示「参数错误」，且不得改到 ID=12。  
2. 批量：混入非法 ID 时，仅处理合法项，不误伤。  
3. 运营端口导入：状态列填 `2`/`-1`/`1abc` 应计入失败/跳过且库中不出现非法 status；挂靠已停用父来源应跳过。  
4. 父来源停用 vs 子端口启用：父停用成功后子启用应失败；先停全部子再停父、再启父再启子应成功。  
5. 运营账号绑定启用端口后，停用该端口应失败；解绑后再停用应成功。  
6. 回归：列表筛选、分页、编辑弹窗、客户历史停用回显（上轮能力）。

---

## 8. Git / 验证命令摘要

| 项 | 结果 |
|---|---|
| 修改范围 | 仅上述 3 个 PHP 文件（相对本轮 P1） |
| `php -l` | 全部通过 |
| Git 写操作 | **未执行** |
| 正式部署 | **未执行** |

---

## 最终结论

**`READY FOR LOCAL BROWSER ACCEPTANCE`**

请按第 7 节做本地浏览器验收后再决定是否进入发布流程。本轮到此停止：**未** commit / push / 部署。
