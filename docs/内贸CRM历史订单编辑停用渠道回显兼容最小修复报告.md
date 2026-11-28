# 内贸CRM历史订单编辑停用渠道回显兼容最小修复报告

> **性质**：最小代码修复实施报告（已改业务代码；未 Git commit/push；未部署正式服务器）  
> **日期**：2026-11-28  
> **依据**：`docs/内贸CRM历史订单编辑停用渠道回显异常专项分析报告.md`  
> **最终结论**：**`READY FOR LOCAL BROWSER ACCEPTANCE`**

---

## 1. 实际修改文件和函数

| 文件 | 改动点 |
|---|---|
| `application/admin/controller/Order.php` | 新增 `stripDisabledChannelLabel()`、`validateOrderEditChannelSnapshot()`；`edit()` GET 渠道/`shopList`/端口回显；`edit()` POST 渠道保护 |
| `application/admin/view/order/edit.html` | 来源 option 的 value/text 分离；`kh_status` 联动改用 `data.value`；端口按名回填兼容「（已停用）」后缀 |

**明确未改**：`Order::add()`、`order/add.html`、库表结构、`crm_order_item` 保存/重建、审核/删除/利润/回款、客户删除、发货 V2、启停用规则、`InquiryChannelStatusService` 本体（仅调用已有 `mergeDisabledPortsByIds`）。

---

## 2. GET 回显修复（P0）

1. 保留启用询盘来源/端口候选（`status=0`）。
2. 读取订单自身快照 `source` / `source_port`（**优先于**客户 leads，避免客户后改渠道覆盖历史订单展示）。
3. 按名称反查历史 `crm_inquiry`（含停用）；同名时用「端口名 + 父来源」消歧；无法唯一确定时**不任意取第一条**。
4. 仅当订单来源不在启用集时，追加**当前订单这一条**历史来源；展示 text=`原名（已停用）`，value=原名。
5. 仅把当前历史端口 ID 经 `mergeDisabledPortsByIds` 并入对应来源分组；不下发全部停用端口。
6. `shopList` 键始终为未加后缀的来源名称。
7. 端口 option value 仍为实际端口 ID；展示名可带「（已停用）」。

---

## 3. 是否完成 POST 校验（P1）

**已完成**（最小保护，未改明细保存链路）。

| 场景 | 行为 |
|---|---|
| 来源+端口均等于原快照 | 允许保留停用历史；强制回写原快照名 |
| 主动改选其他启用渠道/端口 | 允许（须父子匹配且目标 `status=0`） |
| 主动改选其他已停用渠道/端口 | 拒绝 |
| 渠道字段空提交但原有快照 | 回填原值，防止清空 |
| 提交值误带「（已停用）」 | 入库前剥离，不当作改选 |

区分依据：订单存的是名称快照；提交来源名与原 `source` 比较，端口经 ID 反查名称后与原 `source_port` 比较。二者都一致视为「未改动」。

---

## 4. 名称和值分离方式

| 控件 | value | 展示 text |
|---|---|---|
| 询盘来源 | 原始 `inquiry_name` | 启用=原名；历史停用=原名+`（已停用）` |
| 运营端口 | 端口 ID | `port_name` 或 merge 后的「原名（已停用）」 |
| `shopList` 键 | 原始来源名 | — |

前端联动改为使用 `data.value`，避免用带后缀的 text 去查 `shopList`/`yyList`。

---

## 5. 历史渠道匹配规则

1. `crm_inquiry.inquiry_name = 订单.source`（可带组织条件）。
2. 唯一命中 → 采用。
3. 多条同名 → 用订单 `source_port` 在候选父 ID 下查端口；父 ID 唯一则采用。
4. 仍不唯一 → 若恰好一条停用候选可采用；否则标记歧义，**不取第一条**。
5. 端口 ID：在已解析父来源下按 `port_name` 查；命中数≠1 则不写 `source_port_id`，必要时用名称兜底 option（POST 按名保留）。

---

## 6. 订单与客户字段影响

| 字段 | 影响 |
|---|---|
| 订单 `source` / `source_port` | 回显与保存保护；未改则保留快照；禁止写入「（已停用）」 |
| 客户 `inquiry_id` / `port_id` | **本轮不读写** |
| leads `source_port` | 仅当订单 `source_port` 为空时兜底；不再优先于订单快照 |

---

## 7. 是否修改任何产品明细保存逻辑

**否。** `crm_order_item` 的 delete + insertAll 重建逻辑未改动。

---

## 8. 静态和 Mock 测试结果

| # | 用例 | 结果 |
|---|---|---|
| 1 | 启用渠道正常回显 | PASS |
| 2 | 停用来源正常回显 | PASS |
| 3 | 停用端口正常回显 | PASS |
| 4 | 来源+端口同时停用 | PASS |
| 5 | 仅当前历史停用被追加 | PASS |
| 6 | 原始名称仍是 option value | PASS |
| 7 | 带后缀不会写入快照算法 | PASS |
| 8 | `shopList` 以原始名称为键 | PASS |
| 9 | 同名端口按父来源消歧 | PASS |
| 10 | 新建仅启用候选 | PASS |
| 11 | 保留原值 / 拒绝非法改选 / 空提交回填 | PASS |
| 12 | 正常启用订单不夹带无关停用 | PASS |

- 独立 Mock：`runtime/mock_order_disabled_channel_echo_test.php` → **21 PASS / 0 FAIL**
- Reflection 真实库：`validateOrderEditChannelSnapshot` → **6 PASS / 0 FAIL**

---

## 9. 本地数据库验证情况

库：`crmneimao`（只读核对 + Reflection 校验；**未对订单执行保存写测**）。

| 项 | 结果 |
|---|---|
| 询盘 `测试询盘来源` | id=24，`status=1` |
| 端口 `测试运营端口` | id=87，挂 inquiry_id=24，`status=1` |
| 测试订单 | id=**248**，`source/source_port` 仍为上述快照；`crm_shipping_apply` 关联 **0** |
| 模拟 GET 组装 | source option=`测试询盘来源` / text=`…（已停用）`；`shopList['测试询盘来源']=[{id:87,name:…（已停用）}]`；`source_port_id=87` |
| 新建候选 | 不含停用「测试询盘来源」 |

订单 248 明细快照（供后续写入验收对照）：1 行，`crm_order_item.id=814`，`product_id=185`，`qty=1`，`unit_price=10000000.00`；主表 `check_status=2`，`money=10000000.00`，`profit=9899970.00`。

---

## 10. 浏览器未完成的验收项目

| 项目 | 状态 | 说明 |
|---|---|---|
| 第一轮只读：编辑页来源/端口 UI 回显 | **未完成** | 站点 `guoneizuixinone.hnyugong.com` Nginx→PHP 出现 502；Apache Host 可到登录页，自动化无后台会话 |
| 订单详情仍显示原始名称 | **未在浏览器核对** | 详情逻辑本轮未改，预期仍正常 |
| 第二轮写入：未改渠道保存 / 改选启用 / 伪造停用 | **未执行** | 按约束需人工登录后对隔离单操作；本轮未点保存 |

建议人工打开：`http://guoneizuixinone.hnyugong.com/admin/order/edit/id/248.html`（需先保证 PHP/站点网关正常并登录）。

---

## 11. 仍然存在的风险

1. **同名渠道/端口**：名称快照无法 100% 唯一反查 ID 时，回显可能缺 `source_port_id`，依赖名称兜底；不会擅自绑错父来源。
2. **订单编辑仍整表重建明细**（既有行为）：任意成功保存都会 delete+insert `crm_order_item`；验收写测须对照明细 ID/金额。
3. **yyList 运营人员联动**：停用来源可能无运营人员映射（既有数据结构），与本次回显无直接冲突。
4. **本地 Nginx 502**：可能影响浏览器验收环境，与本轮代码无关。

---

## 12. Git 差异与 PHP 语法检查

| 项 | 结果 |
|---|---|
| 改动文件 | `Order.php`、`order/edit.html` |
| `git diff --stat` | `Order.php` +401/−约43 量级；`edit.html` +9/−约1 |
| `git diff --check` | 无空白错误 |
| `php -l Order.php` | No syntax errors |
| Git commit / push / pull / 部署 | **未执行** |

---

## 13. 是否具备人工浏览器验收条件

**具备代码与数据条件，待环境与登录：**

- 代码已落地 P0+P1。
- 本地存在停用渠道测试数据与订单 **#248**（无发货申请，适合后续隔离写测）。
- 需人工：恢复站点 PHP 网关 → 登录后台 → 只读验收编辑页 →（可选）隔离写测并对照明细快照。

---

## 最终结论

**`READY FOR LOCAL BROWSER ACCEPTANCE`**

本轮已停止：未自动提交 Git，未部署正式服务器，未对正式订单执行保存写操作。

---

*报告结束。*
