# 内贸CRM历史订单编辑停用渠道回显异常专项分析报告

> **性质**：只读分析（本轮未改代码、未改库、未改 Git、未执行订单保存/审批/删除）  
> **日期**：2026-11-28  
> **现象摘要**：历史订单所用「测试询盘来源 / 测试运营端口」停用后，订单详情、客户列表、成交客户编辑回显正常；**订单列表 → 编辑订单** 询盘来源落回「请选择询盘来源」，运营端口提示「该来源暂无店铺数据」。

---

## 1. 结论（根因一句话）

订单编辑页 GET 组装 `sourceList` / `shopList` 时 **只查 `status=0`（启用）**，且 **未调用** 客户编辑已有的 `InquiryChannelStatusService::mergeDisabledCurrent` / `mergeDisabledPortsByIds` 历史兜底；前端用订单快照名去选 option / 查 `shopList`，停用后 option 与分组均不存在，于是表现为占位文案。  
受控停用实施报告已明确写出：**`Order.php` 订单删除/编辑未改**（见 `docs/豫工内贸CRM_运营端口与询盘来源受控停用最小实施报告.md` §4）。

状态约定：`crm_inquiry` / `crm_inquiry_port` 中 **`0=启用，1=停用`**（`InquiryChannelStatusService` L14–15）。

---

## 2. 现象对照

| 页面 | 表现 | 原因概要 |
|---|---|---|
| 订单详情 | 历史渠道名称正常 | 来源：缺失则 `array_unshift` 塞入；端口：模板硬编码订单 `source_port` |
| 客户列表 | 历史名称正常 | `column('inquiry_name','id')` / `port_name` **无 status 过滤** |
| 成交客户编辑 | 正确回显 +「已停用」 | 启用集 ∪ `mergeDisabled*`，option **value=ID** |
| **订单编辑** | 来源占位 / 端口「暂无店铺数据」 | 仅启用集；无 merge；来源 option **value=名称** |

---

## 3. 订单编辑渠道初始化全链路

### 3.1 Controller（GET）

**文件**：`application/admin/controller/Order.php`  
**方法**：`edit()`  
- POST 保存：约 L2143–2708  
- GET 渲染：约 L2711–3060  

| 步骤 | 行号 | 行为 |
|---|---|---|
| 询盘来源 | **2840–2846** | `crm_inquiry` + `where('status','=',0)` → `sourceList`（仅名称数组） |
| 运营端口 shopList | **2977–3002** | 遍历上述启用 inquiry，再查 `crm_inquiry_port` + `status=0`，按 **inquiry_name** 分组 |
| 历史端口解析 | **3004–3055** | 尝试从 `crm_leads.source_port`（经 contact）取名；若空再用订单 `source_port`；按 **port_name** 反查 `source_port_id`（**不按 status 过滤**） |
| 输出 | **3058–3060** | `orderInfo` + `order/edit` |

关键片段：

```2840:2846:application/admin/controller/Order.php
        $inquiryList = $inquiryQuery
            ->where('status', '=', 0)
            ->field('id, inquiry_name')
            ->order('inquiry_name', 'asc')
            ->select();
        $sourceList = array_column($inquiryList, 'inquiry_name');
        $this->assign('sourceList', $sourceList);
```

```2983:3001:application/admin/controller/Order.php
            $ports = Db::name('crm_inquiry_port')
                ->where('inquiry_id', $inquiryId)
                ->where('status', '=', 0)
                ->field('id, port_name')
                ...
            if (!empty($shops)) {
                $shopList[$inquiryName] = $shops;
            }
```

**对比同方法内已有「历史删除回显」**：产品（约 L2872–2930）、收款账户等已做删除项追加；**渠道/端口却未做同类兜底**。

**对比客户编辑（正确路径）**：

```3828:3840:application/admin/controller/Client.php
        $svc = new \app\admin\service\InquiryChannelStatusService();
        $channelList = Db::table('crm_inquiry')->where('status', 0)->select();
        ...
        $channelList = $svc->mergeDisabledCurrent($channelList, $curInquiry, 'inquiry_name');
        $portList = Db::table('crm_inquiry_port')->where('status', 0)->select();
        ...
        $portList = $svc->mergeDisabledPortsByIds($portList, $selectedPorts);
```

Merge 实现：`application/admin/service/InquiryChannelStatusService.php`  
- `mergeDisabledCurrent`：**L775–794**（停用行名称追加「（已停用）」）  
- `mergeDisabledPortsByIds`：**L803–827**

### 3.2 模板

**文件**：`application/admin/view/order/edit.html`

| 区域 | 行号 | 行为 |
|---|---|---|
| 询盘来源 select | **1076–1081** | `{volist sourceList}`；option **value=名称**；无 PHP `selected`；占位「请选择询盘来源」 |
| 运营端口 select | **1091–1093** | 初始空；依赖 JS 按来源填充 |
| shopList 注入 | **1953** | `var shopList = {$shopList\|raw}` |

订单页 **不走** `Client/getPortsByInquiry` AJAX；渠道数据全部服务端渲染。

### 3.3 JavaScript 回显

| 函数/逻辑 | 行号 | 行为 |
|---|---|---|
| `fillSourcePortBySourceName` | **1956–1974** | `shopList[sourceName] \|\| []`；空则追加「**该来源暂无店铺数据**」 |
| `setSourcePortByAnyValue` | **1977–2013** | 先 fill，再按 preferId / 名称匹配 option |
| `form.val(..., { source })` | **2522, 2549** | 用 `orderInfo.source` 设值；option 不存在则 LayUI 保持占位 |
| 端口回显调用 | **2586–2597** | `setSourcePortByAnyValue(orderInfo.source, source_port, source_port_id)` |

**故障因果链**：

1. 停用后 `sourceList` 无「测试询盘来源」→ `form.val` 无法选中 → UI「请选择询盘来源」。  
2. `shopList['测试询盘来源']` 不存在（来源未进启用集，或来源在但端口仅 status=0）→ fill 走 else →「该来源暂无店铺数据」。  
3. 即使后端算出了 `source_port_id`，前端 option 列表里没有该 id，仍无法选中。

---

## 4. 新建 vs 编辑是否共用渠道选择器

| 项 | 结论 |
|---|---|
| 是否同一资源文件 | **否**。`order/add.html` 与 `order/edit.html` **各复制一份** 同类 JS（`shopList` + `fillSourcePortBySourceName`） |
| 后端列表策略 | **相同**：`Order::add` GET **L1567–1626** 与 `edit` GET 均为 **仅 `status=0`** |
| 订单编辑是否 AJAX 拉渠道 | **否** |
| 客户编辑 AJAX | `Client::getPortsByInquiry`（L188–218）支持 `include_ids` merge；订单编辑未用 |

新建保持「只启用」是正确产品行为；回归点在 **编辑缺历史合并**，不是新建多过滤了。

---

## 5. 是否仅加载 `status=0` 导致历史丢失

**是。直接根因。**

- 编辑来源：**Order.php L2840–2841**  
- 编辑端口：**Order.php L2983–2985**  
- 新建对称：**Order.php L1572–1573、L1607–1609**  
- 无 `mergeDisabledCurrent` / `mergeDisabledPortsByIds` 调用（全文件订单编辑路径检索无此 Service）

订单详情为何「看起来正常」：

- 来源：`details()` **L3103–3108** 从 `crm_client_status` 取名，缺则 **unshift 订单 `source`**（非 inquiry 表，也不标「已停用」）。  
- 端口：`order/details.html` **L444–446** 直接输出 `$orderInfo.source_port` 为 selected option，**不依赖 shopList**。

---

## 6. 字段关系：`source` / `source_port` vs `inquiry_id` / `port_id`

| 存储 | 字段 | 形态 | 用途 |
|---|---|---|---|
| `crm_client_order` | `source` | **文字名**（询盘来源名） | 订单快照 |
| `crm_client_order` | `source_port` | **文字名**（端口名） | 订单快照 |
| `crm_leads` | `inquiry_id` | **ID** | 客户所属渠道 |
| `crm_leads` | `port_id` | **ID（可 CSV）** | 客户运营端口 |
| `crm_leads` | `source_port`（若有列） | 名称兼容字段 | 编辑回显时可能被订单页优先读到 |

关系：

- 订单 **不存** `inquiry_id`/`port_id`；与客户通过 **名称 ↔ ID 反查**、以及新建时按 contact 从 leads 预填（`Order.php` 约 L1745–1794）关联。  
- 编辑页端口下拉 **option value=端口 ID**，POST 时再反查名称写入 `source_port`（**L2294–2313**）。  
- 来源下拉 **option value=名称**，POST 直接写 `source`（**L2259**）。  
- 客户编辑下拉 **value=ID**，与订单模型不同——**不可无「改名后的 inquiry_name」当订单 option value**。

编辑页端口取值优先级（**L3004–3038**，代码与注释不完全一致）：

1. 若联系人对应 leads 的 `source_port` 非空 → 用 leads；  
2. 否则用订单表 `source_port`。  

停用回归主因仍是下拉未合并；该优先级在 leads/订单名称不一致时会加重错显，属次要风险。

**订单编辑保存是否覆盖客户 `inquiry_id`/`port_id`：**  
当前 `edit()` POST 主路径更新 `crm_client_order` + 重建 `crm_order_item`，**未见**把表单渠道写回 `crm_leads.inquiry_id`/`port_id`。清空风险主要在 **订单自身** `source`/`source_port`，而非直接改客户渠道主键。

---

## 7. 未改渠道直接保存的风险

| 场景 | 风险 |
|---|---|
| 现状 UI（来源/端口空） | `lay-verify="required"`（edit.html L1076、L1091）通常 **拦提交** → 用户难以「原样保存」 |
| 若绕过校验或空串提交 | `source` 按请求写入；`source_port` 先置 `''`，仅非空输入才反查（L2295–2313）→ **可清空订单历史渠道快照** |
| 主动改选其他启用渠道 | 正常覆盖订单 `source`/`source_port` 名称 |
| 停用渠道仍在库中 | 保存侧 **未做** 类似 `Client::validateInquiryAndPortData` 的「保留历史停用 / 拒绝改到其他停用」校验（Client L2856–2913） |
| 客户字段 | 本路径 **不直接** 覆盖 leads 的 `inquiry_id`/`port_id`；若业务上误以为订单编辑同步客户渠道，属预期外认知，非本 bug 主路径 |

**结论**：当前首要是 **回显失败导致无法安全编辑**；一旦空值提交成功，存在 **清空订单渠道快照** 的真实风险。修复回显后，建议同步补「未变更允许停用、主动改选必须启用」校验，与客户编辑对齐。

---

## 8. 最小复用客户编辑回显机制（注意订单特殊性）

### 8.1 可直接复用

- Service：`InquiryChannelStatusService::mergeDisabledCurrent` / `mergeDisabledPortsByIds`  
- 模式：**启用集 ∪ 当前历史 ID**，展示名带「（已停用）」  
- 同页已有先例：订单编辑对 **已删除产品** 的追加回显（L2872–2930）

### 8.2 不可「原样粘贴」的差异（重要）

| 维度 | 客户编辑 | 订单编辑 |
|---|---|---|
| 选中键 | ID | 来源=**名称**；端口=**ID** |
| merge 改名 | 只改展示字段，value 仍是 id → 安全 | 若把 `inquiry_name` 改成「xxx（已停用）」再放进 `sourceList` / 作为 `shopList` 键 → **`form.val(source)` 与 `shopList[orderInfo.source]` 全部对不上** |

### 8.3 建议的最小改法（仍属方案，本轮不改代码）

**范围优先**：仅 `Order::edit` GET（约 L2831–3055）+ 必要时 `edit.html` 展示文案；`add` 保持启用-only。

1. 查启用 `inquiryList` / 端口（现有逻辑保留）。  
2. 用订单 `source` 按 **名称**（必要时 org 条件）查 `crm_inquiry`（含停用），得到当前 inquiry 行。  
3. 对来源：  
   - `sourceList` / `shopList` 的 **键与 option value 必须保持原始 `inquiry_name`（无后缀）**；  
   - 展示可用「名称 +（已停用）」：模板改为 value=原名、text=带标签，或前端单独标灰。  
   - 勿把带后缀的字符串当作 `shopList` 的 key。  
4. 对端口：用 `source_port_id` 或按名+inquiry_id 解析 ID，`mergeDisabledPortsByIds` 并入该来源的 shops；`name` 可带「（已停用）」，**id 不变**。  
5. 端口解析优先订单快照，其次 leads（建议修正 L3004–3038 的「订单优先」语义，避免 leads 旧名抢占）。  
6. 可选加固 POST：对照旧订单 `source`/`source_port`，未变更允许停用；变更目标必须 `status=0`（对齐 `Client::validateInquiryAndPortData`）。

**不建议本轮改**：把订单渠道全面改成存 ID（面过大）；动客户列表/详情；改库。

---

## 9. 新建与主动改选仍只能用启用记录

| 场景 | 保证方式 |
|---|---|
| 新建订单 `Order::add` | 继续只加载 `status=0`（已满足）；不要对 add 做无条件 merge |
| 编辑页改选其他来源 | `form.on('select(kh_status)')`（edit.html L2015+）只从当前 `shopList` 填端口；只要 **未把其它停用渠道批量并入列表**，下拉里仍主要是启用集 + **至多一条当前历史停用** |
| 保存强校验（建议补） | 新选 inquiry/port 必须启用；仅当提交值等于订单原快照时允许停用 |

客户侧同策略：`Client::add` 无 merge（约 L3316–3322）；`edit` 有 merge + `validateInquiryAndPortData`。

---

## 10. 订单编辑重建产品明细的既有风险

**与渠道回显无关，但是编辑保存的既有行为，验收时需知情。**

```2676:2683:application/admin/controller/Order.php
                Db::name('crm_order_item')->where('order_id', $id)->delete();
                if (!empty($itemsData)) {
                    $resItems = Db::name('crm_order_item')->insertAll($itemsData);
```

- 明细由 `OrderService::buildOrderSaveData`（约 L2558–2578）按表单重建。  
- 任意一次成功「编辑保存」都会 **整表替换** 该订单 `crm_order_item`。  
- 渠道修复验收时：若顺带点保存，应核对明细行数/金额未漂；**不要**为测渠道去空提交明细。

---

## 11. 根因归类与责任边界

| 层级 | 判定 |
|---|---|
| 直接根因 | 订单编辑 GET 渠道/端口 **仅 status=0 且无历史 merge** |
| 触发改造 | 询盘来源/运营端口受控停用；文档写明 **Order.php 未改** |
| 前端 | 逻辑本身合理；依赖后端列表完整。文案「该来源暂无店铺数据」是空 shopList 的既有分支 |
| 数据 | 停用不物理删，订单快照名仍在；非数据丢失 |
| 次要问题 | leads `source_port` 优先于订单快照；port_name 反查未限定 inquiry_id（重名风险） |

---

## 12. 最小修改范围（供审核，本轮不实施）

| 优先级 | 文件 | 改动点 |
|---|---|---|
| P0 | `application/admin/controller/Order.php` | `edit()` GET：启用列表后 merge 当前订单历史 inquiry/port；`sourceList`/`shopList` 键保持原名 |
| P0 | `application/admin/view/order/edit.html` | 若需「已停用」展示：option text 与 value 分离（value=原名） |
| P1 | `Order.php` `edit()` POST | 渠道变更校验（保留历史停用 / 拒改到其他停用） |
| P1 | 端口回显取值 | 订单 `source_port` 优先于 leads |
| 明确不动 | `Order::add` 启用-only；客户模块已通路径；DB schema；产品明细重建策略（本需求无关） |

预估：后端数十行 + 模板小改；可复用现有 Service，无需新表。

---

## 13. 浏览器验收方案（修复后；本轮仅设计）

**前置数据（与现网回归一致）**：

1. 启用「测试询盘来源」「测试运营端口」。  
2. 建客户 + 业绩订单，渠道选上述两项并审核/落库成功。  
3. 停用该来源与端口（status=1）。  
4. **不要**改订单表已存的 `source`/`source_port` 文字。

**只读核对（可不保存）**：

| # | 步骤 | 期望 |
|---|---|---|
| A | 订单详情 | 两渠道名仍显示 |
| B | 客户列表 / 成交客户编辑 | 名显示；编辑带「已停用」 |
| C | 订单列表 → 编辑订单 | 询盘来源选中历史名（可标已停用）；运营端口选中历史端口，**不再**「请选择… / 暂无店铺数据」 |
| D | DevTools：`#source` option / `#source_port` option | 存在对应 value；`shopList` 含该来源键 |

**写操作验收（需单独授权轮次）**：

| # | 步骤 | 期望 |
|---|---|---|
| E | 不改渠道，只改无关字段保存 | `source`/`source_port` 不变；客户 `inquiry_id`/`port_id` 不变；明细行合理 |
| F | 新建订单 | 下拉里 **看不见** 已停用测试渠道 |
| G | 编辑订单主动改到其他启用渠道/端口 | 可保存；再改到另一停用渠道应被拒（若已加 P1 校验） |
| H | 保存前后对比 `crm_order_item` 行数与金额 | 无意外丢失（知情重建风险） |

**本轮禁止**：订单保存、审批、删除及任何写库操作。

---

## 14. 关键文件索引

| 路径 | 关键符号 / 行号 |
|---|---|
| `application/admin/controller/Order.php` | `edit` GET 2840–3055；POST source 2259、source_port 2294–2313；明细重建 2676–2683；`add` 1567–1626；`details` 3103–3108 |
| `application/admin/view/order/edit.html` | 来源/端口 1070–1094；JS 1953–2013；回显 2522、2586–2597 |
| `application/admin/view/order/add.html` | 同类 shopList JS ~2089+ |
| `application/admin/view/order/details.html` | 端口硬编码 444–446 |
| `application/admin/controller/Client.php` | `getPortsByInquiry` 188–218；`validateInquiryAndPortData` 2856–2913；`edit` merge 3828–3842；列表 map 369–390 |
| `application/admin/view/client/edit.html` | 渠道/端口 option 276–290 |
| `application/admin/service/InquiryChannelStatusService.php` | 常量 14–15；merge 775–827 |
| `docs/豫工内贸CRM_运营端口与询盘来源受控停用最小实施报告.md` | §4「Order.php 未改」L77–79 |

---

## 15. 审核结论（待确认）

1. **根因成立**：订单编辑未接入停用渠道历史回显，与受控停用改造遗漏范围一致。  
2. **修复方向**：在 `Order::edit` GET 最小复用 `InquiryChannelStatusService`，并处理「名称作 value」与 merge 改名冲突。  
3. **本轮已停止**：未修改任何代码/数据库/Git；等待审核是否进入最小修复实施轮。

---

*报告结束。*
