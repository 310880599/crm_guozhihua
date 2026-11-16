# 内贸 CRM 公共 ESC 修复 P1 补强复核报告

> 承接：《内贸 CRM 公共 ESC 弹层关闭逻辑最小修复实施报告》  
> 约束：仅改 `application/admin/view/common/foot.html`；未改业务页 / PHP / MySQL / Layui 源码；未 Git 提交；未部署。

---

## 结论先说

本轮对 P1-1 / P1-2 / P1-3 做了**证据复核**，并在 `foot.html` 内完成最小补强：

| 问题 | 风险是否真实 | 处理 |
|---|---|---|
| P1-1 自定义预览误判 | 当前业务页**不成立**「默认隐藏节点误拦」；谓词偏宽 | 收紧为「节点位于**可见** `.layui-layer` 内」才视为预览活动 |
| P1-2 页面自有 ESC 双关 | 现有注册顺序下双关两层**概率低**；`stopImmediatePropagation` 不能挡已执行监听，机制缺口真实 | 改为 `capture: true`，保证公共逻辑先于 bubble 业务监听 |
| P1-3 loading 优先级 | **真实**：拒绝/批量通过等流程会在业务弹窗仍打开时再 `layer.load` | loading 可见时整段 ESC 直接停止，不再关下方业务层 |

保留：iframe 焦点修复、有效弹层索引校验、禁止 `closeAll()`。

---

## 1. 实际修改范围

| 文件 | 是否修改 |
|---|---|
| `application/admin/view/common/foot.html` | **是**（本轮唯一改动） |
| `order/details.html` / `pendingindex.html` / 其他业务页 | 否 |
| PHP / MySQL / Layui 源码 | 否 |

---

## 2. P1-1：自定义预览识别可能误判

### 证据

1. `order/details.html`、`pendingindex.html` 中 `#previewWrap` / `#previewZoomImg` **仅出现在** `layer.open({ type:1, content: html })` 动态拼出的字符串里，**静态模板中不存在**这些节点。
2. 预览关闭走 Layui `end` 回调，并清理 `__DETAIL_IMG_PREVIEW_IDX__`；层销毁后节点一并移除，不是「默认存在、CSS 隐藏」。
3. 预览打开时本身就是可关的 type:1 `.layui-layer`，公共逻辑会先走 `getTopEscClosableLayerIndex()` 关掉该层；`hasLocalCustomPreview()` 主要覆盖「节点残留 / 选择器未命中」的兜底路径。

### 判断

「DOM 默认存在但隐藏 → 误拦父 iframe ESC」在**当前相关业务页不成立**。

### 处理（最小收紧）

仍将谓词从「节点存在」改为「节点位于**可见** `.layui-layer` 内」：

- 无节点 / 不在 layer 内 / layer `display:none|visibility:hidden` → 不拦截父壳关闭  
- 真实预览打开时行为不变  

### 残余风险

若某页将来把同名节点写进静态 HTML 且不在 layer 内，公共逻辑不会再因此误拦（符合预期）。极短关闭动画窗口仍需浏览器观察。

---

## 3. P1-2：页面自有 ESC 可能重复关闭

### 证据（注册顺序与传播）

| 页面/监听 | 注册时机 | 阶段 | ESC 行为 |
|---|---|---|---|
| 公共 `foot.html` | `{include file="common/foot"/}` → `layui.use(['layer'])` | 原 bubble，现 **capture** | 关顶层或关父 iframe 一层 |
| `order/details.html` → `keydown.detailsEsc` | foot **之后**的 `layui.use` 内绑定 | bubble | 有预览先关预览并 `stopImmediatePropagation`；否则 `parent.layer.close` |
| `pendingindex.html` → `keydown.galleryPreview` | 仅在预览 `success` 内绑定（更晚） | bubble | ESC 时 `layer.close(layerIndex)`，**不** `stopImmediatePropagation` |
| `client/details.html` | 无自有 ESC；仅按钮 `parent.layer.close` | — | 依赖公共逻辑 |

事件事实：

- `stopImmediatePropagation()` **不能**撤销同目标上**已经执行过**的监听。
- `galleryPreview` 若不 stop，且若它先于公共逻辑执行并同步卸掉预览 DOM，后续公共逻辑可能误判「无本地层」进而 `tryCloseParentFrameLayer()` → **一次 ESC 关两层**（机制上成立）。
- 但在**当前页面**上：公共监听随 foot 更早注册；`detailsEsc` / `galleryPreview` 更晚且原为 bubble → 公共先执行时，成功关闭后 stop 即可挡住后续业务监听。双关两层在现状下**概率低**。

`client/details.html` 打开订单详情为本地 `layer.open({type:2})`，无额外 ESC 监听；嵌套关层仍由公共「一次一层」覆盖。

### 处理（最小可验证）

```text
document.addEventListener('keydown', handler, true); // capture
```

目的：即使业务页在 bubble 上抢先注册，公共逻辑仍先进入；成功关闭（或 loading 阻塞）后再 stop，业务监听收不到同一次 ESC。

预览兜底分支（`hasLocalCustomPreview`）**故意不 stop**，以便 `detailsEsc` / `galleryPreview` 仍可接手。

### 残余风险

1. 其他脚本若也在 **capture** 且更早注册，仍可能先跑（本仓库客户详情相关链未见）。  
2. 未改业务页；`galleryPreview` 自身仍不 stop（依赖公共 capture）。  
3. 需浏览器验证：订单详情预览 ESC、待审核画廊 ESC、无预览时 ESC 关详情，均只退一层。

---

## 4. P1-3：loading / tips 优先级

### 证据

`pendingindex.html` 拒绝提交、批量通过等典型写法：

1. 业务确认/拒绝弹窗仍打开；  
2. `yes` 回调内再 `layer.load(1, { shade: [...] })`；  
3. AJAX 结束才 `layer.close(loading)`，成功后再关业务层。

旧逻辑：`getTopEscClosableLayerIndex()` **跳过** loading 后选中下方业务弹窗 → ESC 可能在 loading 仍显示时关掉拒绝框/确认框。

tips：多为行内悬停提示，一般不应阻止关掉真正的业务弹窗；但也不应被当成可关层，且不宜单独因 tips 穿透关父壳。

### 处理

| 层类型 | ESC 行为 |
|---|---|
| 可见 **loading** | 整段停止（`stopEscEvent`）：不关下方业务层、不关父壳 |
| 可见 **tips** | 不关 tips 自身；**允许**关普通业务层；无业务层时不穿透关父壳 |
| 普通业务层 | 按 z-index 只关最上一层 |

### 残余风险

1. loading 期间用户按 ESC「无响应」是刻意行为（防误关提交中弹窗）。  
2. tips 与父壳同时存在时仍不关父壳（与首轮意图一致）；需确认悬停 tips 残留是否偶发挡住「仅关父壳」——窗口通常很短。  
3. 需浏览器在拒绝提交 loading 闪现时连按 ESC，确认业务弹窗与父壳均未被误关。

---

## 5. 补强后核心流程（伪代码）

```
ESC (capture):
  if 可见 loading:
      stop; return
  idx = 最上层可关业务层(跳过 tips/loading，校验 #layui-layer{times})
  if idx:
      layer.close(idx); stop; return
  if 可见 tips 或 活动自定义预览:
      return          // 不关父壳；预览不 stop，交给页面监听
  if parent.layer.getFrameIndex(window.name) 有效且父 DOM 存在:
      parent.layer.close(idx); stop
```

---

## 6. 静态检查

| 检查项 | 结果 |
|---|---|
| 仅修改 `foot.html` | PASS |
| 文中无 `layer.closeAll(` | PASS |
| 保留 `__ADMIN_ESC_LAYER_BOUND__` | PASS |
| 保留 `getFrameIndex` + 父 DOM 校验 | PASS |
| loading 优先于关业务层 | PASS |
| `addEventListener(..., true)` capture | PASS |
| `hasLocalCustomPreview` 需可见 `.layui-layer` | PASS |
| 抽出脚本 `node --check` | PASS |

---

## 7. 仍需浏览器验证（本轮未做）

1. **P0 回归**：客户详情 iframe 内获焦后 ESC 仍能关壳。  
2. **P1-1**：订单详情未开预览时 ESC 可关；打开预览时第一次只关预览。  
3. **P1-2**：待审核画廊预览 ESC 只关预览，不关外层客户详情；订单详情 `detailsEsc` 与公共逻辑不叠关两层。  
4. **P1-3**：拒绝理由提交后 loading 出现期间连按 ESC，拒绝弹窗与列表层不被误关。  
5. 客户详情内再开订单详情：ESC 先订单、再客户。  
6. 强刷缓存后再测（公共 foot）。

---

## 8. 未扩大范围说明

未修改业务页 ESC、未恢复 `closeAll()`、未改 PHP/MySQL/Layui。若后续仍要在业务页去重监听，需另开任务并单独评估。
