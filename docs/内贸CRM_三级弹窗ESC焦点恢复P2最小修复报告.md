# 内贸 CRM 三级弹窗 ESC 焦点恢复 P2 最小修复报告

> 对应 P1：《内贸 CRM 公共 ESC 弹层关闭逻辑最小修复实施报告》  
> 约束：优先只改 `application/admin/view/common/foot.html`；未改业务页 / PHP / 数据库 / Layui 源码；未 Git 提交；未部署。

---

## 结论先说

**根因判定为 A（第三次 ESC 未进入客户详情 iframe 的公共监听器），不是 B（进入后被 loading/tips/预览拦截）。**

二次 ESC 在订单详情 document 内通过 `tryCloseParentFrameLayer()` → `parent.layer.close()` 关掉订单详情后，Layui 3.1.1 销毁 iframe 时不做焦点恢复；键盘焦点易从「客户详情」逃逸（常见落到后台壳 `main-iframe` 外层），导致第三次 ESC 落不到客户详情的 capture 监听。鼠标再点客户详情内部后，焦点回到该 iframe，ESC 立刻正常——与人工复现完全一致。

已在公共 `foot.html` 增加**条件式**焦点恢复：仅在关闭 iframe 层后、且目标 document 尚未持有可用键盘焦点时，把焦点还给仍存活的父/本页 window。一次 ESC 仍最多关一层；P1 的 loading 保护、有效索引校验、capture 监听均保留。

---

## 1. A / B 区分与代码证据

### 1.1 人工现象对应关系

| 现象 | 对 A 的含义 | 对 B 的含义 |
|---|---|---|
| 第三次 ESC 无反应 | 事件未到达客户详情监听 | 或到达后被拦截 |
| 点击客户详情内部后再 ESC 立刻可关 | 点击把焦点拉回客户详情 document | 若是 loading/tips 拦截，普通点击通常不会清掉这些层 |
| 第一、二次 ESC 正常 | 订单详情 document 当时持有焦点 | — |

### 1.2 第二次 ESC 实际执行的监听器

窗口链：

1. 后台壳 `application/admin/view/index/index.html`（含 `#main-iframe`，也引入 `foot.html`）
2. 待审核列表 `pendingindex`（在 `main-iframe` 内，打开客户详情）
3. 客户详情 iframe（打开订单详情）
4. 订单详情 iframe（打开询盘图片 type:1 预览）

第二次 ESC 时焦点仍在**订单详情** document：

1. `foot.html` capture 监听先执行  
2. 本地无 tip/loading/可关业务层（预览已关）  
3. `tryCloseParentFrameLayer()` → `parent.layer.getFrameIndex(window.name)` → `parent.layer.close(idx)`  
4. `stopImmediatePropagation` → 订单详情页自有 `keydown.detailsEsc` 不会叠关

即：第二次 ESC 的关闭动作发生在**订单详情 document 的公共监听器**里，关的是**客户详情 document 上的订单详情层**。

### 1.3 订单详情销毁后焦点落点（静态推断）

Layui 3.1.1 `layer.close`（`public/static/plugins/layui/lay/modules/layer.js`）对 `type===iframe`：

- 立即移除 shade  
- 默认 `isOutAnim`：约 **200ms** 后 `contentWindow.document.write("")`、移除 iframe、删掉层 DOM  
- **全程无任何 focus 恢复**

后台壳存在 `#main-iframe`（`index/index.html`）。嵌套 iframe 被销毁后，键盘焦点常落到：

- 壳页面 document，或  
- `#main-iframe` 元素本身，或  
- 其它非「客户详情 contentWindow」上下文  

此时：

- 客户详情的 ESC 监听**收不到** keydown → 表现为 A  
- 壳页 ESC 监听即使收到，其 document 上也**没有**客户详情那一层（层挂在 pendingindex），`tryCloseParentFrameLayer` 也关不到客户详情  

点击客户详情内部 → `document.hasFocus()` 回到客户详情 → 第三次逻辑本就可关父壳，故「一点就好」。

### 1.4 为何不是 B

若第三次已进入客户详情监听却被拦截，需满足 `hasLocalLoadingLayer` / `hasLocalTipsLayer` / `hasLocalCustomPreview` 之一。客户详情在订单关闭后通常无这些阻塞；且「点击内容区」并无清除 loading/tips 的必然逻辑，却能恢复 ESC——与 B 不符，与 A 相符。

---

## 2. 实际修改

| 文件 | 变更 |
|---|---|
| `application/admin/view/common/foot.html` | 新增条件式焦点恢复；在「关掉父 iframe 壳」与「本页关掉子 iframe 层」两处调度恢复 |

**未修改**：`client/details.html`、`order/details.html`、`pendingindex.html`、Layui 源码、PHP、数据库。  
（客户详情页 `end` 回调方案未采用：公共层已覆盖 ESC 关子 iframe 路径，无需第二文件。）

### 2.1 新增能力

1. `restoreKeyboardFocus(targetWin)`  
   - `document.hasFocus()` 且 `activeElement` 不是 iframe → **直接跳过**（不抢输入框）  
   - 仅当目标文档未持有焦点时，才条件性 `targetWin.focus()`，再 `body.focus({preventScroll:true})`（body 临时 `tabindex=-1`）  
2. `scheduleKeyboardFocusRestore(targetWin)`  
   - 在**仍存活的** targetWin 上 `setTimeout(0)` + `setTimeout(220)`，覆盖 Layui `isOutAnim≈200ms`  
3. `tryCloseParentFrameLayer` 成功 `parent.layer.close` 后 → `scheduleKeyboardFocusRestore(parentWin)`  
4. 本页 `layer.close(topIdx)` 且该层 `type=iframe` → `scheduleKeyboardFocusRestore(window)`

### 2.2 明确不做的事

- 无条件全局 `window.focus()` / `parent.focus()`  
- `layer.closeAll()`  
- 向上递归关多层  
- 削弱 loading 保护 / 索引校验 / capture  

---

## 3. 修改后预期行为

| 步骤 | 预期 |
|---|---|
| 客户详情获焦 → ESC | 关客户详情 |
| 客户详情 → 订单详情 → ESC | 只关订单详情，并把焦点条件式还给客户详情 |
| 再 ESC（无须鼠标点击） | 关客户详情 |
| 客户详情 → 订单详情 → 图片预览 → 连续三 ESC | 预览 → 订单详情 → 客户详情，各一层 |
| loading 可见 | ESC 不误关业务层 / 父壳 |
| 右上角 × | 不受影响 |

---

## 4. 静态检查

| 检查项 | 结果 |
|---|---|
| 无 `closeAll()` | PASS |
| `__ADMIN_ESC_LAYER_BOUND__` 仍在 | PASS |
| loading 保护 / tips / 自定义预览让路仍在 | PASS |
| capture + 成功后 stopImmediatePropagation 仍在 | PASS |
| 焦点恢复带 `hasFocus` / iframe 判断，非无条件 focus | PASS |
| 抽出脚本 `node --check` | PASS（SYNTAX_OK） |

---

## 5. 请你本地验收（强刷缓存后）

### P2 主路径（必须）

1. 业绩订单 → 待审核订单 → 客户名称 → 历史成交「详情」→ 询盘来源图  
2. ESC ×3：**预览 → 订单详情 → 客户详情**，第三次**不要先点击**  
3. 仅开到订单详情：ESC 关订单后，**不点击**再 ESC，应直接关客户详情  
4. 仅客户详情：ESC 可关  

### 回归

5. loading 期间 ESC 不误关  
6. 一次 ESC 不关多层  
7. × 关闭仍正常  
8. 待审核页自有图片预览 ESC 仍只关预览  

### 可选 DevTools 观察（第三次 ESC 前）

在客户详情 iframe 控制台：

```javascript
document.hasFocus();
document.activeElement && document.activeElement.tagName;
```

修复后第二次 ESC 后短暂应看到客户详情 `hasFocus()===true`（或 body 可接收键）。

---

## 6. 若验收仍失败时的临时调试方案（先勿再改焦点逻辑）

在 `foot.html` 监听器入口临时打日志（验完删除）：

```javascript
console.log('[ESC]', location.pathname, {
  hasFocus: document.hasFocus(),
  active: document.activeElement && document.activeElement.tagName,
  topIdx: /* getTopEscClosableLayerIndex() */,
  loading: /* hasLocalLoadingLayer() */,
  tips: /* hasLocalTipsLayer() */,
  preview: /* hasLocalCustomPreview() */
});
```

| 第三次 ESC 日志 | 含义 |
|---|---|
| 完全无客户详情 pathname 日志 | 仍为 A（焦点未回） |
| 有日志且 loading/tips/preview 为 true | 转为 B，再针对性处理 |
| 有日志且走了 `tryCloseParentFrameLayer` 仍不关 | 查 `getFrameIndex` / 父 DOM |

---

## 附录：关闭链路伪代码（P2）

```
ESC (capture, 每 document 一份):
  if loading: stop; return
  if 本地最上层可关:
      layer.close(top)
      if 该层 type=iframe: scheduleFocusRestore(本 window)  // 0ms + 220ms
      stop; return
  if tips 或自定义预览: return
  if tryCloseParentFrameLayer:
      parent.layer.close(本 iframe 层)
      scheduleFocusRestore(parent window)  // 条件式 hasFocus / body.focus
      stop
```
