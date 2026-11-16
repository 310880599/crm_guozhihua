# 内贸 CRM 公共 ESC 弹层关闭逻辑最小修复实施报告

> 对应排查报告：《内贸 CRM 客户详情 ESC 偶发失效问题排查报告》  
> 约束：仅改 `application/admin/view/common/foot.html`；未改业务页 / Layui 源码 / 数据库；未 Git 提交；未部署。

---

## 结论先说

已在公共 `foot.html` 用「一次 ESC 只关一层」替换原来的 `layer.closeAll()`：

1. 当前文档有可关 Layui 弹层时，只关 z-index 最高的那一层。
2. 本地没有可关弹层、且确认本页是父页 `layer.open({type:2})` 打开的 iframe 时，只关父页这一层。
3. 禁止 `closeAll()`，禁止向上无限递归；同一 document 有重复绑定保护。

经理在客户详情 iframe 内获焦后再按 ESC，应能正常关闭客户详情壳。

---

## 1. 实际修改文件及代码位置

| 文件 | 变更 |
|---|---|
| `application/admin/view/common/foot.html` | 替换原 ESC → `layer.closeAll()`（约原 7–19 行）为完整「顶层一层 / 父 iframe 一层」逻辑 |

未修改：

- `pendingindex.html` / `client/details.html` / `order/details.html` 等业务页
- Layui `layer.js` 源码
- 数据库、配置、其他键盘快捷键

---

## 2. 修改前与修改后的行为

### 修改前

```javascript
document.addEventListener('keydown', function (event) {
    // ...
    layer.closeAll(); // 只作用于「当前 document」的 layer 实例
});
```

| 焦点位置 | ESC 效果 |
|---|---|
| 待审核父页 | `closeAll()` 关掉客户详情（看似正常） |
| 客户详情 iframe 内、无本地弹层 | iframe 自己的 `closeAll()` 无层可关 → **表现为失效** |
| 详情内还有订单详情等本地层 | 一次可能关掉当前文档全部层，仍关不掉父页客户详情壳 |

### 修改后

| 焦点位置 | ESC 效果 |
|---|---|
| 待审核父页，仅客户详情打开 | 关本地最上层（客户详情） |
| 客户详情 iframe 获焦，无本地弹层 | `parent.layer.getFrameIndex` 校验后关父页这一层 |
| 详情内再开订单详情 / 跟进 / 审核确认等 | 先关本地最上层；再按一次 ESC 才关外层 |
| tips / loading 仍在 | 不穿透去关父页 iframe 壳 |
| 存在 `#previewWrap` / `#previewZoomImg` | 公共逻辑不关父层，交给页面自有 ESC（如订单详情） |

---

## 3. 父子 iframe 焦点问题如何解决

根因：键盘事件落在焦点所在 document；子页 `layer.closeAll()` 关不到父页 layer。

解决：子页在「本地无需要 ESC 处理的弹层」时，通过父页 API 关闭**自己所在的那一层**：

1. `parent.layer.getFrameIndex(window.name)` 取索引  
2. 校验返回值非空  
3. 确认 `parent.document.getElementById('layui-layer' + idx)` 仍存在  
4. `parent.layer.close(idx)`  
5. `try/catch` 包裹，跨域或 API 缺失时静默失败  

一次 ESC 最多关一层，不会沿 `parent.parent...` 递归。

---

## 4. 如何识别正确的弹窗索引

针对项目 Layui **layer 3.1.1**：

- 弹层 DOM：`#layui-layer{times}`，属性 `times`、`type`（`dialog|page|iframe|loading|tips`）
- `layer.index` 仅为自增计数，关闭后仍可能指向已销毁层 → **不作为唯一依据**
- 本修复：扫描当前文档 `.layui-layer`，跳过 `tips`/`loading`，用 `#layui-layer{times}` 校验有效性，按计算样式 `z-index` 取最上层 `times`，再 `layer.close(times)`

---

## 5. 如何避免误关其他弹窗

| 措施 | 作用 |
|---|---|
| 禁止 `closeAll()` | 避免一次 ESC 清掉多层 |
| 一次只 `close` 一个 index | 多层时需多次 ESC |
| 跳过 `tips` / `loading` | 不把提示/加载当普通交互层关掉 |
| `hasLocalBlockingLayer` | 仅有 tips/loading 时不穿透关父壳 |
| `hasLocalCustomPreview` | 订单详情等自定义预览节点存在时不关父层 |
| `stopImmediatePropagation`（成功关闭后） | 降低与页面自有 ESC（如 `galleryPreview`、`detailsEsc`）叠关掉两层的风险 |
| `__ADMIN_ESC_LAYER_BOUND__` | 同一 document 不重复注册 |

与页面自有 ESC 的关系（未改业务页）：

- `pendingindex` 图片预览 `keydown.galleryPreview`：公共逻辑通常先关最上层预览并停止冒泡，效果仍是只关预览。
- `order/details.html` 的 `keydown.detailsEsc`：有预览标记时公共逻辑让路；无预览时两边目标都是关父层同一 index，结果一致。

---

## 6. 已完成的静态检查

| 检查项 | 结果 |
|---|---|
| 文中已无 `layer.closeAll()` | PASS |
| 含重复绑定保护 `__ADMIN_ESC_LAYER_BOUND__` | PASS |
| 含 `getFrameIndex` + DOM 存在性校验 | PASS |
| 含 tips/loading 跳过与自定义预览让路 | PASS |
| 成功关闭后 `stopImmediatePropagation` | PASS |
| 抽出脚本 `node --check` 语法检查 | PASS（exit 0） |

未做：浏览器实机点击回归（需你本地验证）。

---

## 7. 需要你在浏览器中手动测试的场景

建议强刷缓存后再测（`foot.html` 为公共模板）。

### P0（原问题）

1. 业绩订单 → 待审核订单 → 点客户名称 → **不点详情内容** → ESC → 应关闭。  
2. 再打开详情 → **点击详情内表格/表单获焦** → ESC → 应关闭（修复点）。  
3. 获焦后再点标题栏 × → 仍应关闭。

### P1（回归）

4. 客户详情内再开历史订单详情：第一次 ESC 只关订单详情，第二次才关客户详情。  
5. 待审核：审核确认 / 拒绝弹窗打开时，ESC 只关该确认层。  
6. 待审核 / 审核页：图片或附件预览打开时，ESC 只关预览。  
7. 客户管理打开客户详情（若同样 type:2）：iframe 内获焦后 ESC 可关。  
8. 跟进记录等本地弹层：ESC 先关跟进层，不误关列表页其它层。  
9. 多层嵌套 iframe：每一层 ESC 只退一层。  
10. 我的订单 / 订单详情：原有预览 ESC、关详情行为仍正常。  
11. 无任何弹层的普通列表页按 ESC：不应误关主框架或其它 tab。

### 观察建议

- 复现修复前：焦点进 iframe 后 ESC 无效。  
- 修复后：同一操作应关闭客户详情。  
- DevTools 可看：获焦后 `document.activeElement` 是否在详情 iframe；按 ESC 后父页 `#layui-layer*` 是否减少一个。

---

## 附录：核心逻辑伪代码

```
ESC:
  idx = 当前文档最上层可关层(跳过 tips/loading，校验 #layui-layer{times})
  if idx: layer.close(idx); stop; return
  if 本地 tips/loading 或自定义预览节点: return  // 不关父层
  if parent.layer.getFrameIndex(window.name) 有效且父 DOM 仍存在:
      parent.layer.close(idx); stop
```
