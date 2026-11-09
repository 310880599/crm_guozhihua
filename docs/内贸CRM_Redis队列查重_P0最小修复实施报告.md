# 内贸 CRM Redis 队列查重 P0 最小修复实施报告

**日期**：2026-11-09  
**范围**：仅本地代码修改；未部署、未 Git 提交、未推送  
**原则**：最小修复状态机与前端轮询；不改匹配规则、库表、Redis/Supervisor 配置  

---

## 1. 实际修改文件清单

| 文件 | 操作 |
|------|------|
| `application/admin/controller/Client.php` | 修改 `conflict()`、`getConflictResult()` |
| `application/admin/view/client/conflict.html` | 重写查重提交与轮询逻辑 |
| `conflict_worker.php` | 补充 processing / failed / done 状态机 |

未修改其他文件。未改数据库结构、客户数据、Redis 配置、Supervisor 配置。

---

## 2. 每个修改位置的函数和行号

### 2.1 `Client.php`

| 函数 | 行号（约） | 变更要点 |
|------|------------|----------|
| `conflict()` | 7379–7480 | 入队前 Lua 原子写 `pending` + `RPUSH`；失败明确返回；清理残留 `pending`；禁止覆盖 `done/failed` |
| `getConflictResult()` | 7483–7535 | `pending/processing→202`；`done→0`；`failed→500`；缺失→404；`done` 无结果→500 |

### 2.2 `conflict_worker.php`

| 位置 | 行号（约） | 变更要点 |
|------|------------|----------|
| 领取后写 `processing` | 27–52 | Lua 条件更新，不覆盖终态 |
| SQL 异常 → `failed` | 60–143 | 不再把异常伪装成空成功 |
| 结果写入 → `done` | 344–385 | 先结果后状态（Lua 原子）；空结果仍 `done`；保存失败标 `failed` |

**未改**：原 SQL 匹配规则、聚合与 `repeat_info` 逻辑、队列名 `waimao_conflict_queue`、状态/结果 Key 前缀。

### 2.3 `conflict.html`

| 位置 | 行号（约） | 变更要点 |
|------|------------|----------|
| `pollState` / 串行轮询 | 277–401 | `setTimeout` 串行；30s 总超时；乱序防护；HTTP 有限重试 |
| `submit(search)` | 403–455 | 提交成功才轮询；防重复点击；新任务停旧轮询 |
| `beforeunload` | 457–460 | 离开页面停止轮询 |

---

## 3. 原问题及修改后的逻辑

### 原问题（上一轮已确认）

1. 任务提交后不写 `pending/processing`  
2. Worker 仅在结束后写 `done`  
3. `getConflictResult()` 把缺失状态当 `404`  
4. 前端每 1s `setInterval`，非 `202` 立刻停  
5. 超过首次轮询窗口的任务被误判失败  

### 修改后

1. 生产者：先 `pending` 再入队（Lua 原子；失败不返回成功）  
2. Worker：领取后 `processing` → 成功写结果并 `done` / 失败写 `failed`  
3. 查询接口：按状态机返回业务码（与 HTTP 状态码区分，仍用 JSON `code`）  
4. 前端：仅在提交成功后串行轮询；`202` 继续等；失败/超时明确提示  

---

## 4. Redis 状态机完整流程

```
[前端提交]
    ↓
conflict(): SET pending (TTL=300) + RPUSH waimao_conflict_queue
    ↓ 返回 code=0 + task_id
[前端串行轮询 getConflictResult]
    ↓
pending / processing → code=202（继续等，最多 30s）
    ↓
Worker BLPOP
    ↓
SET processing（不覆盖 done/failed）
    ↓
查库成功 ──────────────► SET result + SET done → code=0 + data
查库无命中 ────────────► SET result=[] + SET done → code=0 + data=[]
查库/写结果失败 ───────► SET failed (+ error 简码) → code=500
状态 Key 缺失/过期 ─────► code=404
done 但 result 缺失 ────► code=500（不显示“无重复客户”）
```

**Key 约定（保持原前缀）**

- 队列：`waimao_conflict_queue`  
- 状态：`waimao_conflict_status:{task_id}`  
- 结果：`waimao_conflict_result:{task_id}`  
- 错误简码（仅服务端排查）：`waimao_conflict_error:{task_id}`  

**TTL**：状态/结果/错误均为 **300 秒**，大于前端 30 秒等待窗口。

---

## 5. 如何避免状态写入竞态

### 5.1 生产者

- 本地 phpredis 支持 `eval`（已实测 `eval=yes`）。  
- 采用 Lua：`GET` 若已是 `done/failed` 则拒绝；否则 `SET pending` + `RPUSH` 同脚本执行。  
- 降级路径：`SET pending NX EX` → `RPUSH`；入队失败仅删除仍为 `pending` 的残留。  
- **不采用**「先 RPUSH 再无条件 SET processing」方案，避免 Worker 先写 `done` 被生产者覆盖。  

### 5.2 Worker

- `processing`：终态存在则不写。  
- `done`：Lua 原子写 result + status；若当前已是 `failed` 则不覆盖。  
- `failed`：不覆盖已有 `done`。  

### 5.3 残留风险（BLPOP）

Worker 用 `BLPOP` 取出后即离开队列。若进程在写 `done/failed` 前崩溃：

- 任务不会自动回队；  
- 状态可能停在 `pending/processing`；  
- 前端约 30 秒超时结束；  
- Redis 状态约 300 秒后过期。  

本轮未做确认机制 / 死信 / 自动重试（按任务要求）。

---

## 6. 前端如何避免重复轮询及请求乱序

| 机制 | 实现 |
|------|------|
| 串行轮询 | `setTimeout` 链式调用，不用叠加 `setInterval` |
| 总超时 | `POLL_TIMEOUT_MS = 30000`，提示「查询超时，请稍后重试」 |
| 新任务覆盖 | 提交前 `stopPolling()`，切换 `activeTaskId` |
| 乱序防护 | 每次回调比对 `taskId !== pollState.activeTaskId` 则丢弃 |
| 防重复入队 | `searching` 时禁用按钮并提示 |
| 提交成功才轮询 | `code===0` 且存在 `task_id` 后才 `pollOnce` |
| HTTP 异常 | 最多重试 3 次，仍受总超时约束 |
| 页面离开 | `beforeunload` 停止轮询 |
| 真正失败 | `500/404` 立即结束，不永久转圈 |

---

## 7. 本地测试结果及未测试项目

### 7.1 已真实执行

| 项 | 结果 |
|----|------|
| `php -l` Client.php / conflict_worker.php | 通过 |
| Redis 连通（127.0.0.1:26739） | 通过 |
| phpredis `eval` | 可用 |
| 状态机 Lua 模拟（14 项断言） | **PASS=14 FAIL=0** |
| 测后队列 `llen(waimao_conflict_queue)` | **0**（仅清理本脚本临时 job/key，未 FLUSHDB） |

覆盖到的状态映射：

- pending/processing → 202  
- done + 有数据 / 空数组 → 0  
- done 无 result → 500  
- failed → 500  
- 缺失 → 404  
- 禁止 pending 覆盖 done、processing 覆盖 done、done 覆盖 failed  
- TTL 正常  

### 7.2 未在本轮真实执行（需测试/预发或线上验证）

1. Worker 0.5s / 3s / 10s 真实 SQL 端到端成功  
2. 浏览器连续点击、旧请求晚到不覆盖（前端逻辑已实现，未做 UI 自动化）  
3. 人为制造 SQL 异常看前端失败提示  
4. Worker 中途 kill，确认前端 30s 超时  
5. 生产 Supervisor 实际进程命令行核对  
6. 新旧版本混部窗口验证  

---

## 8. 与旧版本的兼容性说明

| 场景 | 行为 |
|------|------|
| 新 Controller + 旧 Worker | 生产者写 `pending`；旧 Worker 仍只在结束写 `done`（不写 processing）。前端对 `pending` 持续 202，一般可用。 |
| 旧 Controller + 新 getConflictResult | 无状态时仍 404（旧缺陷保留）。须尽快上生产者。 |
| 新 getConflictResult + 旧前端 | 旧前端仍可能把非 202 当失败；须同步上新 `conflict.html`。 |
| 老任务无 pending | 状态缺失返回 404，**不会**无限 202。 |
| Key / 队列名 | 仍为 `waimao_conflict_*`，与现网代码一致。 |
| 结果字段 | 保持原 Layui 表格字段结构。 |
| 空命中 vs 失败 | 空命中=`done+[]`；失败=`failed`/`500`。 |

仓库内副本 Worker（`conflict_worker副本*.php`）使用 **`conflict_queue`**（无 `waimao_` 前缀），与当前主链路队列不同；是否仍被某 Supervisor 拉起见第 9 节（待确认）。

---

## 9. 上线前需要检查的 Supervisor 进程

本地仓库**无** Supervisor 配置文件，无法从代码库确认线上实际命令。

上一轮排查已知宝塔进程名（**映射关系仍待确认**）：

| 进程名 | 当时状态 | 待确认项 |
|--------|----------|----------|
| `neimao_chongtuchaxun` | 运行中 | 是否执行本项目根目录 `conflict_worker.php`？队列是否为 `waimao_conflict_queue`？ |
| `crmnew_conflict_worker` | 运行中 | 是否指向其他项目/旧脚本？是否同队列多消费者？ |
| `crm_conflict_worker` | 已停止 | 确认保持停止，避免误启旧 Worker |

**上线前必查命令示例（在服务器执行，本轮未执行）：**

```bash
supervisorctl status
# 对每个相关 program 查看 command= 与 directory=
```

确认：

1. 本项目实际由哪个 program 守护  
2. 是否存在多个进程消费同一 `waimao_conflict_queue`  
3. 发布后必须 reload/restart **正确的那一个** Worker  

---

## 10. 推荐的生产发布顺序及回滚方案

### 推荐顺序

1. **先发** `Client::getConflictResult()`（识别 `pending`，避免只发生产者时旧接口把 pending 当 404）  
2. **同批或紧随** 发 `conflict.html`（串行等待 202）  
3. **再发** `Client::conflict()`（开始写 pending）  
4. **最后重启** 对应 Supervisor 中的 `conflict_worker.php`（processing/failed/done）  
5. 烟雾验证：快查询、慢查询、空结果、失败提示、连续点击  

亦可将 1–3 同批发布 PHP/HTML，再单独重启 Worker；**禁止**只重启未知名称的多个 Worker。

### 回滚

1. 回滚上述 3 个文件到上一版本  
2. 重启同一 Supervisor Worker  
3. 不清理业务 Redis 队列；无需改库  

混部窗口注意：仅回滚 Worker 时，新前端仍可依赖 `pending`+最终 `done`；若回滚 Controller 但保留新前端，缺失状态会 404，体验感知为失败（与旧行为类似）。

---

## 11. 仍然存在的 P1、P2 风险

### P1

1. **BLPOP 无确认**：Worker 崩溃可丢任务（前端超时，不会自动重试）。  
2. **Supervisor 多进程身份未确认**：若两进程同吃 `waimao_conflict_queue`，存在重复消费/竞态风险。  
3. **慢 SQL 超过 30s**：前端超时，但 Worker 仍可能稍后写完（用户需重查；结果 TTL 内可被接口读到，但当前 UI 已停）。  

### P2

1. 内贸项目队列/Key 仍用 `waimao_` 前缀，运维易误解。  
2. Redis 密码仍硬编码在 PHP（历史问题，本轮未改）。  
3. 无死信、无重试、无任务审计表。  
4. 渠道/端口映射查询异常仍被忽略（仅影响名称展示，不标 failed）。  
5. 仓库残留旧 `conflict_worker*副本*.php`，易被误配到 Supervisor。  

---

## 12. 总结：已修复 / 仅分析 / 已测 / 待线上验证

### 已修复

- 入队前初始化 `pending`，提交成功与 Redis 任务存在一致  
- Worker `processing` / `done` / `failed` 生命周期  
- SQL 异常不再伪装成空成功  
- `getConflictResult` 正确映射 pending/processing/done/failed/缺失/结果丢失  
- 前端串行轮询、30s 超时、防重复点击、防乱序覆盖、离开清理  

### 仅分析（未改代码）

- BLPOP 崩溃丢任务的完整确认/重试方案  
- Supervisor 进程与项目的真实映射  
- 队列命名去 `waimao_`、密码外置等治理项  

### 已真实执行的测试

- PHP 语法检查  
- Redis Lua 状态机本地模拟（14/14）  
- 确认 eval 可用；测试后队列长度为 0  

### 仍需线上 / 预发验证

- 0.5s / 3s / 10s 真实查重成功  
- 空结果、SQL 失败、未消费等待、超时文案  
- 连续点击与旧请求乱序  
- Worker 异常退出后的前端收敛  
- TTL 内结果可读  
- Supervisor 正确进程重启后的烟雾测试  

---

**本轮结论**：P0 误判失败的状态机与轮询缺陷已在本地代码修复；部署前必须确认并只重启本项目对应的 Supervisor Worker，完成端到端验证后再视为生产闭环。
