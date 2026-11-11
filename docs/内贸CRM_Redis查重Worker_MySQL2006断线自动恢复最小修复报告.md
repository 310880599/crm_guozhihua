# 内贸 CRM Redis 查重 Worker MySQL 2006 断线自动恢复最小修复报告

## 1. 已确认的生产故障证据

连续两天早上业务员首次查重返回 `code=500`，重启 Supervisor 进程 `neimao_chongtuchaxun` 后立即恢复，白天持续使用正常。

生产日志连续出现：

```text
[conflict_db_debug]
stage=name
exception_class=think\exception\PDOException
exception_code=10501
sqlstate=HY000
driver_code=2006
```

结论：常驻 PHP Worker 持有的 MySQL 连接在夜间空闲后失效（MySQL server has gone away / 驱动码 2006）。异常被 Worker 内部 catch 捕获后标记 `failed` 并 `continue`，未释放失效 PDO，后续任务继续复用死连接，直到人工重启进程。

本次不是 HY093 参数绑定问题。

## 2. ThinkPHP5.1 实际连接管理源码分析

### 2.1 是否自带断线重连

有。`thinkphp/library/think/db/Connection.php`：

| 位置 | 内容 |
|------|------|
| L112 | 默认配置 `'break_reconnect' => false` |
| L127–139 | `$breakMatchStr` 含 `server has gone away`、`Lost connection` 等 |
| L682–700 / L759–777 | `query()` / `execute()` catch 后若 `isBreak($e)` 则 `$this->close()->query/execute(...)` |
| L1826–1837 | `close()`：置空 `linkID/linkWrite/linkRead/links` 并 `free()` |
| L1845–1858 | `isBreak()`：仅当 `break_reconnect===true` 且 message 命中断线字符串时返回 true |

### 2.2 项目是否开启

`config/database.php` L61：`'break_reconnect' => false`（未开启）。

### 2.3 框架重连边界问题（本轮不启用的关键原因）

`isBreak` 为 true 时直接递归调用同一 SQL，**没有重试次数上限**。若 MySQL 持续不可用，可能递归至栈溢出。本轮禁止修改全局数据库配置，也不得叠加无限重试。

### 2.4 相关 API 真实行为

| API | 行为 |
|-----|------|
| `Db::connect($config=[], $name=false)` | `Connection::instance($options, $name)`；`$name===true` 强制新建并覆盖静态缓存（`Connection.php` L186–199；`Db.php` L124–134） |
| `Query::getConnection()` | 返回当前 Connection（`Query.php` L221–224） |
| `Connection::close()` | 释放 PDO 引用，不主动 reconnect |
| 包装异常 | `think\exception\PDOException` 固定 code=`10501`，原始 SQLSTATE/驱动码写入 `getData()['PDO Error Info']`（`exception/PDOException.php` L28–38） |

**不得仅凭 HY000 或 10501 判定断线。**

## 3. 最终选择的重连方案及原因

**方案：Worker 层有限重连（不开启框架 `break_reconnect`）。**

原因：

1. 框架自带重连默认关闭，且无次数上限，不适合常驻 Worker。
2. 禁止修改全局 `config/database.php`。
3. Worker 层可严格限制：单任务主查询最多 1 次额外重试；映射阶段独立最多 1 次；不 RPUSH、不改 task_id/TTL/状态机。
4. 使用本项目真实支持的 TP5.1 API：`close()` + `Db::connect([], true)` + `SELECT 1` 校验。

## 4. 实际修改文件及行号

**唯一业务修改文件：`conflict_worker.php`**

| 行号（约） | 内容 |
|-----------|------|
| 120–174 | `conflict_db_extract_codes()` |
| 176–190 | `conflict_db_is_connection_lost()` |
| 192–216 | `conflict_db_reconnect_log()` |
| 218–251 | `conflict_db_force_reconnect()` |
| 253–277 | `conflict_db_debug_log()`（原诊断逻辑抽取） |
| 375–499 | 名称+联系方式整段只读查询：最多 2 轮（1 次重连重跑） |
| 631–703 | 渠道/端口映射：断线可恢复一次；仍失败保持空展示 |

未修改：Client.php、conflict.html、ThinkPHP 源码、database.php、Redis/Supervisor、表结构。

`git diff --name-only` 仅显示 `conflict_worker.php`。

## 5. 如何识别 2006、2013 等连接异常

`conflict_db_is_connection_lost()` 仅在以下条件为真时返回 true：

- 驱动错误码 `2006` 或 `2013`
- 或 SQLSTATE `08S01`

安全提取路径：

1. 原生 `\PDOException::$errorInfo`
2. ThinkPHP 包装异常 `getData()['PDO Error Info']` 的 `SQLSTATE` / `Driver Error Code`

明确排除：仅 HY000、仅 10501、HY093、语法错误、未知字段等。

## 6. 如何释放失效连接并建立新连接

`conflict_db_force_reconnect()`：

1. 日志 `status=start`
2. `Db::connect()->getConnection()->close()` 释放失效 PDO
3. `Db::connect([], true)` 强制新建 Connection 实例并覆盖静态缓存
4. `Db::query('SELECT 1')` 校验；失败则 `status=failed` 并返回 false（不假装成功）
5. 成功则 `status=success`

重试业务 SQL 时重新 `\think\Db::name(...)` / `table(...)` 构建 Query，不复用旧 Query / bind。

## 7. 如何保证当前任务最多重试一次

主查询：

- `$dbQueryMaxRounds = 2`
- `$dbReconnectUsed` 保证整任务最多调用一次 `conflict_db_force_reconnect`
- 成功恢复后 **整段重跑** 名称 + 联系方式查询（清空 `$rawList/$leadsIds` 与半成品耗时），避免半结果合并
- 重试仍失败 → 原机制 `conflict_mark_failed(..., 'db_query_exception')`
- 不 RPUSH、不改 processing、不延长 TTL、不新 task_id

映射阶段：

- 独立 `$mapReconnectUsed` / `$mapMaxRounds=2`
- 恢复后仍失败：保持原规则（渠道/端口为空，不阻断主流程）
- 覆盖了原 catch 吞掉断线导致死连接继续复用的问题

无无限 sleep；数据库持续不可用时：当前任务 failed，Worker 继续消费下一条（下一条仍可再尝试一次重连）。不会为当前任务无限等待。

重连耗时限制：无额外 sleep；仅 close + 新建 + `SELECT 1` + 业务 SQL 重跑。若 MySQL 连接握手/查询阻塞，仍受 PDO/MySQL 网络超时约束（本轮未改驱动超时配置）。

## 8. 如何保留 HY093 修复与原有查重规则

仍为父查询预绑定：

```php
->bind([
    'cf_concat_like' => [$likeKeyword, \PDO::PARAM_STR],
])
```

闭包内：

```php
->whereOrRaw(
    'CONCAT(c.contact_extra, c.contact_value) LIKE :cf_concat_like'
)
```

未恢复闭包内 `LIKE ?` + bind 数组写法；未直接拼接关键词；OR 模糊条件与 `conflict_escape_like` 逻辑保留。

## 9. 日志示例及性能开销

保留：`[conflict_perf]`、`[conflict_db_debug]`、原 failed 分类与 Redis 状态机。

新增（仅断线路径）：

```text
[conflict_db_reconnect] task_id=xxx stage=name attempt=1 status=start driver_code=2006
[conflict_db_reconnect] task_id=xxx stage=name attempt=1 status=success driver_code=2006
```

失败：

```text
[conflict_db_reconnect] task_id=xxx stage=name attempt=1 status=failed driver_code=2006
```

正常任务无重连日志。`worker_total_ms` 仍从领取成功后计时，包含重试耗时，不含 Redis 排队。日志异常均 try/catch，不影响业务。

## 10. 实际执行的测试结果

本地隔离环境（PHP 7.4.3 + 本地 MySQL + Redis 26739）执行 `_tmp_conflict_db_reconnect_test.php`：

| # | 用例 | 结果 |
|---|------|------|
| 1 | 识别 2006 / 2013 / 08S01 | PASS |
| 2 | 仅 HY000 / 10501 / HY093 / 未知字段不触发 | PASS |
| 3 | 正常 name+contact 查询可执行 | PASS |
| 4 | HY093 命名绑定仍可用 | PASS |
| 5 | **真实** `SET SESSION wait_timeout=3` → sleep 5 → 捕获 driver_code=2006 → 重连成功 → 后续查询成功 | PASS |
| 6 | SQL 语法错误不触发断线重试判定 | PASS |
| 7 | 字段不存在不触发 | PASS |
| 8 | 源码保留 perf/debug/HY093；`dbQueryMaxRounds=2`；未启用 break_reconnect | PASS |
| - | `php -l conflict_worker.php` | No syntax errors |

汇总：`pass=23 fail=0`；`real_gone_away_verified=yes`；`real_reconnect_ok=yes`。

真实断线复现日志摘录：

```text
[conflict_db_debug] task_id=test_wait_timeout stage=name exception_class=think\exception\PDOException exception_code=10501 sqlstate=HY000 driver_code=2006
```

与生产形态一致。

## 11. 未验证的内容及剩余风险

1. **未在生产环境**做断线模拟（按要求禁止）。
2. 未跑完整 Supervisor 整夜空闲后次日首单的生产级端到端观测（需上线后观察早高峰）。
3. MySQL **完全宕机且握手长时间阻塞**时，单次 `SELECT 1`/业务 SQL 仍可能占用 Worker 直至驱动超时；本轮未改 PDO 超时，也不增加无限等待。
4. 映射阶段重连成功但业务映射再次非断线失败时，仍按原规则空渠道/端口（正确）。
5. 前端 30 秒超时未改：若断线 + 重连 + 重查总耗时接近上限，极端情况下前端仍可能超时，但 Worker 侧会尽量完成并写 done。

## 12. 是否满足上线条件

**满足最小上线条件（建议灰度观察一个早高峰）。**

依据：

- 根因与源码证据明确
- 仅改 `conflict_worker.php`
- 有限重试、保留 HY093 / Redis 协议 / 性能与诊断日志
- 本地真实 wait_timeout 断线恢复已验证
- 非断线异常不会误触发重连

## 13. 上线及回滚建议

### 上线

1. 备份当前生产 `conflict_worker.php`
2. 发布新 `conflict_worker.php`
3. `supervisorctl restart neimao_chongtuchaxun`（仅重启该进程）
4. 观察：早高峰是否出现 `[conflict_db_reconnect] ... status=success` 且任务 `done`；确认无敏感信息泄漏；确认不再依赖人工重启

### 回滚

1. 还原备份的 `conflict_worker.php`
2. 重启 `neimao_chongtuchaxun`

本轮：**只改本地代码，未提交、未推送、未部署。**
