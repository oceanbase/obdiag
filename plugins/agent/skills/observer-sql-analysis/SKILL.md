---
name: observer-sql-analysis
description: 标准 SOP：单条 SQL 性能、执行计划、trace 计划包、锁等待。用户问「为什么慢」、提供 trace_id、或描述锁/超时时使用；与 obdiag-performance（整体 ASH/AWR）、observer-log-analysis（observer 日志链）分工。
---

# SQL 深度分析 — SOP

## 触发与边界

- **本 SOP**：围绕 **一条或少量 SQL**、**trace_id**、**执行计划**、**锁等待**。
- **不替代**：时段整体负载、会话堆积 → **`obdiag-performance`**（`gather_ash` 等）；OBProxy 日志 → **`obproxy-log-analysis`**。

## 前置条件

- 尽量明确：**SQL 文本** 或 **trace_id** 或 **sql_id**（视工具能力）；多集群传 **`cluster_config_path`**。

## 分支：选主路径

| 情况 | 主路径 |
|------|--------|
| 用户已给 **trace_id**，要计划/监控包 | **阶段 A** 为主，必要时 **阶段 C** |
| 无 trace_id，要能直接在库上查计划 | **阶段 B**（`db_query` EXPLAIN / 视图）为主 |
| 用户强调 **锁等待、死锁、超时** | **阶段 B** + **阶段 C**（日志 grep 相关 trace/sql） |
| 既要计划又要和 observer 日志对齐 | **阶段 A 或 B** → **阶段 C** |

## 阶段 A：计划监控包（trace_id）

**目的**：拉取与 trace 关联的 **SQL plan monitor** 材料。

**动作**：

- 调用 **`gather_plan_monitor`**，传入 **`trace_id`**；时间参数格式：`from_time="2024-01-01 10:00:00"`（`yyyy-mm-dd hh:mm:ss`）；非默认集群 **`cluster_config_path`**。

**完成标准**：

- 工具成功返回且可知 **输出目录或结果摘要**；否则说明失败原因与下一步（时间窗、权限）。

## 阶段 B：库内计划与锁线索（db_query）

**目的**：在没有或未用 plan monitor 包时，从集群侧拿 **执行计划**、**锁/会话** 信息。

**动作**：

- 使用 **`db_query`** 执行 **只读** SQL：`EXPLAIN` / `EXPLAIN EXTENDED`、相关 `gv$`/`v$` 视图（锁、等待、sql_audit 等，以用户环境与版本为准）。
- 关注：**访问路径、COST、ROWS、PARTITION、是否全表扫**；锁场景关注 **holder/waiter、资源 ID**。

**常用视图速查（以 OceanBase 4.x sys 租户或 DBA 权限为准）**：

| 场景 | 推荐视图 |
|------|---------|
| 锁等待、死锁 | `gv$ob_locks`、`gv$ob_transaction_participants`、`information_schema.innodb_lock_waits` |
| 活跃会话与等待 | `gv$ob_processlist`、`gv$ob_active_session_history` |
| SQL 审计、慢 SQL | `gv$ob_sql_audit`（`elapsed_time > N`、按 `sql_id` 过滤） |
| 执行计划缓存 | `gv$ob_plan_cache_plan_explain`、`gv$ob_plan_cache_stat` |
| 当前事务 | `gv$ob_transaction_participants`、`v$ob_global_transaction` |

**完成标准**：

- 有关键计划字段或锁/会话事实；若查不到，说明可能原因（计划已淘汰、租户权限、时间窗）。

## 阶段 C：日志侧证（observer）

**目的**：用 **observer 日志** 补充错误栈、重试、与 trace 相关的行级上下文。

**动作**：

- **`gather_log`** 带 **`grep`**（trace_id、SQL 指纹片段、错误码）+ 时间范围；或 **`analyze_log`** 做聚合（仍仅集群侧，非 OBProxy）。
- 详见 **`observer-log-analysis`** 的链式要求；若用户同时 OBProxy，切 **`obproxy-log-analysis`**。

**完成标准**：

- 日志与阶段 A/B 结论能 **互证或解释矛盾**；无日志命中时建议缩小/扩大时间或换关键词。

## 阶段 D：文档与参数（可选）

**目的**：参数含义、版本行为与官方表述对齐。

**动作**：调用 **`query_oceanbase_knowledge_base`**，**query 中含 Observer/OBProxy 版本**（若已知）；长上下文放 **`context_text`**。

**完成标准**：知识库结论与 **现场数据** 并列说明；冲突时以数据为准并点明差异。

## 输出格式（必填结构）

1. **表格**：计划关键指标（操作符、COST、ROWS、索引/分区等）或锁等待关键字段。
2. **结论文本**：1–3 条 **可执行** 建议（加索引条件、改写 SQL、调参、扩时间再采日志等）。
3. **下一步**：若需 ASH/AWR、巡检、RCA，指明 skill 或工具。

## 与其它 skill

| 需求 | 使用 |
|------|------|
| ASH/AWR/perf/整体慢 | **`obdiag-performance`** |
| observer 日志采集分析主流程 | **`observer-log-analysis`** |
| OBProxy | **`obproxy-log-analysis`** |
| 巡检 / RCA | **`obdiag-check-rca`** |

## 自检

- 是否把 **`gather_plan_monitor`** 当成「收普通日志」？（应 **`gather_log`**。）
- 有 **trace_id** 时是否优先 **`gather_plan_monitor`** 再视需要 **`db_query`**？
- 输出是否同时包含 **表格 + 建议 + 可选下一步**？
