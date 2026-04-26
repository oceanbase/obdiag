---
name: observer-log-analysis
description: 标准 SOP：OceanBase 集群侧（observer/election/rootservice）日志采集、过滤与 analyze 解读。用户要收集日志、trace_id/关键词过滤、分析错误栈或慢 SQL 在 observer 日志中的线索时使用。OBProxy 日志请用 obproxy-log-analysis。
---

# 日志排障（集群侧）— SOP

## 触发与边界

- **本 SOP 仅覆盖**：observer / election / rootservice 等 **集群节点侧** 日志（`gather_log` + `analyze_log`）。
- **不覆盖 OBProxy / 代理**：用户提到 OBProxy、代理、obproxy_* → 加载 skill **`obproxy-log-analysis`**，**禁止**用本流程的 `gather_log` / `analyze_log` 代替。

## 前置条件

- 时间范围：用户未给出时，默认用 **`since="30m"`**（近 30 分钟）；或询问用户。有 **trace_id / 关键词** 时写入 `grep`。
- 多集群：对相关工具传 **`cluster_config_path`**，或先 **`use`**（见 **`obdiag-multi-cluster`**）。

## 分支：选路径

| 用户意图 | 走哪条 |
|----------|--------|
| 只要拉日志、不要求「分析/解读」 | 阶段 1 完成后可结束；简要告知输出目录。 |
| **采集 + 分析** 同一句，或要求解读错误/慢 SQL 日志 | 阶段 1 → **同一轮内**继续阶段 2（及必要时阶段 3）。 |
| 已有 `obdiag_gather_pack_*` 目录，只要分析 | **跳过阶段 1**，从阶段 2 进入（目录内 `file_list` / `file_read`；大包配合 `run_shell` 的 head/grep）。 |

## 阶段 1：采集（gather_log）

**目的**：从节点拉取集群侧日志到本机 pack。

**动作**：

- 调用 **`gather_log`**（**不要**用 `gather_plan_monitor` 当日志采集）。
- 参数：`since` 或 `from_time`/`to_time`；`grep` 传入 trace_id 或关键词列表；需要落盘时 `store_dir`。
- 非默认集群：传 **`cluster_config_path`**。

**完成标准**：

- 工具返回中已有 **采集输出目录**（如 `obdiag_gather_pack_*`）或可确认的存储路径。

## 阶段 2：解读（与采集链式）

**目的**：基于日志内容回答「错误原因 / 时间线 / 是否需扩大窗口」。

**动作**（按序）：

1. 若刚完成阶段 1：对 **pack 目录** **`file_list`**（可用 `pattern` 缩小范围）；大文件或压缩包用 **`run_shell`** 做 `head`/`tail`/`grep` 或解压（需用户审批）。
2. **统计/聚合类 observer 日志分析**（`obdiag analyze log` 能力范围）：调用 **`analyze_log`**（scope 与 CLI 一致：observer、election、rootservice、all 等）。
3. 若用户要**原文行级**核对：在 pack 内 **`file_read`** 读具体文件片段。

**完成标准**：

- 已结合 **analyze_log 输出和/或 file_read 片段** 给出结论；或明确说明「包内无匹配，建议扩大 `since`/换关键词」。
- **零命中处理**：`analyze_log` 或 `file_read` 中无任何匹配时，**不要**直接回复「未发现问题」——应告知用户当前 `since`/时间窗太窄或 `grep` 关键词未匹配，并建议扩大时间范围（如 `since="2h"`）、去掉 `grep` 过滤再次采集，或换 `scope`（如 `all` 代替 `observer`）。

**同一轮内链式（重要）**：若用户请求包含 **分析/解读**，**不得**仅在 `gather_log` 成功后用一句话结束；须在 **同一 agent 运行** 内继续 `file_list` → `analyze_log` 和/或 `file_read` → 再自然语言总结。

## 阶段 3：深挖（可选）

**目的**：计划与日志交叉验证。

**动作**：若有 **trace_id**，可再 **`gather_plan_monitor`**（计划监控）或 **`gather_log`** 缩小 grep；单条 SQL 性能主线见 **`observer-sql-analysis`**。

**完成标准**：用户问题已闭环或已指向下一 skill（如 SQL 深查、OBProxy、RCA）。

## 工具速查

| 目的 | 工具 |
|------|------|
| 集群侧日志采集 | `gather_log` |
| 集群侧日志 analyze（非 OBProxy） | `analyze_log` |
| 包内浏览 / 读文件 | `file_list`、`file_read` |
| 抽样 / 解压 | `run_shell`（审批） |
| 单条 SQL 计划包（非「收日志」） | `gather_plan_monitor` |

## 与其它 skill

| 需求 | 使用 |
|------|------|
| OBProxy | **`obproxy-log-analysis`** |
| 巡检 / RCA | **`obdiag-check-rca`** |
| 单条 SQL、锁 | **`observer-sql-analysis`** |
| 官方文档参数 | **`oceanbase-knowledge`** |
| OMS → Kafka 同步性能、延迟、KafkaSink | **`oms-kafka-performance`** |

## 输出与自检

- 回复中说明 **时间窗、集群（config）、pack 路径（若有）**。
- **自检**：是否误对 OBProxy 用了 `gather_log`/`analyze_log`？若用户要「采集+分析」，是否在同一轮完成了阶段 2？
