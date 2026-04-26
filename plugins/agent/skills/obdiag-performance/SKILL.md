---
name: obdiag-performance
description: 性能类采集与主机 IO：ASH、AWR、perf 火焰图、sysstat、磁盘 IO。在用户说卡顿、慢、会话堆积、要看 ASH/AWR/火焰图或主机负载时使用；与 observer-sql-analysis、observer-log-analysis 互补。
---

# 性能诊断采集

## 工具选型速查

| 用户说法 / 目标 | 工具 | 说明 |
|-----------------|------|------|
| ASH、活跃会话历史、会话等待 | **`gather_ash`** | 可配合 `from_time`/`to_time`（格式 `yyyy-mm-dd hh:mm:ss`）、`trace_id`、`sql_id`、`wait_class`、`report_type` 等 |
| AWR、gather awr、ParalleSQL 报告包 | **`gather_awr`** | `since` 或 `from_time`/`to_time`；可选 `cluster_name` / `cluster_id` |
| 火焰图、perf、pstack、采样性能 | **`gather_perf`** | 重采集，需用户确认 |
| 主机信息、sysstat、机器侧基线 | **`gather_sysstat`** | 与集群节点主机相关 |
| **单条 SQL** 执行计划监控、给定 **trace_id** 的计划包 | **`gather_plan_monitor`** | **仅** 计划监控，不是「收集日志」 |
| 磁盘 IO 慢、验盘 | **`tool_io_performance`** | 可选 `disk`、`date` |
| 集群整体健康、巡检告警引发性能 | **`check_cluster`** | 与 ASH/AWR 互补；用于发现配置/状态类根因 |
| 场景化根因分析（如 CPU/内存/磁盘）| **`rca_run`** | 先 `rca_list` 看可用 scene，再 `rca_run(scene="...")` |

## 与 `observer-sql-analysis` 的关系

- **`observer-sql-analysis`**：围绕 **一条 SQL**、锁、trace、计划——优先 **`db_query`** + **`gather_plan_monitor`** + **`gather_log`**（grep trace）。
- **本 skill**：偏 **时段内整体负载、ASH/AWR、主机与 perf**；用户未指向具体 SQL 时多用 **`gather_ash`** / **`gather_awr`** / **`gather_sysstat`**。

## 与日志 skill 的关系

- **observer 日志**里的慢 SQL、错误栈 → **`gather_log`** + **`analyze_log`**（**`observer-log-analysis`**）。
- **OBProxy** → **`obproxy-log-analysis`**，不要用 **`analyze_log`** 分析 OBProxy 日志。

## 调用注意

- 上述 gather 类工具多 **`requires_approval`**；执行前简要说明要采什么、大概耗时。
- 非默认集群：所有工具均可传 **`cluster_config_path`**。
- 采集完成后若输出目录在工具返回中，可用 **`file_list`** / **`file_read`** 帮用户看报告路径或摘要。

## 输出

- 说明 **本次用的是哪类采集、时间窗、集群**；结论分点列出；必要时建议下一步（扩大时间窗、补 `gather_log`、或 `check_cluster` / RCA）。

## 典型场景示例流（SOP 片段）

以下为主路径示例，**仍以工具速查表为准**选型；非默认集群全程加 **`cluster_config_path`**。

### 会话堆积 / 整体卡顿、未指向单条 SQL

1. **`gather_ash`**：设定与现象匹配的时间窗；看等待类、top SQL。
2. 若需历史报告包：**`gather_awr`**（时间窗与租户/集群参数按环境）。
3. 若怀疑主机资源：**`gather_sysstat`**；若怀疑磁盘：**`tool_io_performance`**。
4. **完成标准**：已能说明「主要等待/资源瓶颈」或已明确需 **`observer-sql-analysis`** / **`observer-log-analysis`** 跟进。

### 用户已点名具体 SQL / trace

- 转 **`observer-sql-analysis`**（`gather_plan_monitor`、`db_query`、必要时 `gather_log` grep）；本 skill 不强行替代。

### 采集后的解读

- 工具返回中含 pack/报告路径时：**`file_list`** / **`file_read`** 摘要；重采集前说明原因。
- 遇到 **`.tar.gz` / `.zip`** 压缩包时，先用 **`run_shell`**（需用户审批）解压（如 `tar -xzf xxx.tar.gz -C ./extracted`），再 **`file_list`** → **`file_read`** 明文报告；**不可直接 `file_read` 二进制压缩包**。

## 自检

- [ ] 未用 **`gather_plan_monitor`** 代替「收整体性能/日志」类诉求。
- [ ] 单条 SQL 深查是否已引导至 **`observer-sql-analysis`**？
