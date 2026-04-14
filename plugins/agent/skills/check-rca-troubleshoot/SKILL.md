---
name: check-rca-troubleshoot
description: 标准 SOP：集群巡检 check_cluster/check_list 与根因分析 rca_run/rca_list。用户要健康检查、巡检或按场景 RCA 时使用；与 log-troubleshoot、sql-deep-dive、obproxy-log-analysis 分工明确。
---

# 巡检与根因分析（check / RCA）

## 使用场景

- 用户要 **巡检、健康检查、check、集群检查**
- 用户描述 **CPU 高、磁盘满、DDL、内存** 等，且希望走 **RCA 场景化分析**
- 需要先看 **有哪些巡检项 / RCA 场景** 再执行

## 与其它 skill 的分工

| 需求 | 不要用 | 应用 |
|------|--------|------|
| 收集 observer 日志、按 trace 过滤 | 本 skill | **`log-troubleshoot`**（`gather_log` / `analyze_log`） |
| OBProxy 日志 | 本 skill | **`obproxy-log-analysis`**（`gather_obproxy_log` + 文件工具） |
| 单条 SQL 计划 / trace 计划监控 | 本 skill | **`sql-deep-dive`**（`gather_plan_monitor`、`db_query`） |
| 文档级「参数含义、官方怎么说」 | 本 skill | **`oceanbase-knowledge`** |

## 推荐流程

### 巡检（check）

1. **可选**：调用 **`check_list`** 列出可用巡检任务（输出依赖 **当前会话活跃集群** 的配置）。  
   - 用户不确定有哪些检查项、或只要跑全量默认巡检时，**可跳过** `check_list` 直接执行 `check_cluster`。
   - 用户指定特定检查项（如"只跑 OBProxy 内存检查"）时，**先调 `check_list` 确认** 任务名或用例名存在再执行。
2. 调用 **`check_cluster`** 执行巡检，支持以下参数：
   - **Observer 侧**：`cases`（检查用例，逗号分隔）、`observer_tasks`（任务名，优先级高于 cases）
   - **OBProxy 侧**：`obproxy_cases`（检查用例，逗号分隔）、`obproxy_tasks`（任务名，优先级高于 obproxy_cases）
   - 示例：`check_cluster(obproxy_tasks="xxx_task")` 或 `check_cluster(cases="memory_check,disk_check")`
3. 需要落盘时传 **`store_dir`**。若用户点名 **非默认集群**，在 **`check_cluster`** 上传 **`cluster_config_path`**（短名如 `obdiag_test` 或完整 yml 路径）。
4. 若结果里给出报告路径或采集目录，可用 **`file_list`** / **`file_read`** 查看 HTML/文本报告摘要。

### 根因分析（RCA）

1. 调用 **`rca_list`** 查看可用 **scene** 名称（同样基于当前活跃集群配置）。
2. 用户确认或你根据现象选定 **`scene`** 后，调用 **`rca_run`**，传入 **`scene`**；非默认集群加 **`cluster_config_path`**。

## 阶段完成标准（SOP）

| 流程 | 完成标准 |
|------|----------|
| 巡检 | 已执行 **`check_cluster`**（或先 **`check_list`** 再执行）；结果中失败/告警已归纳；若有报告路径已 **`file_list`/`file_read`** 摘要或告知用户路径。 |
| RCA | 已执行 **`rca_run`** 且 **`scene`** 与现象匹配；结论与建议已输出。 |

## 自检清单

- [ ] 日志主路径未误用本 skill 代替 **`log-troubleshoot`** / **`obproxy-log-analysis`**。
- [ ] 需要 **另一集群** 的 `check_list`/`rca_list` 时，已 **`/use`** 切换或向用户说明当前列表对应哪套 config。
- [ ] **`check_cluster`/`rca_run`** 在点名集群时已传 **`cluster_config_path`**（若适用）。

## 多集群注意

- **`check_list` / `rca_list`** 使用会话 **当前 `config_path`**，不接收 `cluster_config_path`。若用户要列的是 **另一套集群** 的任务列表，需先在 agent 里 **`/use <name|path>`** 切换活跃集群（见 **`multi-cluster`** skill），再调用 list。
- **`check_cluster` / `rca_run`** 支持 **`cluster_config_path`**，可在不切换会话的情况下定向某集群。

## 输出

- 用表格或列表归纳 **失败项 / 告警 / RCA 结论**；给出 **下一步**（例如继续 `gather_log`、`db_query`、或知识库查参数）。
