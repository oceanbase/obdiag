---
name: obdiag-check-rca
description: 标准 SOP：集群巡检 check_cluster/check_list 与 RCA 入口（rca_run/rca_list）。用户要健康检查、巡检时使用；RCA 场景映射与手动降级详见 obdiag-rca skill；与 observer-log-analysis、observer-sql-analysis、obproxy-log-analysis 分工明确。
---

# 巡检与根因分析（check / RCA）

## 使用场景

- 用户要 **巡检、健康检查、check、集群检查**
- 用户描述 **CPU 高、磁盘满、DDL、内存** 等，且希望走 **RCA 场景化分析**
- 需要先看 **有哪些巡检项 / RCA 场景** 再执行

## 与其它 skill 的分工

| 需求 | 不要用 | 应用 |
|------|--------|------|
| 收集 observer 日志、按 trace 过滤 | 本 skill | **`observer-log-analysis`**（`gather_log` / `analyze_log`） |
| OBProxy 日志 | 本 skill | **`obproxy-log-analysis`**（`gather_obproxy_log` + 文件工具） |
| OBProxy/ODP 路由、弱读、读写分离、连接配置 | 本 skill | **`obproxy-routing-troubleshooting`** |
| 单条 SQL 计划 / trace 计划监控 | 本 skill | **`observer-sql-analysis`**（`gather_plan_monitor`、`db_query`） |
| 日志盘/数据盘、文件系统、索引或副本空间的手动 SOP | 直接跳 RCA | **`observer-storage-space-troubleshooting`**；若用户要 RCA，仍先用本 skill → **`obdiag-rca`** |
| 备份、归档、恢复窗口、OSS/COS/NFS、oblogminer | 本 skill | **`backup-archive-restore-troubleshooting`** |
| 文档级「参数含义、官方怎么说」 | 本 skill | **`oceanbase-knowledge`** |

## 推荐流程

### 巡检（check）

1. **可选**：调用 **`check_list`** 列出可用巡检任务（输出依赖 **当前会话活跃集群** 的配置）。  
   - 用户不确定有哪些检查项、或只要跑全量默认巡检时，**可跳过** `check_list` 直接执行 `check_cluster`。
   - 用户指定特定检查项（如"只跑 OBProxy 内存检查"）时，**先调 `check_list` 确认** 任务名或用例名存在再执行。
2. 调用 **`check_cluster`** 执行巡检，支持以下参数：
   - **Observer 侧**：`cases`（**套餐名**，来自 `observer_check_package.yaml` 的顶层 key，如 `"ad"`、`"k8s_basic"`、`"deep"`）、`observer_tasks`（**具体任务名或正则，分号分隔**，如 `"cluster.*"` 或 `"disk.data_disk_full;cluster.no_leader"`，优先级高于 `cases`）
   - **OBProxy 侧**：`obproxy_cases`（**套餐名**，如 `"proxy"`）、`obproxy_tasks`（**具体任务名或正则，分号分隔**，优先级高于 `obproxy_cases`）
   - 示例：`check_cluster(observer_tasks="cluster.*")` 或 `check_cluster(cases="k8s_basic")`
3. 需要落盘时传 **`store_dir`**。若用户点名 **非默认集群**，在 **`check_cluster`** 上传 **`cluster_config_path`**（短名如 `obdiag_test` 或完整 yml 路径）。
4. 若结果里给出报告路径或采集目录，可用 **`file_list`** / **`file_read`** 查看 HTML/文本报告摘要。

### 根因分析（RCA）

1. **症状 → scene 映射**：优先加载 **`obdiag-rca`** skill，按其"场景速查表"将用户描述的症状（如"合并卡住"、"断连"、"OOM"）匹配到正确 `scene` 名称；不要凭感觉猜测 scene。
2. 可选：调用 **`rca_list`** 确认该 scene 在当前环境中可用。
3. 调用 **`rca_run(scene="<scene_name>")`**；非默认集群加 **`cluster_config_path`**。
4. **rca_run 无结论时**：按 **`obdiag-rca`** 中对应场景的手动降级步骤继续分析，不要直接告知用户"无法定位"。

## 阶段完成标准（SOP）

| 流程 | 完成标准 |
|------|----------|
| 巡检 | 已执行 **`check_cluster`**（或先 **`check_list`** 再执行）；结果中失败/告警已归纳；若有报告路径已 **`file_list`/`file_read`** 摘要或告知用户路径。 |
| RCA | 已执行 **`rca_run`** 且 **`scene`** 与现象匹配；结论与建议已输出。 |

## 自检清单

- [ ] 日志主路径未误用本 skill 代替 **`observer-log-analysis`** / **`obproxy-log-analysis`**。
- [ ] 需要 **另一集群** 的 `check_list`/`rca_list` 时，已 **`/use`** 切换或向用户说明当前列表对应哪套 config。
- [ ] **`check_cluster`/`rca_run`** 在点名集群时已传 **`cluster_config_path`**（若适用）。

## 多集群注意

- **`check_list` / `rca_list`** 使用会话 **当前 `config_path`**，不接收 `cluster_config_path`。若用户要列的是 **另一套集群** 的任务列表，需先在 agent 里 **`/use <name|path>`** 切换活跃集群（见 **`obdiag-multi-cluster`** skill），再调用 list。
- **`check_cluster` / `rca_run`** 支持 **`cluster_config_path`**，可在不切换会话的情况下定向某集群。

## 输出

- 用表格或列表归纳 **失败项 / 告警 / RCA 结论**；给出 **下一步**（例如继续 `gather_log`、`db_query`、或知识库查参数）。
