---
name: oms-kafka-performance
description: 标准 SOP：OceanBase 社区版经 OMS 同步至 Kafka 的延迟、吞吐与调优。涵盖全量/增量并发与限速、KafkaSink、OMS 平台 limitator 与 JVM、源端 OB 与表结构策略；与 observer 日志、OBProxy、单条 SQL 深查 skill 分工明确。
---

# OceanBase 社区版 → Kafka（OMS）：性能排查与调优

## 使用场景

- 排查 OMS 同步到 Kafka 的 **延迟**、**吞吐不足** 或组件跟不上（如 Incr-Sync 落后 Store）。
- 调优 **全量导入（Full-Import）** 或 **增量同步（Incr-Sync）** 到 Kafka。
- 配置 Kafka 数据源、**KafkaSink** 的读写并发、RPS/BPS 限速、序列化与分区规则。
- 优化 OMS 平台线程、批处理、连接数与 **JVM**，或 OceanBase 服务端与表结构以配合同步负载。

## 与其它 skill 的分工

| 需求 | 不要用本 skill 代替 | 应使用 |
|------|---------------------|--------|
| Observer / election / rootservice 日志 | 本 skill | **`observer-log-analysis`**（`gather_log` / `analyze_log`） |
| OBProxy 日志 | 本 skill | **`obproxy-log-analysis`** |
| 单条 SQL 计划、trace、锁 | 本 skill | **`observer-sql-analysis`** |
| 时段整体 ASH/AWR、主机 perf | 本 skill | **`obdiag-performance`** |
| 官方文档参数、KafkaSink 版本差异 | 仅口述 | **`oceanbase-knowledge`**（`query` 中带 OMS/OB 版本） |

## 工具与取证（Agent）

- **OMS 日志 ≠ `analyze_log`**：`analyze_log` 仅 **observer 侧**。OMS / Ghana / CDC 用 Agent 工具 **`gather_oms_log`**（封装 `obdiag gather oms_log`），成功后在同一轮内对 pack 目录 **`file_list`** / **`file_read`**；遇 **`.tar.gz` / `.zip`** 先 **`run_shell`** 解压（需审批）— 流程对齐 **`obproxy-log-analysis`**。
- **`config.yml` 的 `oms` 段**：必须配置正确，否则 `gather_oms_log` 会失败。CDC 类 scope（如 `cdc`、`libobcdc`）常需 **`oms_component_id`**，格式为 **`IP-进程编号`**（例如 `192.168.1.100-1`）；具体值可在 OMS 控制台「组件管理」或 `obdiag gather oms_log --help` 查看。
- **`gather_oms_log` 失败排查**：优先检查 `config.yml` 中 `oms` 段（主机、端口、SSH 凭据）是否填写完整；其次确认 OMS 机器 SSH 可达且 obdiag 对应版本支持；必要时用 `run_shell` 在 OMS 机器本地查看日志路径。
- 若无 Agent、仅终端：可手动执行 `obdiag gather oms_log ...`；已有 `obdiag_gather_pack_*` 时直接用 **`file_list`** / **`file_read`**。

## 阶段 1：性能排查思路

- **看延迟**：关注 **`oms_sync_delay`** 等；若 Incr-Sync 落后 Store，延迟会逐渐拉大，从消费端并发与资源入手。
- **看瓶颈**：区分 **源端（OceanBase）读压**、**OMS 处理**、**目标端（Kafka）写压**；结合监控与限速配置反推。
- **看限速**：全量/增量均可配 RPS、BPS；实际吞吐还受源、目标与规格影响，限速多为上限与保护。

**完成标准**：能说明当前更怀疑哪一侧（OB / OMS / Kafka），并给出下一步（采日志、看指标、调参建议）。

## 阶段 2：OMS 平台侧调优（全量/增量通用）

- **并发与线程**：如调大 `limitator.platform.threads.number`（例：32→64）。
- **批处理**：如调大 `limitator.select.batch.max`（例：1200→2400）、`limitator.image.insert.batch.max`（例：200→400）。
- **连接数**：如调大 `limitator.datasource.connections.max`（例：50→200）。
- **JVM**：按机器资源增大堆（例：由 `-Xms16g -Xmx16g -Xmn8g` 调至更大规格）。
- **经验量级**：单并发网络流入约 1–2M、RPS 约千级，视字段与行宽而定。

**完成标准**：列出已建议项与风险提示（过高并发压垮源端或 Kafka）。

## 阶段 3：Kafka 同步专项配置

- **全量**：读取/写入并发（最大均 512 等，以文档为准）；全量 RPS/BPS 限速。
- **增量**：写入并发与增量 RPS/BPS；关注 **消息顺序** 策略（见官方文档）。
- **KafkaSink**：序列化（Default、Canal、Debezium、Avro 等）；分区规则（Hash、Table、One 等）。
- **注意**：并发逐步提高并结合监控。

## 阶段 4：OceanBase 源端（配合高负载）

- 日志压缩，减少 clog：`enable_clog_persistence_compress`（默认关）建议开启。
- 合并相关：`merge_thread_count`（建议 4–16，视核数）、`_mini_merge_concurrency`（建议 4–8）、`minor_merge_concurrency`（建议 4–8）。
- 转储/冻结与内存：关注 `freeze_trigger_percentage`（默认 70）与 `memstore_limit_percentage`（默认 50）；高写入时可适当上调。
- 必要时写入限流：`writing_throttling_trigger_percentage`（默认 60，降低可缓解写入压力）。
- **变更提醒**：上述参数均需 `ALTER SYSTEM SET ... = ...`，由 DBA 在维护窗口执行；调整前确认当前值（可通过 `db_query` 查 `gv$ob_parameters`）。

**说明**：Agent 默认 **`db_query` 只读**；涉及 `ALTER SYSTEM` 等变更时输出 **建议 SQL / 运维步骤**，由 DBA 在变更窗口执行。

## 阶段 5：表结构与迁移策略

- 分区表全量：可评估改为非分区（单表规模上限以官方与现场规范为准）；全量前可临时删二级索引，完成后重建。
- 大表、宽表：关注批大小与并发，避免单线程或小批次成瓶颈。

## 阶段 6：文档与社区

- 全量/增量调优、Kafka 数据源与 **KafkaSink** 以 **当前 OMS 版本文档** 为准。
- 延迟指标、顺序、认证（如 GSSAPI）等可查社区问答与云厂商帮助文档。
- 参数名随版本变化时，用 **`query_oceanbase_knowledge_base`** 核对，**query 中写明 OMS 与 OB 版本**。

## 输出与自检

- 输出：**瓶颈判断**、**可执行调优项**（分 OMS / Kafka / OB）、**下一步取证**（日志路径、指标、CLI）。
- **自检**：
  - [ ] 未用 `analyze_log` 充当 OMS 日志分析。
  - [ ] 用户要「采集 + 解读」OMS 日志时，是否在同一轮内调用 **`gather_oms_log`** + `file_list`/`file_read`（及必要时解压）？
  - [ ] 改参类建议是否标明 **需人工执行** 与 **风险**？
