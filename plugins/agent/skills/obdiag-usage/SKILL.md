---
name: obdiag-usage
description: obdiag agent 工具使用指南：配置文件生成、gather/analyze/check/rca 各命令的调用方式、参数说明与典型场景。用户询问"怎么用 obdiag"、"如何采集日志"、"怎么生成配置"、"如何巡检/根因分析"，或 agent 自身不确定该调哪个工具时加载本 skill。
---

# obdiag Agent 工具使用指南

## 一、配置文件生成（generate_obdiag_config）

**触发**：用户尚无 `~/.obdiag/config.yml`，或明确要求生成/更新配置。

### 必填参数

| 参数 | 说明 |
|------|------|
| `ob_cluster_name` | 集群名称（任意字符串） |
| `db_host` | sys 租户连接 IP |
| `tenant_sys_password` | sys 密码（可为空字符串 `""`） |
| `nodes` | 节点列表，每项至少含 `ip` |

### 常用可选参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `db_port` | 数据库端口 | 2881 |
| `tenant_sys_user` | sys 用户 | `root@sys` |
| `global_ssh_username` | 所有节点统一 SSH 用户 | — |
| `global_ssh_password` | SSH 密码 | — |
| `global_ssh_port` | SSH 端口 | 22 |
| `global_ssh_key_file` | SSH 私钥路径 | — |
| `global_home_path` | OB 安装目录（如 `/home/admin/oceanbase`） | — |
| `global_data_dir` | 数据目录 | — |
| `global_redo_dir` | redo 日志目录 | — |
| `obproxy_cluster_name` + `obproxy_nodes` | 同时配置 OBProxy | — |

### 行动原则

1. 用户未提供必填项时，**逐一追问**，不要猜测。
2. 已有 `config.yml` 时，工具会自动备份为 `config.yml.backup_<timestamp>`。
3. 生成后用 `list_obdiag_clusters` 确认写入成功，再继续诊断。

### 示例

```
generate_obdiag_config(
    ob_cluster_name="prod-cluster",
    db_host="10.0.0.1",
    tenant_sys_password="",
    nodes=[{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}, {"ip": "10.0.0.3"}],
    global_ssh_username="admin",
    global_home_path="/home/admin/oceanbase"
)
```

---

## 二、多集群管理

| 工具 | 用途 |
|------|------|
| `list_obdiag_clusters` | 列出 `~/.obdiag/` 下所有集群配置文件 |
| `show_current_cluster` | 显示当前会话使用的配置路径 |

所有 gather/analyze/check/rca 工具均支持 `cluster_config_path` 参数（传短名如 `"test-cluster"` 或完整路径），用于切换到非默认集群。

---

## 三、Gather 采集工具

> 采集完成后，工具输出中会包含 `obdiag_gather_pack_*` 打包目录路径。
> 若用户同时要求分析，**在同一轮内**继续对该目录执行 `file_list` → `file_read` → 总结。

### 3.1 gather_log — 集群侧日志（observer/election/rootservice）

**仅用于 OceanBase 节点侧，不用于 OBProxy 或 OMS。**

| 参数 | 说明 |
|------|------|
| `since` | 相对时间窗，如 `"30m"`、`"1h"`、`"2d"` |
| `from_time` / `to_time` | 绝对时间 `"yyyy-mm-dd hh:mm:ss"` |
| `scope` | `observer` / `election` / `rootservice` / `all`（默认 all） |
| `grep` | 关键词或列表，trace_id 过滤传此参数 |
| `store_dir` | 输出目录 |
| `cluster_config_path` | 非默认集群名或路径 |

```
gather_log(since="30m", scope="observer", grep="ERROR")
gather_log(from_time="2024-01-01 10:00:00", to_time="2024-01-01 11:00:00", grep=["trace_id_xxx"])
```

### 3.2 gather_obproxy_log — OBProxy 日志

**仅用于 OBProxy 节点，不用于 observer。**

| 参数 | 说明 |
|------|------|
| `scope` | `obproxy` / `obproxy_limit` / `obproxy_stat` / `obproxy_digest` / `obproxy_slow` / `obproxy_diagnosis` / `obproxy_error` / `all` |
| `recent_count` | 仅采集最近 N 个日志文件 |
| 其余（`since`/`from_time`/`to_time`/`grep`/`store_dir`/`cluster_config_path`） | 同 gather_log |

### 3.3 gather_oms_log — OMS 组件日志

**需要 `config.yml` 中存在 `oms` 段，否则报错。**

| 参数 | 说明 |
|------|------|
| `scope` | `all` / `ghana` / `supervisor` / `cm` / `cdc` / `libobcdc` / `store` / `console` / `nginx` |
| `oms_component_id` | CDC 类采集必填，格式 `"IP-进程编号"`（如 `"192.168.1.1-1"`），在 OMS 控制台「组件管理」查询 |
| `temp_dir` | 远端临时目录 |
| `recent_count` | 仅采集最近 N 个日志文件 |
| 其余（`since`/`from_time`/`to_time`/`grep`/`store_dir`/`cluster_config_path`） | 同 gather_log |

### 3.4 gather_sysstat — 系统资源快照

采集各节点 CPU、内存、磁盘、网络等系统指标，无时间范围参数。

```
gather_sysstat()
gather_sysstat(store_dir="/tmp/sysstat_out")
```

### 3.5 gather_perf — 性能剖析（火焰图/pstack）

采集实时性能数据，适用于高 CPU/卡顿场景。无额外必填参数。

```
gather_perf()
```

### 3.6 gather_ash — 活跃会话历史（ASH）

| 参数 | 说明 |
|------|------|
| `from_time` / `to_time` | 时间范围 |
| `trace_id` | 按 trace 过滤 |
| `sql_id` | 按 SQL_ID 过滤 |
| `wait_class` | 等待事件类型 |
| `report_type` | `TEXT`（默认）或 `HTML` |
| `svr_ip` / `svr_port` | 指定节点 |
| `tenant_id` | 租户 ID |

### 3.7 gather_awr — AWR 报告

| 参数 | 说明 |
|------|------|
| `since` / `from_time` / `to_time` | 时间范围 |
| `cluster_name` / `cluster_id` | 覆盖集群标识 |
| `store_dir` | 输出目录 |
| `cluster_config_path` | 非默认集群名或路径 |

### 3.8 gather_plan_monitor — SQL 执行计划监控

**仅用于采集指定 trace_id 的执行计划，不用于"收集日志"。收日志请用 gather_log(grep=trace_id)。**

| 参数 | 说明 |
|------|------|
| `trace_id` | SQL trace ID（必填） |
| `store_dir` | 输出目录 |
| `cluster_config_path` | 非默认集群名或路径 |

```
gather_plan_monitor(trace_id="Y123456789-0001")
```

---

## 四、Analyze 分析工具

### analyze_log — 集群侧日志分析

**仅支持 observer/election/rootservice，不支持 OBProxy 和 OMS 日志。**

| 参数 | 说明 |
|------|------|
| `files` | 离线日志文件路径列表（跳过在线采集） |
| `from_time` / `to_time` | 分析时间范围 |
| `scope` | `observer` / `election` / `rootservice` / `all` |
| `log_level` | 最低日志级别：`DEBUG/TRACE/INFO/WDIAG/WARN/EDIAG/ERROR` |
| `cluster_config_path` | 非默认集群 |

**典型用法**：先 `gather_log` 得到 pack 目录，再 `analyze_log` 做统计分析，最后 `file_read` 读原始行级内容。

```
analyze_log(scope="observer", from_time="2024-01-01 10:00:00", to_time="2024-01-01 11:00:00")
analyze_log(files=["/tmp/obdiag_gather_pack_xxx/observer.log"], log_level="ERROR")
```

---

## 五、Check 巡检工具

### 5.1 check_list — 列出可用巡检项

在执行 check 前，可先调用 `check_list` 了解有哪些巡检任务。

```
check_list()
```

### 5.2 check_cluster — 执行巡检

| 参数 | 说明 |
|------|------|
| `cases` | observer **套餐名**（来自 `observer_check_package.yaml` 的顶层 key，如 `"ad"`、`"k8s_basic"`、`"deep"`）；与 `observer_tasks` 互斥，`observer_tasks` 优先 |
| `obproxy_cases` | OBProxy **套餐名**（来自 `obproxy_check_package.yaml`，如 `"proxy"`）；与 `obproxy_tasks` 互斥 |
| `observer_tasks` | 指定 observer **具体任务名或正则**（**分号**分隔），如 `"cluster.*"` 或 `"disk.data_disk_full;cluster.no_leader"` |
| `obproxy_tasks` | 指定 OBProxy **具体任务名或正则**（**分号**分隔） |
| `store_dir` | 输出目录 |
| `cluster_config_path` | 非默认集群的短名或完整路径 |

所有参数均省略 → 加载全部任务，排除 `filter` 套餐中的项，执行完整默认巡检套件。

```
check_cluster()                                              # 全量巡检（排除 filter 套餐）
check_cluster(cases="k8s_basic")                            # 跑 k8s_basic 套餐中的所有任务
check_cluster(observer_tasks="cluster.*")                   # 只跑 cluster 目录下所有任务（正则匹配）
check_cluster(observer_tasks="disk.data_disk_full;cluster.no_leader")  # 指定多个任务（分号分隔）
check_cluster(cases="k8s_basic", cluster_config_path="prod")           # 指定非默认集群
```

---

## 六、RCA 根因分析

### 6.1 rca_list — 列出 RCA 场景

```
rca_list()
```

### 6.2 rca_run — 执行 RCA

| 参数 | 说明 |
|------|------|
| `scene` | RCA 场景名（从 rca_list 或 `obdiag-rca` skill 场景速查表获取） |
| `cluster_config_path` | 非默认集群 |

> **注意**：agent 工具 `rca_run` 不支持 `--env` 参数。部分 scene 在 CLI 层需要 `--env`，通过 agent 调用时直接传 `scene` 即可；若因缺少 env 参数导致失败，进入 `obdiag-rca` 手动降级步骤。

```
rca_run(scene="disconnection")
rca_run(scene="major_hold")
```

**行动原则**：
1. 用户描述故障症状时，加载 **`obdiag-rca`** skill，按其"场景速查表"将症状（如"合并卡住"、"断连"、"OOM"）映射到正确 `scene` 名称；再调 `rca_run`。
2. 不确定 scene 时可先调 `rca_list` 确认 scene 在当前环境中可用。
3. `rca_run` 无结论时，按 **`obdiag-rca`** 的手动降级步骤继续分析，不要直接告知用户"无法定位"。
4. 完全无匹配场景时，转向 `gather_log` + `analyze_log` 手动排查。

---

## 七、辅助工具

### tool_io_performance — 磁盘 IO 检测

```
tool_io_performance(disk="sda")          # 检测 sda 盘
tool_io_performance(disk="data")         # 检测 data 盘
tool_io_performance(date="20240101")     # 查历史数据
```

### tool_sql_syntax — SQL 语法/语义检验

使用 EXPLAIN 验证，**不实际执行 SQL**。

| 参数 | 说明 |
|------|------|
| `sql` | 单条 SQL 语句（必填） |
| `env` | 可选连接覆盖，字符串列表，格式 `["host=127.0.0.1", "port=2881", "user=root@sys"]` |
| `cluster_config_path` | 非默认集群名或路径 |

```
tool_sql_syntax(sql="SELECT * FROM t1 WHERE id = 1")
tool_sql_syntax(sql="SELECT * FROM t1", env=["host=10.0.0.1", "port=2881"])
```

---

## 八、工具选择速查表

| 用户意图 | 应调用的工具 |
|----------|-------------|
| 首次使用，无配置文件 | `generate_obdiag_config` |
| 查看有哪些集群 | `list_obdiag_clusters` |
| 切换集群 | 在后续工具传 `cluster_config_path` |
| 收集 observer/election/rootservice 日志 | `gather_log` |
| 分析 observer 日志内容 | `analyze_log` |
| 收集 OBProxy 日志 | `gather_obproxy_log` |
| 收集 OMS/Ghana/CDC 日志 | `gather_oms_log` |
| 系统资源快照 | `gather_sysstat` |
| 火焰图 / pstack | `gather_perf` |
| 活跃会话历史 | `gather_ash` |
| AWR 性能报告 | `gather_awr` |
| 按 trace_id 收执行计划 | `gather_plan_monitor` |
| 集群健康巡检 | `check_list` → `check_cluster`（全量默认）；`cases=套餐名` 或 `observer_tasks=任务名/正则` 缩小范围 |
| 根因分析 | `rca_list` → `rca_run` |
| 磁盘 IO 检测 | `tool_io_performance` |
| SQL 语法验证 | `tool_sql_syntax` |

---

## 九、通用行动原则

1. **配置优先**：调任何 gather/analyze/check/rca 前，先确认 `show_current_cluster` 返回有效路径；无效则引导用户生成配置。
2. **采集+分析链式**：用户同时要求"采集+分析"时，**在同一轮内**完成：gather → file_list → file_read/analyze_log → 总结，不得仅 gather 后停止。
3. **日志边界严守**：`analyze_log` 仅用于 observer 侧；OBProxy 日志用 `gather_obproxy_log` 后 file_read；OMS 日志用 `gather_oms_log` 后 file_read。
4. **时间窗默认值**：用户未指定时间时默认 `since="30m"`，或询问用户。
5. **零命中处理**：analyze_log 或 file_read 无匹配时，不要回复"未发现问题"——建议扩大 `since`、去掉 `grep` 过滤或换 `scope`。
6. **只读原则**：`db_query` 仅执行只读 SQL；涉及 `ALTER SYSTEM` 等变更时，输出**建议 SQL** 由 DBA 手工执行。
