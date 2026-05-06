---
name: obdiag-triage
description: 未知/未分类问题的统一分诊 SOP：用户描述集群异常但症状不明确、不知从何入手，或问题无法直接对应 rca/observer-log-analysis/observer-sql-analysis 等专项 skill 时，强制先执行全量巡检 check_cluster，再根据巡检结果决定下一步路径。
---

# 未知问题分诊 SOP

## 触发条件

以下任意一条满足即使用本 skill：

- 用户描述问题但症状模糊（"集群慢"、"有报错"、"不稳定"、"有问题"等）
- 无法判断应走 rca、log 分析还是 SQL 分析
- 用户询问"应该怎么排查"、"从哪里开始"
- 当前无任何诊断数据，缺乏基线信息

---

## 第一步：确认配置（必须）

在执行任何诊断前，先确认集群配置已就绪：

```
list_obdiag_clusters()
```

- 若无集群配置 → 调用 `generate_obdiag_config` 引导用户完成配置，再继续。
- 若有多集群 → 明确告知用户当前使用哪个集群，或询问目标集群。

---

## 第二步：执行全量巡检（核心步骤，不可跳过）

```
check_cluster()
```

**不加任何参数**，执行完整默认巡检套件。巡检覆盖：磁盘、内存、网络、租户、参数配置等。

> 即使用户已有明确怀疑方向，也应先跑全量巡检获取基线，避免遗漏根因。

---

## 第三步：解读巡检结果，决定下一步路径

根据 `check_cluster()` 输出，按以下优先级路由：

### 路径 A：巡检发现 CRITICAL / WARNING 项

直接针对告警项深入分析：

| 告警类型 | 后续动作 |
|----------|----------|
| 磁盘空间不足、IO 异常 | 明确要 RCA 时加载 **`obdiag-rca`**；需要手动 SOP 时加载 **`observer-storage-space-troubleshooting`**，再配合 `gather_sysstat()` / `tool_io_performance()` |
| 内存/memstore 告警 | `check_cluster(cases="memory")` 精查 + 加载 **`obdiag-rca`** skill 按症状选 scene（如 `memory_full`、`execute_memory_high`、`major_hold`）后调 `rca_run` |
| 网络相关告警 | `gather_log(since="1h", grep="ERROR")` 过滤网络错误 |
| 租户/Session 异常 | `check_cluster(cases="tenant")` + `gather_log` |
| 参数配置不合理 | 向用户说明该参数的推荐值与风险 |

### 路径 B：巡检正常，但用户仍有明确症状描述

按症状路由到专项 skill：

| 用户症状 | 加载的专项 skill |
|----------|-----------------|
| SQL 慢、执行计划异常 | `observer-sql-analysis` |
| 有具体错误日志 / trace_id | `observer-log-analysis` |
| OBProxy 日志采集/解读 | `obproxy-log-analysis` |
| OBProxy/ODP 路由、弱读、读写分离、连接配置 | `obproxy-routing-troubleshooting` |
| 日志盘/数据盘、-4184/-4264、文件系统、索引或副本空间 | `observer-storage-space-troubleshooting` |
| 备份、归档、恢复窗口、OSS/COS/NFS、oblogminer | `backup-archive-restore-troubleshooting` |
| 已知故障场景（断连、合并卡住等） | `obdiag-rca` skill → `rca_run` 优先 |
| 性能抖动、CPU 高 | `obdiag-performance` |

### 路径 C：巡检正常，症状描述也模糊

采集基线数据供进一步分析：

```
gather_log(since="1h", scope="observer", grep="ERROR")
gather_sysstat()
```

然后分析采集结果，定位有无异常日志，再决定是否继续深入。

---

## 行动原则

1. **巡检优先**：`check_cluster()` 是任何未知问题诊断的起点，不得因"感觉不相关"而跳过。
2. **不猜测**：在没有巡检结果前，不向用户提出"可能是 XXX 问题"的判断。
3. **逐步推进**：每一步工具调用完成后，向用户汇报结果要点，再决定下一步，不要一次调用大量工具。
4. **数据说话**：所有结论必须基于工具返回的实际数据，不基于用户主观描述直接下结论。
5. **超出能力范围**：若巡检和日志分析后仍无法定位，明确告知用户需要哪些额外信息（如 OB 版本、操作时间点、业务变更记录等）。

---

## 快速流程图

```
用户描述问题
    ↓
list_obdiag_clusters()  ← 确认配置
    ↓
check_cluster()         ← 全量巡检（不可跳过）
    ↓
┌─────────────────────────────────────┐
│ CRITICAL/WARNING?                   │
│  Yes → 针对告警项深入分析            │
│  No  → 症状明确? → 专项 skill        │
│        症状模糊? → gather_log +      │
│                   gather_sysstat    │
└─────────────────────────────────────┘
```
