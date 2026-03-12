# REQ-OBD-001 - RCA 离线模式

> **类型**: Enhancement | **优先级**: 高 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 5-7 天

---

## 需求描述

支持 `obdiag rca run --scene=cluster_down --log_dir=./pack_dir/`，基于离线日志/coredump/dmesg 进行分析，**无需 SSH/OB 连接**。

SeekClaw 无法直接访问客户集群，诊断分析完全依赖客户上传的诊断包。RCA 需支持从本地 pack 目录读取日志、coredump、dmesg 等文件进行根因分析。

## 背景

- 当前 RCA 框架依赖 SSH 连接节点、OB 连接执行 SQL
- gather scene `observer.cluster_down` 已存在（cluster_down.yaml），可收集 ps/df/core_pattern 等
- 无 RCA cluster_down 场景；rca_handler 不支持 `--log_dir` 离线模式

## Acceptance Criteria

1. **命令形式**：`obdiag rca run --scene=cluster_down --log_dir=<解压后的 pack 目录>`
2. **数据来源**：从 `--log_dir` 指定目录自动发现 observer.log、rootservice.log、coredump、dmesg、message.log 等
3. **无需连接**：不依赖 config.yml 中的 SSH/OB 配置，可完全离线运行
4. **输出**：与在线 RCA 一致的根因分析报告（可扩展 `--output json`）

## 实现要点

1. **RCA 框架扩展**：rca_handler 支持 `--log_dir` 参数，当指定时进入离线模式
2. **离线上下文**：构建仅包含本地文件路径的 context，不初始化 SshClient、OBConnector
3. **新建 cluster_down RCA**：`plugins/rca/cluster_down.py`，基于 observer.log grep trace_id/segmentation/abort；coredump 用 gdb bt full；kill -62 生成 obstack；-4389、-4138 等错误码
4. **文件发现**：按 gather pack 标准目录结构（如 `{log_dir}/observer_log/`、`{log_dir}/rootservice_log/`）自动发现日志

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/handler/rca/rca_handler.py` | 支持 `--log_dir`，离线模式初始化逻辑 |
| `src/common/diag_cmd.py` | rca run 命令增加 `--log_dir` 选项 |
| `plugins/rca/cluster_down.py` | 新建，集群宕机离线 RCA 场景 |
| `plugins/gather/tasks/observer/cluster_down.yaml` | 参考现有 gather 结构，确保 RCA 可解析 |

## 版本兼容

- RCA 框架需支持离线模式，不影响现有在线场景
- 4.x 日志格式、coredump 解析兼容

## 预估工时

5-7 天
