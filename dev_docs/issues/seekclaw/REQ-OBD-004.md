# REQ-OBD-004 - 统一 --log_dir 参数

> **类型**: Enhancement | **优先级**: 中 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 1-2 天

---

## 需求描述

analyze log / analyze memory / rca 支持 `--log_dir` 指定解压后的 pack 目录，自动发现 observer.log 等标准路径。

## 背景

- 当前 analyze log 使用 `--files` 需用户手动指定文件列表
- analyze memory、rca 各有不同的输入方式
- 统一 `--log_dir` 可简化 SeekClaw 与用户的使用流程，一次指定目录即可

## Acceptance Criteria

1. **analyze log**：`obdiag analyze log --log_dir=./pack_dir/` 自动发现目录下 observer.log、rootservice.log、election.log 等
2. **analyze memory**：`obdiag analyze memory --log_dir=./pack_dir/` 自动发现相关日志
3. **rca**：`obdiag rca run --scene=xxx --log_dir=./pack_dir/` 自动发现所需日志（与 REQ-OBD-001 协同）
4. **路径约定**：按 obdiag gather 标准输出结构（如 `{log_dir}/observer_log/`、`{log_dir}/rootservice_log/`）或常见解压结构发现

## 实现要点

1. **公共工具**：新建 `src/common/pack_discovery.py` 或类似模块，`discover_log_files(log_dir)` 返回标准路径映射
2. **各 handler 集成**：analyze_log、analyze_memory、rca_handler 在 `--log_dir` 存在时，调用 discovery 获取文件列表，替代 `--files` 或 SSH 拉取
3. **优先级**：`--log_dir` 与 `--files` 可二选一；若同时指定，以 `--log_dir` 为准或报错提示

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/common/pack_discovery.py` | 新建，目录结构与日志发现逻辑 |
| `src/handler/analyzer/analyze_log.py` | 支持 `--log_dir`，调用 discovery |
| `src/handler/analyzer/analyze_memory.py` | 支持 `--log_dir` |
| `src/handler/rca/rca_handler.py` | 支持 `--log_dir`（与 REQ-OBD-001 协同） |
| `src/common/diag_cmd.py` | 各命令增加 `--log_dir` 选项 |

## 预估工时

1-2 天
