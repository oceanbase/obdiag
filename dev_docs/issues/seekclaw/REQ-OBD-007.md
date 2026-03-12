# REQ-OBD-007 - 无 config 的离线分析

> **类型**: Enhancement | **优先级**: 低 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 0.5 天

---

## 需求描述

analyze log --files、analyze pack 在无 config.yml 时也可运行（使用最小默认 config），降低客户使用门槛。

## 背景

- 部分客户环境可能未配置 obdiag config.yml
- 离线分析（--files、--pack_dir）理论上不需要集群配置
- 当前实现可能强依赖 config 导致报错

## Acceptance Criteria

1. **analyze log --files**：无 config 或 config 不完整时，使用最小默认值（如 file_number_limit=20、file_size_limit=2GB）正常运行
2. **analyze pack**：同上，不依赖 cluster_config、obproxy_config 等
3. **友好提示**：若因 config 缺失导致部分功能不可用，给出明确提示而非崩溃

## 实现要点

1. **默认 config**：在 handler 初始化时，若 inner_config 为空，注入最小默认 basic_config
2. **条件分支**：analyze_log、analyze_pack 中跳过依赖 SSH/OB 的逻辑，仅执行文件解析
3. **init_config**：允许 nodes 为空，后续逻辑根据「是否有节点」分支

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/handler/analyzer/analyze_log.py` | 无 config 时使用默认值，跳过 SSH 相关 |
| `src/handler/analyzer/analyze_pack.py` | 同上 |
| `src/common/core.py` | 加载 config 失败时允许继续（仅对 analyze 子命令） |

## 预估工时

0.5 天
