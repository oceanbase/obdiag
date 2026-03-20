# REQ-OBD-002 - 诊断包解析命令

> **类型**: Feature | **优先级**: 高 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 3-5 天

---

## 需求描述

新增 `obdiag analyze pack --pack_dir=xxx [--output=json]`，识别 pack 目录结构，自动调用 analyze log 等子分析，输出统一诊断报告（health_score、findings、action_plan）。

## 背景

- SeekClaw 接收客户上传的 obdiag gather 包，需自动解析并生成结构化诊断结论
- 当前无「一键解析 pack」命令，需手动解压、指定文件、分别执行 analyze log 等
- 需统一输出格式便于 Agent 解析

## Acceptance Criteria

1. **命令形式**：`obdiag analyze pack --pack_dir=<解压后的 pack 根目录> [--output=json|text]`
2. **目录识别**：自动识别 obdiag gather 标准目录结构（observer_log、rootservice_log、election_log 等）
3. **自动调用**：内部调用 analyze log、analyze memory（若存在）等，无需用户指定文件列表
4. **统一报告**：输出包含 `health_score`（0-100）、`findings`（问题列表）、`action_plan`（建议动作）的 JSON 或文本报告

## 实现要点

1. **新建 analyze pack 子命令**：`ObdiagAnalyzePackCommand`，注册到 `obdiag analyze` 下
2. **Pack 结构解析**：遍历 pack_dir，识别 `obdiag_gather_pack_*` 或标准 gather 输出结构，定位 observer.log、rootservice.log 等
3. **编排调用**：按发现的日志类型调用 `AnalyzeLogHandler`（可复用 `analyze_log_offline` 逻辑）
4. **报告聚合**：汇总各子分析结果，生成统一 JSON schema

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/common/diag_cmd.py` | 新增 `ObdiagAnalyzePackCommand`，注册 analyze pack |
| `src/handler/analyzer/analyze_pack.py` | 新建，pack 解析与编排逻辑 |
| `src/common/core.py` | 注册 analyze_pack handler |
| `src/handler/analyzer/analyze_log.py` | 可被 analyze_pack 调用，支持 `--output json`（见 REQ-OBD-003） |

## 依赖

- REQ-OBD-003（analyze log 结构化输出）建议先实现，便于 pack 报告聚合
- REQ-OBD-004（统一 --log_dir）可简化 pack_dir 与 log_dir 的复用

## 预估工时

3-5 天
