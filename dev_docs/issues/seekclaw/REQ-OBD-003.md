# REQ-OBD-003 - analyze log 结构化输出

> **类型**: Enhancement | **优先级**: 高 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 1-2 天

---

## 需求描述

`obdiag analyze log --files xxx --output json`，输出机器可读的 JSON（by_ret_code、findings、summary），便于 Agent 解析。

## 背景

- SeekClaw Agent 需解析 analyze log 结果做后续推理
- 当前 analyze log 仅输出文本/表格，无结构化 JSON
- Phase 1 MVP 可先基于此能力解析日志分析结果

## Acceptance Criteria

1. **命令形式**：`obdiag analyze log --files <file1> [file2 ...] --output json`（在线模式也支持 `--output json`）
2. **JSON 结构**：包含 `by_ret_code`（按错误码归并）、`findings`（问题摘要）、`summary`（统计信息）
3. **兼容**：`--output` 默认 text，保持现有交互体验；指定 json 时仅输出 JSON 到 stdout，不输出表格

## 实现要点

1. **AnalyzeLogHandler**：增加 `--output` 参数解析，json 模式下调用 `__export_json()`
2. **JSON Schema 示例**：
   ```json
   {
     "by_ret_code": {
       "-4013": { "count": 10, "sample": "...", "first_time": "...", "last_time": "..." },
       "-6004": { "count": 5, "sample": "...", "first_time": "...", "last_time": "..." }
     },
     "findings": [
       { "ret_code": "-4013", "severity": "ERROR", "summary": "内存不足", "count": 10 }
     ],
     "summary": { "total_errors": 15, "tenant_count": 2, "time_range": "..." }
   }
   ```
3. **复用**：`__parse_log_lines` 已有 error_dict、trace_id_list，在此基础上生成 JSON

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/handler/analyzer/analyze_log.py` | 增加 `--output` 参数，`__export_json()` 方法 |
| `src/common/diag_cmd.py` | `ObdiagAnalyzeLogCommand` 增加 `--output` 选项 |
| `src/handler/analyzer/analyze_log_offline.py` | 若独立实现，需同步支持 `--output json` |

## 预估工时

1-2 天
