# REQ-OBD-006 - 日志语义模式匹配

> **类型**: Enhancement | **优先级**: 中 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 2-3 天

---

## 需求描述

analyze log 或独立模块支持配置化 log_patterns（如 "disk is almost full"→P0），输出匹配到的模式及建议动作，与 ob-log-diagnoser 技能对齐。

## 背景

- SeekClaw 的 ob-log-diagnoser 技能需识别日志中的语义模式（如磁盘满、内存不足）
- 当前 analyze log 主要按 ret_code 归并，缺少语义级模式匹配
- 配置化便于扩展，无需改代码即可新增模式

## Acceptance Criteria

1. **配置化**：log_patterns 可配置（如 config.yml 或独立 yaml），格式如 `pattern: "disk is almost full"` → `severity: P0`、`suggestion: "扩容数据盘"`
2. **输出**：analyze log 输出中增加「语义匹配」小节，列出匹配到的模式、行号/上下文、建议动作
3. **与 ret_code 协同**：语义匹配与按 ret_code 归并并存，可交叉引用

## 实现要点

1. **Pattern 配置**：新建 `resources/log_patterns.yaml` 或纳入 config，支持正则与简单字符串
2. **匹配逻辑**：在 `__parse_log_lines` 或后处理阶段，对每行日志执行 pattern 匹配
3. **输出格式**：在现有报告/JSON 中增加 `semantic_matches` 数组
4. **与 ob-log-diagnoser 对齐**：参考 SeekClaw 技能定义的 pattern 列表，确保一致

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/handler/analyzer/analyze_log.py` | 集成 pattern 匹配，输出 semantic_matches |
| `resources/log_patterns.yaml` | 新建，默认 pattern 配置 |
| `src/common/constant.py` 或 config | 支持从配置加载 log_patterns |

## 预估工时

2-3 天
