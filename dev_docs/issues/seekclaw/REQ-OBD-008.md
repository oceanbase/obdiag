# REQ-OBD-008 - L2 命令参数校验

> **类型**: Security Enhancement | **优先级**: 低 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 1 天 | **实现方**: SeekClaw

---

## 需求描述

对 ObdiagRemoteExecutor 白名单命令，增加参数白名单校验（禁止 --store_dir 等注入风险），避免 `$(...)` 等命令替换。

## 背景

- SeekClaw L2 模式允许授权客户远程执行 obdiag 命令
- 若不对参数校验，恶意用户可能通过 `--store_dir=$(malicious)` 等注入
- 需在 SeekClaw 侧对下发的 obdiag 命令做参数白名单校验

## 说明

**本需求由 SeekClaw 项目实现**，非 obdiag 源码修改。obdiag 需确保：

1. 文档明确各命令的合法参数及格式
2. 若存在 ObdiagRemoteExecutor 或类似调用方，可提供参数规范供校验参考

## Acceptance Criteria（SeekClaw 侧）

1. **白名单**：仅允许预定义的 obdiag 子命令及参数（如 `rca run --scene=xxx`、`analyze log --files xxx`）
2. **禁止**：`--store_dir`、`--temp_dir` 等可写路径参数；`$(...)`、`` `...` `` 等命令替换
3. **校验失败**：拒绝执行并返回明确错误

## obdiag 侧可配合工作

- 提供「L2 安全命令清单」文档，列出允许远程执行的命令及合法参数
- 若 obdiag 有 ObdiagRemoteExecutor 相关代码，可标注需校验的参数

## 涉及文件（SeekClaw）

- SeekClaw 项目中的 ObdiagRemoteExecutor、命令解析与校验逻辑

## 预估工时

1 天（SeekClaw 侧）
