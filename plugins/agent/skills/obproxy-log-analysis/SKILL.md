---
name: obproxy-log-analysis
description: 标准 SOP：OBProxy 日志采集（gather_obproxy_log）、包内解读（file_list/file_read/run_shell 解压）、可选巡检。用户提到 OBProxy/代理/obproxy_diagnosis 等时使用；与 observer 侧 observer-log-analysis 互斥 analyze_log。
---

# OBProxy 日志：采集与分析

## 与 observer 日志的区别

- **Observer 侧**（observer / election / rootservice）：收集用 `gather_log`，统计类分析用 `analyze_log`（`obdiag analyze log`）。
- **OBProxy**：收集用 **`gather_obproxy_log`**。**不要**用 `analyze_log` 分析 OBProxy 日志——`analyze_log` 在实现上固定拉取 **target=observer** 的日志，解析逻辑也面向 observer 日志格式，**不支持** OBProxy。

## 使用场景

- 用户要采集、查看、分析、解读 **OBProxy** / **代理** / **obproxy_*** 日志
- 用户提到 **obproxy_diagnosis**、**obproxy_error**、**obproxy_slow** 等 scope
- 用户已在某次采集中得到 `obdiag_gather_pack_*` 目录，想**分析其中 OBProxy 内容**

## 同一轮对话内链式完成（重要）

模型常在 **`gather_obproxy_log` 成功后就说「采集完成」并结束本轮**，**不会**自动再调 `file_list` / `file_read`。若用户一句里包含 **采集 + 分析/解读**，或明确要 **分析** OBProxy 日志，必须在 **同一轮 agent 运行里** 连续调用工具：`gather_obproxy_log` → `file_list`（采集输出目录）→ `file_read`（相关文件）→ 最后再用自然语言总结。仅当用户**只要采集、不要解读**时，才可以 gather 后直接回复。

## 推荐流程

### 1. 采集（需要从新从机器拉日志时）

- 调用 **`gather_obproxy_log`**（不要调用 `gather_log`）。
- 常用参数：
  - **`since`** / **`from_time`** + **`to_time`**：时间范围
  - **`scope`**：`obproxy`、`obproxy_diagnosis`、`obproxy_error`、`obproxy_slow`、`obproxy_digest`、`obproxy_stat`、`obproxy_limit`、`all` 等
  - **`grep`**：关键词过滤
  - **`store_dir`**：输出目录（可选）
- 执行完成后，记下返回结果中的**采集输出目录**（例如 `obdiag_gather_pack_...`）。

### 2. 解读已采集的包（用户说「分析 / 解读 / 看看」且已有采集目录时）

- **不要**为了「分析」再次 `gather_obproxy_log`，除非用户明确要求**重新拉取**（换时间窗、换 scope、或确认包里缺文件）。
- 在**已有采集目录**下：
  1. 使用 **`file_list`** 列出子目录与文件；必要时用 **`pattern`**（如 `*.tar.gz`、`*.zip`、`*diagnosis*`）缩小范围。
  2. **若为压缩包**（常见：`*.tar.gz`、`*.tgz`、`*.zip`）：日志往往在包内，**先解压再读**。
     - 使用 **`run_shell`**（需用户审批）在采集目录或子目录执行解压，例如：
       - `tar -tzf xxx.tar.gz | head`（先看包内结构）
       - `mkdir -p ./extracted && tar -xzf xxx.tar.gz -C ./extracted`
       - `unzip -l xxx.zip` / `unzip -q xxx.zip -d ./extracted`
     - 解压成功后对 **`./extracted`（或实际解压目录）** 再 **`file_list`** → **`file_read`**。
     - 若环境未提供 `run_shell`，则明确告诉用户在本机终端执行上述命令，解压后再用 `file_list`/`file_read` 指向解压后的路径。
  3. 对**明文日志文件**使用 **`file_read`**（注意单文件大小上限；过大时用 `run_shell` 执行 `head`/`tail`/`grep` 抽样，或让用户缩小范围）。
  4. 根据内容归纳错误、时间线、建议下一步（例如是否需扩大 `since`、换 `scope`、或结合 `check_cluster`）。

### 3. 结构化巡检（可选）

- 调用 **`check_cluster`** 工具，传入 **`obproxy_tasks`**（指定任务名，优先级高）或 **`obproxy_cases`**（指定检查项，优先级低）以运行 OBProxy 内置检查项：
  ```
  check_cluster(obproxy_tasks="obproxy_task1,obproxy_task2")
  # 或
  check_cluster(obproxy_cases="obproxy_check1,obproxy_check2")
  ```
  与「读原始 diagnosis 文本」不同，走的是 `obdiag check run` 的**内置规则引擎**。
- 不确定有哪些可用检查项时，先调 **`check_list`** 查看完整列表，再按需传参。

## 阶段完成标准（SOP）

| 阶段 | 完成标准 |
|------|----------|
| 1 采集 | 已调用 `gather_obproxy_log` 且掌握 **输出 pack 路径**（或明确失败原因）。 |
| 2 解读 | 已对目标目录 **`file_list`**，对明文日志 **`file_read`** 或 **`run_shell`** 解压/抽样；**不得**对 `.tar.gz`/`.zip` 直接当文本读。 |
| 3 巡检（若做） | 已执行对应 check 工具或已给出 CLI 替代说明。 |

## 自检清单

- [ ] 未使用 `gather_log` / `analyze_log` 处理 OBProxy 正文。
- [ ] 用户要「采集+分析」时，**同一轮**内已完成 gather → list/read（或解压）→ 总结。
- [ ] 已有全量包时，优先在包内定位 diagnosis，避免无意义重复 `gather_obproxy_log`。

## 工具速查

| 目的 | 工具 |
|------|------|
| 拉 OBProxy 日志 | `gather_obproxy_log` |
| 在采集结果目录里找文件 | `file_list` |
| 解压 `.tar.gz` / `.zip`、查看包内列表、`head`/`grep` 抽样 | `run_shell`（需用户确认，勿执行破坏性/无关命令） |
| 读取日志内容 | `file_read` |
| Observer 日志统计/分析 | `analyze_log`（**仅 observer 侧，勿用于 OBProxy**） |

## 常见错误

- 用 **`gather_log`** 采 OBProxy → 错误，应改为 **`gather_obproxy_log`**。
- 用 **`analyze_log`**「分析 OBProxy」→ 错误，应对采集目录 **`file_list` / `file_read`**（或由模型基于内容解读）。
- 用户已有全量 `gather_obproxy_log(..., scope=all)` 的包，又只说「分析 obproxy_diagnosis」→ 优先在包内 **定位 diagnosis 文件** 再读，避免无意义重复采集。
- 看见 **`.tar.gz` / `.zip` 仍直接用 `file_read`** → 错误；应先 **`run_shell` 解压** 或让用户解压，再读解压出的文本日志。
