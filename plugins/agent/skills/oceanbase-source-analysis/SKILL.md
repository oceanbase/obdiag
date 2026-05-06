---
name: oceanbase-source-analysis
description: OceanBase 社区版源码分析 SOP。当日志/RCA/巡检无法给出确定性结论时，通过读取本地 OceanBase 社区版源码进行精准验证。覆盖错误码溯源、函数/参数行为确认、调用链追踪、跨版本行为对比。需用户提供本地源码路径（源码约 3-5 GB）。
---

# OceanBase 源码分析 SOP

## 触发条件

以下任意一条满足时使用本 skill：

- 用户问某错误码（如 `-4013`）在什么条件下触发
- RCA / 日志分析已指向某模块，需源码级确认
- 用户问某参数或行为在源码中的实现细节
- 用户问某行为在两个版本间是否有变化
- 用户明确说"看一下源码"、"源码里怎么写的"

**不触发本 skill（路由到其他 skill）**：

| 需求 | 应用 |
|------|------|
| 参数含义、官方文档说明 | `oceanbase-knowledge` |
| 集群当前运行状态查询 | `db_query` |
| 日志有明确错误栈 | `observer-log-analysis` |
| RCA 场景化分析 | `obdiag-rca` |

---

## 第一步：获取源码路径

### 1.1 读取缓存路径

```
run_shell: cat ~/.obdiag/tmp/oceanbase_source_path 2>/dev/null
```

- **文件不存在或为空** → 跳到 1.2
- **文件存在，有内容（记为 `<cached_path>`）**：
  - 验证路径是否可达：`run_shell: ls <cached_path>/CMakeLists.txt <cached_path>/.git/config 2>/dev/null`
  - 路径可达 → 告知用户："检测到上次使用的源码路径：`<cached_path>`，是否继续使用？"
    - 用户确认 → 以 `<cached_path>` 为 `<src_path>`，进入第二步
    - 用户拒绝 → 跳到 1.2
  - 路径不可达 → 提示："上次记录的路径 `<cached_path>` 已失效，请重新提供"，跳到 1.2

### 1.2 向用户请求路径

向用户展示以下说明，**不得跳过**：

> **注意**：OceanBase 源码约 3-5 GB，请确保：
> 1. 已将源码 clone 到磁盘空间充足的稳定目录（不会被系统清理）
> 2. 该目录在本 session 期间不会被删除或移动
>
> 如尚未 clone，可执行（`--depth=1` 仅拉最新提交，节省空间）：
> ```bash
> git clone --depth=1 https://github.com/oceanbase/oceanbase.git <your_path>
> # 或指定版本：
> git clone --depth=1 --branch 4.3.5 https://github.com/oceanbase/oceanbase.git <your_path>
> ```
>
> 请提供源码所在目录的绝对路径：

用户提供路径后（记为 `<src_path>`），进入 1.3。

### 1.3 验证路径合法性

```
run_shell: ls <src_path>/CMakeLists.txt <src_path>/.git/config
```

- 任一文件不存在 → 报错："`<src_path>` 不是合法的 OceanBase git 仓库，请检查路径后重新提供"，返回 1.2
- 验证通过 → 将路径写入缓存：
  ```
  run_shell: mkdir -p ~/.obdiag/tmp && echo "<src_path>" > ~/.obdiag/tmp/oceanbase_source_path
  ```

---

## 第二步：版本验证与切换

### 2.1 获取源码当前版本

```
run_shell: git -C <src_path> describe --tags --exact-match 2>/dev/null || git -C <src_path> rev-parse --short HEAD
```

记录返回值为 `<src_version>`（如 `4.3.5`、`v4.3.5` 或短 commit hash）。

### 2.2 检查本地改动

```
run_shell: git -C <src_path> status --short
```

- 有输出 → ⚠️ 告知用户："检测到源码含本地未提交改动，分析结论仅供参考，不代表官方版本行为"（**不阻止后续分析**）

### 2.3 获取目标版本

**有在线集群时**：
```
db_query("SELECT version()")
```
取返回值前三段（如 `4.3.5.0` → 目标版本 `4.3.5`）。

**无在线集群时**：询问用户"请问您要分析的 OceanBase 版本是？"，用户给出版本号（记为 `<target_version>`）。

### 2.4 版本对比与切换

- `<src_version>` 与 `<target_version>` **匹配** → ✅ 直接进入第三步
- **不匹配** → 提示用户：
  > "当前源码版本为 `<src_version>`，目标集群版本为 `<target_version>`。建议切换源码版本以确保分析准确，是否执行 `git checkout <target_version>`？
  > **注意**：此操作会修改工作区文件，如有本地改动请先备份。"
  - 用户同意 → 执行切换（见 2.5）
  - 用户拒绝 → 在后续所有结论中注明版本差异："⚠️ 以下分析基于 `<src_version>` 源码，与集群版本 `<target_version>` 存在差异，结论仅供参考"

### 2.5 执行版本切换

先尝试不带 `v` 前缀：
```
run_shell: git -C <src_path> checkout <target_version>
```

失败时尝试带 `v` 前缀：
```
run_shell: git -C <src_path> checkout v<target_version>
```

两种格式均失败时，列出可用版本供用户选择：
```
run_shell: git -C <src_path> tag | grep -E "^[v]?4\." | sort -V | tail -30
```
提示用户从列表中选择最接近的版本后重试。

---

## 第三步：定位分析目标

根据用户问题，从以下模块地图确定搜索目录（**不得在仓库根目录直接 grep**）：

| 问题域 | 源码路径 |
|--------|---------|
| 错误码定义 | `deps/oblib/src/lib/ob_errno.h` |
| 事务管理 | `src/storage/tx/` |
| 内存表 / MemTable | `src/storage/memtable/` |
| 合并 / Compaction | `src/storage/compaction/` |
| SSTable / 存储引擎 | `src/storage/blocksstable/` |
| SQL 执行引擎 | `src/sql/engine/` |
| SQL 优化器 | `src/sql/optimizer/` |
| RootService | `src/rootserver/` |
| 日志同步 (Paxos/Raft) | `src/logservice/` |
| 参数定义 | `src/share/parameter/` |
| 系统变量 | `src/share/system_variable/` |
| 网络层 | `deps/oblib/src/rpc/` |
| 租户管理 | `src/rootserver/` (ob_tenant_manager*) |
| 锁管理 | `src/storage/tx/ob_lock_*` |

---

## 第四步：源码分析

### 档位一：单点查询（确认函数/错误码/参数）

```
# 1. 先列出命中文件（不全文输出，避免输出过大）
run_shell: grep -rn "<keyword>" <src_path>/<module_dir> \
           --include="*.h" --include="*.cpp" -l

# 2. 针对命中文件，读取关键片段
file_read <src_path>/<file>  (配合 offset + limit 精确定位)

# 3. 给出结论，注明来源
```

> **搜索量控制**：单次 grep 命中文件超过 50 个时，停止并提示用户缩小关键词或指定更精确的子目录。

### 档位二：调用链追踪

```
# 1. 找入口函数（症状关键词 grep）
run_shell: grep -rn "<symptom_keyword>" <src_path>/<module_dir> \
           --include="*.cpp" -n

# 2. 向下追踪（函数在 .cpp 中的实现）
run_shell: grep -rn "<function_name>" <src_path>/<module_dir> \
           --include="*.cpp" -n

# 3. 向上追踪（函数被哪里调用）
run_shell: grep -rn "<function_name>" <src_path>/src \
           --include="*.cpp" -l

# 4. file_read 关键文件片段，逐层推进
```

**调用链追踪上限**：最多追踪 4 层。超出后说明：
> "调用链超出静态分析范围，建议配合 `gather_perf`（`obdiag-performance` skill）做运行时 perf/pstack 采样，或结合 gdb 分析。"

### 跨版本行为对比

需要对比两个版本的行为时：

1. 记录当前分析完的文件路径和关键行
2. 提示用户切换到另一版本：执行 `git checkout <other_version>`（需用户审批）
3. 用相同路径和关键词重新读取
4. 对比差异，给出结论

---

## 输出规范

每次分析结论必须包含：

- **源码版本**：`基于 OceanBase <src_version> 社区版源码`
- **来源文件**：`<相对路径>:<行号>`
- **结论**：直接回答用户问题
- **版本差异警告**（如有）：注明与集群版本的差距

示例：
> 基于 OceanBase 4.3.5 社区版源码：
> 错误码 `-4013`（`OB_TIMEOUT`）在 `src/storage/tx/ob_trans_service.cpp:1247` 中，当事务超过 `ob_trx_timeout` 设定时间且未收到心跳时触发。
> ⚠️ 当前分析版本（4.3.5）与集群版本（4.3.3）存在差异，上述行为在 4.3.3 中可能不同。

---

## 自检清单

- [ ] 已验证路径是合法的 OceanBase 社区版 git 仓库
- [ ] 已检查并告知用户源码版本与集群版本的匹配情况
- [ ] grep 搜索指定了具体子目录，未在根目录全仓搜索
- [ ] 结论中注明了源码版本和来源文件路径
- [ ] 有本地改动时已向用户说明

---

## 企业版检测

若在 `CMakeLists.txt` 或 `cmake/init.cmake` 中发现企业版标志（如 `OCEANBASE_ENTERPRISE`、`OB_BUILD_ENTERPRISE`），立即告知：
> "⚠️ 检测到企业版源码特征，本 skill 仅支持社区版分析，继续分析可能产生偏差，建议使用社区版源码。"
并询问用户是否继续。
