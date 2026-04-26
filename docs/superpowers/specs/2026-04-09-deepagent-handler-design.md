# obdiag agent — Deep Agents SDK 迁移设计

> 日期：2026-04-09
> 分支：change-agent-cli-0409（独立分支，激进替换，不保留旧实现）

---

## 背景与目标

当前 `src/handler/agent/` 基于 pydantic-ai，存在以下问题：

- `handler.py`（1400+ 行）：history 截断、DeferredToolRequests 审批循环、session 序列化手写实现，复杂且有已知 bug
- pydantic-ai toolset 依赖 `RunContext` 依赖注入，耦合度高
- 无 subagent 并行能力、无原生 compact/summarization

**目标：** 用 `deepagents` SDK（`create_deep_agent()`，基于 LangGraph）完整替换实现。

已验证 SDK 版本：`deepagents==0.5.1`，`langchain==1.2.15`，`langgraph==1.1.6`

---

## 实际 SDK API（已验证）

```python
from deepagents import create_deep_agent
from deepagents.backends import LocalShellBackend
from langgraph.checkpoint.memory import MemorySaver

agent = create_deep_agent(
    model=model,                 # BaseChatModel 实例，或 "provider:model" 字符串
    tools=[fn1, fn2, ...],       # plain Python 函数列表（langchain @tool 或普通函数）
    system_prompt="...",         # 拼接在 SDK 基础 prompt 前
    backend=LocalShellBackend(), # 启用 execute（run_shell）；StateBackend 禁用 shell
    checkpointer=MemorySaver(),  # session 持久化；True = in-memory；False = 无持久化
    interrupt_on={               # human-in-the-loop：tool name -> True/False/InterruptOnConfig
        "gather_log": True,
        "rca_run": True,
    },
    skills=["~/.obdiag/agent/skills"],  # SKILL.md 目录列表
)
# 返回 CompiledStateGraph，接口：
result = agent.invoke(
    {"messages": [{"role": "user", "content": "..."}]},
    config={"configurable": {"thread_id": "session_id"}},
)
# 流式：
for chunk in agent.stream(..., stream_mode="updates"):
    ...
```

**内置工具（自动注册，无需实现）：**
`write_todos`, `ls`, `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `execute`, `task`

---

## 架构

### 切换点

```python
# src/common/core.py:631
from src.handler.agent.handler import AiAgentHandler as AiAssistantHandler
```

`handler.py` 重写为 Deep Agents 实现，`AiAgentHandler` 类名不变，`core.py` 无需改动。

### 目录结构（替换后）

```
src/handler/agent/
  __init__.py              # 导出 AiAgentHandler（不变）
  handler.py               # 重写：~350 行，DeepAgents 实现
  models.py                # 保留：AgentDependencies、AgentConfig（复用，去掉 pydantic-ai 导入）
  config.py                # 保留：load_agent_config（复用）
  executor.py              # 保留：execute_obdiag_command、OBDIAG_COMMANDS（复用）
  cluster_resolve.py       # 保留：集群路径解析（复用）
  tool_output_limits.py    # 保留：truncate_for_agent（复用）
  toolsets/
    __init__.py            # 重写：导出工厂函数
    obdiag.py              # 重写：create_obdiag_tools(config_path_getter, stdio) -> list
    database.py            # 重写：create_db_tools(deps_getter) -> list
    config_gen.py          # 重写：create_config_gen_tools(stdio) -> list
    knowledge_base.py      # 重写：create_knowledge_tools(token_getter) -> list
    # file_ops.py          # 删除：使用 DeepAgents 内置文件工具
    # sql_validator.py     # 保留：validate_sql()，被 database.py 调用

# 删除的文件：
  agent.py                 # 删除：pydantic-ai Agent 定义
```

---

## 组件设计

### 1. 工具工厂（`toolsets/*.py`）

每个工具文件暴露一个工厂函数，用闭包注入 session 级依赖（替换 RunContext）。工具函数是 **plain Python 函数**，DeepAgents 通过 docstring 提取工具描述。

```python
# toolsets/obdiag.py

def create_obdiag_tools(config_path_getter: Callable[[], str], stdio) -> list:
    """
    config_path_getter: () -> str
        返回当前活动的 cluster config 路径（闭包，支持 /use 切换后动态更新）
    """
    def gather_log(
        since: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
        scope: str = "all",
        grep: list[str] | None = None,
        store_dir: str | None = None,
        cluster_config_path: str | None = None,
    ) -> str:
        """Gather observer-side OceanBase logs (obdiag gather log).

        scope: observer/election/rootservice/all
        grep: list of keywords to filter
        cluster_config_path: optional short name or full path
        """
        cfg = cluster_config_path or config_path_getter()
        args = {k: v for k, v in {
            "since": since, "from": from_time, "to": to_time,
            "scope": scope, "grep": grep, "store_dir": store_dir,
        }.items() if v is not None}
        result = execute_obdiag_command("gather_log", args, cfg, stdio)
        return truncate_for_agent(
            format_command_output(result, "Log gathering completed.", "Log gathering failed."),
            label="obdiag",
        )

    # ... 其余 15 个工具同样模式

    return [
        gather_log, gather_obproxy_log, gather_oms_log,
        gather_sysstat, gather_perf, gather_ash, gather_awr,
        gather_plan_monitor, analyze_log, check_cluster, check_list,
        rca_run, rca_list, tool_io_performance, tool_sql_syntax,
        list_obdiag_clusters, show_current_cluster,
    ]
```

**关键设计：**
- `config_path_getter` 是 callable，不是值——`/use` 切换集群后工具自动使用新路径
- 不再有 `RunContext`，不再有 pydantic-ai 依赖
- 工具列表与旧版 16 个工具完全对应

### 2. DB 工具

```python
# toolsets/database.py

def create_db_tools(deps_getter: Callable[[], AgentDependencies]) -> list:
    def db_query(sql: str, cluster_config_path: str | None = None) -> str:
        """Execute a read-only SQL query (SELECT/SHOW/DESCRIBE/EXPLAIN only)."""
        deps = deps_getter()
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            return error_msg
        connector = deps.get_db_connector(cluster_config_path)
        # ... 执行查询
    return [db_query]
```

### 3. AiAgentHandler（`handler.py`，~350 行）

```python
class AiAgentHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options
        self._deps: AgentDependencies | None = None
        self._agent: CompiledStateGraph | None = None
        self._thread_id: str = ""
        self._config_path_ref: dict = {"v": ""}  # 闭包可变引用，支持 /use

    def handle(self) -> ObdiagResult:
        config_dict = load_agent_config(stdio=self.stdio)
        self._init_agent(config_dict)
        msg = Util.get_option(self.options, "m")
        if msg:
            return self._run_single_shot(msg)
        return self._run_interactive(config_dict)
```

**`_init_agent`：**

```python
def _init_agent(self, config_dict: dict):
    config = AgentConfig.from_dict(config_dict)

    # 构建 LangChain 模型
    from langchain.chat_models import init_chat_model
    model = init_chat_model(
        model=f"{config.provider}:{config.model}",
        api_key=config.api_key or None,
        base_url=config.base_url or None,
    )

    # 构建 deps
    cluster_config = read_obcluster_config(OBDIAG_CONFIG_PATH)
    self._deps = AgentDependencies(
        cluster_config=cluster_config,
        stdio=self.stdio,
        config_path=OBDIAG_CONFIG_PATH,
        oceanbase_knowledge_bearer_token=config.oceanbase_knowledge_bearer_token,
    )
    self._config_path_ref["v"] = self._deps.config_path

    def config_path_getter() -> str:
        return self._config_path_ref["v"]

    def deps_getter() -> AgentDependencies:
        return self._deps

    # 收集工具
    tools = (
        create_obdiag_tools(config_path_getter, self.stdio)
        + create_db_tools(deps_getter)
        + create_config_gen_tools(self.stdio)
        + (create_knowledge_tools(lambda: config.oceanbase_knowledge_bearer_token)
           if config.oceanbase_knowledge_enabled else [])
    )

    # tool approval：需审批的工具列表
    _APPROVAL_TOOLS = [
        "gather_log", "gather_obproxy_log", "gather_oms_log", "gather_sysstat",
        "gather_perf", "gather_ash", "gather_awr", "gather_plan_monitor",
        "analyze_log", "check_cluster", "rca_run", "db_query",
    ]
    interrupt_on = {t: True for t in _APPROVAL_TOOLS} if config.tool_approval else {}

    # skills
    skills = None
    if config.skills_enabled:
        sd = config.skills_directory or DEFAULT_SKILLS_DIRECTORY
        if os.path.isdir(sd):
            skills = [sd]

    self._agent = create_deep_agent(
        model=model,
        tools=tools,
        system_prompt=_build_system_prompt(config),
        backend=LocalShellBackend(),
        checkpointer=MemorySaver(),
        interrupt_on=interrupt_on or None,
        skills=skills,
    )
```

**交互循环：**

```python
def _run_interactive(self, config_dict: dict) -> ObdiagResult:
    self._thread_id = _new_thread_id()
    _print_welcome(config_dict, self.stdio, self.console)
    prompt_str = config_dict.get("ui", {}).get("prompt", "obdiag agent> ")

    while True:
        try:
            user_input = self._read_input(prompt_str)
        except (EOFError, KeyboardInterrupt):
            break
        if not user_input.strip():
            continue

        # slash 命令处理
        done = self._handle_slash(user_input, config_dict)
        if done is True:
            break
        if done is False:
            continue

        # 发送给 agent，流式渲染
        self._chat_and_render(user_input)

    return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent session ended"})
```

**流式渲染（`_chat_and_render`）：**

```python
def _chat_and_render(self, user_input: str):
    config = {"configurable": {"thread_id": self._thread_id}}
    response_text = ""

    for chunk in self._agent.stream(
        {"messages": [{"role": "user", "content": user_input}]},
        config=config,
        stream_mode="updates",
    ):
        # chunk 是 {"node_name": state_update} 格式
        for node_name, update in chunk.items():
            if node_name == "__interrupt__":
                # tool approval 请求
                response_text = self._handle_interrupt(update, config)
                return
            messages = (update or {}).get("messages", [])
            for msg in messages:
                if hasattr(msg, "content") and isinstance(msg.content, str):
                    # tool call trace
                    if hasattr(msg, "tool_calls") or hasattr(msg, "tool_use"):
                        _print_tool_trace(msg, self.stdio, config_dict=None)
                    else:
                        response_text += msg.content

    _render_response(response_text, self.console, self.stdio)
```

### 4. Interrupt（Tool Approval）处理

当 `interrupt_on` 配置的工具被调用时，`stream()` 会返回含 `__interrupt__` 的 chunk。

```python
def _handle_interrupt(self, interrupt_value, config: dict) -> str:
    """显示 tool approval 请求，获取用户决策，resume agent。"""
    # interrupt_value 包含 tool name 和 args
    tool_name = interrupt_value.get("tool", "?")
    args_preview = json.dumps(interrupt_value.get("args", {}), ensure_ascii=False)[:200]
    self.stdio.print(f"\n[Tool approval] {tool_name}: {args_preview}")
    decision = input("Execute? [y/n/edit]: ").strip().lower()

    from langgraph.types import Command
    if decision == "y":
        resume_cmd = Command(resume={"decision": "approve"})
    elif decision == "n":
        resume_cmd = Command(resume={"decision": "reject"})
    else:
        return "Tool cancelled."

    response_text = ""
    for chunk in self._agent.stream(resume_cmd, config=config, stream_mode="updates"):
        for node_name, update in chunk.items():
            for msg in (update or {}).get("messages", []):
                if hasattr(msg, "content") and isinstance(msg.content, str):
                    response_text += msg.content
    return response_text
```

### 5. Slash 命令

保留对用户有价值的命令：

| 命令 | 实现方式 |
|---|---|
| `/help`, `/?` | 打印帮助文本 |
| `/exit`, `/quit`, `/q` | break 循环 |
| `/use <name>` | `self._deps.switch_cluster(name)` + 更新 `_config_path_ref["v"]` |
| `/cluster` | `self._deps.current_cluster_info()` |
| `/tools` | 列出注册工具名 |
| `/clear` | 重置 `_thread_id`（新 session） |
| `/save` | 打印 thread_id（checkpointer 自动持久化） |

删除：`/compact`（SDK 自动管理）、`/usage`（不再手动累计）、`/sessions`、`/history`

### 6. Single-shot 模式

```python
def _run_single_shot(self, msg: str) -> ObdiagResult:
    self._thread_id = _new_thread_id()
    config = {"configurable": {"thread_id": self._thread_id}}
    result = self._agent.invoke(
        {"messages": [{"role": "user", "content": msg}]},
        config=config,
    )
    text = result["messages"][-1].content if result.get("messages") else ""
    return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": text})
```

---

## 删除的复杂度（对比旧 handler.py）

| 组件 | 旧实现 | 新实现 |
|---|---|---|
| History 截断 | `_truncate_history_safe` ~60 行 | SDK 自动管理 |
| Auto-compact | `_compact_conversation` ~80 行 | `SummarizationMiddleware` 内置 |
| Tool 审批循环 | `DeferredToolRequests` 手动循环 ~60 行 | `interrupt_on` + `Command(resume=...)` |
| Session 序列化 | 手写 JSON + RunUsage ~100 行 | `MemorySaver` checkpointer |
| Usage 累计 | `_accumulate_run_usage` ~40 行 | 删除（不计入） |
| History API 兼容 | `_is_tool_only_message` 等 ~80 行 | SDK 内部 |
| pydantic-ai RunContext | 所有工具函数签名 | 闭包注入 |
| **总计削减** | **~420 行 + pydantic-ai 依赖** | |

---

## 依赖变更

**新增（已在 venv 安装）：**
```
deepagents==0.5.1
langchain>=1.2.15
langchain-anthropic
langchain-core
langgraph>=1.1.6
```

**移除：**
```
pydantic-ai
pydantic-ai-skills      # skills 改用 deepagents skills 参数
```

加入 `requirements.txt`（替换 pydantic-ai 相关条目）。

---

## 测试策略

1. `obdiag agent -m "列出集群"` → single-shot 返回正确结果
2. 交互模式：`/use <name>`, `/cluster`, `/tools` 命令正常
3. Tool approval：`gather_log` 触发 interrupt，y/n 决策正常恢复
4. 文件工具：`read_file`, `ls`, `grep` 内置工具可用
5. `shell execute`：`run_shell` 经由 `LocalShellBackend` 执行

---

## 不在本次范围内

- Session 跨进程持久化（`MemorySaver` 仅 in-process；跨 session 恢复需 `langgraph-checkpoint-sqlite`，单独迭代）
- LangSmith tracing（`LANGSMITH_TRACING=true` 环境变量即可，无需代码改动）
- MCP server 集成（旧版 pydantic-ai MCP 配置不直接映射；单独迭代）
- Knowledge base 工具（结构与旧版相同，直接移植）
