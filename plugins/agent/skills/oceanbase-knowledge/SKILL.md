---
name: oceanbase-knowledge
description: 官方 OceanBase 知识库（文档/参数/概念）。用户问手册级问题、参数含义、架构与运维说明时使用；不用于查询实时集群状态。需在 agent.yml 中开启 oceanbase_knowledge.enabled 并配置 bearer_token。网关 POST /retrieval；component 仅允许固定枚举（见正文，默认 oceanbase）。
---

# OceanBase 知识库查询

## 网关约定（与 `query_oceanbase_knowledge_base` 一致）

- **Method / path：** `POST {GATEWAY_BASE_URL}/gateway/retrieval`（`GATEWAY_BASE_URL` 为 origin，无尾部斜杠）。
- **鉴权：** `Authorization: Bearer {access_key}`，与 `agent.yml` 中 `oceanbase_knowledge.bearer_token` 一致。
- **JSON body：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `query` | string | 否 | 主问题；网关侧通常有长度上限（工具侧合并 `context_text` 后总长度 ≤4096） |
| `component` | string | 否 | **仅允许下列取值之一**（勿自定义）；省略时工具默认 **`oceanbase`** |
| `version` | string | 否 | 文档或产品线版本，如 Observer 版本（最大 64 字符） |

- **源码中的默认底座：** `src/handler/agent/toolsets/knowledge_base.py` 内 `OCEANBASE_KNOWLEDGE_GATEWAY_BASE`；可用环境变量 **`OCEANBASE_KNOWLEDGE_GATEWAY_BASE`** 覆盖（无尾部斜杠），便于联调内网网关。优先级：环境变量 > `agent.yml` > 代码默认值。

### `component` 可选项（仅此列表，勿用其它字符串）

与网关约定一致，源码常量 **`OCEANBASE_KNOWLEDGE_ALLOWED_COMPONENTS`** 与之同步：

`oceanbase-standalone`，`oceanbase-kv`，`connector-c`，`connector-j`，`connector-odbc`，`ecob`，`oas`，`oat`，`ob-operator`，`obci`，`obd`，`obdiag`，`ob-loader-dumper`，`oblogproxy`，`ocp`，`odc`，`odm`，`odp`，`oma`，`oms`，`tugraph`，`oceanbase_cloud`，`oceanbase`。

未指定时默认 **`oceanbase`**（库内通用文档检索）。问题明确对应 **obdiag 工具链**、**OCP**、**OMS**、**ODC** 等时，将 **`component`** 设为表中**对应**项（如纯 obdiag 手册可用 **`obdiag`**）。

### 手动 curl 示例（勿提交真实密钥）

```bash
curl -sS -X POST "${GATEWAY_BASE_URL}/gateway/retrieval" \
  -H "Authorization: Bearer ${ACCESS_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "test query",
    "component": "oceanbase",
    "version": "1.0.0"
  }'
```

## 使用场景

- 用户 **明确要求查知识库 / 官方文档**（如「查询知识库」「查一下知识库」「文档里怎么说」）：**必须先调用** `query_oceanbase_knowledge_base` 再作答，不要仅凭常识推断「这类问题知识库没有」。
- 用户问 **文档里怎么说**：某参数含义、默认值、是否可动态改
- **概念与架构**：租户、Unit、副本、日志流、合并等与官方表述一致的定义
- **运维与最佳实践**：备份恢复、升级注意点、推荐配置（以知识库为准）
- 用户贴了 **错误片段 / 配置片段**，希望结合官方资料解释时：用 `context_text` 把原文一并传给知识库

## 不要用知识库代替的场景

- **当前集群实况**：版本号、是否在跑、实时指标 → 用 `db_query`、`show_current_cluster`、obdiag 工具等
- **本机文件、采集包路径、SSH 执行结果** → 用 `file_list` / `file_read` / `run_shell` / gather 类工具
- 需要 **执行诊断命令或 SQL** → 用对应 obdiag / `db_query` 工具

## 版本与组件信息（强烈建议写入 query）

官方文档与行为常随 **Observer / OBProxy / OMS** 等版本变化。只要用户已知或你已从 `show_current_cluster`、`db_query` 等拿到版本，**应把版本写进主问题 `query`**，便于检索到对应文档与排错路径。

- **推荐写法**：在一句里同时写清「问什么 + 各组件版本」，例如：  
  `怎么查询断连接问题，observer 版本 4.3.0.0，obproxy 版本 4.3.0.0`  
  （连接异常、会话断开等同义问题同样适用；用户若说「断链接」等口语，可原样写入 `query`。）
- **仅有部分版本**：写上已知的即可，并注明未知（如「OBProxy 版本未知」），避免模型默认成错误代际。
- **超长时**：合并后的 `query`（含 `context_text`）须 ≤**4096 个字符**（Python `len()` 计量，即 Unicode 字符数，中文每字算 1 个字符）；核心问题保留在 `query`，长日志、配置段放到 **`context_text`**。

## 工具与调用

- 工具名：**`query_oceanbase_knowledge_base`**
- **`query`**：主问题；宜短而准。**已知 Observer/OBProxy 等版本**可写进本字段，或使用下面的 **`version`**。
- **`context_text`**（可选）：附加材料；与 `query` 拼接后发给网关（共享 4096 字符上限）。
- **`component`**（可选）：**必须为上一节列表中的某一个**；省略时默认 **`oceanbase`**。
- **`version`**（可选）：版本号字符串（如 `4.3.0.0`），对应网关 JSON 的 **`version`** 字段。

## 配置前提

- 在 **`~/.obdiag/config/agent.yml`** 中设置 **`oceanbase_knowledge.enabled: true`** 后，才会注册 **`query_oceanbase_knowledge_base`**；否则工具不可用，请引导用户查阅官网文档，**勿编造知识库原文**。
- 同时配置非空 **`oceanbase_knowledge.bearer_token`** 后才会请求官方网关；已启用但未配 token 时，调用工具会返回如何开通的说明。

## 输出与后续

- 回答基于官方知识库检索，**引用时说明来自知识库**；与现场现象冲突时，以 **实际诊断数据** 为准并说明差异。
- 若上游响应中仍包含 **`conversation_id`**（兼容旧形态），可按返回说明做追问；当前网关以 **`/retrieval`** 为主，多轮行为以线上实现为准。
