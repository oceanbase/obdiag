---
name: obdiag-multi-cluster
description: 标准 SOP：多集群发现 list_obdiag_clusters、当前会话 show_current_cluster、/use 切换与 cluster_config_path 定向。多套 ~/.obdiag/*.yml、切换默认集群、或一句话点名某集群时使用。
---

# 多集群与会话

## 何时加载

- 用户问 **有哪些集群、配置文件、当前连的是哪个**
- 用户说 **用 obdiag_test、切到 xxx.yml、只查生产那套**
- 同一对话里 **不同请求针对不同集群** —— 必须在对应工具上带 **`cluster_config_path`**，避免误操作默认集群

## 工具

| 工具 | 作用 |
|------|------|
| **`list_obdiag_clusters`** | 列出 `~/.obdiag/` 下发现的集群配置文件（短名与路径） |
| **`show_current_cluster`** | 当前会话 **活跃** 的 `config_path` 及解析结果 |

## 决策：`/use` 还是只传 `cluster_config_path`？

| 用户诉求 | 推荐 |
|----------|------|
| 一次性对 **obdiag_test** 做 gather/check，**不改**会话默认 | 各工具传 **`cluster_config_path="obdiag_test"`**，**不必** `/use`。 |
| 之后 **`check_list` / `rca_list`** 也要针对 **另一套** 集群 | 先 **`/use`**（短名或 yml 完整路径）切换会话活跃配置，再 list（因 list 无 `cluster_config_path`）。 |
| 用户明确说 **以后默认用生产** | 在 agent 里执行 **`/use`** 加短名或完整路径切换。 |
| 同一轮对话里 **两个集群交替操作** | **每个工具调用**带对的 **`cluster_config_path`**，勿混用上一轮假设。 |

## 推荐流程

1. 用户不确定环境时：先 **`list_obdiag_clusters`**，再 **`show_current_cluster`** 确认默认值。
2. 用户已说 **「对 obdiag_test 做 xxx」**：对相关诊断工具传 **`cluster_config_path="obdiag_test"`**（或完整路径）；**无需**先切换，除非用户明确要求改会话默认。
3. 用户说 **「以后默认用某某」**：在 **obdiag agent 交互模式** 使用 **`/use <短名|完整路径>`**（行首斜杠内置命令）切换活跃集群；切换后 **`check_list` / `rca_list`** 等依赖当前配置的列表会与新区一致。

## `cluster_config_path` 规则（与实现一致）

- **短名**：如 `obdiag_test` → 解析为 `~/.obdiag/obdiag_test.yml`（可带或不带 `.yml` 后缀，按解析逻辑）
- **绝对路径**：直接传入扩展后的路径

**绝大多数 obdiag 工具**（`gather_*`、`check_cluster`、`rca_run`、`analyze_log`、`db_query` 等）都支持 **`cluster_config_path`**。

## 限制（避免漏传）

- **`check_list`、`rca_list`** **没有** `cluster_config_path` 参数，它们读取 **当前会话** 的 `config_path`。要看 **另一集群** 的清单时：先 **`/use` 切换**，或告知用户当前列表对应的是 **哪一个配置文件**。
- 一句话里混用两个集群时：**每个工具调用** 显式传入正确的 **`cluster_config_path`**，不要依赖上一轮残留假设。

## 与知识库

- 版本、文档问题仍用 **`query_oceanbase_knowledge_base`**；多集群只影响 **对哪套环境跑命令**，不改变知识库用法。

## 阶段完成标准与自检（SOP）

**完成标准**：用户已知 **有哪些配置**、**当前默认是哪套**、下一步操作应带 **`cluster_config_path` 还是已 `/use`**。

**自检**：

- [ ] 需要异集群的 **`check_list`/`rca_list`** 时是否已 `/use` 或说明限制？
- [ ] 点名集群的一次性诊断是否已传 **`cluster_config_path`**？
