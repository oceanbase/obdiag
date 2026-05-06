---
name: obproxy-routing-troubleshooting
description: OBProxy/ODP 连接解析、弱读与只读副本路由、读写分离、分区键解析、SHOW CREATE 兼容、get_lock、cluster not exist、2013、Unknown thread id、新建表经代理慢等路由配置类问题；日志采集解读使用 obproxy-log-analysis。
---

# OBProxy 路由与连接配置排障

## 触发与边界

- **本 SOP**：OBProxy/ODP 路由策略、弱读、只读副本、读写分离、租户/集群名解析、`proxyro`、`rs_list`、OB 2.0 协议、代理侧内存与内部元数据查询超时。
- **不替代**：OBProxy 日志采集/包内解读 → **`obproxy-log-analysis`**；单条 SQL 执行计划/锁 → **`observer-sql-analysis`**。

## 前置条件

- 尽量明确：OBProxy/ODP 版本、OceanBase 版本、连接串、是否多 ODP/VIP、是否经 2883、是否在事务中。
- 需要配置时登录 `root@proxysys`，用 `SHOW proxyconfig` / `ALTER proxyconfig`。
- 多集群传 `cluster_config_path`；涉及 Observer 侧事实时用 `db_query` 查 sys/租户视图。

## 分支：选主路径

| 现象 | 主路径 | 来源 |
|------|--------|------|
| `decode varchar param value failed`、远程事务多 | 阶段 A：分区键在 SQL 前 4KB 内 | `sop_001` |
| 弱读 hint 仍到 leader、读写分离不生效 | 阶段 B：弱读与 `proxy_route_policy` | `sop_133`, `sop_213`, `sop_251`, `sop_252` |
| `cluster not exist`、`proxyro` 认证失败 | 阶段 C：租户/集群解析与 `proxyro` | `sop_041`, `sop_075`, `sop_095`, `sop_141` |
| OBD 部署后代理连不上 | 阶段 D：`rs_list` | `sop_124` |
| 单台 ODP `ERROR 2013` | 阶段 E：ODP 内存与 connection freeze | `sop_147` |
| MySQL 迁移只想用用户名连接 | 阶段 F：`enable_full_username` | `sop_152` |
| `GET_LOCK` / `RELEASE_LOCK` 报 1235 | 阶段 G：OB 2.0 协议 | `sop_187` |
| `SHOW CREATE TABLE` 缺全局索引或 local/global | 阶段 H：DDL 兼容模式 | `sop_203`, `sop_222` |
| 新建表代理慢、直连快，tablet 很多 | 阶段 I：代理内部元数据查询超时 | `sop_253` |
| `KILL` 报 `Unknown thread id` | 阶段 J：同一台 ODP 执行 SHOW/KILL | `sop_139` |

## 阶段 A：分区键与 4KB 解析

OBProxy 只解析 SQL 前 4KB 内的路由信息。`INSERT` 中 longtext 等大字段排在分区键前时，可能出现 `decode varchar param value failed`，导致随机路由和远程事务增多。

**动作**：把分区键列尽量放在列序前部，避免大字段先于分区键。

## 阶段 B：弱读与读写分离

弱读 hint 只声明一致性，是否优先 follower 还取决于代理路由策略。

```sql
-- 优先使用会话级或 SQL hint，避免先改全局
SET SESSION ob_read_consistency = 'weak';
SELECT /*+READ_CONSISTENCY(WEAK)*/ * FROM t1;

-- 代理侧优先 follower：会影响该 ODP 后面的客户端，变更前确认范围和回滚值
ALTER proxyconfig SET proxy_route_policy = 'follower_first';
```

- `proxy_route_policy` 默认空串时，弱读仍可能到 leader。
- `SET GLOBAL ob_read_consistency` 和 `ALTER proxyconfig` 都有全局/ODP 级影响；优先用 session/hint 复现，确需变更时记录旧值、变更范围和回滚 SQL。
- OceanBase 4.x 中 `ob_proxy_readonly_transaction_routing_policy` 已废弃。
- 标准事务内的 SELECT 不能单独路由到 follower；建议 `autocommit=1` 下单独弱读，并配合 `follower_first`。
- 无 WHERE 的 `explain route` 走 `__all_dummy`，显示 follower 是设计行为，不要误判。
- 可用 `gv$sql_audit` 看实际 `svr_ip`、`plan_type`，并检查是否缺分区键。

## 阶段 C：集群/租户解析与 `proxyro`

| 现象 | 判断与动作 |
|------|------------|
| `ERROR 4669: cluster not exist`，root 可连但业务用户不行 | 完整用户名/租户/集群串过长时缩短命名。 |
| `cluster not exist` 且使用 https config server | 老版本 OBProxy 对 `obproxy_config_server_url=https` 支持不完整；改 rslist/http 或升级。 |
| 修改 `proxyro` 后 `reading authorization packet` | 在 `root@proxysys` 查 `proxy_config` 的密码项，与 Observer sys 租户 `proxyro` 密码保持一致；OCP 场景解除关联、改密、重新关联。 |
| `proxyro@sys` 1045 | 默认禁止 `proxyro` 登录；需要时 `ALTER PROXYCONFIG SET skip_proxyro_check='true';` |

## 阶段 D：`rs_list`

OBD 部署的 Proxy 连不上而直连 Observer 正常时，核对部署配置中的 `rs_list` 是否等于真实 OB 节点地址；错误则修正后重部署。

## 阶段 E：单台 ODP 2013

`ERROR 2013 ... reading authorization packet` 仅发生在一台 ODP 时，查 `obproxy_error.log` 里的 2013 和 trace，再到 `obproxy.log` 看 `connection freeze` / 内存不足。

```sql
ALTER proxyconfig SET proxy_mem_limited = '4G';
```

按实际内存调整，不要机械套用 4G。

## 阶段 F：仅用户名连接固定租户

适用于 rslist 模式、单租户代理。

```sql
ALTER proxyconfig SET enable_full_username = 'false';
ALTER proxyconfig SET proxy_tenant_name = '<tenant_name>';
```

该 ODP 实质只连 `proxy_tenant_name` 指定租户；多租户需要多个 ODP。

## 阶段 G：`GET_LOCK` 与 OB 2.0 协议

经 2883 执行 `GET_LOCK` / `RELEASE_LOCK` 报 `ERROR 1235` 时，检查旧版 ODP 是否仍启用单 leader 路由并走 MySQL 协议。

```sql
ALTER proxyconfig SET enable_ob_protocol_v2 = true;
ALTER proxyconfig SET enable_single_leader_node_routing = false;
ALTER proxyconfig SET client_session_id_version = 2;
```

ODP 4.3.1.4+ 默认更合理；热升级后仍可能保留旧值。

## 阶段 H：`SHOW CREATE TABLE` 兼容模式

经代理 `SHOW CREATE TABLE` 不显示全局索引或 local/global 时，检查 `_show_ddl_in_compat_mode` 和代理 `init_sql`。

```sql
SHOW proxyconfig LIKE 'init_sql';
ALTER proxyconfig SET init_sql='set _show_ddl_in_compat_mode = 0;';
```

多 ODP 节点需分别设置。也可在会话中 `SET _show_ddl_in_compat_mode = 0;` 后重试。

## 阶段 I：新建表经代理慢

超多 tablet 下，OBProxy 内部查询 `__all_virtual_proxy_sub_partition` 等虚拟表可能超过 `short_async_task_timeout`。

```sql
ALTER proxyconfig SET short_async_task_timeout = '20s';
CALL DBMS_STATS.GATHER_TABLE_STATS('oceanbase', '__all_virtual_tablet_to_ls', method_opt=>'for all columns size 1');
CALL DBMS_STATS.GATHER_TABLE_STATS('oceanbase', '__all_virtual_proxy_sub_partition', method_opt=>'for all columns size 1');
```

也可升级 OBProxy。

## 阶段 J：多 ODP 下 KILL

OBProxy < 4.3.0 且多 ODP + VIP 时，`SHOW PROCESSLIST` 和 `KILL` 必须落在同一台 ODP。先确认 session 所在 ODP，再经同一 ODP 执行 `KILL`。

## 输出格式

1. **结论表**：症状、命中分支、关键参数/SQL、来源 SOP。
2. **可执行动作**：列出需要执行的 `ALTER proxyconfig`、会话变量、SQL 改写或部署修正。
3. **边界说明**：注明事务上下文、版本差异、多 ODP 节点是否需要逐台处理。

## 自检

- 弱读问题是否区分 `READ_CONSISTENCY(WEAK)` 与 `proxy_route_policy='follower_first'`？
- `cluster not exist` 是否分别排查命名过长、https config server、rslist、固定租户？
- `SHOW CREATE TABLE` 问题是否避免误判为索引丢失？
- 只需要看 OBProxy 日志时，是否切到 `obproxy-log-analysis`？
