---
name: obdiag-rca
description: OceanBase 集群所有根因分析场景的统一 SOP。覆盖事务超时/回滚/断连、内存不足、合并卡住、日志盘满、DDL 失败、Schema 泄漏、GC 异常、弱一致性读、OMS 等。始终先执行 rca_run，如 RCA 脚本无法给出结论再按本 skill 降级到手动日志采集分析。
---

# OceanBase RCA 根因分析统一 SOP

## 使用原则

1. **rca_run 优先**：每个场景必须先尝试 `rca_run(scene=xxx)`，不得直接跳到手动步骤。
2. **结果判断**：rca_run 输出含明确 `suggest` 或 `root cause` → 直接向用户汇报，结束；输出为"无法定位"或空 → 进入降级手动分析。
3. **降级**：按本 skill 中对应场景的手动步骤执行，逐步推进，不要一次性调用大量工具。
4. **场景参数**：部分 scene 需要 `--env` 参数（见各场景说明中"必填 env 参数"列）。**注意：agent 工具 `rca_run` 不支持传 env**，需要 env 参数的场景直接调用 `rca_run(scene="xxx")` 即可——obdiag 底层会自动读取上下文；若 rca_run 因缺少 env 参数而失败，直接进入手动降级分析步骤。

---

## 场景速查表

| 用户描述症状 | scene 名 | 必填 env 参数 | 降级标签 |
|------------|---------|------------|--------|
| 语句超时、ob_query_timeout | `transaction_execute_timeout` | `err_type="statement is timeout"`, `tenant_id` | [语句超时] |
| 事务超时 | `transaction_execute_timeout` | `err_type="transaction is timeout"` | [事务超时] |
| 事务不结束、长事务 | `transaction_not_ending` | `tx_id`（可选） | [长事务] |
| 锁等待超时 | `transaction_wait_timeout` | — | [锁等待] |
| 锁冲突、行锁 | `lock_conflict` | `error_msg`（可选） | [锁冲突] |
| 事务回滚 | `transaction_rollback` | — | [事务回滚] |
| 事务其他错误 | `transaction_other_error` | — | [事务其他] |
| 事务中断连 | `transaction_disconnection` | — | [事务断连] |
| 断连、OBProxy 连接断开 | `disconnection` | — | [断连] |
| 挂起事务 | `suspend_transaction` | — | [挂起事务] |
| 内存不足、OOM | `memory_full` | — | [内存不足] |
| SQL 执行内存高、work area 内存 | `execute_memory_high` | — | [执行内存高] |
| 合并卡住、major compaction hold | `major_hold` | — | [合并卡住] |
| 转储异常、freeze/dump | `freeze_dump` | — | [转储异常] |
| Replay 卡住 | `replay_hold` | — | [Replay卡住] |
| 日志盘满、clog 磁盘满 | `clog_disk_full` | `tenant_id`（可选），`ls_id`（可选） | [日志盘满] |
| DDL 失败 | `ddl_failure` | — | [DDL失败] |
| DDL 磁盘满 | `ddl_disk_full` | — | [DDL盘满] |
| 索引 DDL 报错 | `index_ddl_error` | — | [索引DDL] |
| Schema 泄漏 | `schema_leak` | — | [Schema泄漏] |
| 删除 observer 节点报错 | `delete_server_error` | — | [删节点] |
| 分区自动分裂调度错误 | `split_schedule_error` | — | [分裂调度] |
| Unit GC 问题 | `unit_gc` | — | [Unit GC] |
| GC 异常 | `gc_troubleshooting` | — | [GC异常] |
| 日志报错 / 错误码 | `log_error` | — | [日志报错] |
| 弱一致性读问题 | `weak_read_troubleshooting` | — | [弱读] |
| OMS 全量事务 | `oms_full_trans` | — | [OMS全量] |
| OMS OBCDC 问题 | `oms_obcdc` | — | [OMS OBCDC] |

---

## 标准执行流程

### Step 1：收集必要参数后执行 rca_run

```
rca_list()                    # 可选：确认 scene 名及所需参数
rca_run(scene="<scene_name>") # agent 工具只接受 scene 和 cluster_config_path，无 env 参数
```

**若 rca_run 成功给出根因建议 → 向用户汇报，结束本 skill。**

### Step 2：rca_run 不足时，按降级路径手动分析

---

## 降级手动分析

### [语句超时]

**常见原因**：ob_query_timeout 设置过小；队列积压；location cache 频繁重试；锁冲突；全表扫描。

```sql
-- 1. 确认超时设置（单位：微秒，默认 10,000,000 = 10s）
SELECT * FROM oceanbase.CDB_OB_SYS_VARIABLES WHERE tenant_id=<tenant_id> AND NAME='ob_query_timeout';
SHOW PARAMETERS LIKE 'syslog_level';

-- 2. 检查队列积压（日志关键词）
-- gather_log grep: "dump tenant"
-- 解读：req_queue:total_size=<N>，N != 0 说明请求积压
```

```
-- 3. 若有 trace_id，采集日志
gather_log(since="30m", grep=["<trace_id>", "timeout_timestamp", "dump tenant"])
```

- `timeout_timestamp - cur_query_start_time >= ob_query_timeout` → 超时属正常行为，调大参数即可
- `timeout_timestamp - cur_query_start_time < ob_query_timeout` → 非预期提前超时，需进一步排查

重试错误码排查（频繁出现说明 location cache 失效导致重试堆积超时）：
```
gather_log(since="30m", grep=["ret=-4038", "ret=-4225", "ret=-4653", "ret=-6005"])
```

---

### [事务超时]

```sql
-- 1. 确认事务超时参数
SHOW PARAMETERS LIKE 'ob_trx_timeout';
SHOW PARAMETERS LIKE 'ob_trx_idle_timeout';
```

```
-- 2. 检查队列积压
gather_log(since="1h", grep=["dump tenant", "req_queue"])

-- 3. 检查锁冲突
gather_log(since="30m", grep=["mvcc_write conflict"])
```

---

### [长事务] / [挂起事务]

`rca_run` 需要 `tx_id` 参数；如用户未提供，先从系统视图找到活跃长事务。

```sql
-- 1. 找长事务
SELECT TX_ID, TENANT_ID, STATE, CTX_CREATE_TIME, TX_EXPIRED_TIME, SESSION_ID
FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS
WHERE STATE='ACTIVE'
ORDER BY CTX_CREATE_TIME ASC LIMIT 20;

-- 2. 查指定事务详情
SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID='<tx_id>';

-- 3. 查 session 信息
SELECT * FROM oceanbase.GV$OB_PROCESSLIST WHERE time > 60 ORDER BY time DESC LIMIT 20;
```

- STATE='ACTIVE' + SESSION_ID 非空 → 仍有活跃 SQL，告知用户该 session 的 SQL
- STATE 非 ACTIVE（commit 阶段）→ 事务卡在提交阶段，检查副本日志同步
- `CTX_CREATE_TIME = TX_EXPIRED_TIME` → 事务已到期

```sql
-- 4. follower 落后排查（事务卡 replay 时）
SELECT * FROM oceanbase.__all_virtual_log_stat WHERE tenant_id=<tid> AND ls_id=<ls_id>;
-- 关注 end_lsn 是否明显落后
```

---

### [锁等待] / [锁冲突]

**error_msg 含 "Shared lock conflict"**：

```
gather_log(since="30m", grep=["lock_for_read need retry"])
-- 从日志中提取 data_trans_id → 再 gather_log grep data_trans_id
-- 说明：读锁被提交阶段的事务占用，建议用 transaction_not_ending scene 继续分析
```

**error_msg 含 "Lock wait timeout exceeded"**：

```
gather_log(since="30m", grep=["mvcc_write conflict"])
-- 从日志中提取 conflict_tx_id → 再 gather_log grep conflict_tx_id
-- 说明：行锁被另一事务持有，可等待或 kill 阻塞 session
```

前提：`syslog_level` 需含 WDIAG（读锁）/ INFO（行锁），否则日志会缺失。

```sql
-- OB 4.2+ 可直接查锁等待
SELECT * FROM oceanbase.GV$OB_LOCKS LIMIT 20;
SELECT * FROM oceanbase.CDB_OB_DEADLOCK_HISTORY ORDER BY create_time DESC LIMIT 10;
```

---

### [事务回滚] / [事务断连] / [事务其他]

```
gather_log(since="30m", grep=["rollback", "disconnect", "ERROR"])
analyze_log(log_path="<gather output path>")
```

---

### [断连]

> 此 scene 依赖 OBProxy 节点，需配置文件中有 `obproxy_nodes`。

```
-- 1. 采集 OBProxy diagnosis 日志
gather_obproxy_log(since="30m", grep=["CONNECTION](trace_type", "disconnect", "error"])

-- 2. 检查 OBProxy 参数
```

```sql
SHOW PROXYCONFIG LIKE 'request_buffer_length';
-- 过小时需调大（参考 issue #575）
```

---

### [内存不足]

```sql
-- 1. 内存配置
SHOW PARAMETERS LIKE 'memory_limit%';
SHOW PARAMETERS LIKE 'memstore_limit_percentage';

-- 2. memtable freeze 状态（关键）
SELECT * FROM oceanbase.__all_virtual_minor_freeze_info;
-- state 非正常时，进一步看 memstore_info

-- 3. memstore 详情（找 is_active='NO' 的行）
SELECT * FROM oceanbase.__all_virtual_memstore_info
WHERE is_active='NO' ORDER BY start_scn LIMIT 10;
```

**解读 freeze_state**：
- `NOT_READY_FOR_FLUSH`：
  - `unsubmitted_count == unsynced_count != 0` → 可能是多源数据问题
  - `unsubmitted_count=0, write_ref_count!=0, unsynced_count!=0` → 日志回调未完成
  - 三者均为 0 → replay/callback 未推过 memtable 右边界，检查 ReplayService
- `READY_FOR_FLUSH`：flush 条件满足但未执行，检查 compaction diagnose 和 DAG 积压
- `RELEASED`：refcount 未清零，通常需 OceanBase 支持

```sql
-- 4. compaction 诊断
SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info;
SELECT * FROM oceanbase.__all_virtual_dag LIMIT 20;
```

---

### [转储异常]

```sql
-- 1. 转储进度
SELECT * FROM oceanbase.GV$OB_TABLET_COMPACTION_PROGRESS
WHERE TYPE='MINI_MERGE' ORDER BY START_TIME ASC LIMIT 20;

-- 2. DAG 调度器积压
SELECT * FROM oceanbase.__all_virtual_dag_scheduler
WHERE svr_ip='<ip>' AND svr_port=<port> AND tenant_id=<tid>;

-- 3. Compaction 建议
SELECT * FROM oceanbase.GV$OB_COMPACTION_SUGGESTIONS LIMIT 20;
```

```
gather_log(since="1h", grep=["freeze", "minor_freeze", "FAILED", "dag_ret"])
```

- minor freeze FAILED：在日志中匹配 `dag_ret` 和 `tablet_id/ls_id`
- 长时间运行的 minor freeze：检查 memstore 压力、DAG 队列、磁盘/CPU

---

### [合并卡住]

```sql
-- 1. 合并整体状态
SELECT * FROM oceanbase.DBA_OB_MAJOR_COMPACTION;
-- IS_ERROR='YES' → 合并出错；SUSPEND_SCN 非空 → 合并被挂起

-- 2. checksum 错误
SELECT * FROM oceanbase.CDB_OB_TABLET_CHECKSUM_ERROR_INFO LIMIT 10;

-- 3. compaction 诊断详情
SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info;

-- 4. DDL 任务（DDL 未完成可阻塞合并）
SELECT * FROM oceanbase.DBA_OB_DDL_TASK_PROGRESS LIMIT 20;

-- 5. 重复索引名排查（Issue #607）
SELECT TABLE_NAME, INDEX_NAME, COUNT(*) AS cnt
FROM oceanbase.CDB_INDEXES
GROUP BY INDEX_NAME HAVING cnt > 1 LIMIT 10;
```

```
gather_log(since="2h", grep=["weak_read_timestamp", "log disk space is almost full",
                              "generate_weak_read_timestamp_", "major_hold", "compaction"])
```

---

### [Replay卡住]

```sql
SELECT * FROM oceanbase.__all_virtual_log_stat WHERE tenant_id=<tid>;
-- 对比各副本 end_lsn，落后较多的副本即为瓶颈
```

```
gather_log(since="1h", grep=["replay", "replay_hold", "apply_log", "ObLogReplayService"])
analyze_log(log_path="<gather output path>")
```

---

### [日志盘满] / [DDL盘满]

```sql
-- 1. 日志盘阈值配置
SHOW PARAMETERS LIKE 'log_disk_utilization_threshold';
SHOW PARAMETERS LIKE 'log_disk_utilization_limit_threshold';

-- 2. 找卡住的 checkpoint（clog checkpoint 未推进）
-- gather_log grep: "clog checkpoint no change", "ObLSTxService::get_rec_scn"

-- 3. 各 LS 状态
SELECT * FROM oceanbase.GV$OB_LOG_STAT WHERE tenant_id=<tid> LIMIT 20;
```

```
gather_sysstat()   -- 确认实际磁盘使用率
gather_log(since="1h", grep=["clog checkpoint no change", "disk full", "no space left"])
```

---

### [DDL失败] / [索引DDL]

```sql
-- 1. DDL 任务进度
SELECT * FROM oceanbase.DBA_OB_DDL_TASK_PROGRESS LIMIT 20;

-- 2. RS 事件历史
SELECT * FROM oceanbase.DBA_OB_ROOTSERVICE_EVENT_HISTORY
WHERE event LIKE '%ddl%' ORDER BY timestamp DESC LIMIT 30;

-- 3. DDL 错误信息
SELECT * FROM oceanbase.DBA_OB_DDL_TASK_PROGRESS WHERE status='FAILED' LIMIT 10;
```

```
gather_log(since="1h", grep=["ddl", "INDEX", "OB_ERR", "ERROR"])
```

---

### [Schema泄漏]

```sql
-- 1. 内存占用
SELECT * FROM oceanbase.GV$OB_MEMORY
WHERE context LIKE '%schema%' ORDER BY hold DESC LIMIT 20;

-- 2. 活跃连接数 vs 连接池配置
SELECT COUNT(*) FROM oceanbase.GV$OB_PROCESSLIST;

-- 3. schema 版本刷新
SELECT svr_ip, svr_port, tenant_id, refreshed_schema_version, received_schema_version
FROM oceanbase.GV$OB_SERVER_SCHEMA_INFO LIMIT 20;
```

若连接数正常但内存不释放，检查应用连接池是否正确关闭连接。

---

### [删节点] / [Unit GC] / [GC异常] / [分裂调度]

```sql
-- RS 事件历史（最近 30 条）
SELECT * FROM oceanbase.DBA_OB_ROOTSERVICE_EVENT_HISTORY
ORDER BY timestamp DESC LIMIT 30;

-- Unit 状态
SELECT * FROM oceanbase.DBA_OB_UNITS WHERE status != 'NORMAL' LIMIT 20;

-- LS 状态
SELECT * FROM oceanbase.GV$OB_LOG_STAT LIMIT 20;
```

```
gather_log(since="1h", grep=["gc", "unit_gc", "server_manager", "split", "ERROR"])
```

---

### [日志报错]

```
-- 1. 用错误码或关键词过滤
gather_log(since="1h", grep=["ret=-<错误码>", "ERROR"])
analyze_log(log_path="<gather output path>")
```

常见错误码参考：`-4012`（超时）、`-5024`（主键冲突）、`-4016`（内部错误）、`-6213`（事务超时）。

---

### [弱读]

```sql
-- 配置
SHOW PARAMETERS LIKE 'weak_read_version_refresh_interval';
SELECT * FROM oceanbase.GV$OB_WEAK_READ_SERVICE LIMIT 20;
```

```
gather_log(since="30m", grep=["weak_read", "generate_weak_read_timestamp", "replica_readable"])
```

---

### [OMS全量] / [OMS OBCDC]

```
gather_oms_log(since="1h")
analyze_log(log_path="<gather output path>")
```

对于 OBCDC 类问题，重点查 `gather_oms_log` 输出中的 component 日志（Ghana / CDC），用 `file_list` + `file_read` 逐层查看。

---

## 降级后的汇报原则

- 说明 `rca_run` 的局限（如：脚本未覆盖当前版本、依赖数据不完整、缺少必要 env 参数等）。
- 手动分析结论必须基于工具返回数据，不得凭推测下结论。
- 若手动步骤仍无法定位，明确列出还需要哪些信息（OB 版本、发生时间点、业务变更、syslog_level 设置等）。
- 可引导用户将采集包上传至 OceanBase 社区论坛或提 issue 进行深度分析。
