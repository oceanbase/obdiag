---
name: observer-storage-space-troubleshooting
description: Observer 侧日志盘、数据盘、磁盘分配失败、-4184、-4264、-4009、Structure needs cleaning、fallocate、合并 -4016、索引或加副本空间不足、单表恢复占盘、归档卡 GC 等存储空间问题；日志链使用 observer-log-analysis。
---

# Observer 存储与磁盘空间排障

## 触发与边界

- **本 SOP**：clog/log disk、data disk、文件系统、数据文件分配、归档阻塞 GC、索引 DDL 空间放大、加副本/加 zone 空间不足、单表恢复临时占盘。
- **不替代**：observer 日志采集分析 → **`observer-log-analysis`**；统一 RCA 入口 → **`obdiag-rca`**；单 SQL 性能 → **`observer-sql-analysis`**。用户明确要 RCA 或命中已有 RCA scene（如 `clog_disk_full`、`ddl_disk_full`）时，先走 `obdiag-rca`，本 SOP 作为 RCA 不足时的手动降级。
- **与备份归档 skill 分工**：归档 checkpoint、恢复窗口、对象存储路径/权限 → **`backup-archive-restore-troubleshooting`**；扩容后 LS `wait_offline`、GC 被归档位点阻塞 → 本 SOP。
- **避免误判**：同分区 SSTable 各节点大小不同、三副本 join 出多条记录通常是预期行为。

## 前置条件

- 明确错误码、报错文本、时间点、租户、server/zone；涉及补副本时明确目标 zone。
- 多集群传 `cluster_config_path`。
- 能执行只读 SQL；文件系统问题需要节点侧 `df`、`dmesg`、挂载信息。

## 分支：选主路径

| 现象 | 主路径 | 来源 |
|------|--------|------|
| `errcode=-4264` / `Log out of disk space` | 阶段 A：日志盘 | `sop_044` |
| `Fail to fallocate block file` | 阶段 B：文件系统类型 | `sop_055` |
| `errcode=-4184` / `Failed to alloc block from io device` | 阶段 C：数据盘分配 | `sop_146` |
| `Row size too large` | 阶段 D：`lob_inrow_threshold` | `sop_149` |
| LS `wait_offline`、GC `wait archive` | 阶段 E：归档阻塞 GC | `sop_151` |
| `Structure needs cleaning` / `-4009` | 阶段 F：XFS/IO 损坏 | `sop_154` |
| 合并 `-4016` deserialize | 阶段 G：坏块/IO | `sop_163` |
| 单表恢复失败且数据盘近满 | 阶段 H：临时租户全量占盘 | `sop_174` |
| 建索引 `4184`，OCP 显示还有空间 | 阶段 I：索引空间放大 | `sop_179` |
| 加 zone/副本 `4184` | 阶段 J：副本扩容空间 | `sop_227` |
| 租户内存结构疑问 | 阶段 K：内存不是磁盘 | `sop_228` |
| SSTable 大小不一致 | 阶段 L：预期行为 | `sop_229` |

## 阶段 A：日志盘满（-4264）

```sql
SELECT zone, CONCAT(svr_ip,':',svr_port) observer,
  ROUND(LOG_DISK_CAPACITY/1024/1024/1024) logdisk_total,
  ROUND((LOG_DISK_CAPACITY-LOG_DISK_ASSIGNED)/1024/1024/1024) logdisk_free
FROM GV$OB_SERVERS;

SELECT tenant_id, tenant_name,
  CAST(log_disk_size/1024/1024/1024 AS DECIMAL(15,2)) log_disk_size_G,
  CAST(log_disk_in_use/1024/1024/1024 AS DECIMAL(15,2)) log_disk_use_G
FROM __all_virtual_unit a, dba_ob_tenants b
WHERE a.tenant_id=b.tenant_id;
```

运行中先根据上面 SQL 找到 **受影响租户和对应 unit**，再增大该 unit 的 `LOG_DISK_SIZE`；`sop_044` 的典型场景是 **sys 租户日志盘太小**。通常按内存 3-4 倍评估。

```sql
ALTER RESOURCE UNIT <unit_name> LOG_DISK_SIZE '18G';
```

无法启动时可临时用启动参数拉起，成功后恢复阈值：

```bash
./bin/observer -o "log_disk_size=18G,log_disk_utilization_threshold=95,log_disk_utilization_limit_threshold=98"
```

```sql
ALTER SYSTEM SET log_disk_utilization_limit_threshold=95;
ALTER SYSTEM SET log_disk_utilization_threshold=80;
```

## 阶段 B：`fallocate` 失败

`Fail to fallocate block file` 常见于数据目录所在文件系统不支持。检查：

```bash
df -T
lsblk -f
```

数据目录建议使用 **ext4** 或 **xfs**。

## 阶段 C：数据盘分配失败（-4184）

`Failed to alloc block from io device` 表示数据文件或数据盘空间不足。处理方向是扩容数据盘/数据文件或释放空间；不要只看 OCP 剩余百分比，需结合实际错误节点和租户。

## 阶段 D：行宽与 `lob_inrow_threshold`

OceanBase 4.2.4+ `lob_inrow_threshold` 默认变大，可能导致 `Row size too large`。

```sql
CREATE TABLE t (...) lob_inrow_threshold = 4096;
```

## 阶段 E：归档阻塞 LS GC

扩容后 LS `wait_offline`、GC 日志出现 `wait archive` 时，先查归档位点和延迟时间。

```sql
SELECT name, value FROM gv$ob_parameters
WHERE name='ls_gc_delay_time' AND tenant_id=<tenant_id>;

SELECT * FROM cdb_ob_archivelog WHERE tenant_id=<tenant_id>;

SELECT *, scn_to_timestamp(checkpoint_scn)
FROM __all_virtual_ls_log_archive_progress
WHERE tenant_id=<tenant_id>;
```

节点侧检查 `observer.log` 中 `T<tenant_id>_GC`，再修复 NFS 挂载或目录权限。

## 阶段 F：文件系统损坏（-4009）

`Structure needs cleaning`、`OB_IO_ERROR`、`-4009` 多指向 XFS/IO 问题。

```bash
dmesg -T
# 谨慎操作：确认设备后再停服务/卸载/修复
umount <mount_point>
xfs_repair /dev/<device>
```

不要随意使用 `xfs_repair -L`；修复失败时考虑换节点或下线 observer。

## 阶段 G：合并 -4016 deserialize

合并报 `Fail to deserialize record header`、`ob_micro_block_cache.cpp`、`-4016` 时，先看 OS 层磁盘/IO/坏块：

```bash
dmesg -T
```

处理方向：换盘、修 OS、降压后重试；若现场证据不指向硬件，再升级为 OceanBase 支持问题。

## 阶段 H：单表恢复占盘

OceanBase 4.2.1+ 单表恢复可能先把全量备份拉到临时租户，数据盘需要容纳全量。空间不足时扩容，或在更大集群恢复后迁表。4.3.5 BP1+ 对单表恢复中间占盘有优化。

## 阶段 I：索引 DDL 空间放大

建索引 `ERROR 4184: Server out of disk space` 时，优先跑 RCA：

```bash
obdiag rca run --scene=ddl_disk_full --input_parameters='{"tenant_name":"<tenant>","table_name":"<table>","action_type":"add_index","index_name":"<index>"}'
```

4.2.3 前索引构建空间放大更明显；4.2.3+ 放大约 1.5 倍，仍需预留空间。

## 阶段 J：加 zone / 加副本空间不足

加副本 `4184` 常见于触达 `data_disk_usage_limit_percentage`。检查宏块和各 zone 空间：

```sql
SELECT * FROM __all_virtual_macro_block_marker_status;
```

处理方向：回滚补副本、原 zone 扩容或加机分散后再补新 zone，最后回收机器。

## 阶段 K：内存不是磁盘

租户 `memory_size` 内包含 Meta、MemStore、SQL work area、KVCache 等。`memstore_limit_percentage`、`ob_sql_work_area_percentage` 等问题不要误归类为数据盘满。

## 阶段 L：SSTable 大小不一致

同分区 SSTable 各副本大小不同、join 出三条相似记录，多数是 MINI/MINOR 与三副本视角下的正常表现；MAJOR 才更强调一致。

## 输出格式

1. **事实表**：错误码、节点/租户、关键 SQL 或 OS 证据、命中分支。
2. **处理建议**：扩 log disk、扩 data disk、修 NFS/权限、修文件系统、回滚补副本、升级版本等。
3. **下一步工具**：需要日志链时使用 `observer-log-analysis`；RCA 优先看 `obdiag-rca` 的 `ddl_disk_full`、`clog_disk_full` 等场景。

## 自检

- `-4264` 是否已区分运行中改 unit 与无法启动时临时启动参数？
- `4184` 是否区分数据盘分配、索引 DDL 放大、加副本空间三类？
- 文件系统问题是否先看 `dmesg`，并明确 `xfs_repair` 风险？
- 单表恢复是否考虑临时租户全量占盘？
