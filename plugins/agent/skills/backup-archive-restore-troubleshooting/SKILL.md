---
name: backup-archive-restore-troubleshooting
description: 备份、日志归档、物理恢复、恢复时间窗、NFS/OSS/COS 归档路径、对象存储权限、备租户恢复源、ob_admin 读取归档位点、oblogminer schema_meta 等问题；OMS 迁移同步问题不使用本 skill。
---

# 备份 / 归档 / 恢复排障

## 触发与边界

- **本 SOP**：归档不推进、归档长期 BEGINNING、对象存储路径/权限、物理恢复 `4018`、恢复源误配、集群不可用时读归档 checkpoint、oblogminer 缺 `schema_meta`。
- **不替代**：OMS 迁移同步问题；通用 observer 日志采集分析见 **`observer-log-analysis`**；全局分诊见 **`obdiag-triage`**；扩容后 LS `wait_offline`、GC 被归档阻塞见 **`observer-storage-space-troubleshooting`**。

## 前置条件

- 明确 OceanBase 版本、租户、`tenant_id`、归档路径、恢复目标时间/SCN。
- 对 NFS/权限问题，需要能登录对应 Observer leader 节点。
- 对对象存储问题，需要实际访问用户的 bucket/path 权限信息。

## 分支：选主路径

| 现象 | 主路径 | 来源 |
|------|--------|------|
| 归档 checkpoint 不推进 | 阶段 A：定位卡住 LS 与 leader | `sop_069` |
| 归档长期 BEGINNING | 阶段 A：NFS/权限 | `sop_070` |
| OSS/COS `format file does not exist` 或归档后 core | 阶段 B：对象存储路径与版本 | `sop_033`, `sop_064` |
| OCP/OBS 备份 `no I/O operation permission` | 阶段 C：对象存储权限 | `sop_037` |
| 物理恢复 `4018` / `no enough log for restore` | 阶段 D：恢复时间窗 | `sop_048` |
| 备租户恢复源报不允许指向自己 | 阶段 E：restore source 与 cluster_id | `sop_047` |
| 集群不可用但要归档 checkpoint | 阶段 F：`ob_admin dump_backup` | `sop_076` |
| oblogminer 缺 `schema_meta` | 阶段 G：数据字典归档参数 | `sop_202` |

## 阶段 A：归档不推进 / BEGINNING

先从归档进度表找到卡住的日志流，再定位 leader 节点检查 NFS/权限。

```sql
SELECT *, scn_to_timestamp(checkpoint_scn)
FROM __all_virtual_ls_log_archive_progress
WHERE tenant_id=<tenant_id> AND piece_id=<piece_id>;

SELECT * FROM __all_virtual_ls_status
WHERE tenant_id=<tenant_id>;

SELECT * FROM gv$ob_log_stat
WHERE tenant_id=<tenant_id> AND ls_id=<ls_id>;
```

租户侧或版本差异场景可用：

```sql
SELECT *, scn_to_timestamp(checkpoint_scn)
FROM CDB_OB_LS_LOG_ARCHIVE_PROGRESS
WHERE tenant_id=<tenant_id> AND piece_id=<piece_id>;

SELECT svr_ip FROM gv$ob_log_stat
WHERE tenant_id=<tenant_id> AND ls_id=<ls_id> AND role='leader';
```

在 leader 节点：

```bash
df -h
# observer.log 中查 Permission denied、failed to create parent dir 等归档路径错误
```

修复 NFS 挂载或目录权限后，归档状态应继续推进。

## 阶段 B：对象存储路径与版本

| 现象 | 动作 |
|------|------|
| 4.2.1.2 前版本配置 OSS 后归档/备份导致节点 core | 仅限该窄场景的应急处置：先记录原 `LOG_ARCHIVE_DEST`，确认业务方接受归档中断和恢复窗口缺口风险，再停 OSS 访问并拉起集群；业务租户执行 `ALTER SYSTEM NOARCHIVELOG;`、`ALTER SYSTEM SET LOG_ARCHIVE_DEST='';`；升级到 4.2.1.2+ 后重新规划归档。 |
| COS/对象存储 `format file does not exist` | 不要预创建与 `LOG_ARCHIVE_DEST` 完全一致的末级“目录”对象；只允许预创建上级路径。 |

## 阶段 C：对象存储权限

OCP/OBS 备份目的地报 `no I/O operation permission` 时，用实际访问用户验证同一路径是否具备：

- 创建文件
- 下载文件
- 删除文件
- 重命名文件

缺删除等权限也可能导致 I/O permission 报错。

## 阶段 D：物理恢复日志不足（4018）

按时间恢复时，恢复点必须满足：

```text
CDB_OB_BACKUP_SET_FILES.MIN_RESTORE_SCN_DISPLAY
  <= restore_timestamp
  <= CDB_OB_ARCHIVELOG.CHECKPOINT_SCN_DISPLAY
```

按 SCN 恢复时，恢复点必须满足：

```text
CDB_OB_BACKUP_SET_FILES.MIN_RESTORE_SCN
  <= restore_scn
  <= CDB_OB_ARCHIVELOG.CHECKPOINT_SCN
```

若归档存在中断，可恢复区间不能跨中断。

## 阶段 E：备租户恢复源

`set tenant itself as log restore source is not allowed` 时：

1. 确认 `log restore source` 没有填成本租户。
2. 跨集群恢复时确认主备集群 `cluster_id` 不同；手工部署或显式指定 `cluster_id` 时尤其容易相同。

## 阶段 F：集群不可用时读 checkpoint

正常情况下直接查：

```sql
SELECT * FROM cdb_ob_archivelog;
```

集群不可用时用 `ob_admin` 从归档路径解析：

```bash
export OB_ADMIN_LOG_DIR=/tmp
./ob_admin dump_backup -d<archive_path>
```

从输出中读取 `checkpoint_scn`。

## 阶段 G：oblogminer 缺 `schema_meta`

归档目录缺 `schema_meta` 时，先核对升级路径与数据字典归档参数。

```sql
SELECT * FROM __all_cluster_event_history;
SHOW PARAMETERS LIKE 'dump_data_dictionary_to_log_interval';
```

如果升级经过 4.2.5 BP1，可能因 `dump_data_dictionary_to_log_interval=0` 导致数据字典不写归档。可在 sys 租户设置：

```sql
ALTER SYSTEM SET dump_data_dictionary_to_log_interval = '24h';
```

等待后检查新的 piece 是否生成 `schema_meta`。

## 输出格式

1. **事实表**：症状、归档路径/tenant/LS、已查视图、结论。
2. **恢复边界**：明确可恢复的时间或 SCN 区间，注明是否跨归档中断。
3. **动作清单**：修 NFS/权限、调整对象存储路径、补权限、修 restore source、升级版本或调整参数。

## 自检

- 归档卡住是否定位到具体 LS 和 leader 节点，而不是只看租户总状态？
- 物理恢复 `4018` 是否同时核对备份集 `MIN_RESTORE` 与归档 `CHECKPOINT`？
- 对象存储是否排除末级目录预创建和删除权限缺失？
- 备租户问题是否核对本租户误填与 `cluster_id`？
- oblogminer 问题是否核对升级历史和 `dump_data_dictionary_to_log_interval`？
