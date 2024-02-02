## gather scenes 命令

该命令可以一键执行将某些问题场景所需要的排查信息统一捞回，解决分布式节点信息捞取难的通点

## 查看当前支持的场景

```shell script
obdiag gather scene list
```

```bash
obdiag gather scene list

[Other Problem Gather Scenes]:
------------------------------------------------------------------------------------------
command                                                      info_en               info_cn
------------------------------------------------------------------------------------------
obdiag gather scene run --scene=other.application_error   [application error]   [应用报错问题]
------------------------------------------------------------------------------------------

[Obproxy Problem Gather Scenes]:
----------------------------------------------------------------------------------
command                                           info_en             info_cn
----------------------------------------------------------------------------------
obdiag gather scene run --scene=obproxy.restart   [obproxy restart]   [obproxy无故重启]
----------------------------------------------------------------------------------

[Observer Problem Gather Scenes]:
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
command                                                                                                              info_en                         info_cn
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
obdiag gather scene run --scene=observer.backup                                                                      [backup problem]                [数据备份问题]
obdiag gather scene run --scene=observer.backup_clean                                                                [backup clean]                  [备份清理问题]
obdiag gather scene run --scene=observer.clog_disk_full                                                              [clog disk full]                [clog盘满]
obdiag gather scene run --scene=observer.compaction                                                                  [compaction]                    [合并问题]
obdiag gather scene run --scene=observer.cpu_high                                                                    [High CPU]                      [CPU高]
obdiag gather scene run --scene=observer.delay_of_primary_and_backup                                                 [delay of primary and backup]   [主备库延迟]
obdiag gather scene run --scene=observer.log_archive                                                                 [log archive]                   [日志归档问题]
obdiag gather scene run --scene=observer.long_transaction                                                            [long transaction]              [长事务]
obdiag gather scene run --scene=observer.memory                                                                      [memory problem]                [内存问题]
obdiag gather scene run --scene=observer.perf_sql --env "{db_connect: '-hxx -Pxx -uxx -pxx -Dxx', trace_id: 'xx'}"   [SQL performance problem]       [SQL性能问题]
obdiag gather scene run --scene=observer.recovery                                                                    [recovery]                      [数据恢复问题]
obdiag gather scene run --scene=observer.restart                                                                     [restart]                       [observer无故重启]
obdiag gather scene run --scene=observer.rootservice_switch                                                               [rootservice switch]            [有主改选或者无主选举的切主]
obdiag gather scene run --scene=observer.sql_err --env "{db_connect: '-hxx -Pxx -uxx -pxx -Dxx', trace_id: 'xx'}"    [SQL execution error]           [SQL 执行出错]
obdiag gather scene run --scene=observer.suspend_transaction                                                         [suspend transaction]           [悬挂事务]
obdiag gather scene run --scene=observer.unit_data_imbalance                                                         [unit data imbalance]           [unit迁移/缩小 副本不均衡问题]
obdiag gather scene run --scene=observer.unknown                                                                     [unknown problem]               [未能明确问题的场景]
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## 快速使用

```shell script
obdiag gather scene run --scene={SceneName}
```
### 关联动态可配参数：
```shell script
--scene={SceneName}

SceneName是对需要执行收集的场景


Example1:
obdiag gather scene run --scene=observer.unknown
```


## task编写教程
一个task表示一个独立的场景，可以理解为一个专业的，用yaml编写的，用obdiag识别的脚本文件。

### 开始编写前
编写前需要确定yaml需要放在哪

可以先进入~/.obdiag/inner_config.yml文件中设置 gather.scenes_base_path 所标识的目录里，看下编写的采集场景是否属于已有的大类，若没有就创建一个文件夹用于声明这个大类

例：


```ssh script
#先进入${gather.scenes_base_path} ,并创建我们的示例文件test.yaml(以observer为测试目标)
cd ~/.obdiag/gather/tasks/observer
touch test.yaml
```

以上便完成了编写前的步骤


### 开始编写
开始编写就是开始编辑我们的test.yaml

```yaml script
# 首先需要声明下这个场景的作用，为了让大家看得懂

info: "for test"
```
简单的内容已经结束，开始复杂的编写，注意细节

#### task编写

task的作用是声明场景采集执行的步骤，其基础结构是一个list




为什么task是一个list？
- 是为了兼容不同版本可能导致的步骤的不同

task的一个元素的结构如下

| 参数名     | 是否必填 |  |                                                          |  |
|---------| --- | --- |----------------------------------------------------------| --- |
| version | 否 | 表示适用的版本，使用方式见下示例 | 用str的形式表示范围，需要完整的数字的版本号，3.x版本为三位，4.x版本为四位如：[3.1.1,3.2.0]，版本支持遵循左开又闭的原则 |  |
| steps    | 是 | 所执行步骤 | 为list结构                                                  |  |

如下就是一个示例

```yaml script
info: testinfo
task:
  - version: "[3.1.0,3.2.4]"
    steps:
    	{steps_object}
  - version: [4.2.0.0,4.3.0.0]
    steps:
    	{steps_object}
 ```
steps又是一个list，用来表示具体的多个执行流程

steps的一个元素的结构即单个流程，如下

| 参数名       | 是否必填 |                                                                               |
|-----------|------|-------------------------------------------------------------------------------|
| type      | 是    | 表示适用的执行类型，目前支持 ssh/sql/log/obproxy_log/sysstat, 后续会持续增加支持的类型                       |
| {ssh/sql/log/obproxy_log/sysstat} | 是    | 根据所选的类型提供的参数，这块比较依赖代码里的对执行类型的逻辑说明，本章节后续会对支持的进行类型进行详细的使用说明                     |
各种类型示例如下，"step:" 仅为一个标记，无实际作用


##### ssh
远程执行指令并获取对应的返回值
```yaml
step:
  type: ssh
  ssh: wc -l /proc/${task_OBServer_pid}/maps | awk '{print $1}'

```
##### sql
执行sql并获取对应的值
```yaml
step:
 type: sql
 sql: select tenant_name from oceanbase.__all_tenant from where tenant_id=${taskTenantId};
```

##### log
收集observer的日志
```yaml
step:
 type: log
 grep: "" # 过滤字段
```

##### obproxy_log
收集 obproxy 的日志
```yaml
step:
 type: obproxy_log
 grep: "" # 过滤字段
```

##### sysstat
收集主机的信息
```yaml
step:
 type: sysstat
 sysstat: ""
```