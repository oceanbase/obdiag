## gather ash命令

该命令用户收集性能报告报告
```
$obdiag gather ash -h
Usage: obdiag gather ash [options]

Options:
  --trace_id=TRACE_ID   The TRACE.ID of the SQL to be sampled, if left blank
                        or filled with NULL, indicates that TRACE.ID is not
                        restricted.
  --sql_id=SQL_ID       The SQL.ID, if left blank or filled with NULL,
                        indicates that SQL.ID is not restricted.
  --wait_class=WAIT_CLASS
                        Event types to be sampled.
  --report_type=REPORT_TYPE
                        Report type, currently only supports text type.
  --from=FROM           specify the start of the time range. format: 'yyyy-mm-
                        dd hh:mm:ss'
  --to=TO               specify the end of the time range. format: 'yyyy-mm-dd
                        hh:mm:ss'
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.


```

例子：
```shell script
obdiag gather ash 

```

执行结果：
```buildoutcfg
gather_ash_report start ...
gather from_time: 2024-05-08 11:18:59, to_time: 2024-05-08 11:48:59
from_time: 2024-05-08 11:18:59, to_time: 2024-05-08 11:48:59, sql_id: None, trace_id: None, report_type: TEXT, wait_class: None, store_dir: ./
save ash report file name: ./gather_pack_20240508114859/ash_report_20240508114859.txt

Gather ash_report results stored in this directory: ./gather_pack_20240508114859

Trace ID: e6af30b2-0ced-11ef-89ff-02420b9e4df1
If you want to view detailed obdiag logs, please run: obdiag display-trace e6af30b2-0ced-11ef-89ff-02420b9e4df1

```