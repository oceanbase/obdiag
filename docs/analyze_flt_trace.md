## analyze flt_trace

### Step 1: 查找疑似慢的sql
在sql audit中，如果有明确的SQL语句可以通过通过query_sql查到疑似慢sql的 flt_trace_id, 例如：
```shell script
OceanBase(root@test)>select query_sql, flt_trace_id from oceanbase.gv$ob_sql_audit where query_sql like 'select @@version_comment limit 1';
+----------------------------------+--------------------------------------+
| query_sql                        | flt_trace_id                         |
+----------------------------------+--------------------------------------+
| select @@version_comment limit 1 | 00060aa3-d607-f5f2-328b-388e17f687cb |
+----------------------------------+--------------------------------------+
1 row in set (0.001 sec)
```
其中flt_trace_id为00060aa3-d607-f5f2-328b-388e17f687cb.

### Step 2: 设置配置文件

例子
```shell script
obdiag config -h192.168.1.1 -uroot@sys -p***** -P2881
```

### Step 3: 执行全链路诊断命令
```shell script
$ obdiag analyze flt_trace -h
Usage: obdiag analyze flt_trace [options]

Options:
  --flt_trace_id=FLT_TRACE_ID
                        flt trace id, . format: xxxxxxxx-xxxx-xxxx-xxxx-
                        xxxxxxxxxxxx
  --files=FILES         specify files
  --top=TOP             top leaf span
  --recursion=RECURSION
                        Maximum number of recursion
  --output=OUTPUT       Print the result to the maximum output line on the
                        screen
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.
```
