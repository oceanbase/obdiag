## gather awr命令

该命令用户收集性能报告报告
```
$ obdiag gather awr -h
Usage: obdiag gather <command> [options]

Available commands:

all          Gather oceanbase diagnostic info

clog         Gather clog

log          Gather oceanbase logs from oceanbase machines

obproxy_log  Gather obproxy log from obproxy machines

perf         Gather perf

plan_monitor Gather ParalleSQL information

scene        Gather scene diagnostic info

slog         Gather slog

stack        Gather stack

sysstat      Gather Host information


Options:
  -h, --help     Show help and exit.
  -v, --verbose  Activate verbose output.

```

执行结果：
```buildoutcfg
Gather AWR Summary:
+-----------+-----------+--------+--------+----------------------------------------------------------------------------------------+
| Cluster   | Status    | Size   | Time   | PackPath                                                                               |
+===========+===========+========+========+========================================================================================+
| demo1     | Completed | 4.602M | 29 s   | gather_pack_20220627005659/OBAWR_obcluster_demo1_20220625160100_20220625180100.html    |
+-----------+-----------+--------+--------+----------------------------------------------------------------------------------------+
```