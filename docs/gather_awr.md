## gather awr命令

该命令用户收集性能报告报告
```
$ obdiag gather awr -h
Usage: obdiag gather awr [options]

Options:
  --cluster_name=CLUSTER_NAME
                        cluster_name from ocp
  --cluster_id=CLUSTER_ID
                        cluster_id from ocp
  --from=FROM           specify the start of the time range. format: 'yyyy-mm-
                        dd hh:mm:ss'
  --to=TO               specify the end of the time range. format: 'yyyy-mm-dd
                        hh:mm:ss'
  --since=SINCE         Specify time range that from 'n' [d]ays, 'n' [h]ours
                        or 'n' [m]inutes. before to now. format: <n> <m|h|d>.
                        example: 1h.
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.

```

例子：
```shell script
obdiag gather awr --cluster_name obtest --cluster_id 1

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