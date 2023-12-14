## gather awr命令

该命令用户收集性能报告报告
```
$ obdiag gather awr -h
usage: obdiag gather awr [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                         [-c config] --cluster_name cluster_name

According to the input parameters, gather the awr of the specified range (whether it is time range), compress and pack, and transmit to
the specified path of the obdiag machine.

optional arguments:
  -h, --help            show this help message and exit
  --from datetime datetime
                        specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.
  --to datetime datetime
                        specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.
  --since 'n'<m|h|d>    Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>.
                        example: 1h.
  --store_dir store_dir
                        the dir to store gather result, current dir by default.
  -c config             obdiag custom config
  --cluster_name cluster_name
                        cluster name.

Example: obdiag gather awr --cluster_name demo1 --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

执行结果：
```buildoutcfg
Gather AWR Summary:
+-----------+-----------+--------+--------+----------------------------------------------------------------------------------------+
| Cluster   | Status    | Size   | Time   | PackPath                                                                               |
+===========+===========+========+========+========================================================================================+
| demo1     | Completed | 4.602M | 29 s   | gather_pack_20220627005659/OBAWR_obcluster_jingshun_20220625160100_20220625180100.html |
+-----------+-----------+--------+--------+----------------------------------------------------------------------------------------+
```