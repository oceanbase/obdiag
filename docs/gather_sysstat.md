## gather sysstat命令

收集主机dmesg信息、主机cpu\内存信息
```
$ obdiag gather sysstat -h
Usage: obdiag gather sysstat [options]

Options:
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.
```

执行结果：
```buildoutcfg
Example: obdiag gather sysstat

结果：

Summary:
+----------------+-----------+---------+--------+------------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                               |
+================+===========+=========+========+========================================================================+
| 192.168.2.11   | Completed | 45.209K | 5 s    | gather_pack_20220729164233/sysstat_192.168.2.11_20220729164233.zip     |
+----------------+-----------+---------+--------+------------------------------------------------------------------------+
| 192.168.2.12   | Completed | 42.170K | 5 s    | gather_pack_20220729164233/sysstat_192.168.2.12_20220729164233.zip     |
+----------------+-----------+---------+--------+------------------------------------------------------------------------+
```