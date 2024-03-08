## gather log命令
通过 gather log命令，可以指定时间范围的来去搜集目标主机上的OceanBase日志(后续会陆续开放除OceanBase运行日志外其他信息的搜集)。
```
$ obdiag gather log -h
usage: obdiag gather log [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                         [-c config] [--scope scope] [--grep grep [grep ...]] [--encrypt encrypt]

According to the input parameters, gather the logs of the specified range (whether it is time range), compress and pack, and transmit to
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
  --scope scope         log type constrains, choices=[observer, election, rootservice, all], default=all
  --grep grep [grep ...]
                        specify keywords constrain
  --encrypt encrypt     Whether the returned results need to be encrypted, choices=[true, false], default=false

Example: obdiag gather log --scope observer --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00
```

例子：
```shell script
$ obdiag gather log --scope observer --from 2022-06-25 10:25:00 --to 2022-06-25 18:30:00 --grep STORAGE --encrypt true

...
ZipFileInfo:
+----------------+-----------+
| Node           | LogSize   |
+================+===========+
| 192.168.2.11   | 36.184M   |
+----------------+-----------+
...

ZipFileInfo:
+----------------+-----------+
| Node           | LogSize   |
+================+===========+
| 192.168.2.12   | 44.176M   |
+----------------+-----------+
...

Gather Ob Log Summary:
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+
| Node           | Status    | Size     | Password         | Time   | PackPath                                                            |
+================+===========+==========+==================+========+=====================================================================+
| 192.168.2.11   | Completed | 36.762M  | **************** | 19 s   | gather_pack_20220701183246/ob_log_192.168.2.11_20220701183247.zip   |
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+
| 192.168.2.12   | Completed | 638.200M | **************** | 718 s  | gather_pack_20220701183246/ob_log_192.168.2.12_20220701183918.zip   |
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+

```
注意：如果选择加密模式，对于收集到的日志压缩的时候进行了加密处理，Password是zip包解压缩的密码。默认是不加密的模式。

