## gather obproxy_log命令
通过 gather obproxy_log命令，可以指定时间范围的来去搜集目标主机上的ObProxy日志。
```
./obdiag gather obproxy_log -h
usage: ./obdiag gather obproxy_log [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                                    [--obproxy_install_dir obproxy_install_dir] [--scope scope] [--grep grep] [--encrypt encrypt]

According to the input parameters, gather the logs of the specified range (whether it is time range), compress and pack, and transmit to the specified path
of the obdiag machine.

optional arguments:
  -h, --help            show this help message and exit
  --from datetime datetime
                        specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.
  --to datetime datetime
                        specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.
  --since 'n'<m|h|d>    Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.
  --store_dir store_dir
                        the dir to store gather result, current dir by default.
  --obproxy_install_dir obproxy_install_dir
                        the dir to obproxy log dir
  --scope scope         log type constrains, choices=[obproxy, obproxy_digest, obproxy_stat, obproxy_slow, obproxy_limit, all], default=all
  --grep grep           specify keywords constrain
  --encrypt encrypt     Whether the returned results need to be encrypted, choices=[true, false], default=false

Example: ./obdiag gather obproxy_log --scope obproxy --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00
```

例子：
```shell script
./obdiag gather obproxy_log --scope obproxy --from 2022-06-25 10:25:00 --to 2022-06-25 18:30:00 --encrypt true

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

Gather ObProxy Log Summary:
+----------------+-----------+----------+------------------+--------+--------------------------------------------------------------------------+
| Node           | Status    | Size     | Password         | Time   | PackPath                                                                 |
+================+===========+==========+==================+========+==========================================================================+
| 192.168.2.11   | Completed | 36.762M  | HYmVourcUyRNP8Om | 19 s   | gather_pack_20220701183246/obproxy_log_192.168.2.11_20220701183247.zip   |
+----------------+-----------+----------+------------------+--------+--------------------------------------------------------------------------+
| 192.168.2.12   | Completed | 638.200M | 1RicMaiLUUNfemnj | 718 s  | gather_pack_20220701183246/obproxy_log_192.168.2.12_20220701183918.zip   |
+----------------+-----------+----------+------------------+--------+--------------------------------------------------------------------------+

```
注意：如果选择加密模式，对于收集到的日志压缩的时候进行了加密处理，Password是zip包解压缩的密码。默认是不加密的模式。

