## gather obproxy_log命令
通过 gather obproxy_log命令，可以指定时间范围的来去搜集目标主机上的ObProxy日志。
```
$ obdiag gather obproxy_log -h
Usage: obdiag gather obproxy_log [options]

Options:
  --from=FROM           specify the start of the time range. 'format: yyyy-mm-
                        dd hh:mm:ss'
  --to=TO               specify the end of the time range. 'format: yyyy-mm-dd
                        hh:mm:ss'
  --since=SINCE         Specify time range that from 'n' [d]ays, 'n' [h]ours
                        or 'n' [m]inutes. before to now. format: <n> <m|h|d>.
                        example: 1h.
  --scope=SCOPE         log type constrains, choices=[observer, election,
                        rootservice, all]
  --grep=GREP           specify keywords constrain
  --encrypt=ENCRYPT     Whether the returned results need to be encrypted,
                        choices=[true, false]
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.
```

例子：
```shell script
$ obdiag gather obproxy_log --scope obproxy --from "2022-06-25 10:25:00" --to "2022-06-25 18:30:00" --encrypt true

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
| 192.168.2.11   | Completed | 36.762M  | **************** | 19 s   | gather_pack_20220701183246/obproxy_log_192.168.2.11_20220701183247.zip   |
+----------------+-----------+----------+------------------+--------+--------------------------------------------------------------------------+
| 192.168.2.12   | Completed | 638.200M | **************** | 718 s  | gather_pack_20220701183246/obproxy_log_192.168.2.12_20220701183918.zip   |
+----------------+-----------+----------+------------------+--------+--------------------------------------------------------------------------+

```
注意：如果选择加密模式，对于收集到的日志压缩的时候进行了加密处理，Password是zip包解压缩的密码。默认是不加密的模式。

