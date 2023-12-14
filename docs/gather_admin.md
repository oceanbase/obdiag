## gather admin命令

通过ob_admin工具能解析clog和slog文件，并对所选时间范围内的clog和slog进行一键收集。
```
$ obdiag gather clog -h
usage: obdiag gather clog [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                          [-c config] [--encrypt encrypt]

According to the input parameters, gather the clog of the specified range (whether it is time range), compress and pack, and transmit to
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
  --encrypt encrypt     Whether the returned results need to be encrypted, choices=[true, false], default=false

Example: obdiag gather clog --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

执行结果:
```shell script
$ obdiag gather clog --from 2023-01-16 18:25:00 --to 2023-01-17 01:30:00

Gather clog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/clog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```

```
$ obdiag gather slog -h
usage: obdiag gather slog [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                          [-c config] [--encrypt encrypt]

According to the input parameters, gather the slog of the specified range (whether it is time range), compress and pack, and transmit to
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
  --encrypt encrypt     Whether the returned results need to be encrypted, choices=[true, false], default=false

Example: obdiag gather slog --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

执行结果:
```shell script
$ obdiag gather slog --from 2023-01-16 18:25:00 --to 2023-01-17 01:30:00

Gather slog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/slog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```