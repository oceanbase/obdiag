## gather admin命令

通过ob_admin工具能解析clog和slog文件，并对所选时间范围内的clog和slog进行一键收集。
```
$ obdiag gather clog -h
Usage: obdiag gather clog [options]

Options:
  --from=FROM           specify the start of the time range. 'format: yyyy-mm-
                        dd hh:mm:ss'
  --to=TO               specify the end of the time range. 'format: yyyy-mm-dd
                        hh:mm:ss'
  --since=SINCE         Specify time range that from 'n' [d]ays, 'n' [h]ours
                        or 'n' [m]inutes. before to now. format: <n> <m|h|d>.
                        example: 1h.
  --encrypt=ENCRYPT     Whether the returned results need to be encrypted,
                        choices=[true, false]
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.

```

执行结果:
```shell script
$ obdiag gather clog --from "2023-01-16 18:25:00" --to "2023-01-17 01:30:00"

Gather clog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/clog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```

```
$ obdiag gather slog -h
Usage: obdiag gather slog [options]

Options:
  --from=FROM           specify the start of the time range. 'format: yyyy-mm-
                        dd hh:mm:ss'
  --to=TO               specify the end of the time range. 'format: yyyy-mm-dd
                        hh:mm:ss'
  --since=SINCE         Specify time range that from 'n' [d]ays, 'n' [h]ours
                        or 'n' [m]inutes. before to now. format: <n> <m|h|d>.
                        example: 1h.
  --encrypt=ENCRYPT     Whether the returned results need to be encrypted,
                        choices=[true, false]
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.

```

执行结果:
```shell script
$ obdiag gather slog --from "2023-01-16 18:25:00" --to "2023-01-17 01:30:00"

Gather slog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/slog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```