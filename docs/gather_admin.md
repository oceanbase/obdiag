## gather admin命令

通过ob_admin工具能解析clog和slog文件，并对所选时间范围内的clog和slog进行一键收集。
```
usage: ./obdiag gather clog [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                                 [--ob_install_dir ob_install_dir] [--mode mode] --cluster_name cluster_name [--encrypt encrypt]

Example: ./obdiag gather clog --cluster_name demo1 --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

执行结果:
```shell script
./obdiag gather clog --cluster_name demo1 --from 2023-01-16 18:25:00 --to 2023-01-17 01:30:00

Gather clog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/clog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```

```
usage: ./obdiag gather slog [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                                 [--ob_install_dir ob_install_dir] [--mode mode] --cluster_name cluster_name [--encrypt encrypt]

Example: ./obdiag gather clog --cluster_name demo1 --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

执行结果:
```shell script
./obdiag gather slog --cluster_name demo1 --from 2023-01-16 18:25:00 --to 2023-01-17 01:30:00

Gather slog Summary:
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
| Node           | Status    | Size    | Time   | PackPath                                                             |
+================+===========+=========+========+======================================================================+
| 192.168.2.11   | Completed | 15.762K | 6 s    | gather_pack_20230118002457/slog_192.168.2.11  _20230118002458.zip    |
+----------------+-----------+---------+--------+----------------------------------------------------------------------+
```