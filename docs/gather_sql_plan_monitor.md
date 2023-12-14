## gather plan_monitor命令
```shell script
$ obdiag gather plan_monitor -h
usage: obdiag gather plan_monitor [-h] [--store_dir store_dir] [-c config] --trace_id trace_id

According to the input parameters, gather the sql plan monitor of the specified trace_id compress and pack, and transmit to the specified
path of the obdiag machine.

optional arguments:
  -h, --help            show this help message and exit
  --store_dir store_dir
                        the dir to store gather result, current dir by default.
  -c config             obdiag custom config
  --trace_id trace_id   sql trace id

Example: obdiag gather plan_monitor --trace_id xxxxx
```

