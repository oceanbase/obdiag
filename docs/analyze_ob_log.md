## analyze log命令
通过 analyze log命令，可以指定时间范围去分析目标主机上的OceanBase日志以及传递OceanBase的日志文件进行分析。
```
$ obdiag analyze log -h
usage: obdiag analyze log [-h] [--from datetime datetime] [--to datetime datetime] [--since 'n'<m|h|d>] [--store_dir store_dir]
                            [--ob_install_dir ob_install_dir] [--scope scope] [--files files [files ...]] [--grep grep [grep ...]]

According to the input parameters, analyze observer logs

optional arguments:
  -h, --help            show this help message and exit
  --from datetime datetime
                        specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.
  --to datetime datetime
                        specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.
  --since 'n'<m|h|d>    Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.
  --store_dir store_dir
                        the dir to store gather result, current dir by default.
  --ob_install_dir ob_install_dir
                        the dir to ob log dir
  --scope scope         log type constrains, choices=[observer, election, rootservice, all], default=all
  --log_level log_level
                        log level constrains, choices=[DEBUG, TRACE, INFO,
                        WDIAG, WARN, EDIAG, ERROR], default=WDIAG
  --files files [files ...]
                        specify file
  --grep grep [grep ...]
                        specify keywords constrain

Example1: obdiag analyze log --scope observer --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 
Example2: obdiag analyze log --scope observer --since 1h --grep STORAGE 
Example3: obdiag analyze log --files observer.log.20230831142211247 
Example4: obdiag analyze log --files ./log/
```

例子：
```shell script
$ obdiag analyze log --scope observer --from 2023-10-08 10:25:00 --to 2023-10-08 11:30:00

...
FileListInfo:
+----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Node           | LogList                                                                                                                                                                                                               |
+================+=======================================================================================================================================================================================================================+
| 192.168.2.11   | ['observer.log.20231008104204260', 'observer.log.20231008111305072', 'observer.log.20231008114410668', 'observer.log.wf.20231008104204260', 'observer.log.wf.20231008111305072', 'observer.log.wf.20231008114410668'] |
+----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
...


Analyze OceanBase Online Log Summary:
+----------------+-----------+------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| Node           | Status    | FileName                                                                     |   ErrorCode | Message                                                                                                                       |   Count |
+================+===========+==============================================================================+=============+===============================================================================================================================+=========+
| 192.168.2.11   | Completed | analyze_pack_20231008171201/192_168_2_11/observer.log.20231008104204260      |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |       2 |
+----------------+-----------+------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 192.168.2.11   | Completed | analyze_pack_20231008171201/192_168_2_11/observer.log.20231008111305072      |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |       8 |
+----------------+-----------+------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 192.168.2.11   | Completed | analyze_pack_20231008171201/192_168_2_11/observer.log.20231008114410668      |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |      10 |
+----------------+-----------+------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 192.168.2.11   | Completed | analyze_pack_20231008171201/192_168_2_11/observer.log.20231008114410668      |       -4009 | IO error                                                                                                                      |      20 |
+----------------+-----------+------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
For more details, please run cmd 'cat analyze_pack_20231008171201/result_details.txt'
```

```shell script
$ ls -lh test/
-rw-r--r--  1 admin  staff   256M Oct  8 17:24 observer.log.20231008104204260
-rw-r--r--  1 admin  staff   256M Oct  8 17:24 observer.log.20231008111305072
-rw-r--r--  1 admin  staff   256M Oct  8 17:24 observer.log.20231008114410668
-rw-r--r--  1 admin  staff    18K Oct  8 17:24 observer.log.wf.20231008104204260
-rw-r--r--  1 admin  staff    19K Oct  8 17:24 observer.log.wf.20231008111305072
-rw-r--r--  1 admin  staff    18K Oct  8 17:24 observer.log.wf.20231008114410668

$ obdiag analyze log --files test/

Analyze OceanBase Offline Log Summary:
+-----------+-----------+-----------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| Node      | Status    | FileName                                                              |   ErrorCode | Message                                                                                                                       |   Count |
+===========+===========+=======================================================================+=============+===============================================================================================================================+=========+
| 127.0.0.1 | Completed | analyze_pack_20231008172144/127_0_0_1_/observer.log.20231008104204260 |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |       2 |
+-----------+-----------+-----------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 127.0.0.1 | Completed | analyze_pack_20231008172144/127_0_0_1_/observer.log.20231008111305072 |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |       8 |
+-----------+-----------+-----------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 127.0.0.1 | Completed | analyze_pack_20231008172144/127_0_0_1_/observer.log.20231008114410668 |       -5006 | You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use |      10 |
+-----------+-----------+-----------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
| 127.0.0.1 | Completed | analyze_pack_20231008172144/127_0_0_1_/observer.log.20231008114410668 |       -4009 | IO error                                                                                                                      |      20 |
+-----------+-----------+-----------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------+
For more details, please run cmd 'cat analyze_pack_20231008172144/result_details.txt'
```
