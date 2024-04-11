## analyze log
'obdiag analyze log' can specify a time range to analyze the OceanBase logs on the target host and the log files passing OceanBase for analysis.
```
$ obdiag analyze log -h
Usage: obdiag analyze log [options]

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
  --log_level=LOG_LEVEL
                        oceanbase logs greater than or equal to this level
                        will be analyze, choices=[DEBUG, TRACE, INFO, WDIAG,
                        WARN, EDIAG, ERROR]
  --files=FILES         specify files
  --store_dir=STORE_DIR
                        the dir to store gather result, current dir by
                        default.
  -c C                  obdiag custom config
  -h, --help            Show help and exit.
  -v, --verbose         Activate verbose output.
```

Example：
```shell script
$ obdiag analyze log --scope observer --from "2023-10-08 10:25:00" --to "2023-10-08 11:30:00"

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
For more details, please run cmd ' cat analyze_pack_20231008171201/result_details.txt '
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
For more details, please run cmd ' cat analyze_pack_20231008172144/result_details.txt '
```
