
# Oceanbase Diagnosis Gather
故障场景下，现场的前线服务人员无法进行故障问题排查的时候，需要求助于后端的技术人员进行专家服务，这种情况下，交互一般都是非常耗时且冗长的，技术人员提出一个要求，比如查询一个SQL，前线服务人员查出来结果后返回给技术人员，然后再要求看下一个内容，有时候，两边的人员都有可能被某个事情中断，导致消息没办法有效率的应答，从而使得整个排查时间特别长，技术人员在共享消息的时候也是比较零碎，不利于问题排查。因此有必要提供一套工具来提升故障问题排查的交互流程。

诊断信息收集(Oceanbase Diagnosis Gather) ODG工具的任务是将各类工具产生的信息进行回收汇总，这个汇总后的结果文件就是前后端人员之间相互传递的信息；

在第一期中，仅实现日志的收集、压缩、打包功能；

# 编译安装包：
- 执行环境: python >= 3.6.5 或者python > 2.7.5

```shell script
cd ob-diagnosis-gather
./build/build.sh

```
说明：编译后的包是在./build/路径下的tar包，可直接使用解压后使用


# 安装部署说明：
## 安装环境要求
- 执行环境: python >= 3.6.5 或者python > 2.7.5
- 环境依赖：需要环境中支持zip 命令
- ODG是集中式采集,部署环境只需要在网络可连通到ob集群所要采集诊断信息的机器节点即可，可以不是ob所在的节点。

```shell script
# 解压编译包
tar zxvf ob-diagnosis-gather-xxxx-xxxxxxxx.tar.gz
cd ob-diagnosis-gather

```

# 参数配置说明：
## 参数配置
所有的配置文件都在conf目录下:

```bash
node_config.json #所要收集的节点配置
ocp_config.json #如果你所在的ob集群是被ocp接管的，那么你可以通过配置此文件快速的进行配置生成以及awr报告的收集
odg_conf.json #odg本身的配置文件，一般不用修改
```

使用的时候需要根据实际情况配置2个配置文件。
- ocp_config.json(该文件不是必须配置的，如果你所在的ob集群是被ocp接管的话，你可以配置此文件来实现node配置的快速配置化以及通过Ocp来收集awr报告). 如果你部署的ob集群没有通过ocp来接管，你可以忽略ocp_config.json文件，直接配置node_config.json即可
```bash
{
  "ocp": {
    "is_exits": true,
    "url": "http://ip:port", #ocp的访问地址
    "user": "xxxx",
    "password": "******" # 直接填实际密码
  },
  # ocp对应的metadb的信息
  "ocp_meta": {
    "is_exits": true,
    "ip": "xx.xx.xx.xx",
    "port": "xxx",
    "database": "xxxx",
    "user": "xxxxx",
    "password": "*******" # 直接填实际密码
  }
}
```
- node_config.json
```bash
{
  "nodes": [
    {
      "ip": "xx.xx.xx.xx", # 希望收集的主机ip地址
      "port": 22, # 主机ssh的端口号
      "user": "xxxx", # 能够SSH到主机的用户
      "password": "******", # 能够SSH到主机的密码,明文密码
      "private_key": "" # 如果是公钥私钥的方式，此处配置的是实际的密钥文件路径
    },
    {
      "ip": "xx.xx.xx.xx",
      "port": 22,
      "user": "xxxx",
      "password": "******",
      "private_key": ""
    }
  ]
}
```

- odg_conf.json
```bash
{
  "logger_conf": {
    "mode": "odg", ## 固定模式，暂不支持修改
    "log_dir": "/tmp/ob-diagnosis-gather/log", # 日志目录
    "log_filename": "odg.log", # 日志名
    "log_level": "INFO", # 日志级别
    "file_handler_log_level": "DEBUG", # 文件句柄相关的日志级别
    "stdout_handler_log_level": "DEBUG" # 标准输出的日志级别
  }
}

```
说明：odg_conf.json文件一般不用修改，保持默认即可

Tips: 当有ocp的时候，你如果想要收集某个集群下的所有主机的，你也可以通过./odg_ctl config来快速生成node_config.json的配置内容,使用方式如下:
```
./odg_ctl config -h
usage: ./odg_ctl config [-h] --cluster cluster_name --cluster_id cluster_id

Quick build config

optional arguments:
  -h, --help            show this help message and exit
  --cluster_name cluster_name
                        cluster name
  --cluster_id cluster_id
                        cluster id

Example: ./odg_ctl config --cluster_name demo1 --cluster_id xxx

```

# 功能介绍
通过 `./odg_ctl -h` 的命令，可以查看 Oceanbase Diagnosis Gather 的使用帮助。
```
usage: ./odg_ctl [-h] {config,gather} ...

Oceanbase Diagnosis Gather

positional arguments:
  {config,gather}
    config         Quick build config
    gather         Gather logs and other information

optional arguments:
  -h, --help       show this help message and exit

```

## gather log命令
通过 gather log命令，可以指定时间范围的来去搜集目标主机上的OceanBase日志(后续会陆续开放除OceanBase运行日志外其他信息的搜集)。
```
./odg_ctl gather log -h
usage: ./odg_ctl gather log [-h] [--from datetime datetime] [--to datetime datetime] [--scope scope] [--grep grep] [--encrypt encrypt]
                            [--ob_log_dir ob_log_dir] [--store_dir store_dir]

According to the input parameters, gather the logs of the specified range (whether it is time range), compress and pack, and transmit to the
specified path of the odg machine.

optional arguments:
  -h, --help            show this help message and exit
  --from datetime datetime
                        specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.
  --to datetime datetime
                        specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.
  --since 'n'<m|h|d>    Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.
  --scope scope         log type constrains, choices=[observer, election, rootservice, all], default=all
  --grep grep           specify keywords constrain
  --encrypt encrypt     Whether the returned results need to be encrypted, choices=[true, false], default=false
  --ob_log_dir ob_log_dir
                        the dir to ob log dir
  --store_dir store_dir
                        the dir to store logs, current dir by default.

Example: ./odg_ctl gather log --scope observer --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00

```

例子：
```shell script
./odg_ctl gather log --scope observer --from 2022-06-25 10:25:00 --to 2022-06-25 18:30:00 --grep STORAGE --encrypt true

...
ZipFileInfo:
+----------------+-----------+
| Node           | LogSize   |
+================+===========+
| xx.xx.xx.xx    | 36.184M   |
+----------------+-----------+
...

ZipFileInfo:
+----------------+-----------+
| Node           | LogSize   |
+================+===========+
| xx.xx.xx.xx    | 44.176M   |
+----------------+-----------+
...

Gather Ob Log Summary:
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+
| Node           | Status    | Size     | Password         | Time   | PackPath                                                            |
+================+===========+==========+==================+========+=====================================================================+
| xx.xx.xx.xx    | Completed | 36.762M  | ******           | 19 s   | gather_pack_20220701183246/ob_log_xx.xx.xx.xx_20220701183247.zip    |
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+
| xx.xx.xx.xx    | Completed | 638.200M | ******           | 718 s  | gather_pack_20220701183246/ob_log_xx.xx.xx.xx_20220701183918.zip    |
+----------------+-----------+----------+------------------+--------+---------------------------------------------------------------------+

```
注意：如果选择加密模式，对于收集到的日志压缩的时候进行了加密处理，Password是zip包解压缩的密码。默认是不加密的模式。
