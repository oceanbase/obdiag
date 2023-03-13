## gather perf命令

- 支持一键获取observer进程的"扁鹊图" ,ob运行态的调用关系，用"sample"代指
- 支持一键获取observer进程的"perf 火焰图"， 用"flame"代指
- 支持一键获取observer进程的"pstack 火焰图", 用"pstack"代指

```
./obdiag gather perf [-h]

Example: ./obdiag gather perf --scope all
```

执行结果
```shell script
Example: ./obdiag gather perf --scope all

Gather Perf Summary:
+----------------+-----------+----------+--------+-------------------------------------------------------------------+
| Node           | Status    | Size     | Time   | PackPath                                                          |
+================+===========+==========+========+===================================================================+
| 192.168.2.11   | Completed | 368.178K | 90 s   | gather_pack_20230117140836/perf_192.168.2.11_20230117140836.zip   |
+----------------+-----------+----------+--------+-------------------------------------------------------------------+
| 192.168.2.12   | Completed | 368.178K | 90 s   | gather_pack_20230117140836/perf_192.168.2.12_20230117140836.zip   |
+----------------+-----------+----------+--------+-------------------------------------------------------------------+

```
scope 可选项: ["sample", "flame", "pstack", "all"]
- sample:表示采集扁鹊图
- flame: 表示采集ob的Perf火焰图
- pstack: 表示采集pstack火焰图
- all: 表示扁鹊图、ob的Perf火焰图、pstack火焰图都采集，默认值

## 查看结果
收集到的数据是通过perf工具进行采集的，可以通过Flame Graph中的工具对数据进行图形转化

### 图形转化步骤

1. 解压收集到的数据
解压采集到的文件，例如解压：perf_192.168.2.11_20230117140836.zip，解压之后有如下文件
```shell script
flame.viz flame.data pstack.viz  sample.data sample.viz
```
对应关系如下：
- flame.viz（perf 火焰图)
- pstack.viz（pstack 火焰图)
- sample.viz（扁鹊图）




