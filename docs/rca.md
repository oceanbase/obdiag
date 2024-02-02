## rca命令

通过内部对一些已知问题的归纳分析，通过对


## 快速使用

```shell script
obdiag rca run --scene={scene_name}
obdiag rca list
```
### 关联动态可配参数：
```shell script
scene_name是需要执行的根因分析场景的名称,可以通过obdiag rca list获取

```

### 关联持久化参数：
持久化参数主要是部分日常不会修改的参数，依赖于{obdiag安装目录}/conf/inner_config.yml

若使用rpm方式进行安装，inner_config.yml位于
```shell script
/user/local/oceanbase-diagnostic-tool/conf/config.yml
```

rca功能所关联的配置项在"rca"下，基本上的参数均无需变更或更改频率较低
```yaml script
rca:
  result_path: "./rca/" # rca报告保存的地址
```
