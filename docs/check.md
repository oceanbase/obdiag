## check命令

通过内部对一些已知问题的归纳分析，对用户现有集群进行数据采集分析。巡检模块的目的是依赖已有的案例提供多个检测项对用户集群进行分析，发现已存在或可能会导致集群出现异常问题的原因分析并提供运维建议。

## 注意
在独立使用check能力，且未配置ocp的情况下，请务必完整配置config.yml内的OBCLUSTER、NODES信息
巡检极为依赖配置信息

## 快速使用

```shell script
obdiag check
```
### 关联动态可配参数：
```shell script
--cases={CasesName}

CasesName是对需要执行的巡检项目的集合名,巡检集合保存在"~/.obdiag/{check_target}_check_package.yaml" ,每次仅能选择一个集合
若未设定则默认执行所有的巡检项

{check_target}目前已支持的类型:
observer  ->check_package.yaml
obproxy   ->obproxy_check_package.yaml


Example:
obdiag check --cases= ad
obdiag check --obproxy_cases= proxy
obdiag check --cases=ad --obproxy_cases=proxy
```

### 关联持久化参数：
持久化参数主要是部分日常不会修改的参数，依赖于conf/inner_config.yml

若使用rpm方式进行安装，config.yml位于
```shell script
/user/local/oceanbase-diagnostic-tool/conf/inner_config.yml
```

check功能所关联的配置项在"CHECK"下，基本上的参数均无需变更或更改频率较低
```yaml script
check:
  ignore_version: false
  work_path: "~/.obdiag/check"
  report:
    report_path: "./check_report/"
    export_type: table
  package_file: "~/.obdiag/check/check_package.yaml"
  tasks_base_path: "~/.obdiag/check/tasks/"
```
ignore_version: 表示是否需要在执行巡检项时跳过版本匹配
work_path: 巡检场景的存储目录
report: 下主要是对报告的参数进行配置
- report_path: 表示输出报告的路径
- export_type: 表示输出报告的类型，目前支持table 、json 、xml后续需要支持的可以提交issue
package_file: 表示巡检项集合的保存路径
tasks_base_path: 表示巡检项所保存的头路径,下面存储了不同check_target的巡检项目文件

  

## task编写教程
task是一个独立的巡检场景，可以理解为一个专业的，用yaml编写的，用obdiag识别的脚本文件。

task会包含一些用于巡检的前置声明，用于实现对ob进行更为专业的巡检
### 开始编写前
编写前需要确定yaml需要放在哪

可以先进入conf.yml文件中设置CHECK.tasks_base_path所标识的目录里，看下分析下编写的巡检场景是否属于已有的大类，若没有就创建一个文件夹用于声明这个大类

例：


```ssh script
#先进入${CHECK.tasks_base_path} ,然后创建一个文件夹test,并创建我们的示例文件test.yaml(以observer为测试目标)
cd ~/.obdiag/check/tasks/observer
mkdir test
cd test
touch test.yaml
```

以上便完成了编写前的步骤


### 开始编写
开始编写就是开始编辑我们的test.yaml

```yaml script
# 首先需要声明下这个场景的作用，为了让大家看得懂

info: "for test"
```
简单的内容已经结束，开始复杂的编写，注意细节

#### task编写

task的作用是声明巡检执行的步骤，其基础结构是一个list




为什么task是一个list？
- 是为了兼容不同版本可能导致的步骤的出入、或者压根这个巡检项目没法有

task的一个元素的结构如下

| 参数名     | 是否必填 |  |                                                          |  |
|---------| --- | --- |----------------------------------------------------------| --- |
| version | 否 | 表示适用的版本，使用方式见下示例 | 用str的形式表示范围，需要完整的数字的版本号，3.x版本为三位，4.x版本为四位如：[3.1.1,3.2.0] |  |
| steps    | 是 | 所执行步骤 | 为list结构                                                  |  |

如下就是一个示例

```yaml script
info: testinfo
task:
  - version: "[3.1.0,3.2.4]"
    steps:
    	{steps_object}
  - version: [4.2.0.0,4.3.0.0]
    steps:
    	{steps_object}
 ```
steps又是一个list，用来表示具体的多个执行流程

steps的一个元素的结构即单个流程，如下

| 参数名       | 是否必填 |                                                                               |
|-----------|------|-------------------------------------------------------------------------------|
| type      | 是    | 表示适用的执行类型，目前支持get_system_parameter/ssh/sql,后续会持续增加支持的类型                       |
| {ssh/sql} | 是    | 根据所选的类型提供的参数，这块比较依赖代码里的对执行类型的逻辑说明，本章节后续会对支持的进行类型进行详细的使用说明                     |
| result    | 否    | 结构为一个单独的对象，用于对这个步骤结束后需要进行的操作进行解析，如校验结果逻辑，逻辑不通过时需要报错的文本信息进行说明等等。具体本章节后续会进行详细说明 |

各种类型示例如下，"step:" 仅为一个标记，无实际作用

##### get_system_parameter
```yaml
step:
  type: get_system_parameter
  parameter_name: parameter
  result:
    set_value: servervm.max_map_count
  

```
##### ssh
远程执行指令并获取对应的返回值
```yaml
step:
  type: ssh
  ssh: wc -l /proc/${task_OBServer_pid}/maps | awk '{print $1}'
  result:
    set_value: observerMaps

```
##### sql
执行sql并获取对应的值
```yaml
step:
 type: sql
 sql: select tenant_name from oceanbase.__all_tenant from where tenant_id=${taskTenantId};
 result:
  set_value: tenant_name

```



### result（verify功能）
这个字段也是verify功能的主要依赖字段，用于对task获取结果的验证

| 参数名         | 是否必填 |                                 |                                                                  |
|-------------|------|---------------------------------|------------------------------------------------------------------|
| set_value   | 否    | 将执行后的值赋值,作为一个适用于整个task的变量       | 例如set_value: max_map_count                                       |
| verify_type | 否    | 默认为base，一般需要和verify联动           | 用于设置验证的方式，base即为通过verify的表达式进行验证，true或false，同时提供了以下常见的判断类型，减少编写量 |
| verify      | 否    | 服务于verify_type                  | 用于验证执行结果是否符合预期，若不符合，会输出errMsg部分的信息。                              |
| report_type | 否    | 用于设置本步骤若出现verify为false需要执行的告警级别 | 默认告警级别为critical                                                  |
| err_msg     | 否    | 用于非正常执行时答应的日志，支持配置全局变量          | 在verify为false的时候所输出的msg建议配置了verify，就一定要配上err_msg                 |

目前verify_type支持的类型，除了base外的类型仅适用于int类型。

between：判断set_value的值是否在verify提供的范围内；

max：是否小于verify提供的值

min：是否大于min提供的值

equal：是否等于verify（兼容字符串或int，但是${set_value}和verify必须是同类型）

base：
verify表达式会用于替换如下shell式子中的new_expr内进行执行验证，在编写verify时可以手动在本地进行逻辑验证
```
if ${new_expr}; then
    echo "true"
else
    echo "false"
fi
```

