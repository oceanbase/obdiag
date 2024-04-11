<p align="center">
    <a href="https://github.com/oceanbase/oceanbase-diagnostic-tool/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase-diagnostic-tool/releases/latest">
        <img alt="license" src="https://img.shields.io/badge/dynamic/json?color=blue&label=release&query=tag_name&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase-diagnostic-tool%2Freleases%2Flatest" />
    </a>
    <a href="https://img.shields.io/badge/python%20-3.8.0%2B-blue.svg">
        <img alt="pyversions" src="https://img.shields.io/badge/python%20-3.8.0%2B-blue.svg" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase-diagnostic-tool">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase-diagnostic-tool" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase-diagnostic-tool">
        <img alt="forks" src="https://img.shields.io/badge/dynamic/json?color=blue&label=forks&query=forks&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase-diagnostic-tool" />
    </a>
    <a href="https://www.oceanbase.com/docs/obdiag-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
</p>

# OceanBase Diagnostic Tool (obdiag)

## 简介
OceanBase Diagnostic Tool (obdiag) 是一款专门OceanBase打造的敏捷诊断工具，功能包括诊断信息收集、分析、巡检，可以在OceanBase集群不同的部署模式下（OCP，OBD或用户根据文档手工部署）实现一键执行。

## 项目价值 & 愿景

![项目价值](./images/obdiag_cn.png)

# 安装 obdiag

可通过如下方式安装obdiag

## 方式一: 通过rpm包安装
```shell script
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y oceanbase-diagnostic-tool
sh /usr/local/oceanbase-diagnostic-tool/init.sh
```

## 方式二：源码安装
源码编译环境确保有如下依赖
- gcc
- wget
- python-devel
- mysql-devel

源码安装需要在python >= 3.8的环境下进行

```shell
pip3 install -r requirements3.txt
sh dev_init.sh
source ~/.bashrc
```

# obdiag config
配置文件的默认路径是`~/.obdiag/config.yml` . 可直接编辑配置文件或者通过`obdiag config <option>` 命令快速生成配置
```shell script
obdiag config -h <db_host> -u <sys_user> [-p password] [-P port]
```

# obdiag 功能

## obdiag 巡检功能
- [一键巡检](./docs/check.md)

## obdiag 一键场景化信息采集功能
- [一键场景化信息采集](./docs/gather_scene.md)

## obdiag 一键信息采集功能

- [一键收集OB日志](./docs/gather_ob_log.md)
- [一键收集AWR报告](./docs/gather_awr.md)
- [一键收集主机信息](./docs/gather_sysstat.md)
- [一键收集slog/clog日志](./docs/gather_admin.md)
- [一键收集火焰图信息](./docs/gather_perf.md)
- [一键收集OB堆栈信息](./docs/gather_ob_stack.md)
- [一键收集并行SQL的执行详情信息](./docs/gather_sql_plan_monitor.md)
- [一键收集OBPROXY日志](./docs/gather_obproxy_log.md)
- [一键收集AWR报告](./docs/gather_awr.md)
- [一键收集全部诊断信息](./docs/gather_all.md)

## obdiag 一键分析功能说明
- [一键分析OB日志](./docs/analyze_ob_log.md)
- [一键全链路诊断](./docs/analyze_flt_trace.md)

## obdiag 一键场景化根因分析功能
- [一键场景化根因分析](./docs/rca.md)

# 许可证

OceanBase Diagnostic Tool 使用 [MulanPSL - 2.0](http://license.coscl.org.cn/MulanPSL2) 许可证。
您可以免费复制及使用源代码。当您修改或分发源代码时，请遵守木兰协议。


## 贡献

我们热烈欢迎并高度赞赏您的贡献。您可以通过以下几种方式做出贡献：

- 向我们提出一个[issue](https://github.com/oceanbase/oceanbase-diagnostic-tool/issues)。
- 提交请求。

## 支持

如果您在使用 OceanBase LogProxy 时遇到任何问题，欢迎联系我们寻求帮助：

- [GitHub Issue](https://github.com/oceanbase/oceanbase-diagnostic-tool/issues)
- [官方网站](https://www.oceanbase.com/docs/obdiag-cn)
