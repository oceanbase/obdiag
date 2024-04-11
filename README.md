English | [中文版](README-CN.md)

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

## Overview
OceanBase Diagnostic Tool (obdiag) is a quick diagnostic tool for open-source OceanBase software. The features include gather\analyze\check OceanBase Diagnostic information. It can be executed with one click in different deployment modes of OceanBase clusters (OCP, OBD, or manually deployed by users according to documentation).

## Project Value & Vision
![Project Value](./images/obdiag_en.png)

# Install obdiag
You can install obdiag by using these methods:

## Method 1: Install obdiag by using RPM packages
```shell script
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y oceanbase-diagnostic-tool
sh /usr/local/oceanbase-diagnostic-tool/init.sh
```

## Method 2: Install obdiag by using the source code

Before you install obdiag by using the source code, make sure that you have installed these dependencies:

- gcc
- wget
- python-devel
- mysql-devel

To install obdiag on Python3.8, run these commands:

```shell
pip3 install -r requirements3.txt
sh dev_init.sh
source ~/.bashrc
```

# obdiag config
The path of the configuration file for the diagnosed cluster is stored in `~/.obdiag/config.yml` .You can directly edit the configuration file or generate the configuration file through the `obdiag config <option>` command
```shell script
obdiag config -h <db_host> -u <sys_user> [-p password] [-P port]
```

# obdiag Fuctions

## obdiag check Fuctions
- [check](./docs/check.md)

## obdiag gather scene Fuctions
- [gather scene](./docs/gather_scene.md)

## obdiag gather Fuctions

- [gather log](./docs/gather_ob_log.md)
- [gather sysstat](./docs/gather_sysstat.md)
- [gather slog/clog](./docs/gather_admin.md)
- [gather perf](./docs/gather_perf.md)
- [gather obstack](./docs/gather_ob_stack.md)
- [gather sql plan monitor](./docs/gather_sql_plan_monitor.md)
- [gather obproxy_log](./docs/gather_obproxy_log.md)
- [gather awr](./docs/gather_awr.md)
- [gather all](./docs/gather_all.md)

## obdiag analyze Fuctions
- [analyze log](./docs/analyze_ob_log.md)
- [analyze flt trace log](./docs/analyze_flt_trace.md)

## obdiag rca Fuctions
- [rca](./docs/rca.md)

# Licencing
OceanBase Database is under MulanPubL - 2.0 license. You can freely copy and use the source code. When you modify or
distribute the source code, please obey the MulanPubL - 2.0 license.


# Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

- Raise us an [issue](https://github.com/oceanbase/oceanbase-diagnostic-tool/issues).
- Submit Pull Requests.

# Support

In case you have any problems when using obdiag, welcome reach out for help:

- [GitHub Issue](https://github.com/oceanbase/oceanbase-diagnostic-tool/issues)
- [Official Website](https://www.oceanbase.com/docs/obdiag-cn)

