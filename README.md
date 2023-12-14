English | [中文版](README-CN.md)

![License](https://img.shields.io/:license-MulanPSL2-blue.svg)
![pyversions](https://img.shields.io/badge/python%20-3.8.0%2B-blue.svg)
[![Github stars](https://img.shields.io/github/stars/oceanbase/oceanbase-diagnostic-tool?style=flat-square)](https://github.com/oceanbase/oceanbase-diagnostic-tool)
[![OpenIssue](https://img.shields.io/github/issues/oceanbase/oceanbase-diagnostic-tool)](https://github.com/oceanbase/oceanbase-diagnostic-tool/issues)


# Oceanbase Diagnostic Tool (obdiag)
OceanBase Diagnostic Tool (obdiag) is is a quick diagnostic tool for open-source OceanBase software. The features include gather\analyze\check OceanBase Diagnostic information. It can be executed with one click in different deployment modes of OceanBase clusters (OCP, OBD, or manually deployed by users according to documentation).

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
pip install -r requirements3.txt
cd rpm && sh build.sh build_obdiag
sh init.sh
```

# obdiag config
The path of the configuration file for the diagnosed cluster is stored in `~/.obdiag/config.yml` .You can directly edit the configuration file or generate the configuration file through the `obdiag config <option>` command
```shell script
obdiag config -h <db_host> -u <sys_user> [-p password] [-P port]
```

# obdiag Fuctions

## obdiag check Fuctions
- [check](./docs/check.md)

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

