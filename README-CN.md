<p align="center">
    <a href="https://github.com/oceanbase/obdiag/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
    <a href="https://github.com/oceanbase/obdiag/releases/latest">
        <img alt="license" src="https://img.shields.io/badge/dynamic/json?color=blue&label=release&query=tag_name&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Fobdiag%2Freleases%2Flatest" />
    </a>
    <a href="https://img.shields.io/badge/python%20-3.8.0%2B-blue.svg">
        <img alt="pyversions" src="https://img.shields.io/badge/python%20-3.8.0%2B-blue.svg" />
    </a>
    <a href="https://github.com/oceanbase/obdiag">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Fobdiag" />
    </a>
    <a href="https://github.com/oceanbase/obdiag">
        <img alt="forks" src="https://img.shields.io/badge/dynamic/json?color=blue&label=forks&query=forks&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Fobdiag" />
    </a>
    <a href="https://www.oceanbase.com/docs/obdiag-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
</p>

# OceanBase Diagnostic Tool (obdiag)

## 简介
OceanBase Diagnostic Tool (obdiag) 是一款专门为OceanBase打造的敏捷诊断工具，功能包括诊断信息收集、分析、巡检，可以在OceanBase集群不同的部署模式下（OCP，OBD或用户根据文档手工部署）实现一键执行。

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
源码安装需要在python >= 3.8的环境下进行

```shell
pip3 install -r requirements3.txt
./dev_init.sh
source ~/.bashrc
```

# obdiag config
配置文件的默认路径是`~/.obdiag/config.yml` . 可直接编辑配置文件或者通过`obdiag config <option>` 命令快速生成配置
```shell script
obdiag config -h <db_host> -u <sys_user> [-p password] [-P port]
```

# obdiag 功能
- 一键集群巡检
- 一键诊断分析
- 一键根因分析
- 一键信息采集
- 一键集群洞察


# 参与贡献

obdiag 期望构建一个开放的社区，我们欢迎任何形式的贡献，您可以：
- 通过 [Issues](https://github.com/oceanbase/obdiag/issues) 提交 bug。
- 通过 [Discussion](https://github.com/oceanbase/obdiag/discussions) 参与或发起讨论。
- 通过 [Pull requests](https://github.com/oceanbase/obdiag/pulls) 提交问题修复或者功能特性。

# Roadmap Ahead

| 版本 | 迭代周期 |发布日期| 功能点 | 
|---------|--------|-------|---------|
|1.6.0| 2024.01|2024.01.31| <ul><li> 场景化信息采集 </li><li> 场景化根因分析 </li></ul> |
|2.0.0|2024.03| 2024.04.11|<ul><li> context改造，场景化扩展能力增强 </li><li> 支持在线更新巡检、采集的task </li><li> 根因分析二期 </li></ul>|
|2.1.0|2024.04| 2024.05.13|<ul><li> 根因分析场景扩展 </li><li> 新增 ash 报告 采集 </li></ul>|
|2.2.0|2024.05| 2024.06.14 |<ul><li> 根因分析场景扩展 </li><li> 巡检场景扩展 </li></ul>|
|2.3.0|2024.06| 2024.07.24 |<ul><li> 根因分析场景扩展 </li><li> 新增基础采集功能: tabledump </li><li> 新增参数/变量比对分析功能 </li><li> 执行底座改造，支持 k8s 部署的 OceanBase 集群诊断 </li></ul>|
|2.4.0|2024.07| 2024.09.03 |<ul><li> 易用性改造 </li><li> 索引空间分析 </li></ul>|
|2.5.0|2024.09| 2024.10.18 |<ul><li> 场景化一键展示集群信息功能 </li><li> 队列积压分析 </li><li> 支持对收集的observer日志按指定规则进行敏感内容脱敏 </li></ul>|
|2.6.0|2024.10| - |<ul><li> SQL 诊断 </li><li> 根因分析场景扩展 </li></ul>|
|2.7.0|2024.11| - |<ul><li> 根因分析场景扩展 </li></ul>|
|3.0.0|2024.12| - |<ul><li> 插件化改造 </li><li> 支持 OMS 诊断 </li></ul>|
|4.0.0|2025.01| - |<ul><li> AI 化探索 </li></ul>|

# 支持

如果您在使用 OceanBase LogProxy 时遇到任何问题，欢迎联系我们寻求帮助：

- [GitHub Issue](https://github.com/oceanbase/obdiag/issues)
- [官方网站](https://www.oceanbase.com/docs/obdiag-cn)

# 开发者专区

## 加入我们
请添加 OB社区小助手（微信号：obce666）并备注“obdiag SIG”，工作人员会联系并指引您加入 SIG 的相关事宜。期待您的积极参与与宝贵贡献！

## 开发者行动指南

1. Fork并克隆你的专属副本：在我们的GitHub仓库页面右上角点击“Fork”按钮，这样就在你的账号下创建了一份仓库副本。接下来，使用Git命令将这份副本克隆到你的本地计算机上：

```bash
git clone https://github.com/your_username/your_repo_here.git
```

2. 修改代码：无论是修正漏洞还是增添新功能，你的每一份贡献都至关重要。

3. 通过 black 工具统一格式化你的代码 🎨

```bash
black -S -l 256 {source_file_or_directory}
```

4. 提交你的改动：完成优化后，利用Git提交你的修改。

5. 推送到你的Fork仓库：将你的改动推送到GitHub上的Fork仓库中。

6. 发起合并请求（PR）：回到你的GitHub分叉页面，点击“Open pull request”按钮。对比你的分支与原始仓库的主分支，描述你的改动内容，然后提交PR！

7. 🚀 自动化流程接棒此处 🤖

接下来，就交给我们的GitHub Actions流水线：
- 代码检查：PR一旦触发，流水线会自动代码格式化检查以及安全检查。
- 自动构建：PR一旦触发，就会自动将你的改动编译成RPM和DEB两种软件包。


# 许可证

OceanBase Diagnostic Tool 使用 [MulanPSL - 2.0](http://license.coscl.org.cn/MulanPSL2) 许可证。
您可以免费复制及使用源代码。当您修改或分发源代码时，请遵守木兰协议。
