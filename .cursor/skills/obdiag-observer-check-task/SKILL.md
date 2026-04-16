---
name: obdiag-observer-check-task
description: >-
  Implements or extends OceanBase obdiag observer-side Python check tasks under
  plugins/check/tasks/observer, syncs artifacts to ~/.obdiag for local runs, and
  validates with SQL plus obdiag check. Use when adding or changing observer
  check tasks, GV$OB_PARAMETERS inspections, TaskBase plugins, or
  observer_check_package.yaml cases.
---

# obdiag Observer 巡检任务开发

## 创建分支（开发前）

在仓库根目录执行，保证从最新 **`master`** 派生功能分支：

```bash
git fetch origin master
git checkout master
git pull origin master
git checkout -b <branch-name>    # 例：500-feature-1231、500-issue-1232
# 开发完成后首次推送：
git push -u origin <branch-name>
```

## 适用场景

- 在 **`plugins/check/tasks/observer/`** 下新增或修改 **Python 巡检任务**（继承 **`TaskBase`**）。
- 需要连真实集群验证 **`GV$OB_PARAMETERS`** / SQL 行为。
- 本地执行 **`obdiag check run`** 时依赖 **`~/.obdiag/check`** 下的任务与套餐文件。

## 实现步骤

1. **找参照**：在 `plugins/check/tasks/observer/` 同目录或 `cluster/` 下找类似任务（例如参数类：`memory_limit_percentage.py`、`memstore_limit_percentage.py`），对齐 **`execute`** / **`get_task_info`** / 异常与 **`self.report.add_*`** 风格。
2. **新建模块**：
   - 路径：`plugins/check/tasks/observer/<category>/<task_stem>.py`。
   - 文件末尾暴露与 **文件名同 stem** 的实例（如 `ob_vector_memory_limit_percentage = ObVectorMemoryLimitPercentage()`），供 `CheckHandler` 按 stem 动态加载。
3. **逻辑要点**：
   - 无库连接时用 **`add_critical`**；版本不满足时用 **`add_warning`** 并说明不支持（与现有任务一致）。
   - 版本门槛用 **`super().check_ob_version_min("x.y.z.w")`**（四段版本号）。
   - 查参数字典视图时注意 **多行**（例如按 **`TENANT_ID`** 一行）；告警文案建议带 **`SVR_IP`**，多租户时带 **`tenant_id`**。
4. **是否写入套餐（可选）**：
   - **`plugins/check/observer_check_package.yaml`** 仅在需要把任务纳入某个 **`cases`** 套餐（如 `k8s_basic`）时修改。
   - 不写入套餐时，仍可通过 **`--observer_tasks=<folder>.<stem>`** 单独执行（例如 **`cluster.ob_vector_memory_limit_percentage`**）。

## 本地运行前：同步到 ~/.obdiag

运行时默认从 **`inner_config` 的 `check.work_path`**（常为 **`~/.obdiag/check`**）加载：

- 任务：`{work_path}/tasks/observer/.../*.py`
- 套餐：`{work_path}/observer_check_package.yaml`

仓库内源文件在 **`plugins/check/`**。改完仓库后需拷到用户目录（避免交互式覆盖提示可用 **`/bin/cp -f`**）：

```bash
mkdir -p ~/.obdiag/check/tasks/observer/<category>
/bin/cp -f plugins/check/tasks/observer/<category>/<task>.py ~/.obdiag/check/tasks/observer/<category>/
# 若改了套餐 YAML：
/bin/cp -f plugins/check/observer_check_package.yaml ~/.obdiag/check/observer_check_package.yaml
```

## 环境与验证

1. **虚拟环境**：仓库根目录 **`source .venv/bin/activate`**；依赖不全时 **`pip install -e .`**。
2. **裸 SQL**（可选）：用 **`mysql -h... -P... -uroot@sys`** 等对 **`oceanbase.GV$OB_PARAMETERS`**（或任务所用视图）跑与巡检一致的 SQL，确认列名、多租户行、取值范围。
3. **obdiag**：

```bash
obdiag check run --observer_tasks=<folder>.<task_stem> -c ~/.obdiag/config.yml --store_dir ./check_report/
```

查看输出中的 observer 报告路径；全量套餐巡检时用 **`--cases=<package>`**（仅当已在 **`observer_check_package.yaml`** 注册）。

## 提交与 PR

- 改动保持 **仅涉及该需求相关文件**；合并说明里链到对应 **GitHub issue**。
- 分支命名可与工单或内部编号对齐（见上文 **创建分支**）。

## 自检清单

- [ ] 新任务模块 **stem** 与 **`observer_tasks`** 中 **`cluster.xxx`** 的 **`xxx`** 一致。
- [ ] **`get_task_info()`** 含 **`name`**、**`info`**，必要时 **`issue_link`**。
- [ ] 版本与 SQL 行为在目标集群上已验证。
