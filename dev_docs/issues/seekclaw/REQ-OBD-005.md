# REQ-OBD-005 - gather 输出 manifest

> **类型**: Enhancement | **优先级**: 中 | **来源**: [seekclaw.md](../../seekclaw.md) 第十七章 | **预估**: 0.5 天

---

## 需求描述

gather 完成后在 pack 根目录生成 `manifest.json`，包含 obdiag_version、scene、ob_version、nodes、components、gather_time，便于 Agent 识别 pack 类型。

## 背景

- SeekClaw 接收上传的 pack，需快速识别其内容（如 observer 日志、plan-monitor、cluster_down 等）
- 当前 pack 无自描述文件，需遍历目录结构推断
- manifest 可显著降低 Agent 解析成本

## Acceptance Criteria

1. **生成时机**：gather 任一 scene 完成后，在 pack 根目录（如 `obdiag_gather_pack_<timestamp>/`）生成 `manifest.json`
2. **字段**：至少包含 `obdiag_version`、`scene`、`ob_version`、`nodes`、`components`、`gather_time`
3. **多 scene**：若一次 gather 包含多个 scene，manifest 可扩展为数组或合并描述

## 实现要点

1. **Gather 收尾逻辑**：在 `GatherLogHandler` 或各 gather scene 完成后的统一收尾处，写入 manifest.json
2. **Schema 示例**：
   ```json
   {
     "obdiag_version": "2.x.x",
     "scene": "observer.cluster_down",
     "ob_version": "4.2.0",
     "nodes": ["192.168.1.1", "192.168.1.2"],
     "components": ["observer", "rootservice"],
     "gather_time": "2025-03-12T10:00:00Z"
   }
   ```
3. **兼容**：不影响现有 gather 流程，仅新增文件

## 涉及文件

| 文件 | 修改内容 |
|------|----------|
| `src/handler/gather/gather_log/base.py` 或 gather 主流程 | 收尾时写入 manifest.json |
| `plugins/gather/` 各 scene | 提供 scene、components 等元数据 |

## 预估工时

0.5 天
