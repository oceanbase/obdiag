#!/bin/bash

# ============================================
# 快速测试脚本：Collation 检查功能
# 简化版本，用于快速验证功能
# ============================================

# 配置（请根据实际情况修改）
HOST="${1:-127.0.0.1}"
PORT="${2:-2881}"
USER="${3:-test@test}"
PASS="${4:-test}"
DB="${5:-test_collation_db}"

echo "========================================="
echo "快速测试：Collation 一致性检查"
echo "========================================="
echo "连接信息: ${USER}@${HOST}:${PORT}/${DB}"
echo ""

# 1. 创建测试表（简化版）
echo "[1/4] 创建测试表..."
obclient -h${HOST} -P${PORT} -u${USER} -p${PASS} <<EOF
CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS t1_bin, t2_general;
CREATE TABLE t1_bin (id INT, name VARCHAR(50) COLLATE utf8mb4_bin);
CREATE TABLE t2_general (id INT, name VARCHAR(50) COLLATE utf8mb4_general_ci);
INSERT INTO t1_bin VALUES (1, 'test');
INSERT INTO t2_general VALUES (1, 'test');
EOF

# 2. 执行测试 SQL
echo "[2/4] 执行测试 SQL（需要添加 /*+ monitor */ hint）..."
echo "请在 obclient 中手动执行以下 SQL："
echo ""
echo "SELECT /*+ monitor */ t1.id, t1.name, t2.name as name2"
echo "FROM ${DB}.t1_bin t1"
echo "INNER JOIN ${DB}.t2_general t2 ON t1.name = t2.name"
echo "WHERE t1.id = 1;"
echo ""
read -p "按 Enter 继续，或输入 trace_id: " TRACE_ID

# 3. 如果没有提供 trace_id，尝试查询
if [ -z "$TRACE_ID" ]; then
    echo "[3/4] 查询 trace_id..."
    sleep 2
    TRACE_ID=$(obclient -h${HOST} -P${PORT} -u${USER} -p${PASS} -D${DB} -N -e "
        SELECT trace_id 
        FROM oceanbase.gv\$ob_sql_audit 
        WHERE database_name='${DB}' 
          AND query_sql LIKE '%monitor%'
        ORDER BY request_time DESC LIMIT 1;
    " 2>/dev/null | head -1)
    
    if [ -z "$TRACE_ID" ]; then
        echo "无法自动获取 trace_id，请手动查询："
        echo "SELECT trace_id FROM oceanbase.gv\$ob_sql_audit WHERE database_name='${DB}' ORDER BY request_time DESC LIMIT 1;"
        read -p "请输入 trace_id: " TRACE_ID
    fi
fi

if [ -z "$TRACE_ID" ]; then
    echo "错误: 未提供 trace_id"
    exit 1
fi

echo "使用 trace_id: $TRACE_ID"

# 4. 运行 gather plan_monitor
echo "[4/4] 运行 gather plan_monitor..."
obdiag gather plan_monitor \
    --trace_id "$TRACE_ID" \
    --env "db_connect='-h${HOST} -P${PORT} -u${USER} -p${PASS} -D${DB}'" \
    --store_dir ./test_collation_quick_result

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✓ 测试完成！"
    echo "========================================="
    echo "报告路径: ./test_collation_quick_result/sql_plan_monitor_report.html"
    echo ""
    echo "请打开报告查看 Collation 检查结果："
    echo "  - 应该显示 Collation 不一致警告"
    echo "  - 列出不同的 collation: utf8mb4_bin, utf8mb4_general_ci"
    echo "  - 显示详细的表格信息"
else
    echo "错误: gather plan_monitor 执行失败"
    exit 1
fi
