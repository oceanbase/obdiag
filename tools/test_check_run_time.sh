#!/bin/bash
# 测试 obdiag check run 的执行耗时
#
# 用法:
#   ./tools/test_check_run_time.sh                    # 默认全量 check run
#   ./tools/test_check_run_time.sh --cases=ad        # 指定 cases 子集
#   REPEAT=3 ./tools/test_check_run_time.sh          # 重复 3 次取平均

REPEAT=${REPEAT:-1}
EXTRA_ARGS="$*"
[[ "$*" != *inner_config* ]] && EXTRA_ARGS="$EXTRA_ARGS --inner_config=obdiag.basic.telemetry=False"

echo "============================================================"
echo "obdiag check run 耗时测试"
echo "============================================================"
echo "重复次数: $REPEAT"
echo "参数: $EXTRA_ARGS"
echo "------------------------------------------------------------"

total=0
for ((i=1; i<=REPEAT; i++)); do
  printf "[%d/%d] 执行中... " "$i" "$REPEAT"
  start=$(date +%s.%N)
  obdiag check run $EXTRA_ARGS >/dev/null 2>&1
  code=$?
  end=$(date +%s.%N)
  elapsed=$(echo "$end - $start" | bc)
  total=$(echo "$total + $elapsed" | bc)
  echo "耗时: ${elapsed}s, 退出码: $code"
done

echo "------------------------------------------------------------"
avg=$(echo "scale=2; $total / $REPEAT" | bc)
echo "平均耗时: ${avg}s"
echo "============================================================"
