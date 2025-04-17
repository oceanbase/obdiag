#!/bin/bash
set -e
function check_error_log {
  echo "Executing command: $1"
  output=$($1)
  echo "$output"
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    exit 1
  fi
}
# cp ~/.obdiag/config.yml ./config.yml for "obdiag {command} -c ./config.yml"
cp ~/.obdiag/config.yml ./config.yml
echo "=================obdiag check================="
check_error_log  "obdiag check"
echo "=================obdiag check list================="
check_error_log  "obdiag check list"
echo "=================obdiag check run================="
check_error_log  "obdiag check run"
echo "=================obdiag check run --store_dir ./check================="
check_error_log  "obdiag check run --store_dir ./check"
echo "=================obdiag check run --report_type yaml================="
check_error_log  "obdiag check run --report_type yaml"
echo "=================obdiag check run -c ./config.yml================="
check_error_log  "obdiag check run -c ./config.yml"
echo "=================obdiag check run --cases=ad================="
check_error_log  "obdiag check run --cases=ad"
echo "=================obdiag check run --cases=column_storage_poc================="
check_error_log  "obdiag check run --cases=column_storage_poc"
echo "=================obdiag check run --cases=build_before================="
check_error_log  "obdiag check run --cases=build_before"
#echo "=================obdiag check run --cases=sysbench_run================="
#check_error_log  "obdiag check run --cases=sysbench_run"
#echo "=================obdiag check run --cases=sysbench_free================="
#check_error_log  "obdiag check run --cases=sysbench_free"
#echo "=================obdiag check run --obproxy_cases=proxy================="
#check_error_log  "obdiag check run --obproxy_cases=proxy"
