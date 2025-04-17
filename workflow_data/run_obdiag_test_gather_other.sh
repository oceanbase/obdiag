#!/bin/bash
set -e
function check_error_log {
  echo "Executing command: $1 --inner_config=\"obdiag.basic.telemetry=False\""
  output=$($1 --inner_config="obdiag.basic.telemetry=False")
  echo "$output"
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    exit 1
  fi
}

#echo "=================obdiag gather sysstat================="
#check_error_log  "obdiag gather sysstat"
#echo "=================obdiag gather sysstat --store_dir ./sysstat================="
#check_error_log  "obdiag gather sysstat --store_dir ./sysstat"
##echo "=================obdiag gather plan_monitor================="
##check_error_log  "obdiag gather plan_monitor"
#echo "=================obdiag gather stack================="
#check_error_log  "obdiag gather stack"
#echo "=================obdiag gather stack --store_dir ./stack================="
#check_error_log  "obdiag gather stack --store_dir ./stack"
#echo "=================obdiag gather perf --count 1000000================="
#check_error_log  "obdiag gather perf --count 1000000"
#echo "=================obdiag gather all================="
#check_error_log  "obdiag gather all"
#echo "=================obdiag gather all --since 1d================="
#check_error_log  "obdiag gather all --since 1d"
#echo "=================obdiag gather all --scope observer================="
#check_error_log  "obdiag gather all --scope observer"
#echo "=================obdiag gather all --grep rootservice================="
#check_error_log  "obdiag gather all --grep rootservice"
echo "=================obdiag gather ash================="
check_error_log  "obdiag gather ash"
echo "=================obdiag gather ash --report_type TEXT================="
check_error_log  "obdiag gather ash --report_type TEXT"
echo "=================obdiag gather ash --store_dir ./ash================="
check_error_log  "obdiag gather ash --store_dir ./ash"



