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
# cp ~/.obdiag/config.yml ./config.yml for "obdiag {command} -c ./config.yml"
cp ~/.obdiag/config.yml ./config.yml
echo "=================obdiag analyze log================="
check_error_log  "obdiag analyze log"
echo "=================obdiag analyze log --since 1d================="
check_error_log  "obdiag analyze log --since 1d"
echo "=================obdiag analyze log --scope rootservice================="
check_error_log  "obdiag analyze log --scope rootservice"
echo "=================obdiag analyze log --grep observer================="
check_error_log  "obdiag analyze log --grep observer"
echo "=================obdiag analyze log --store_dir ./log================="
check_error_log  "obdiag analyze log --store_dir ./log"
echo "=================obdiag analyze log --log_level INFO================="
check_error_log  "obdiag analyze log --log_level INFO"
echo "=================obdiag analyze log --temp_dir ./log================="
check_error_log  "obdiag analyze log --temp_dir ./log"
#echo "=================obdiag analyze flt_trace================="
#check_error_log  "obdiag analyze flt_trace"
#echo "=================obdiag analyze variable================="
#check_error_log  "obdiag analyze variable"
echo "=================obdiag analyze parameter================="
check_error_log  "obdiag analyze parameter"
# version>4.2.2
#echo "=================obdiag analyze parameter default --store_dir ./parameter================="
#check_error_log  "obdiag analyze parameter default --store_dir ./parameter"
#echo "=================obdiag analyze parameter diff --store_dir ./parameter================="
#check_error_log  "obdiag analyze parameter diff --store_dir ./parameter"
echo "=================obdiag analyze memory================="
check_error_log  "obdiag analyze memory"
echo "=================obdiag analyze memory --store_dir ./memory================="
check_error_log  "obdiag analyze memory --store_dir ./memory"
echo "=================obdiag analyze memory --since 1d================="
check_error_log  "obdiag analyze memory --since 1d"
#echo "=================obdiag analyze index_space================="
#check_error_log  "obdiag analyze index_space"
#echo "=================obdiag analyze queue --tenant sys================="
#check_error_log  "obdiag analyze queue --tenant sys"

