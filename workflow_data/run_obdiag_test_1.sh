#!/bin/bash
# error code save file
touch error_code.txt
function check_error_log {
#  echo "Executing command: $1 --inner_config=\"obdiag.basic.telemetry=False\""
  output=$($1 --inner_config="obdiag.basic.telemetry=False")
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    echo "$output"
    echo "1"  > error_code.txt
  fi
}
date "+%Y-%m-%d %H:%M:%S"
# cp ~/.obdiag/config.yml ./config.yml for "obdiag {command} -c ./config.yml"
cp ~/.obdiag/config.yml ./config.yml
ls /root/ob/run/
ls /root/ob/log/
check_error_log  "obdiag gather log" &
check_error_log  "obdiag gather log --since 1d" &
check_error_log  "obdiag gather log --scope all" &
check_error_log  "obdiag gather log --grep observer" &
check_error_log  "obdiag gather log --store_dir ./zkc" &
check_error_log  "obdiag gather scene" &
check_error_log  "obdiag gather scene list" &
check_error_log  "obdiag gather scene run --scene=other.application_error" &
#echo "=================obdiag gather scene run --scene=obproxy.restart================="
#check_error_log  "obdiag gather scene run --scene=obproxy.restart" &
check_error_log  "obdiag gather scene run --scene=observer.clog_disk_full -v" &
check_error_log  "obdiag gather scene run --scene=observer.cluster_down" &
check_error_log  "obdiag gather scene run --scene=observer.compaction" &
check_error_log  "obdiag gather scene run --scene=observer.delay_of_primary_and_backup" &
check_error_log  "obdiag gather scene run --scene=observer.io" &
check_error_log  "obdiag gather scene run --scene=observer.log_archive" &
check_error_log  "obdiag gather scene run --scene=observer.long_transaction" &
check_error_log  "obdiag gather scene run --scene=observer.recovery" &
check_error_log  "obdiag gather scene run --scene=observer.restart" &
check_error_log  "obdiag gather scene run --scene=observer.rootservice_switch" &
check_error_log  "obdiag gather scene run --scene=observer.unknown" &
check_error_log  "obdiag gather scene run --scene=observer.base" &
check_error_log  "obdiag gather ash" &
check_error_log  "obdiag gather ash --report_type TEXT" &
check_error_log  "obdiag gather ash --store_dir ./ash" &
check_error_log  "obdiag rca list"
#echo "=================obdiag rca run================="
#check_error_log  "obdiag rca run"
#echo "=================obdiag rca run --scene=major_hold================="
#check_error_log  "obdiag rca run --scene=major_hold"
#echo "=================obdiag rca run --scene=disconnection --env since=1d================="
#check_error_log  "obdiag rca run --scene=disconnection --env since=1d"
#echo "=================obdiag rca run --scene=lock_conflict================="
#check_error_log  "obdiag rca run --scene=lock_conflict"
#echo "=================obdiag rca run --scene=log_error================="
#check_error_log  "obdiag rca run --scene=log_error"
#echo "=================obdiag rca run --scene=transaction_rollback================="
#check_error_log  "obdiag rca run --scene=transaction_rollback"
#echo "=================obdiag rca run --scene=transaction_disconnection================="
#check_error_log  "obdiag rca run --scene=transaction_disconnection"
#echo "=================obdiag rca run --scene=clog_disk_full================="
#check_error_log  "obdiag rca run --scene=clog_disk_full"
#echo "=================obdiag update================="
#check_error_log  "obdiag update"
wait
date "+%Y-%m-%d %H:%M:%S"

# Check if error_code.txt contains any data
if [[ -s error_code.txt ]]; then
  echo "Errors detected. Exiting with status 1."
  exit 1
else
  echo "No errors detected. Exiting with status 0."
  exit 0
fi