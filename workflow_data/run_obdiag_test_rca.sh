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
#cp ~/.obdiag/config.yml ./config.yml
echo "=================obdiag rca list================="
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

