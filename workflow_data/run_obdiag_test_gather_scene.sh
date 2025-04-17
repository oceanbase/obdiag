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

echo "=================obdiag gather scene================="
check_error_log  "obdiag gather scene"
echo "=================obdiag gather scene list================="
check_error_log  "obdiag gather scene list"
echo "=================obdiag gather scene run --scene=other.application_error================="
check_error_log  "obdiag gather scene run --scene=other.application_error"
#echo "=================obdiag gather scene run --scene=obproxy.restart================="
#check_error_log  "obdiag gather scene run --scene=obproxy.restart"
echo "=================obdiag gather scene run --scene=observer.clog_disk_full================="
check_error_log  "obdiag gather scene run --scene=observer.clog_disk_full"
echo "=================obdiag gather scene run --scene=observer.cluster_down================="
check_error_log  "obdiag gather scene run --scene=observer.cluster_down"
echo "=================obdiag gather scene run --scene=observer.compaction================="
check_error_log  "obdiag gather scene run --scene=observer.compaction"
echo "=================obdiag gather scene run --scene=observer.delay_of_primary_and_backup================="
check_error_log  "obdiag gather scene run --scene=observer.delay_of_primary_and_backup"
echo "=================obdiag gather scene run --scene=observer.io================="
check_error_log  "obdiag gather scene run --scene=observer.io"
echo "=================obdiag gather scene run --scene=observer.log_archive================="
check_error_log  "obdiag gather scene run --scene=observer.log_archive"
echo "=================obdiag gather scene run --scene=observer.long_transaction================="
check_error_log  "obdiag gather scene run --scene=observer.long_transaction"
echo "=================obdiag gather scene run --scene=observer.recovery================="
check_error_log  "obdiag gather scene run --scene=observer.recovery"
echo "=================obdiag gather scene run --scene=observer.restart================="
check_error_log  "obdiag gather scene run --scene=observer.restart"
echo "=================obdiag gather scene run --scene=observer.rootservice_switch================="
check_error_log  "obdiag gather scene run --scene=observer.rootservice_switch"
echo "=================obdiag gather scene run --scene=observer.unknown================="
check_error_log  "obdiag gather scene run --scene=observer.unknown"
echo "=================obdiag gather scene run --scene=observer.base================="
check_error_log  "obdiag gather scene run --scene=observer.base"


