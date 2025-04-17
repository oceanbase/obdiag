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
echo "=================obdiag display list================="
check_error_log  "obdiag display list"
echo "=================obdiag display scene list================="
check_error_log  "obdiag display scene list"
echo "=================obdiag display run================="
check_error_log  "obdiag display run"
echo "=================obdiag display scene run --scene=observer.cluster_info================="
check_error_log  "obdiag display scene run --scene=observer.cluster_info"
echo "=================obdiag display scene run --scene=observer.rs================="
check_error_log  "obdiag display scene run --scene=observer.rs"
echo "=================obdiag display scene run --scene=observer.server_info================="
check_error_log  "obdiag display scene run --scene=observer.server_info"
echo "=================obdiag display scene run --scene=observer.tenant_info --env tenant_name=sys================="
check_error_log  "obdiag display scene run --scene=observer.tenant_info --env tenant_name=sys"
echo "=================obdiag display scene run --scene=observer.unit_info================="
check_error_log  "obdiag display scene run --scene=observer.unit_info"
echo "=================obdiag display scene run --scene=observer.zone_info================="
check_error_log  "obdiag display scene run --scene=observer.zone_info"
echo "=================obdiag display scene run --scene=observer.all_tenant================="
check_error_log  "obdiag display scene run --scene=observer.all_tenant"
echo "=================obdiag display scene run --scene=observer.cpu================="
check_error_log  "obdiag display scene run --scene=observer.cpu"
echo "=================obdiag display scene run --scene=observer.leader --env level=all================="
check_error_log  "obdiag display scene run --scene=observer.leader --env level=all"
echo "=================obdiag display scene run --scene=observer.processlist_stat================="
check_error_log  "obdiag display scene run --scene=observer.processlist_stat"
echo "=================obdiag display scene run --scene=observer.memory================="
check_error_log  "obdiag display scene run --scene=observer.memory"
echo "=================obdiag display scene run --scene=observer.processlist --env tenant_name=sys================="
check_error_log  "obdiag display scene run --scene=observer.processlist --env tenant_name=sys"

