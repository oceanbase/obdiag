#!/bin/bash
EXIT_CODE=0
function check_error_log {
#  echo "Executing command: $1 --inner_config=\"obdiag.basic.telemetry=False\""
  output=$($1 --inner_config="obdiag.basic.telemetry=False")
#  echo "$output"
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    EXIT_CODE=1
  fi
}
date "+%Y-%m-%d %H:%M:%S"
# cp ~/.obdiag/config.yml ./config.yml for "obdiag {command} -c ./config.yml"
cp ~/.obdiag/config.yml ./config.yml &
check_error_log  "obdiag check" &
check_error_log  "obdiag check list" &
check_error_log  "obdiag check run" &
check_error_log  "obdiag check run --store_dir ./check" &
check_error_log  "obdiag check run --report_type yaml" &
check_error_log  "obdiag check run -c ./config.yml" &
check_error_log  "obdiag check run --cases=a" &
check_error_log  "obdiag check run --cases=column_storage_poc" &
check_error_log  "obdiag check run --cases=build_before" &
#echo "=================obdiag check run --cases=sysbench_run================="
#check_error_log  "obdiag check run --cases=sysbench_run"
#echo "=================obdiag check run --cases=sysbench_free================="
#check_error_log  "obdiag check run --cases=sysbench_free"
#echo "=================obdiag check run --obproxy_cases=proxy================="
#check_error_log  "obdiag check run --obproxy_cases=proxy" &
check_error_log  "obdiag analyze log" &
check_error_log  "obdiag analyze log --since 1d" &
check_error_log  "obdiag analyze log --scope rootservice" &
check_error_log  "obdiag analyze log --grep observer" &
check_error_log  "obdiag analyze log --store_dir ./log" &
check_error_log  "obdiag analyze log --log_level INFO" &
check_error_log  "obdiag analyze log --temp_dir ./log" &
#echo "=================obdiag analyze flt_trace================="
#check_error_log  "obdiag analyze flt_trace"
#echo "=================obdiag analyze variable================="
#check_error_log  "obdiag analyze variable" &
check_error_log  "obdiag analyze parameter" &
# version>4.2.2
#echo "=================obdiag analyze parameter default --store_dir ./parameter================="
#check_error_log  "obdiag analyze parameter default --store_dir ./parameter"
#echo "=================obdiag analyze parameter diff --store_dir ./parameter================="
#check_error_log  "obdiag analyze parameter diff --store_dir ./parameter" &
check_error_log  "obdiag analyze memory" &
check_error_log  "obdiag analyze memory --store_dir ./memory" &
check_error_log  "obdiag analyze memory --since 1d" &
#echo "=================obdiag analyze index_space================="
#check_error_log  "obdiag analyze index_space"
#echo "=================obdiag analyze queue --tenant sys================="
#check_error_log  "obdiag analyze queue --tenant sys" &
check_error_log  "obdiag display list" &
check_error_log  "obdiag display scene list" &
check_error_log  "obdiag display run" &
check_error_log  "obdiag display scene run --scene=observer.cluster_info" &
check_error_log  "obdiag display scene run --scene=observer.rs" &
check_error_log  "obdiag display scene run --scene=observer.server_info" &
check_error_log  "obdiag display scene run --scene=observer.tenant_info --env tenant_name=sys" &
check_error_log  "obdiag display scene run --scene=observer.unit_info" &
check_error_log  "obdiag display scene run --scene=observer.zone_info" &
check_error_log  "obdiag display scene run --scene=observer.all_tenant" &
check_error_log  "obdiag display scene run --scene=observer.cpu" &
check_error_log  "obdiag display scene run --scene=observer.leader --env level=all" &
check_error_log  "obdiag display scene run --scene=observer.processlist_stat" &
check_error_log  "obdiag display scene run --scene=observer.memory" &
check_error_log  "obdiag display scene run --scene=observer.processlist --env tenant_name=sys" &
check_error_log  "obdiag gather log" &
check_error_log  "obdiag gather log --since 1d" &
check_error_log  "obdiag gather log --scope all" &
check_error_log  "obdiag gather log --grep observer" &
check_error_log  "obdiag gather log --store_dir ./zkc" &
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
#check_error_log  "obdiag gather all --grep rootservice" &
check_error_log  "obdiag gather scene" &
check_error_log  "obdiag gather scene list" &
check_error_log  "obdiag gather scene run --scene=other.application_error" &
#echo "=================obdiag gather scene run --scene=obproxy.restart================="
#check_error_log  "obdiag gather scene run --scene=obproxy.restart" &
check_error_log  "obdiag gather scene run --scene=observer.clog_disk_full" &
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
echo $EXIT_CODE
exit $EXIT_CODE