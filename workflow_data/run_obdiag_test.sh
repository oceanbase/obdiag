#!/bin/bash
set -e
function check_error_log {
  output=$($1)
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    exit 1
  fi
}
# analyer
# obdiag analyze log
echo "=================obdiag analyze log================="
check_error_log "obdiag analyze log"
echo "=================obdiag analyze log --since 1d================="
check_error_log "obdiag analyze log --since 1d"
echo "=================obdiag analyze log --scope rootservice================="
check_error_log "obdiag analyze log --scope rootservice"
echo "=================obdiag analyze log --grep observer================="
check_error_log "obdiag analyze log --grep observer"
echo "=================obdiag analyze log --store_dir ./log================="
check_error_log "obdiag analyze log --store_dir ./log"
echo "=================obdiag analyze log --log_level INFO================="
check_error_log "obdiag analyze log --log_level INFO"
echo "=================obdiag analyze log --temp_dir ./log================="
check_error_log "obdiag analyze log --temp_dir ./log"
# check
echo "=================obdiag check================="
check_error_log "obdiag check"
echo "=================obdiag check list================="
check_error_log "obdiag check list"
echo "=================obdiag check run================="
check_error_log "obdiag check run"
echo "=================obdiag check run --store_dir ./check================="
check_error_log "obdiag check run --store_dir ./check"
echo "=================obdiag check run --report_type yaml================="
check_error_log "obdiag check run --report_type yaml"
echo "=================obdiag check run --cases=ad================="
check_error_log "obdiag check run --cases=ad"
echo "=================obdiag check run --cases=column_storage_poc================="
check_error_log "obdiag check run --cases=column_storage_poc"
echo "=================obdiag check run --cases=build_before================="
check_error_log "obdiag check run --cases=build_before"
echo "=================obdiag check run --cases=sysbench_run================="
check_error_log "obdiag check run --cases=sysbench_run"
echo "=================obdiag check run --cases=sysbench_free================="
check_error_log "obdiag check run --cases=sysbench_free"
# rca
check_error_log "obdiag rca list"

