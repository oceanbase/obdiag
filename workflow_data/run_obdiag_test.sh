#!/bin/bash
set -e
function check_error_log {
  output=$($1)
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $1. Failing the job."
    exit 1
  fi
}
# check
check_error_log "obdiag check list"
check_error_log "obdiag check run --case=11"
check_error_log "obdiag rca list"
