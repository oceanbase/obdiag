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
#cp ~/.obdiag/config.yml ./config.yml

#echo "=================obdiag update================="
#check_error_log  "obdiag update"

