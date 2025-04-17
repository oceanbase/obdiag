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

echo "=================obdiag gather log================="
check_error_log  "obdiag gather log"
echo "=================obdiag gather log --since 1d================="
check_error_log  "obdiag gather log --since 1d"
echo "=================obdiag gather log --scope all================="
check_error_log  "obdiag gather log --scope all"
echo "=================obdiag gather log --grep observer================="
check_error_log  "obdiag gather log --grep observer"
echo "=================obdiag gather log --store_dir ./zkc================="
check_error_log  "obdiag gather log --store_dir ./zkc"
