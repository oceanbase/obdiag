#!/bin/bash
set -uo pipefail  # Exit on undefined vars, pipe failures (but allow command failures for background jobs)

echo "Tag: ${tag:-unknown}"
# Initialize result files
touch error_code.txt
touch pass_case.txt

function check_error_log {
  local cmd="$1"
  echo "Executing command: $cmd --inner_config=\"obdiag.basic.telemetry=False\""
  
  # Capture both stdout and stderr, and exit code
  local output
  local exit_code=0
  output=$($cmd --inner_config="obdiag.basic.telemetry=False" 2>&1) || exit_code=$?
  
  # Check for ERROR in output
  if echo "$output" | grep -q "\[ERROR\]"; then
    echo "Error detected in obdiag output for command: $cmd. Failing the job."
    local command_to_run
    command_to_run=$(echo "$output" | grep "please run:" | sed -n 's/.*please run: //p' || true)
    if [ -n "$command_to_run" ]; then
      echo "Executing extracted command: $command_to_run"
      eval "$command_to_run" || true
    fi
    echo "1" >> error_code.txt
    return 1
  elif [ $exit_code -ne 0 ]; then
    echo "Command failed with exit code $exit_code: $cmd"
    echo "1" >> error_code.txt
    return 1
  else
    echo "1" >> pass_case.txt
    return 0
  fi
}
# Version comparison functions
compare_versions_greater() {
    local v1=$1
    local v2=$2

    # Handle "latest" as highest version
    if [ "$v1" == "latest" ]; then
        return 0  # latest > any version
    fi
    if [ "$v2" == "latest" ]; then
        return 1  # any version < latest
    fi

    # 将版本号分割成数组
    IFS='.' read -r -a v1_parts <<< "$v1"
    IFS='.' read -r -a v2_parts <<< "$v2"

    # 获取较长的版本号长度
    local max_length=${#v1_parts[@]}
    if [ ${#v2_parts[@]} -gt $max_length ]; then
        max_length=${#v2_parts[@]}
    fi

    # 逐一比较版本号的每一部分
    for ((i=0; i<max_length; i++)); do
        local v1_part=${v1_parts[i]:-0} # 如果某部分不存在，默认为0
        local v2_part=${v2_parts[i]:-0}

        if ((v1_part > v2_part)); then
            return 0 # v1 > v2
        elif ((v1_part < v2_part)); then
            return 1 # v1 < v2
        fi
    done

    # 如果所有部分都相等
    return 0
}

# Check if version is greater than or equal to min_version
version_ge() {
    local version=$1
    local min_version=$2
    
    if [ "$version" == "latest" ]; then
        return 0
    fi
    
    # Exact match
    if [ "$version" == "$min_version" ]; then
        return 0
    fi
    
    # Check if version > min_version
    if compare_versions_greater "$version" "$min_version"; then
        return 0
    else
        return 1
    fi
}

# Check if version is less than max_version
version_lt() {
    local version=$1
    local max_version=$2
    
    if [ "$version" == "latest" ]; then
        return 1  # latest is never < any version
    fi
    
    if compare_versions_greater "$max_version" "$version"; then
        return 0
    else
        return 1
    fi
}

# Check if version is in range [min_version, max_version)
version_in_range() {
    local version=$1
    local min_version=$2
    local max_version=$3
    
    version_ge "$version" "$min_version" && version_lt "$version" "$max_version"
}

# Check if version matches specific version or is in range
version_matches() {
    local version=$1
    shift
    local versions=("$@")
    
    for v in "${versions[@]}"; do
        if [ "$version" == "$v" ]; then
            return 0
        fi
    done
    return 1
}

# Conditional command execution based on version
run_if_version() {
    local condition=$1
    shift
    local cmd="$*"
    
    if eval "$condition"; then
        check_error_log "$cmd"
    else
        echo "Skipping command (version condition not met): $cmd"
    fi
}

# Record start time
START_TIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "Test started at: $START_TIME"
df -h

# Copy config file if it exists
if [ -f ~/.obdiag/config.yml ]; then
  cp ~/.obdiag/config.yml ./config.yml
else
  echo "Warning: ~/.obdiag/config.yml not found, using default config"
fi


check_error_log  "obdiag check" &
check_error_log  "obdiag check list" &
check_error_log  "obdiag check run" &
check_error_log  "obdiag check run --store_dir ./check" &
check_error_log  "obdiag check run --report_type yaml" &
check_error_log  "obdiag check run -c ./config.yml" &
check_error_log  "obdiag check run --cases=ad" &
check_error_log  "obdiag check run --cases=column_storage_poc" &
check_error_log  "obdiag check run --cases=build_before"
# TODO: Need actual task names
#check_error_log  "obdiag check run --observer_tasks <task_name>" &
#check_error_log  "obdiag check run --obproxy_tasks <task_name>" &
check_error_log  "obdiag check run --report_type json" &
check_error_log  "obdiag check run --report_type xml" &
check_error_log  "obdiag check run --report_type html" &
check_error_log  "obdiag check list --all" &
wait
# Clean up generated files
rm -rf check_report obdiag_* 2>/dev/null || true
#echo "=================obdiag check run --cases=sysbench_run================="
#check_error_log  "obdiag check run --cases=sysbench_run"
#echo "=================obdiag check run --cases=sysbench_free================="
#check_error_log  "obdiag check run --cases=sysbench_free"
#echo "=================obdiag check run --obproxy_cases=proxy================="
#check_error_log  "obdiag check run --obproxy_cases=proxy" &
check_error_log  "obdiag analyze log"
check_error_log  "obdiag analyze log --since 1d"
check_error_log  "obdiag analyze log --scope rootservice"
check_error_log  "obdiag analyze log --grep observer"
check_error_log  "obdiag analyze log --store_dir ./log"
check_error_log  "obdiag analyze log --log_level INFO"
check_error_log  "obdiag analyze log --temp_dir ./log" &
check_error_log  "obdiag analyze log --tenant_id sys" &
check_error_log  "obdiag analyze log --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# TODO: Need log files for offline analysis
#check_error_log  "obdiag analyze log --files <log_file1> --files <log_file2>"
# TODO: Need flt_trace_id parameter
#check_error_log  "obdiag analyze flt_trace --flt_trace_id <trace_id>"
# TODO: Need trace log files for offline analysis
#check_error_log  "obdiag analyze flt_trace --files <trace_file1> --files <trace_file2>"
#check_error_log  "obdiag analyze flt_trace --flt_trace_id <trace_id> --top 10 --recursion 5 --output 100"
# TODO: Need variable file parameter
#check_error_log  "obdiag analyze variable diff --file <variable_file>"
check_error_log  "obdiag analyze parameter" &
# analyze parameter default requires: 4.2.2.0 ≤ version < 4.3.0.0 or version ≥ 4.3.1.0
if version_in_range "$tag" "4.2.2.0" "4.3.0.0" || version_ge "$tag" "4.3.1.0"; then
  check_error_log  "obdiag analyze parameter default --store_dir ./parameter"
  # TODO: Need parameter file
  #check_error_log  "obdiag analyze parameter default --file <parameter_file>"
else
  echo "Skipping obdiag analyze parameter default (requires 4.2.2.0 ≤ version < 4.3.0.0 or version ≥ 4.3.1.0)"
fi
check_error_log  "obdiag analyze parameter diff --store_dir ./parameter" &
# TODO: Need parameter file
#check_error_log  "obdiag analyze parameter diff --file <parameter_file>"
check_error_log  "obdiag analyze memory"
check_error_log  "obdiag analyze memory --store_dir ./memory"
check_error_log  "obdiag analyze memory --since 1d"
check_error_log  "obdiag analyze memory --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# TODO: Need log files for offline analysis
#check_error_log  "obdiag analyze memory --files <log_file1> --files <log_file2>"
#check_error_log  "obdiag analyze memory --files <log_file1> --version <ob_version>"
# TODO: Need actual database/table/index parameters
#check_error_log  "obdiag analyze index_space --tenant_name sys --database test --table_name test_table --index_name test_index"
#check_error_log  "obdiag analyze index_space --tenant_name sys --database test --table_name test_table --column_names c1,c2,c3"
check_error_log  "obdiag analyze queue --tenant sys" &
check_error_log  "obdiag analyze queue --tenant sys --queue 100" &
check_error_log  "obdiag analyze queue --tenant sys --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
wait
# Clean up generated files
rm -rf obdiag_* 2>/dev/null || true

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
check_error_log  "obdiag display scene run --scene=observer.cluster_info --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
check_error_log  "obdiag gather log" &
check_error_log  "obdiag gather log --since 1d" &
check_error_log  "obdiag gather log --scope all" &
check_error_log  "obdiag gather log --grep observer" &
check_error_log  "obdiag gather log --store_dir ./test"
check_error_log  "obdiag gather log --recent_count 5" &
check_error_log  "obdiag gather log --redact password" &
check_error_log  "obdiag gather log --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
check_error_log  "obdiag gather sysstat" &
check_error_log  "obdiag gather sysstat --store_dir ./sysstat" &
check_error_log  "obdiag gather stack" &
check_error_log  "obdiag gather stack --store_dir ./stack" &
check_error_log  "obdiag gather perf --count 1000000" &
check_error_log  "obdiag gather perf --scope sample" &
check_error_log  "obdiag gather perf --scope flame" &
check_error_log  "obdiag gather perf --store_dir ./perf" &
check_error_log  "obdiag gather all" &
check_error_log  "obdiag gather all --since 1d" &
check_error_log  "obdiag gather all --scope observer" &
check_error_log  "obdiag gather all --grep rootservice" &
check_error_log  "obdiag gather all --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
check_error_log  "obdiag gather slog" &
check_error_log  "obdiag gather slog --since 1d" &
check_error_log  "obdiag gather slog --store_dir ./slog" &
check_error_log  "obdiag gather slog --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
check_error_log  "obdiag gather clog" &
check_error_log  "obdiag gather clog --since 1d" &
check_error_log  "obdiag gather clog --store_dir ./clog" &
check_error_log  "obdiag gather clog --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# TODO: Need cluster_name or cluster_id from OCP
# check_error_log  "obdiag gather awr --cluster_name <cluster_name>"
# check_error_log  "obdiag gather awr --cluster_id <cluster_id> --since 1d"
# check_error_log  "obdiag gather awr --cluster_name <cluster_name> --store_dir ./awr"
# check_error_log  "obdiag gather awr --cluster_name <cluster_name> --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'"
# check_error_log  "obdiag gather obproxy_log" &
# check_error_log  "obdiag gather obproxy_log --since 1d" &
# check_error_log  "obdiag gather obproxy_log --scope all" &
# check_error_log  "obdiag gather obproxy_log --store_dir ./obproxy_log" &
# check_error_log  "obdiag gather obproxy_log --recent_count 5" &
# check_error_log  "obdiag gather obproxy_log --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# check_error_log  "obdiag gather oms_log" &
# check_error_log  "obdiag gather oms_log --since 1d" &
# check_error_log  "obdiag gather oms_log --scope all" &
# check_error_log  "obdiag gather oms_log --store_dir ./oms_log" &
# check_error_log  "obdiag gather oms_log --recent_count 5" &
# check_error_log  "obdiag gather oms_log --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# TODO: Need oms_component_id for CDC logs
#check_error_log  "obdiag gather oms_log --oms_component_id <component_id>"
check_error_log  "obdiag gather parameter" &
check_error_log  "obdiag gather parameter --store_dir ./parameter" &
check_error_log  "obdiag gather variable" &
check_error_log  "obdiag gather variable --store_dir ./variable" &
# check_error_log  "obdiag gather core" &
# check_error_log  "obdiag gather core --since 1d" &
# check_error_log  "obdiag gather core --store_dir ./core" &
# check_error_log  "obdiag gather core --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" &
# TODO: Need actual database/table parameters and password
#check_error_log  "obdiag gather tabledump --database test --table test_table --user root@sys --password <password>"
#check_error_log  "obdiag gather tabledump --database test --table test_table --user root@sys --password <password> --store_dir ./tabledump"
# TODO: Need trace_id parameter and database connection info
#check_error_log  "obdiag gather plan_monitor --trace_id <trace_id>"
#check_error_log  "obdiag gather plan_monitor --trace_id <trace_id> --store_dir ./plan_monitor"
#check_error_log  "obdiag gather plan_monitor --trace_id <trace_id> --env host=127.0.0.1 --env port=2881 --env user=root@sys --env password=<password>"
# TODO: Need trace_id parameter and database connection info
#check_error_log  "obdiag gather dbms_xplan --trace_id <trace_id>"
#check_error_log  "obdiag gather dbms_xplan --trace_id <trace_id> --scope all"
#check_error_log  "obdiag gather dbms_xplan --trace_id <trace_id> --env host=127.0.0.1 --env port=2881 --env user=root@sys --env password=<password>"
wait
# Clean up generated files
rm -rf obdiag_* 2>/dev/null || true

check_error_log  "obdiag gather scene" &
check_error_log  "obdiag gather scene list" &
check_error_log  "obdiag gather scene run --scene=other.application_error" &
#echo "=================obdiag gather scene run --scene=obproxy.restart================="
#check_error_log  "obdiag gather scene run --scene=obproxy.restart" &
check_error_log  "obdiag gather scene run --scene=observer.clog_disk_full" &
check_error_log  "obdiag gather scene run --scene=observer.cluster_down" &
check_error_log  "obdiag gather scene run --scene=observer.compaction" &
check_error_log  "obdiag gather scene run --scene=observer.delay_of_primary_and_backup" &
check_error_log  "obdiag gather scene run --scene=observer.io"
check_error_log  "obdiag gather scene run --scene=observer.log_archive"
check_error_log  "obdiag gather scene run --scene=observer.long_transaction" &
check_error_log  "obdiag gather scene run --scene=observer.recovery" &
check_error_log  "obdiag gather scene run --scene=observer.restart"
check_error_log  "obdiag gather scene run --scene=observer.rootservice_switch"
check_error_log  "obdiag gather scene run --scene=observer.unknown" 
check_error_log  "obdiag gather scene run --scene=observer.base"
check_error_log  "obdiag gather scene run --scene=observer.base --skip_type ssh" &
check_error_log  "obdiag gather scene run --scene=observer.base --skip_type sql" &
check_error_log  "obdiag gather scene run --scene=observer.base --redact password" &
check_error_log  "obdiag gather scene run --scene=observer.base --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'" 


# observer.perf_sql: prerequisite SQL to produce a trace_id, then gather perf_sql scene
# Connection defaults for CI: 127.0.0.1:2881, user root, empty password. Override via OB_HOST, OB_PORT, OB_USER, OB_PASSWORD.
PERF_SQL_HOST="${OB_HOST:-127.0.0.1}"
PERF_SQL_PORT="${OB_PORT:-2881}"
PERF_SQL_USER="${OB_USER:-root}"
PERF_SQL_PWD="${OB_PASSWORD:-}"
PERF_SQL_DB="${OB_DATABASE:-oceanbase}"
PERF_SQL_TRACE_ID=""
if command -v obclient >/dev/null 2>&1; then
  obclient -h"$PERF_SQL_HOST" -P"$PERF_SQL_PORT" -u"$PERF_SQL_USER" ${PERF_SQL_PWD:+-p"$PERF_SQL_PWD"} "$PERF_SQL_DB" -N -e "CREATE TABLE IF NOT EXISTS game ( round INT PRIMARY KEY, team VARCHAR(10), score INT ) PARTITION BY HASH(round) PARTITIONS 3;" 2>/dev/null || true
  obclient -h"$PERF_SQL_HOST" -P"$PERF_SQL_PORT" -u"$PERF_SQL_USER" ${PERF_SQL_PWD:+-p"$PERF_SQL_PWD"} "$PERF_SQL_DB" -N -e "INSERT IGNORE INTO game VALUES (1, 'CN', 4),(2, 'CN', 5), (3, 'JP', 3),(4, 'CN', 4),(5, 'US', 4),(6, 'JP', 4);" 2>/dev/null || true
  obclient -h"$PERF_SQL_HOST" -P"$PERF_SQL_PORT" -u"$PERF_SQL_USER" ${PERF_SQL_PWD:+-p"$PERF_SQL_PWD"} "$PERF_SQL_DB" -N -e "select /*+ parallel(3) */ team, sum(score) total from game group by team;" 2>/dev/null || true
  PERF_SQL_TRACE_ID=$(obclient -h"$PERF_SQL_HOST" -P"$PERF_SQL_PORT" -u"$PERF_SQL_USER" ${PERF_SQL_PWD:+-p"$PERF_SQL_PWD"} "$PERF_SQL_DB" -N -e "SELECT last_trace_id();" 2>/dev/null | tail -1 | tr -d '\r\n ')
fi
if [ -n "$PERF_SQL_TRACE_ID" ]; then
  check_error_log  "obdiag gather scene run --scene=observer.perf_sql --env host=$PERF_SQL_HOST --env port=$PERF_SQL_PORT --env user=$PERF_SQL_USER --env password=$PERF_SQL_PWD --env database=$PERF_SQL_DB --env trace_id=$PERF_SQL_TRACE_ID"
else
  echo "Skipping obdiag gather scene run --scene=observer.perf_sql (obclient not found or last_trace_id() empty)"
fi


# gather ash commands
# Basic ash commands (all versions)
# TODO: Need trace_id or sql_id parameter
#check_error_log  "obdiag gather ash"
#check_error_log  "obdiag gather ash --report_type TEXT"
#check_error_log  "obdiag gather ash --store_dir ./ash"
#check_error_log  "obdiag gather ash --trace_id <trace_id>"
#check_error_log  "obdiag gather ash --sql_id <sql_id>"
#check_error_log  "obdiag gather ash --wait_class <wait_class>"

# gather ash --report_type html requires version >= 4.2.4.0
if version_ge "$tag" "4.2.4.0"; then
  # TODO: Need trace_id or sql_id parameter
  #check_error_log  "obdiag gather ash --report_type html"
  #check_error_log  "obdiag gather ash --report_type html --trace_id <trace_id>"
  echo "Version >= 4.2.4.0, ash html report available (requires trace_id - TODO)"
else
  echo "Skipping obdiag gather ash --report_type html (requires version >= 4.2.4.0)"
fi

# gather ash with svr_ip/svr_port/tenant_id requires version >= 4.3.5.0
if version_ge "$tag" "4.3.5.0"; then
  # TODO: Need svr_ip, svr_port, tenant_id parameters
  #check_error_log  "obdiag gather ash --svr_ip <ip> --svr_port <port> --tenant_id <tenant_id>"
  #check_error_log  "obdiag gather ash --trace_id <trace_id> --svr_ip <ip> --svr_port <port> --tenant_id <tenant_id>"
  echo "Version >= 4.3.5.0, ash with svr_ip/svr_port/tenant_id available (requires parameters - TODO)"
else
  echo "Skipping obdiag gather ash with svr_ip/svr_port/tenant_id (requires version >= 4.3.5.0)"
fi

#check_error_log  "obdiag gather ash --trace_id <trace_id> --from '2025-01-01 00:00:00' --to '2025-01-02 00:00:00'"
wait
# Clean up generated files
rm -rf obdiag_* 2>/dev/null || true

check_error_log  "obdiag rca list"
check_error_log  "obdiag rca run --scene=replay_hold"
check_error_log  "obdiag rca run --scene=memory_full"
check_error_log  "obdiag rca run --scene=delete_server_error --env svr_ip=127.0.0.1 --env svr_port=2881"
check_error_log  "obdiag rca run --scene=major_hold"
check_error_log  "obdiag rca run --scene=disconnection --env since=1d"
check_error_log  "obdiag rca run --scene=lock_conflict"
check_error_log  "obdiag rca run --scene=log_error"
check_error_log  "obdiag rca run --scene=transaction_rollback"
check_error_log  "obdiag rca run --scene=transaction_disconnection"
check_error_log  "obdiag rca run --scene=clog_disk_full"
check_error_log  "obdiag rca run --scene=transaction_wait_timeout"
check_error_log  "obdiag rca run --scene=transaction_other_error"
check_error_log  "obdiag rca run --scene=replay_hold --report_type json" &
check_error_log  "obdiag rca run --scene=replay_hold --report_type xml" &
check_error_log  "obdiag rca run --scene=replay_hold --report_type yaml" &
check_error_log  "obdiag rca run --scene=replay_hold --report_type html" &
check_error_log  "obdiag config"
# TODO: Need database connection parameters
#check_error_log  "obdiag config -h <host> -P <port> -u <user> -p <password>"
check_error_log  "obdiag update"
check_error_log  "obdiag update --force" &
# TODO: Need cheat file path
#check_error_log  "obdiag update --file <cheat_file_path>"
check_error_log  "obdiag tool config_check --help"
check_error_log  "obdiag tool io_performance --disk=clog"
check_error_log  "obdiag tool io_performance --disk=data"
# TODO: Need date parameter for historical data
#check_error_log  "obdiag tool io_performance --disk=clog --date 20250808"
check_error_log  "obdiag tool crypto_config --help"
# TODO: Need key and file parameters
#check_error_log  "obdiag tool crypto_config --key <key> --file <config_file>"
#check_error_log  "obdiag tool crypto_config --key <key> --file <config_file> --encrypted_file <encrypted_file>"
check_error_log  "obdiag tool ai_assistant --help"
# TODO: Need trace_id parameter
#check_error_log  "obdiag display-trace <trace_id>"


# Version-specific command execution summary
echo ""
echo "=================Version Check Summary================="
echo "Current tag: $tag"
echo ""
echo "Version-specific features:"
if [ "$tag" == "latest" ]; then
  echo "  ✓ All features enabled (latest version)"
elif version_ge "$tag" "4.3.5.0"; then
  echo "  ✓ All features enabled (version >= 4.3.5.0)"
  echo "  ✓ gather ash with svr_ip/svr_port/tenant_id"
elif version_ge "$tag" "4.2.4.0"; then
  echo "  ✓ gather ash --report_type html (version >= 4.2.4.0)"
  if version_in_range "$tag" "4.2.2.0" "4.3.0.0" || version_ge "$tag" "4.3.1.0"; then
    echo "  ✓ analyze parameter default"
  else
    echo "  ✗ analyze parameter default (requires 4.2.2.0 ≤ version < 4.3.0.0 or version ≥ 4.3.1.0)"
  fi
elif version_ge "$tag" "4.2.2.0"; then
  echo "  ✗ gather ash --report_type html (requires version >= 4.2.4.0)"
  if version_in_range "$tag" "4.2.2.0" "4.3.0.0"; then
    echo "  ✓ analyze parameter default"
  else
    echo "  ✗ analyze parameter default (requires 4.2.2.0 ≤ version < 4.3.0.0 or version ≥ 4.3.1.0)"
  fi
else
  echo "  ✗ gather ash --report_type html (requires version >= 4.2.4.0)"
  echo "  ✗ analyze parameter default (requires 4.2.2.0 ≤ version < 4.3.0.0 or version ≥ 4.3.1.0)"
fi
echo "========================================================"
echo ""


wait

# Record end time and calculate duration
END_TIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "Test completed at: $END_TIME"

# Count results
if [ -f pass_case.txt ]; then
  pass_case=$(wc -l < pass_case.txt | tr -d ' ')
else
  pass_case=0
fi

if [ -f error_code.txt ]; then
  error_case=$(wc -l < error_code.txt | tr -d ' ')
else
  error_case=0
fi

echo "=================Test Summary================="
echo "Passed cases: $pass_case"
echo "Failed cases: $error_case"
echo "Total cases: $((pass_case + error_case))"
echo "=============================================="

# Check if error_code.txt contains any data
if [ -s error_code.txt ]; then
  echo "Errors detected. Exiting with status 1."
  exit 1
else
  echo "No errors detected. Exiting with status 0."
  exit 0
fi
