#!/usr/bin/env bash

CURRENT_USER_ID=$(id -u)
CURRENT_USER_NAME=$(logname 2>/dev/null || echo "$SUDO_USER" | awk -F'[^a-zA-Z0-9_]' '{print $1}')

if [ "$CURRENT_USER_ID" -eq 0 ]; then
    if [ -n "$SUDO_USER" ]; then
        USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
    else
        USER_HOME=/root
    fi
else
    USER_HOME="$HOME"
fi

if [[ $# == 1 && $1 == "-f" ]]; then
    FORCE_DEPLOY="1"
else
    FORCE_DEPLOY="0"
fi

WORK_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

if [ ${OBDIAG_HOME} ]; then
    OBDIAG_HOME=${OBDIAG_HOME}
else
    OBDIAG_HOME="${USER_HOME}/.obdiag"
fi

mkdir -p ${OBDIAG_HOME} && cd ${OBDIAG_HOME}
mkdir -p ${OBDIAG_HOME}/check
mkdir -p ${OBDIAG_HOME}/log
mkdir -p ${OBDIAG_HOME}/display

# Clean rca old *scene.py files
find ${OBDIAG_HOME}/rca -maxdepth 1 -name "*_scene.py" -type f -exec rm -f {} + 2>/dev/null

\cp -rf ${WORK_DIR}/plugins/*  ${OBDIAG_HOME}/

bashrc_file=~/.bashrc
if [ -e "$bashrc_file" ]; then
  ALIAS_OBDIAG_EXIST=$(grep "alias obdiag='sh" ~/.bashrc | head -n 1)
  if [[ "${ALIAS_OBDIAG_EXIST}" != "" ]]; then
      echo "need update obdiag alias"
      echo "alias obdiag='obdiag'" >> ~/.bashrc
  fi
fi

source  ${WORK_DIR}/init_obdiag_cmd.sh

cd -
output_file=${OBDIAG_HOME}/version.yaml
version_line=$(/opt/oceanbase-diagnostic-tool/obdiag --version 2>&1 | grep -oP 'OceanBase Diagnostic Tool: \K[\d.]+')
if [ -n "$version_line" ]; then
    content="obdiag_version: \"$version_line\""

    # Write or update the version information to the file
    echo "$content" > "$output_file"
    
    echo "obdiag version information has been successfully written to $output_file"
else
    echo "failed to retrieve obdiag version information."
fi

chown -R ${CURRENT_USER_NAME}: ${OBDIAG_HOME}

echo "Init obdiag finished"
