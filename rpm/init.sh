#!/usr/bin/env bash
# Get current effective user information
CURRENT_USER_ID=$(id -u)

# Determine if the script is executed via sudo (not simply based on SUDO_USER)
IS_SUDO_EXECUTED=0
if [ "$CURRENT_USER_ID" -eq 0 ] && [ -n "$SUDO_USER" ]; then
    IS_SUDO_EXECUTED=1
else
    IS_SUDO_EXECUTED=0
fi

# Set user and home directory
if [ $IS_SUDO_EXECUTED -eq 1 ]; then
    # It was executed via sudo, use the original user
    CURRENT_USER_NAME="$SUDO_USER"
    USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
else
    # Otherwise, use the current user
    CURRENT_USER_NAME=$(id -un)
    USER_HOME="$HOME"
fi

echo "CURRENT_USER_NAME = $CURRENT_USER_NAME"
echo "USER_HOME = $USER_HOME"

if [ -z "$USER_HOME" ]; then
    echo "Error: Could not determine home directory for current user."
    exit 1
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

# Also copy plugins to root user's .obdiag directory
if [ "$CURRENT_USER_ID" -eq 0 ]; then
    # Running as root directly, ensure access to /root/.obdiag
    mkdir -p /root/.obdiag
    \cp -rf ${WORK_DIR}/plugins/* /root/.obdiag/
    chown -R root:root /root/.obdiag
elif [ "$IS_SUDO_EXECUTED" -eq 1 ]; then
    # Executed via sudo, attempt to copy to root directory
    mkdir -p /root/.obdiag
    \cp -rf ${WORK_DIR}/plugins/* /root/.obdiag/
    chown -R root:root /root/.obdiag
else
    echo "Warning: Not running as root or with sudo privileges, cannot copy to /root/.obdiag"
fi

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
