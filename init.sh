#!/bin/bash

if [[ $# == 1 && $1 == "-f" ]]; then
    FORCE_DEPLOY="1"
else
    FORCE_DEPLOY="0"
fi

WORK_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

if [ ${OBDIAG_HOME} ]; then
    OBDIAG_HOME=${OBDIAG_HOME}
else
    OBDIAG_HOME="${HOME}/.obdiag"
fi

mkdir -p ${OBDIAG_HOME} && cd ${OBDIAG_HOME}
mkdir -p ${OBDIAG_HOME}/check
mkdir -p ${OBDIAG_HOME}/log
if [ -d "${WORK_DIR}/check" ]; then
    cp -rf ${WORK_DIR}/check  ${OBDIAG_HOME}/
fi

if [ -d "${WORK_DIR}/gather" ]; then
    cp -rf ${WORK_DIR}/gather  ${OBDIAG_HOME}/
fi

if [ -d "${WORK_DIR}/gather" ]; then
    cp -rf ${WORK_DIR}/gather  ${OBDIAG_HOME}/
fi

if [ -d "${WORK_DIR}/example" ]; then
    cp -rf ${WORK_DIR}/example  ${OBDIAG_HOME}/
fi

if [ -d "${WORK_DIR}/rca" ]; then
    cp -rf ${WORK_DIR}/rca  ${OBDIAG_HOME}/
fi


ALIAS_OBDIAG_EXIST=$(grep "alias obdiag='sh" ~/.bashrc | head -n 1)
if [[ "${ALIAS_OBDIAG_EXIST}" != "" ]]; then
    echo "need update obdiag alias"
    echo "alias obdiag='obdiag'" >> ~/.bashrc
fi

source  ${WORK_DIR}/init_obdiag_cmd.sh

if [ -d "${OBDIAG_HOME}/check_package.yaml" ]; then
    echo "${OBDIAG_HOME}/*check_package.yaml and ${OBDIAG_HOME}/tasks  has been discarded. If you have made any changes to these files on your own, please transfer the relevant data to *check_package.yaml in ${OBDIAG_HOME}/check/"
fi
echo "Init obdiag finished"
