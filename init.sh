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

if [ -d "${WORK_DIR}/tasks" ]; then
    cp -rf ${WORK_DIR}/tasks  ${OBDIAG_HOME}/
elif [ -d "${WORK_DIR}/handler/checker/tasks" ]; then
    cp -rf ${WORK_DIR}/handler/checker/tasks ${OBDIAG_HOME}/
fi

if [ -d "${WORK_DIR}/example" ]; then
    cp -rf ${WORK_DIR}/example  ${OBDIAG_HOME}/
fi

cp -rf ${WORK_DIR}/*check_package.yaml ${OBDIAG_HOME}/

ALIAS_OBDIAG_EXIST=$(grep "alias obdiag='sh" ~/.bashrc | head -n 1)
if [[ "${ALIAS_OBDIAG_EXIST}" != "" ]]; then
    echo "need update obdiag alias"
    echo "alias obdiag='obdiag'" >> ~/.bashrc
fi

source  ${WORK_DIR}/init_obdiag_cmd.sh

echo "Init obdiag finished"
