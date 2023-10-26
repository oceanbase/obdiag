#!/bin/bash
echo "============prepare work env start============"

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

if [ ! -e "OceanBase.repo" ]; then
    wget -q https://mirrors.aliyun.com/oceanbase/OceanBase.repo
fi

echo "============update .bashrc============"

ALIAS_OBDIAG_EXIST=$(grep "alias obdiag=" ~/.bashrc | head -n 1)

if [[ "${ALIAS_OBDIAG_EXIST}" != "" ]]; then
    echo "need update obdiag alias"
fi

echo "export OBDIAG_INSTALL_PATH=${WORK_DIR}" >> ~/.bashrc
echo "alias obdiag='sh ${WORK_DIR}/obdiag'" >> ~/.bashrc
source ~/.bashrc
echo "============prepare work env ok!============"
