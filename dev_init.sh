#!/usr/bin/env bash

PROJECT_PATH=$(cd "$(dirname "$0")"; pwd)
WORK_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

get_python_version() {
    python3 -c "import sys; print(sys.version_info[0], sys.version_info[1])"
}

check_python_version() {
    local version_output=$(python3 -c "import sys; print(sys.version_info.major, sys.version_info.minor)")
    IFS=' ' read -ra version <<< "$version_output"

    major=${version[0]}
    minor=${version[1]}

    if (( major < 3 || (major == 3 && minor < 8) )); then
        echo "Your Python3 version is less than 3.8. Please updating Python3..."
        exit 1
    fi
}

install_requirements() {
    REQ_FILE="${PROJECT_PATH}/requirements3.txt"
    if [[ -f "$REQ_FILE" ]]; then
        echo "Installing packages listed in $REQ_FILE..."
        pip3 install -r "$REQ_FILE"
    else
        echo "No requirements3.txt file found at the expected location."
    fi
}

copy_file(){
    if [ ${OBDIAG_HOME} ]; then
        OBDIAG_HOME=${OBDIAG_HOME}
    else
        OBDIAG_HOME="${HOME}/.obdiag"
    fi

    mkdir -p ${OBDIAG_HOME} && cd ${OBDIAG_HOME}
    mkdir -p ${OBDIAG_HOME}/check
    mkdir -p ${OBDIAG_HOME}/gather
    mkdir -p ${OBDIAG_HOME}/display
    cp -rf ${WORK_DIR}/plugins/* ${OBDIAG_HOME}/
    if [ -d "${WORK_DIR}/example" ]; then
        cp -rf ${WORK_DIR}/example  ${OBDIAG_HOME}/
    fi

}
copy_file
echo "File initialization completed"

check_python_version

source  ${WORK_DIR}/rpm/init_obdiag_cmd.sh

echo "Creating or updating alias 'obdiag' to run 'python3 ${PROJECT_PATH}/src/main.py'"
echo "alias obdiag='python3 ${PROJECT_PATH}/src/main.py'" >> ~/.bashrc
source ~/.bashrc
echo "Initialization completed successfully!"