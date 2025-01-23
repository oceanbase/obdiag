#!/usr/bin/env bash

PROJECT_PATH=$(cd "$(dirname "$0")"; pwd)
WORK_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

build_rpm() {
    clean_old_rpm_data
    download_obstack
    export RELEASE=`date +%Y%m%d%H%M`
    sed -i 's/pip install -r requirements3.txt/curl https:\/\/bootstrap.pypa.io\/get-pip.py -o get-pip.py\n\
python3 get-pip.py\n\
pip3 install -r requirements3.txt/' ./rpm/oceanbase-diagnostic-tool.spec
    cat ./rpm/oceanbase-diagnostic-tool.spec
    yum install rpm-build -y
    rpmbuild -bb ./rpm/oceanbase-diagnostic-tool.spec
    find ~/rpmbuild -name oceanbase-diagnostic-tool-*.rpm
}

download_obstack() {
  echo "check obstack"
  mkdir -p ./dependencies/bin
  # download obstack
  if [ -f ./dependencies/bin/obstack_aarch64 ]; then
    echo "obstack_aarch64 exist, skip download"
    return
  else
    echo "downland aarch64 obstack."
    obutils_aarch64_url="https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/observer/v4.3.5_CE/oceanbase-ce-utils-4.3.5.0-100000202024123117.el7.aarch64.rpm"
    wget ${obutils_aarch64_url} -O ./obutils.rpm
    rpm2cpio obutils.rpm | cpio -idv
    cp -f ./usr/bin/obstack ./dependencies/bin/obstack_aarch64
    rm -rf ./usr
    rm -rf obutils.rpm
  fi
  if [ -f ./dependencies/bin/obstack_x86_64 ]; then
    echo "obstack_x86_64 exist, skip download"
    return
  else
    echo "downland x64 obstack."
    obutils_x64_url="https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/observer/v4.3.5_CE/oceanbase-ce-utils-4.3.5.0-100000202024123117.el7.x86_64.rpm"
    wget ${obutils_x64_url} -O ./obutils.rpm
    rpm2cpio obutils.rpm | cpio -idv
    cp -f ./usr/bin/obstack ./dependencies/bin/obstack_x86_64
    rm -rf ./usr
    rm -rf obutils.rpm
  fi
}


clean_old_rpm_data() {
    rm -rf ./rpmbuild
    rm -rf ./build
    rm -rf ./dist
    rm -rf ./src/obdiag.py
    rm -rf ./BUILDROOT
    rm -rf ./get-pip.py
    rm -rf ./obdiag.spec
    echo "Clean old rpm data success"
}

clean_files() {
    rm -rf ./obdiag_gather_pack_* ./obdiag_analyze_pack_* ./obdiag_analyze_flt_result* ./obdiag_check_report
}


initialize_environment() {
    export PYTHONPATH=$PYTHONPATH:$PROJECT_PATH
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

    backup_obdiag_folders() {
        local backup_dir="${OBDIAG_HOME}/dev_backup"
        mkdir -p "$backup_dir"
        local datestamp=$(date +%Y%m%d_%H%M%S)
        local tar_file="$backup_dir/obdiag_backup_$datestamp.tar.gz"
        tar -czf "$tar_file" -C "${OBDIAG_HOME}" check display gather rca 2>/dev/null
        if [ $? -eq 0 ]; then
            echo "Backup completed: $tar_file"
        else
            echo "No folders found to back up or backup failed."
        fi
    }

    remove_existing_folders() {
        for folder in check display gather rca; do
            if [ -d "${OBDIAG_HOME}/$folder" ]; then
                echo "Removing existing ${OBDIAG_HOME}/$folder"
                rm -rf "${OBDIAG_HOME}/$folder"
            fi
        done
    }

    if [ -z "${OBDIAG_HOME}" ]; then
        OBDIAG_HOME="${HOME}/.obdiag"
    fi

    mkdir -p "${OBDIAG_HOME}"

    backup_obdiag_folders

    remove_existing_folders

    copy_file
    check_python_version
    install_requirements
    download_obstack

    source  ${WORK_DIR}/rpm/init_obdiag_cmd.sh

    echo "Creating or updating alias 'obdiag' to run 'python3 ${PROJECT_PATH}/src/main.py'"
    echo "export PYTHONPATH=\$PYTHONPATH:${PROJECT_PATH}" >> ~/.bashrc
    echo "alias obdiag='python3 ${PROJECT_PATH}/src/main.py'" >> ~/.bashrc
    source ~/.bashrc
    echo "Initialization completed successfully!"
}

show_help() {
    echo "Usage: $0 {pack|clean|init|format}"
    echo "  pack   - Start packaging rpm"
    echo "  clean  - Clean result files"
    echo "  init   - Initialize dev environment"
    echo "  format - Format code with black"
    echo "  download_obstack - Download obstack"
}

format_code() {
    # Check if black is installed, if not, try to install it.
    if ! command -v black &> /dev/null; then
        echo "Black is not installed. Attempting to install..."
        pip3 install --user black
    fi

    # Run black on the project directory with specified options.
    black -S -l 256 .
}

case "$1" in
    pack)
        build_rpm
        ;;
    clean)
        clean_files
        ;;
    init)
        initialize_environment
        ;;
    format)
        format_code
        ;;
    download_obstack)
        download_obstack
        ;;
    *)
        show_help
        ;;
esac