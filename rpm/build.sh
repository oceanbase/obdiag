#!/bin/bash

python_bin='python'
W_DIR=`pwd`
VERSION=${VERSION:-'2.2.0'}


function python_version()
{
    return `$python_bin -c 'import sys; print (sys.version_info.major)'`
}

function ispy3()
{
    python_version
    if [ $? != 3 ]; then
        echo "No switch command is provided, so use the default switch command: 'source py-env-activate py38'"
        source py-env-activate py38
    fi
}

function ispy2()
{
    python_version
    if [ $? != 2 ]; then
        echo 'need python2'
        exit 1
    fi
}

function cd2workdir()
{
    cd $W_DIR
    DIR=`dirname $0`
    cd $DIR
}

function pacakge_obdiag()
{
    ispy3
    cd2workdir
    DIR=`pwd`
    RELEASE=${RELEASE:-'1'}
    export RELEASE=$RELEASE
    export VERSION=$VERSION
    pip install -r ../requirements3.txt
    rm -fr rpmbuild
    mkdir -p rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
    rpmbuild --define "_topdir $DIR/rpmbuild" -bb oceanbase-diagnostic-tool.spec
    rpms=`find rpmbuild/RPMS/ -name oceanbase-diagnostic-tool-\*` || exit 1
    for rpm in ${rpms[@]}; do
        cp $rpm ./
    done
    rm -fr rpmbuild
}

function get_python()
{
    if [ `id -u` != 0 ] ; then
        echo "Please use root to run"
    fi

    obd_dir=`dirname $0`
    python_path=`which python`
    for bin in ${python_path[@]}; do
        if [ -e $bin ]; then
            python_bin=$bin
            break 1
        fi
    done

    if [ ${#python_path[*]} -gt 1 ]; then
        read -p "Enter python path [default $python_bin]:"
        if [ "x$REPLY" != "x" ]; then
            python_bin=$REPLY
        fi
    fi
}

function build()
{
    ispy3
    req_fn='requirements3'
    cd2workdir
    DIR=`pwd`
    cd ..
    if [ `git log |head -n1 | awk -F' ' '{print $2}'` ]; then
        CID=`git log |head -n1 | awk -F' ' '{print $2}'`
        BRANCH=`git rev-parse --abbrev-ref HEAD`
    else
        CID='UNKNOWN'
        BRANCH='UNKNOWN'
    fi
    DATE=`date '+%b %d %Y %H:%M:%S'`
    VERSION="$VERSION".`date +%s`
    BUILD_DIR="$DIR/.build"
    rm -fr $BUILD_DIR
    mkdir -p $BUILD_DIR/lib/site-packages
    cp -f obdiag_main.py obdiag.py
    pip install -r $req_fn.txt || exit 1
    rm -f obdiag.py oceanbase-diagnostic-tool.spec
    chmod +x /usr/bin/obdiag
    chmod -R 755 /usr/obdiag/*
    chown -R root:root /usr/obdiag/*
    find /usr/obdiag -type f -exec chmod 644 {} \;
    echo -e 'Installation of obdiag finished successfully\n'
}

case "x$1" in
    xrpm_obdiag)
        pacakge_obdiag
    ;;
    xbuild)
        get_python
        build
    ;;
esac
