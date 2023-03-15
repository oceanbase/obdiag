#!/bin/bash

BUILD_SHELL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OBDIAG_VERSION=`cat obdiag-VER.txt`
RELEASE_ID=`date +%Y%m%d%H%M%S`

cd $BUILD_SHELL_DIR
mkdir oceanbase-diagnostic-tool
cd ..
cp -r ./common ./build/oceanbase-diagnostic-tool/common
cp -r ./resources ./build/oceanbase-diagnostic-tool/resources
cp -r ./docs ./build/oceanbase-diagnostic-tool/docs
cp -r ./conf ./build/oceanbase-diagnostic-tool/conf
cp -r ./dependencies ./build/oceanbase-diagnostic-tool/dependencies
cp -r ./handler ./build/oceanbase-diagnostic-tool/handler
cp -r ./ocp ./build/oceanbase-diagnostic-tool/ocp
cp -r ./utils ./build/oceanbase-diagnostic-tool/utils
cp README.md ./build/oceanbase-diagnostic-tool/
cp clean_all_result.sh ./build/oceanbase-diagnostic-tool/
cp obdiag_client.py ./build/oceanbase-diagnostic-tool/
cp obdiag ./build/oceanbase-diagnostic-tool/
cp obdiag_main.py ./build/oceanbase-diagnostic-tool/
cp requirements2.txt ./build/oceanbase-diagnostic-tool/
cp requirements3.txt ./build/oceanbase-diagnostic-tool/

cd ./build/oceanbase-diagnostic-tool/
mkdir -p ./dependencies/python2/site-packages
mkdir -p ./dependencies/python2/libs
mkdir -p ./dependencies/python3/site-packages
mkdir -p ./dependencies/python3/libs
mkdir -p ./dependencies/bin

pip2 install -t ./dependencies/python2/site-packages/ --default-timeout=600 -r requirements2.txt -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

/usr/bin/python3 -m pip install -t ./dependencies/python3/site-packages/ --default-timeout=600 -r requirements3.txt -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

cd ./dependencies/python2/site-packages
for so_file in `find . -name '*.so*' | xargs ldd | grep -v 'linux-vdso.so.1' |  grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' |  sort | uniq`; do cp ${so_file} ../libs; done
cat > sitecustomize.py <<EOF
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
EOF
cd ../../..


cd ./dependencies/python3/site-packages
for so_file in `find . -name '*.so*' | xargs ldd | grep -v 'linux-vdso.so.1' |  grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' |  sort | uniq`; do cp ${so_file} ../libs; done
cd ../..

tar zcvf oceanbase-diagnostic-tool-$OBDIAG_VERSION-$RELEASE_ID.tar.gz ./oceanbase-diagnostic-tool/*
