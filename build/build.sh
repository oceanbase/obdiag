#!/bin/bash

BUILD_SHELL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ODG_VERSION="0.1.0"
RELEASE_ID=`date +%Y%m%d%H%M%S`

cd $BUILD_SHELL_DIR
mkdir ob-diagnosis-gather
cd ..
cp -r ./common ./build/ob-diagnosis-gather/common
cp -r ./conf ./build/ob-diagnosis-gather/conf
cp -r ./dependencies ./build/ob-diagnosis-gather/dependencies
cp -r ./handler ./build/ob-diagnosis-gather/handler
cp -r ./ocp ./build/ob-diagnosis-gather/ocp
cp -r ./utils ./build/ob-diagnosis-gather/utils
cp README.md ./build/ob-diagnosis-gather/
cp clean_all_result.sh ./build/ob-diagnosis-gather/
cp odg_client.py ./build/ob-diagnosis-gather/
cp odg_ctl ./build/ob-diagnosis-gather/
cp odg_main.py ./build/ob-diagnosis-gather/
cp requirements2.txt ./build/ob-diagnosis-gather/
cp requirements3.txt ./build/ob-diagnosis-gather/

cd ./build/ob-diagnosis-gather/
mkdir -p ./dependencies/python2/site-packages
mkdir -p ./dependencies/python2/libs
mkdir -p ./dependencies/python3/site-packages
mkdir -p ./dependencies/python3/libs

pip2 install -t ./dependencies/python2/site-packages/ --default-timeout=600 -r requirements2.txt

pip3 install -t ./dependencies/python3/site-packages/ --default-timeout=600 -r requirements3.txt


cd ./dependencies/python2/site-packages
for so_file in `find . -name '*.so*' | xargs ldd | grep -v 'linux-vdso.so.1' |  grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' |  sort | uniq`; do cp ${so_file} ../libs; done
cd ../../..


cd ./dependencies/python3/site-packages
for so_file in `find . -name '*.so*' | xargs ldd | grep -v 'linux-vdso.so.1' |  grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' |  sort | uniq`; do cp ${so_file} ../libs; done
cd ../../../..

tar zcvf ob-diagnosis-gather-$ODG_VERSION-$RELEASE_ID.tar.gz ./ob-diagnosis-gather/*