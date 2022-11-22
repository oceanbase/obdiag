#!/bin/bash

BUILD_SHELL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ODG_VERSION=`cat odg-VER.txt`
RELEASE_ID=`date +%Y%m%d%H%M%S`

cd $BUILD_SHELL_DIR
mkdir oceanbase_diagnosis_gather
cd ..
cp -r ./common ./build/oceanbase_diagnosis_gather/common
cp -r ./conf ./build/oceanbase_diagnosis_gather/conf
cp -r ./dependencies ./build/oceanbase_diagnosis_gather/dependencies
cp -r ./handler ./build/oceanbase_diagnosis_gather/handler
cp -r ./ocp ./build/oceanbase_diagnosis_gather/ocp
cp -r ./utils ./build/oceanbase_diagnosis_gather/utils
cp README.md ./build/oceanbase_diagnosis_gather/
cp clean_all_result.sh ./build/oceanbase_diagnosis_gather/
cp odg_client.py ./build/oceanbase_diagnosis_gather/
cp odg_ctl ./build/oceanbase_diagnosis_gather/
cp odg_main.py ./build/oceanbase_diagnosis_gather/
cp requirements2.txt ./build/oceanbase_diagnosis_gather/
cp requirements3.txt ./build/oceanbase_diagnosis_gather/

cd ./build/oceanbase_diagnosis_gather/
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

tar zcvf oceanbase_diagnosis_gather-$ODG_VERSION-$RELEASE_ID.tar.gz ./oceanbase_diagnosis_gather/*