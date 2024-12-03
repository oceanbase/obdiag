#!/usr/bin/env bash

# 开始打包
export RELEASE=`date +%Y%m%d%H%M`
sed -i 's/pip install -r requirements3.txt/curl https:\/\/bootstrap.pypa.io\/get-pip.py -o get-pip.py\n\
python3 get-pip.py\n\
pip3 install -r requirements3.txt/' ./rpm/oceanbase-diagnostic-tool.spec
cat ./rpm/oceanbase-diagnostic-tool.spec
rpmbuild -bb ./rpm/oceanbase-diagnostic-tool.spec
# 展示对应的包路径
find / -name oceanbase-diagnostic-tool-*.rpm 