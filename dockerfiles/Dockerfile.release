FROM registry.openanolis.cn/openanolis/anolisos:8.9
WORKDIR /root
RUN yum install -y yum-utils
RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
RUN yum install -y oceanbase-diagnostic-tool
RUN sh /opt/oceanbase-diagnostic-tool/init.sh