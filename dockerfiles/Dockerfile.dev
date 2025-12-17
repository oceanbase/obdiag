FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/centos:centos7.9.2009

ENV PATH="/opt/miniconda/bin:$PATH"
ENV LANGUAGE en_US
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8


# 基础环境设置
RUN rm -rf /etc/yum.repos.d/* \
&& curl -o /etc/yum.repos.d/CentOS-aliyun.repo http://mirrors.aliyun.com/repo/Centos-7.repo \
&& yum clean all \
&& yum makecache \
&& yum install -y wget gcc git \
&& wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
&& bash Miniconda3-latest-Linux-x86_64.sh -p /opt/miniconda -b \
&& rm -rf Miniconda3-latest-Linux-x86_64.sh \
&& conda create -n obdiag -y python=3.11 \
&& source /opt/miniconda/bin/activate obdiag \
&& python3 -m pip install --upgrade pip wheel

COPY ./requirements3.txt /workspaces/obdiag/requirements3.txt
RUN /opt/miniconda/envs/obdiag/bin/pip install -r /workspaces/obdiag/requirements3.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN echo "source /opt/miniconda/bin/activate obdiag" >> ~/.bashrc