FROM centos:7
RUN rm -rf /etc/yum.repos.d/*
RUN curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo
RUN yum install -y wget rpm-build
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --no-check-certificate
RUN sh Miniconda3-latest-Linux-x86_64.sh -p /opt/miniconda3 -b
RUN export PATH=/opt/miniconda3/bin:$PATH
RUN /opt/miniconda3/bin/conda init
RUN /opt/miniconda3/bin/conda create --name obdiag python=3.8 -y
RUN source /opt/miniconda3/bin/activate obdiag
RUN /opt/miniconda3/envs/obdiag/bin/python3.8  -m pip install --upgrade pip setuptools wheel
RUN yum install -y gcc gcc-c++ make