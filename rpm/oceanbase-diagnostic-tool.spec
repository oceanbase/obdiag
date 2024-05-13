Name: oceanbase-diagnostic-tool
Version:2.2.0
Release: %(echo $RELEASE)%{?dist}
Summary: oceanbase diagnostic tool program
Group: Development/Tools
Url: git@github.com:oceanbase/oceanbase-diagnostic-tool.git
License: Commercial
# BuildRoot:  %_topdir/BUILDROOT
%define debug_package %{nil}
%define __os_install_post %{nil}
%define _build_id_links none
AutoReqProv: no

%description
oceanbase diagnostic tool program

%install
RPM_DIR=$OLDPWD
SRC_DIR=$OLDPWD
BUILD_DIR=$OLDPWD/rpmbuild
cd $SRC_DIR/
rm -rf build.log build dist oceanbase-diagnostic-tool.spec
DATE=`date`
VERSION="$RPM_PACKAGE_VERSION"

cd $SRC_DIR
pwd
pip install -r requirements3.txt
cp -f main.py obdiag.py
sed -i  "s/<B_TIME>/$DATE/" ./common/version.py  && sed -i "s/<VERSION>/$VERSION/" ./common/version.py
mkdir -p $BUILD_DIR/SOURCES ${RPM_BUILD_ROOT}
mkdir -p $BUILD_DIR/SOURCES/site-packages
mkdir -p $BUILD_DIR/SOURCES/resources
mkdir -p $BUILD_DIR/SOURCES/check/tasks
mkdir -p $BUILD_DIR/SOURCES/gather/tasks
mkdir -p $BUILD_DIR/SOURCES/rca
mkdir -p $BUILD_DIR/SOURCES/dependencies/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool
pyinstaller --hidden-import=decimal -p $BUILD_DIR/SOURCES/site-packages -F obdiag.py
rm -f obdiag.py oceanbase-diagnostic-tool.spec

\cp -rf $SRC_DIR/example $BUILD_DIR/SOURCES/example
\cp -rf $SRC_DIR/resources $BUILD_DIR/SOURCES/
\cp -rf $SRC_DIR/dependencies/bin $BUILD_DIR/SOURCES/dependencies
\cp -rf $SRC_DIR/handler/checker/tasks $BUILD_DIR/SOURCES/check
\cp -rf $SRC_DIR/handler/gather/tasks $BUILD_DIR/SOURCES/gather
\cp -rf $SRC_DIR/handler/rca/scene/* $BUILD_DIR/SOURCES/rca
\cp -rf $SRC_DIR/init.sh $BUILD_DIR/SOURCES/init.sh
\cp -rf $SRC_DIR/init_obdiag_cmd.sh $BUILD_DIR/SOURCES/init_obdiag_cmd.sh
\cp -rf $SRC_DIR/conf $BUILD_DIR/SOURCES/conf
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/lib/
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/dependencies/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/gather
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/check
mkdir -p ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/rca

\cp -rf $SRC_DIR/dist/obdiag ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/obdiag
\cp -rf $BUILD_DIR/SOURCES/site-packages ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/lib/site-packages
\cp -rf $BUILD_DIR/SOURCES/resources ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/resources
\cp -rf $BUILD_DIR/SOURCES/dependencies/bin ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/dependencies
\cp -rf $BUILD_DIR/SOURCES/example ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/conf ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/init.sh ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/init_obdiag_cmd.sh ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/check/* ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/check
mv ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/check/tasks/*.yaml ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/check/
\cp -rf $BUILD_DIR/SOURCES/gather/tasks ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/gather
\cp -rf $BUILD_DIR/SOURCES/rca/* ${RPM_BUILD_ROOT}/usr/local/oceanbase-diagnostic-tool/rca


%files
%defattr(-,root,root,0777)
/usr/local/oceanbase-diagnostic-tool/*

%post
chmod -R 755 /usr/local/oceanbase-diagnostic-tool/*
chown -R root:root /usr/local/oceanbase-diagnostic-tool/*
find /usr/local/oceanbase-diagnostic-tool/obdiag -type f -exec chmod 644 {} \;
ln -sf /usr/local/oceanbase-diagnostic-tool/obdiag /usr/bin/obdiag
chmod +x /usr/local/oceanbase-diagnostic-tool/obdiag
cp -rf /usr/local/oceanbase-diagnostic-tool/init_obdiag_cmd.sh /etc/profile.d/obdiag.sh
sh /usr/local/oceanbase-diagnostic-tool/init.sh
echo -e 'Please execute the following command to init obdiag:\n'
echo -e '\033[32m source /usr/local/oceanbase-diagnostic-tool/init.sh \n \033[0m'
