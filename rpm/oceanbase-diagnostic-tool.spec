Name: oceanbase-diagnostic-tool
Version:3.2.0
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
cp -f src/main.py src/obdiag.py
sed -i  "s/<B_TIME>/$DATE/" ./src/common/version.py  && sed -i "s/<VERSION>/$VERSION/" ./src/common/version.py
mkdir -p $BUILD_DIR/SOURCES ${RPM_BUILD_ROOT}
mkdir -p $BUILD_DIR/SOURCES/site-packages
mkdir -p $BUILD_DIR/SOURCES/resources
mkdir -p $BUILD_DIR/SOURCES/dependencies/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/bin
mkdir -p ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool
cp -rf $SRC_DIR/src $BUILD_DIR/SOURCES/site-packages/
pyinstaller --hidden-import=decimal -p $BUILD_DIR/SOURCES/site-packages -F src/obdiag.py
rm -f obdiag.py oceanbase-diagnostic-tool.spec

cd $SRC_DIR
\cp -rf $SRC_DIR/example $BUILD_DIR/SOURCES/example
\cp -rf $SRC_DIR/resources $BUILD_DIR/SOURCES/
\cp -rf $SRC_DIR/dependencies/bin $BUILD_DIR/SOURCES/dependencies
\cp -rf $SRC_DIR/plugins $BUILD_DIR/SOURCES/
\cp -rf $SRC_DIR/rpm/init.sh $BUILD_DIR/SOURCES/init.sh
\cp -rf $SRC_DIR/rpm/init_obdiag_cmd.sh $BUILD_DIR/SOURCES/init_obdiag_cmd.sh
\cp -rf $SRC_DIR/rpm/obdiag_backup.sh $BUILD_DIR/SOURCES/obdiag_backup.sh
\cp -rf $SRC_DIR/conf $BUILD_DIR/SOURCES/
mkdir -p ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/lib/
mkdir -p ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/dependencies/bin
find $SRC_DIR -name "obdiag"
\cp -rf $SRC_DIR/dist/obdiag ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/obdiag
\cp -rf $BUILD_DIR/SOURCES/site-packages ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/lib/site-packages
\cp -rf $BUILD_DIR/SOURCES/resources ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/resources
\cp -rf $BUILD_DIR/SOURCES/dependencies/bin ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/dependencies
\cp -rf $BUILD_DIR/SOURCES/example ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/conf ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/conf
\cp -rf $BUILD_DIR/SOURCES/init.sh ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/init_obdiag_cmd.sh ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/obdiag_backup.sh ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/
\cp -rf $BUILD_DIR/SOURCES/plugins ${RPM_BUILD_ROOT}/opt/oceanbase-diagnostic-tool/


%files
%defattr(-,root,root,0777)
/opt/oceanbase-diagnostic-tool/*

%post
chmod -R 755 /opt/oceanbase-diagnostic-tool/*
chown -R root:root /opt/oceanbase-diagnostic-tool/*
find /opt/oceanbase-diagnostic-tool/obdiag -type f -exec chmod 644 {} \;
ln -sf /opt/oceanbase-diagnostic-tool/obdiag /usr/bin/obdiag
chmod +x /opt/oceanbase-diagnostic-tool/obdiag
cp -rf /opt/oceanbase-diagnostic-tool/init_obdiag_cmd.sh /etc/profile.d/obdiag.sh
/opt/oceanbase-diagnostic-tool/obdiag_backup.sh
/opt/oceanbase-diagnostic-tool/init.sh
echo -e 'Please execute the following command to init obdiag:\n'
echo -e '\033[32m source /opt/oceanbase-diagnostic-tool/init.sh \n \033[0m'