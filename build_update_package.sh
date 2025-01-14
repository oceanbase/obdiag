#!/bin/bash
echo "Start packaging plugins"
cd plugins && tar -cvf data.tar *
a=$(sha256sum data.tar | awk '{print $1}')
echo "obdiag_version: \"3.1.0\"" > version.yaml
echo "remote_tar_sha: \"$a\"" >> version.yaml
cp -rf version.yaml ../
cp -rf data.tar ../
rm -rf version.yaml
rm -rf data.tar