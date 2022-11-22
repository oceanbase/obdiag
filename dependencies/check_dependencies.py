#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@file: check_dependencies.py
@desc:
"""


def check_client_dependencies():
    install_flag = False
    try:
        import mysql.connector
        import pprint
        import threading
        import tabulate
        import requests
        import json
        import contextlib
        import os
        import tarfile
        import zipfile
        import zstandard
        import base64
        import hashlib
        import argparse
        import paramiko
        import traceback
        install_flag = True
    except Exception as err:
        print("import  error!!!,cause:[{0}]".format(err))
        pass
    finally:
        return install_flag


if __name__ == "__main__":
    if check_client_dependencies():
        print("\033[1;32m check all dependencies installed, you can use odg normal! \033[0m")
    else:
        print("\033[1;31m check dependencies failed, you need resolve dependencies \033[0m")
