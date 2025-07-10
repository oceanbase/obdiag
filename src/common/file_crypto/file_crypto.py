#!/usr/bin/env python
# -*- coding: UTF-8 -*
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""
@time: 2025/7/10
@file: file_crypto.py
@desc:
"""
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class FileEncryptor:
    def __init__(self, context, stdio=None):
        self.salt = b'obdiag'
        if context is None:
            self.stdio = stdio
        else:
            self.stdio = context.stdio

    def generate_key_from_password(self, password):
        """Generate encryption key from password"""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key

    def encrypt_file(self, file_path, password, save=False):
        """Encrypt file"""
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                self.stdio.error(f"Error: File '{file_path}' does not exist")
                raise FileNotFoundError(f"File '{file_path}' does not exist")

            # Generate key
            key = self.generate_key_from_password(password)
            fernet = Fernet(key)

            # Read original file
            with open(file_path, 'rb') as file:
                file_data = file.read()

            # Encrypt data
            encrypted_data = fernet.encrypt(file_data)

            # Save encrypted file
            if save:
                encrypted_file_path = file_path + '.encrypted'
                if os.path.exists(encrypted_file_path):
                    self.stdio.error(f"Error: Encrypted file '{encrypted_file_path}' already exists. Please backup it first")
                    raise FileExistsError(f"Encrypted file '{encrypted_file_path}' already exists. Please backup it first")
                with open(encrypted_file_path, 'wb') as file:
                    file.write(encrypted_data)

                self.stdio.print(f"File encrypted successfully: {encrypted_file_path}. Please remember your password")
            return encrypted_data

        except Exception as e:
            self.stdio.error(f"Encryption failed: {str(e)}")
            return False

    def decrypt_file(self, encrypted_file_path, password, save=False):
        """Decrypt file"""
        try:
            # Check if encrypted file exists
            if not os.path.exists(encrypted_file_path):
                self.stdio.verbose(f"Error: Encrypted file '{encrypted_file_path}' does not exist")
                raise FileNotFoundError(f"Encrypted file '{encrypted_file_path}' does not exist")

            # Check file extension
            if not encrypted_file_path.endswith('.encrypted'):
                self.stdio.error(f"Error: File '{encrypted_file_path}' is not an encrypted file")
                return False

            # Generate key
            key = self.generate_key_from_password(password)
            fernet = Fernet(key)

            # Read encrypted file
            with open(encrypted_file_path, 'rb') as file:
                encrypted_data = file.read()

            # Decrypt data
            decrypted_data = fernet.decrypt(encrypted_data)
            if save:
                # Save decrypted file
                original_file_path = encrypted_file_path[:-10]  # Remove .encrypted suffix
                if os.path.exists(original_file_path):
                    self.stdio.error(f"Error: Decrypted file '{original_file_path}' already exists. Please backup it first")
                    raise FileExistsError(f"Decrypted file '{original_file_path}' already exists. Please backup it first")
                with open(original_file_path, 'wb') as file:
                    file.write(decrypted_data)

                self.stdio.verbose(f"File decrypted successfully: {original_file_path}")

            return decrypted_data

        except Exception as e:
            self.stdio.error(f"Decryption failed: {str(e)}")
            return False

    def check_encrypt_file(self, file_path, encrypted_file_path, password):
        """Check if file is encrypted"""
        try:
            data = self.decrypt_file(encrypted_file_path, password, save=False)
            # Read original file
            with open(file_path, 'rb') as file:
                file_data = file.read()
            if str(file_data) == str(data):
                self.stdio.print(f"File is encrypted successfully")
                return True
            else:
                self.stdio.error("{} and {} is not same".format(file_path, encrypted_file_path))
                return False
        except Exception as e:
            self.stdio.error(f"Check encrypt file failed: {str(e)}")
            return False
