import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class FileEncryptor:
    def __init__(self, context):
        self.salt = b'obdiag'
        self.context = context
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

    def encrypt_file(self, file_path, password):
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
            encrypted_file_path = file_path + '.encrypted'
            with open(encrypted_file_path, 'wb') as file:
                file.write(encrypted_data)

            self.stdio(f"File encrypted successfully: {encrypted_file_path}. Please remember your password")
            return True

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
                with open(original_file_path, 'wb') as file:
                    file.write(decrypted_data)

                self.stdio.verbose(f"File decrypted successfully: {original_file_path}")

            return decrypted_data

        except Exception as e:
            self.stdio.error(f"Decryption failed: {str(e)}")
            return False
