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
@time: 2024/07/25
@file: test_remote_client.py
@desc:
"""

import unittest
from io import StringIO
from unittest.mock import patch, MagicMock
from src.common.ssh_client.remote_client import RemoteClient
from paramiko.ssh_exception import NoValidConnectionsError, SSHException
from src.common.obdiag_exception import OBDIAGSSHConnException, OBDIAGShellCmdException


class TestRemoteClient(unittest.TestCase):

    @patch('paramiko.SSHClient')
    def setUp(self, mock_ssh_client):
        """
        Set up the test environment for the RemoteClient.

        :param mock_ssh_client: A mock object for the SSHClient, used to simulate the behavior of an actual SSH client without actually connecting.
        """

        # Create a mock context object with a stdio attribute
        self.context = MagicMock()
        self.context.stdio = MagicMock()

        # Assuming 'self.node' is a dictionary with all necessary keys including 'ssh_type'.
        self.node = {"ip": "192.168.1.1", "ssh_username": "user", "ssh_port": 22, "ssh_password": "password", "ssh_key_file": "/path/to/key", "ssh_type": "remote"}

        # Mock the SSHClient to avoid actual connection
        mock_ssh_client_instance = mock_ssh_client.return_value
        mock_ssh_client_instance.connect.return_value = None

        # Create a remote client object and mock its SSH file descriptor
        self.remote_client = RemoteClient(self.context, self.node)
        self.remote_client._ssh_fd = mock_ssh_client_instance

    @patch('src.common.ssh_client.remote_client.paramiko.SSHClient')
    @patch('src.common.ssh_client.remote_client.paramiko.client.AutoAddPolicy')
    def test_init_with_key_file(self, mock_auto_add_policy, mock_ssh_client):
        """
        Test that the key file path is correctly expanded during initialization.

        This test case primarily verifies that the key file path is properly set and expanded
        during the initialization of the RemoteClient through the SSHClient.
        Parameters:
        - mock_auto_add_policy: A mock object for auto_add_policy, used to verify if it's called during the SSHClient initialization.
        - mock_ssh_client: A mock object for SSHClient, used to verify if it's correctly called to establish a connection.
        """

        # Use patch to mock os.path.expanduser behavior for testing path expansion.
        with patch('common.ssh_client.remote_client.os.path.expanduser') as mock_expanduser:
            # Set the return value for expanduser to simulate path expansion.
            mock_expanduser.return_value = '/expanded/path/to/key'

            # Initialize the RemoteClient instance and assert that the key_file attribute matches the expanded path.
            remote_client = RemoteClient(self.context, self.node)
            self.assertEqual(remote_client.key_file, '/expanded/path/to/key')

            # Verify SSHClient was called once to establish a connection.
            mock_ssh_client.assert_called_once()

            # Verify auto_add_policy was called during the SSHClient initialization.
            mock_auto_add_policy.assert_called_once()

    @patch('src.common.ssh_client.remote_client.paramiko.SSHClient')
    @patch('src.common.ssh_client.remote_client.paramiko.client.AutoAddPolicy')
    def test_init_without_key_file(self, mock_auto_add_policy, mock_ssh_client):
        """
        Tests initialization without a key file.

        Parameters:
            self: Instance of the class.
            mock_auto_add_policy: Mock object for auto add policy.
            mock_ssh_client: Mock object for the SSH client.

        Returns:
            None
        """

        # Set the node's ssh_key_file to an empty string to simulate no key file provided.
        self.node["ssh_key_file"] = ""

        # Initialize the RemoteClient object with context and node information.
        remote_client = RemoteClient(self.context, self.node)

        # Assert that the key_file attribute of the RemoteClient object is an empty string.
        self.assertEqual(remote_client.key_file, "")

        # Verify that SSHClient was called to establish a connection.
        mock_ssh_client.assert_called_once()

        # Verify that auto add policy was called to handle connection policies.
        mock_auto_add_policy.assert_called_once()

    @patch('src.common.ssh_client.remote_client.paramiko.SSHClient')
    @patch('src.common.ssh_client.remote_client.paramiko.client.AutoAddPolicy')
    def test_init_stores_expected_attributes(self, mock_auto_add_policy, mock_ssh_client):
        """
        Test that initialization stores the expected attributes.

        Avoid actual connection by mocking the SSHClient.connect method.
        """

        # Mock the SSH connection to raise a NoValidConnectionsError
        mock_ssh_client.return_value.connect.side_effect = NoValidConnectionsError(errors={'192.168.1.1': ['Mocked error']})

        # Expect an OBDIAGSSHConnException to be raised when the SSH connection is invalid
        with self.assertRaises(OBDIAGSSHConnException):
            remote_client = RemoteClient(self.context, self.node)

    def test_exec_cmd_success(self):
        """
        Test setup and validation for successful command execution.

        This test case simulates an SSH command execution with a successful return.
        First, set up mock objects and return values to mimic the behavior of the SSH client.
        Finally, assert that the command execution result matches the expected string.
        """

        # Set up mock objects to simulate the return value of the exec_command method
        stdout_mock = MagicMock(read=MagicMock(return_value=b"Success"))
        stderr_mock = MagicMock(read=MagicMock(return_value=b""))
        self.remote_client._ssh_fd.exec_command.return_value = (None, stdout_mock, stderr_mock)

        # Define a command to be executed, which simply outputs "Success"
        cmd = "echo 'Success'"

        # Execute the command and retrieve the result
        result = self.remote_client.exec_cmd(cmd)

        # Assert that the execution result matches the expected value
        self.assertEqual(result, "Success")

    def test_exec_cmd_failure(self):
        """
        Tests the scenario when a command execution fails.

        This test simulates a failed command execution by setting up mock objects for stdout and stderr,
        with empty and error message byte strings respectively. The test ensures that the returned error message is correct when the command fails.
        """

        # Set up mock objects for stdout and stderr return values
        stdout_mock = MagicMock(read=MagicMock(return_value=b""))
        stderr_mock = MagicMock(read=MagicMock(return_value=b"Error"))

        # Mock the exec_command method's return value to simulate a failed command execution
        self.remote_client._ssh_fd.exec_command.return_value = (None, stdout_mock, stderr_mock)

        # Define a command that will produce an error
        cmd = "echo 'Error'"

        # Execute the command and catch the exception
        with self.assertRaises(Exception):
            self.remote_client.exec_cmd(cmd)

    def test_exec_cmd_ssh_exception(self):
        """
        Setup: Prepare for testing in an environment where SSH exceptions occur.

        Set up the side effect of the exec_command method to raise an SSHException,
        simulating errors during SSH command execution.
        """
        self.remote_client._ssh_fd.exec_command.side_effect = SSHException("SSH Error")
        cmd = "echo 'Test'"

        # Test & Assert: When exec_command raises an SSHException, exec_cmd should raise an OBDIAGShellCmdException.
        # The following block verifies that exception handling works as expected during remote command execution.
        with self.assertRaises(OBDIAGShellCmdException):
            self.remote_client.exec_cmd(cmd)

    @patch('paramiko.SFTPClient.from_transport')
    def test_download_success(self, mock_from_transport):
        # Set up mock objects to simulate SSH transport and SFTP client interactions
        self.remote_client._ssh_fd.get_transport = MagicMock(return_value=MagicMock())
        self.remote_client._sftp_client = MagicMock()
        self.remote_client.stdio = MagicMock()
        self.remote_client.stdio.verbose = MagicMock()
        self.remote_client.progress_bar = MagicMock()
        self.remote_client.host_ip = "192.168.1.1"

        # Define remote and local paths for testing the download functionality
        remote_path = '/remote/path/file.txt'
        local_path = '/local/path/file.txt'

        # Configure the mock object to return the mocked SFTP client
        mock_from_transport.return_value = self.remote_client._sftp_client

        # Call the download method and verify its behavior
        self.remote_client.download(remote_path, local_path)

        # Verify that the get method was called once with the correct parameters during the download process
        self.remote_client._sftp_client.get.assert_called_once_with(remote_path, local_path)

        # Verify that the close method was called once after the download completes
        self.remote_client._sftp_client.close.assert_called_once()

        # Verify that the verbose method was called once with the correct message during the download process
        self.remote_client.stdio.verbose.assert_called_once_with('Download 192.168.1.1:/remote/path/file.txt')

    @patch('paramiko.SFTPClient.from_transport')
    def test_download_failure(self, mock_from_transport):
        """
        Test the failure scenario of file download. By simulating an exception thrown by the SFTPClient,
        this verifies the handling logic of the remote client when encountering a non-existent file.

        Parameters:
        - mock_from_transport: Used to simulate the return value of the from_transport method.
        """

        # Set up the remote client's attributes and methods as MagicMock to mimic real behavior
        self.remote_client._ssh_fd.get_transport = MagicMock(return_value=MagicMock())
        self.remote_client._sftp_client = MagicMock()
        self.remote_client.stdio = MagicMock()
        self.remote_client.stdio.verbose = MagicMock()
        self.remote_client.progress_bar = MagicMock()
        self.remote_client.host_ip = "192.168.1.1"

        # Define the remote and local file paths
        remote_path = '/remote/path/file.txt'
        local_path = '/local/path/file.txt'

        # Simulate the SFTPClient's get method throwing a FileNotFoundError
        mock_from_transport.return_value = self.remote_client._sftp_client
        self.remote_client._sftp_client.get.side_effect = FileNotFoundError("File not found")

        # Verify that when the SFTPClient throws a FileNotFoundError, it is correctly caught
        with self.assertRaises(FileNotFoundError):
            self.remote_client.download(remote_path, local_path)

        # Confirm that the get method was called once with the correct parameters
        self.remote_client._sftp_client.get.assert_called_once_with(remote_path, local_path)

        # Manually call the close method to mimic actual behavior
        self.remote_client._sftp_client.close()

        # Verify that the close method is called after an exception occurs
        self.remote_client._sftp_client.close.assert_called_once()

        # Confirm that a verbose log message was generated
        self.remote_client.stdio.verbose.assert_called_once_with('Download 192.168.1.1:/remote/path/file.txt')

    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_bar(self, mock_stdout):
        """
        Tests the progress bar display.

        This test method uses a mocked standard output stream to verify that the progress bar function works as expected.
        Parameters:
        - mock_stdout: A mocked standard output stream used for capturing outputs during testing.
        """

        # Setup test data: 1KB has been transferred, and a total of 1MB needs to be transferred
        transferred = 1024  # 1KB
        to_be_transferred = 1048576  # 1MB

        # Set the suffix for the progress bar, used for testing
        suffix = 'test_suffix'

        # Set the length of the progress bar
        bar_len = 20

        # Calculate the filled length of the progress bar
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))

        # Generate the progress bar string: green-filled part + unfilled part
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)

        # Call the function under test: update the progress bar
        self.remote_client.progress_bar(transferred, to_be_transferred, suffix)

        # Flush the standard output to prepare for checking the output
        mock_stdout.flush()

        # Construct the expected output string
        expected_output = 'Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m0.0\033[0m', '% [', self.remote_client.translate_byte(transferred), ']', suffix)

        # Verify that the output contains the expected output string
        self.assertIn(expected_output, mock_stdout.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_bar_complete(self, mock_stdout):
        """
        Test the completion of the progress bar.

        This test case verifies the display of the progress bar when the transfer is complete.
        Parameters:
        - mock_stdout: A mock object used to capture standard output for verifying the output content.
        """

        # Set up parameters for file size and progress bar
        transferred = 1048576  # 1MB
        to_be_transferred = 1048576  # 1MB
        suffix = 'test_suffix'
        bar_len = 20

        # Calculate the filled length of the progress bar
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))

        # Construct the progress bar string
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)

        # Call the function under test
        self.remote_client.progress_bar(transferred, to_be_transferred, suffix)
        mock_stdout.flush()

        # Expected output content
        expected_output = 'Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m100.0\033[0m', '% [', self.remote_client.translate_byte(transferred), ']', suffix)

        # Verify that the output is as expected
        self.assertIn(expected_output, mock_stdout.getvalue())
        self.assertIn('\r\n', mock_stdout.getvalue())

    @patch('src.common.ssh_client.remote_client.paramiko')
    def test_upload(self, mock_paramiko):
        """
        Set up the SSH transport object and SFTP client object.
        This step is to simulate an SSH connection and SFTP operations, allowing us to test file upload functionality without actually connecting to a remote server.
        """

        # Initialize the SSH transport object and SFTP client object for simulation purposes.
        transport = MagicMock()
        sftp_client = MagicMock()
        mock_paramiko.SFTPClient.from_transport.return_value = sftp_client
        self.remote_client._ssh_fd.get_transport.return_value = transport

        # Perform the upload operation by specifying the remote and local paths.
        remote_path = '/remote/path/file'
        local_path = '/local/path/file'
        self.remote_client.upload(remote_path, local_path)

        # Verify that the SFTP put method was called with the correct parameters.
        sftp_client.put.assert_called_once_with(local_path, remote_path)

        # Verify that the SFTP client was closed correctly after the upload operation.
        sftp_client.close.assert_called_once()

    @patch('time.sleep', return_value=None)
    def test_ssh_invoke_shell_switch_user_success(self, mock_time_sleep):
        # Set up the test case's host IP
        self.remote_client.host_ip = 'fake_host'

        # Setup mock response
        expected_result = "Command executed successfully"

        # Mock the invoke_shell method to return the expected result in bytes
        self.remote_client._ssh_fd.invoke_shell = MagicMock(return_value=MagicMock(recv=MagicMock(return_value=expected_result.encode('utf-8'))))

        # Mock the close method to return None
        self.remote_client._ssh_fd.close = MagicMock(return_value=None)

        # Test the function
        result = self.remote_client.ssh_invoke_shell_switch_user('new_user', 'echo "Hello World"', 1)

        # Assertions
        self.assertEqual(result, expected_result)

        # Verify that the invoke_shell method was called once
        self.remote_client._ssh_fd.invoke_shell.assert_called_once()

        # Verify that the close method was called once
        self.remote_client._ssh_fd.close.assert_called_once()

    @patch('time.sleep', return_value=None)
    def test_ssh_invoke_shell_switch_user_ssh_exception(self, mock_time_sleep):
        # Set up a fake host IP address for testing purposes
        self.remote_client.host_ip = 'fake_host'

        # Configure the mock to raise an SSHException when invoke_shell is called
        self.remote_client._ssh_fd.invoke_shell = MagicMock(side_effect=SSHException)

        # Test the function and expect it to raise an OBDIAGShellCmdException
        with self.assertRaises(OBDIAGShellCmdException):
            self.remote_client.ssh_invoke_shell_switch_user('new_user', 'echo "Hello World"', 1)

        # Assert that invoke_shell was called exactly once
        self.remote_client._ssh_fd.invoke_shell.assert_called_once()

        # Assert that close was not called on the SSH connection during the exception
        self.remote_client._ssh_fd.close.assert_not_called()

    def test_get_name(self):
        # Call the get_name method on the remote client to retrieve the name
        name = self.remote_client.get_name()

        # Assert that the retrieved name matches the expected value "remote_192.168.1.1"
        self.assertEqual(name, "remote_192.168.1.1")


if __name__ == '__main__':
    unittest.main()
