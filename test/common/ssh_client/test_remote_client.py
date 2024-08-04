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

from io import StringIO
import paramiko
import unittest
from unittest.mock import patch, MagicMock
from common.ssh_client.remote_client import RemoteClient
from context import HandlerContext
from common.obdiag_exception import OBDIAGShellCmdException, OBDIAGSSHConnException


class TestRemoteClient(unittest.TestCase):
    def setUp(self):
        """
        Sets up the test environment.

        This method is called before each test case to prepare the necessary context and parameters.
        It includes creating a `HandlerContext` instance, defining a node information dictionary,
        initializing a `RemoteClient` instance, and setting up mock objects for the `RemoteClient`
        to enable testing without actual remote operations.
        """

        # Create a HandlerContext instance to simulate the context object in the test environment.
        self.context = HandlerContext()

        # Define a node information dictionary including IP address, SSH login details, etc.,
        # for configuration of remote connections during the test process.
        self.node = {
            "ip": "127.0.0.1",
            "ssh_username": "root",
            "ssh_port": 22,
            "ssh_password": "your_password",
            "ssh_key_file": "",
        }

        # Initialize a RemoteClient instance based on the context and node information,
        # to simulate remote client operations.
        self.remote_client = RemoteClient(context=self.context, node=self.node)

        # Set up mock objects for the SSH file descriptor and SFTP client of the RemoteClient instance,
        # to simulate SSH and SFTP operations in tests without actual remote connections.
        self.remote_client._ssh_fd = MagicMock()
        self.remote_client._sftp_client = MagicMock()

        # Set up a mock object for the standardized input/output (stdio) of the RemoteClient instance,
        # to simulate interactions between the remote client and the remote host.
        self.remote_client.stdio = MagicMock()

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_password(self, mock_ssh_client):
        """Tests SSH connection using password"""
        self._disabled_rsa_algorithms = None
        remote_client = RemoteClient(self.context, self.node)
        mock_ssh_client.assert_called_once()
        mock_ssh_client().connect.assert_called_once_with(hostname=self.node['ip'], username=self.node['ssh_username'], password=self.node['ssh_password'], port=self.node['ssh_port'], disabled_algorithms=self._disabled_rsa_algorithms)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_key_file(self, mock_ssh_client):
        """
        Test the SSH connection using a password.

        This test method verifies that an SSH connection can be initialized correctly with a password.

        Parameters:
        - mock_ssh_client: A mock SSH client object used to test the SSH connection calls.

        Returns:
        None
        """

        # Initialize the list of disabled RSA algorithms as None, indicating that no RSA algorithms are disabled by default for the SSH connection.
        self._disabled_rsa_algorithms = None
        self.node['ssh_key_file'] = '/path/to/keyfile'

        # Create a remote client instance to simulate the SSH connection.
        remote_client = RemoteClient(self.context, self.node)

        # Verify that the connect method of mock_ssh_client was called exactly once.
        # This ensures that the attempt to establish an SSH connection in the test case is performed as expected.
        mock_ssh_client.assert_called_once()

        # Verify the detailed parameters of the SSH connection, ensuring that the connection uses the correct hostname, username, password, port, and disabled algorithm settings.
        mock_ssh_client().connect.assert_called_once_with(hostname=self.node['ip'], username=self.node['ssh_username'], key_filename=self.node['ssh_key_file'], port=self.node['ssh_port'], disabled_algorithms=self._disabled_rsa_algorithms)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_authentication_exception(self, mock_ssh_client):
        """
        Test the scenario when authentication fails.

        By simulating an SSH client connection that raises an AuthenticationException, this test verifies that the initialization of RemoteClient behaves as expected when authentication fails.

        Parameters:
        - mock_ssh_client: A mock object used to simulate the behavior of the SSH client.

        Exceptions:
        - Expectation is set for the paramiko.AuthenticationException to be raised when the SSH client's connection fails.
        """

        # Set up the mock_ssh_client's connect method to raise a paramiko.AuthenticationException to simulate a failed authentication scenario
        mock_ssh_client.return_value.connect.side_effect = paramiko.AuthenticationException

        # Assert that the initialization of RemoteClient raises the expected paramiko.AuthenticationException
        with self.assertRaises(paramiko.AuthenticationException):
            RemoteClient(self.context, self.node)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_connection_exception(self, mock_ssh_client):
        """
        Test whether an exception is thrown when the connection fails.

        This method simulates a scenario where the SSH connection attempt fails,
        ensuring that an appropriate exception is raised during the initialization
        of the RemoteClient class. This is crucial for verifying error handling mechanisms.

        Parameters:
        - mock_ssh_client: A mocked SSH client object used for testing. It throws an exception
        when the connection attempt fails.

        Expected Result:
        When the connection fails, an exception containing the message "Connection failed" is expected.
        """

        # Configure the mocked SSH client to throw an exception on connection attempts
        mock_ssh_client().connect.side_effect = Exception("Connection failed")

        # Expect an exception to be raised during the initialization of RemoteClient
        with self.assertRaises(Exception) as context:
            RemoteClient(self.context, self.node)

        # Verify that the thrown exception contains the message "Connection failed"
        self.assertIn("Connection failed", str(context.exception))

    def test_exec_cmd_success(self):
        """
        Test successful execution of a command.

        This test case simulates a successful execution of a command on a remote client.
        It sets up the return value of the `exec_command` method to mimic an SSH command execution,
        including a successful command output ('success') and an empty error output. Then it calls
        the `exec_cmd` method and verifies that its return value matches the expected outcome,
        ensuring that the command is correctly handled and returns the expected result when executed successfully.
        """

        # Simulate the return value of the exec_command method for a successful command execution.
        self.remote_client._ssh_fd.exec_command.return_value = (MagicMock(), MagicMock(read=MagicMock(return_value=b'success')), MagicMock(read=MagicMock(return_value=b'')))

        # Call the exec_cmd method and get the result.
        result = self.remote_client.exec_cmd('ls')

        # Assert that the result matches the expected outcome, i.e., the command execution success should return 'success'.
        self.assertEqual(result, 'success')

    def test_exec_cmd_failure(self):
        """
        Test the failure scenario when executing a command.
        This test case verifies that when an invalid command is executed, the returned result matches the expected error message.
        """

        # Mock the return values for executing a command via SSH to simulate a failure scenario.
        # Here, we simulate the three return values from executing a command: stdin, stdout, and stderr.
        # stdout returns an empty string, and stderr returns 'error', indicating a command execution error.
        self.remote_client._ssh_fd.exec_command.return_value = (MagicMock(), MagicMock(read=MagicMock(return_value=b'')), MagicMock(read=MagicMock(return_value=b'error')))

        # Execute an invalid command using the exec_cmd method and store the result in the variable 'result'.
        result = self.remote_client.exec_cmd('invalid_command')

        # Assert that the value of 'result' is 'error' to verify that error handling works as expected.
        self.assertEqual(result, 'error')

    def test_exec_cmd_ssh_exception(self):
        """
        Test handling of SSH exceptions during command execution.

        This test case aims to verify that when an exception occurs during the execution of a command over SSH,
        the correct custom exception, `OBDIAGShellCmdException`, is raised, and that the exception message contains
        the expected error message.

        Raises:
            OBDIAGShellCmdException: Thrown when the SSH command execution fails.
        """

        # Configure the mock object's exec_command method to raise a paramiko.SSHException
        self.remote_client._ssh_fd.exec_command.side_effect = paramiko.SSHException('SSH error')

        # Use assertRaises to check if calling exec_cmd raises the OBDIAGShellCmdException
        with self.assertRaises(OBDIAGShellCmdException) as context:
            self.remote_client.exec_cmd('ls')

        # Verify that the exception message contains the expected error message
        self.assertIn('Execute Shell command on server 127.0.0.1 failed', str(context.exception))

    @patch('paramiko.SFTPClient.from_transport')
    def test_download(self, mock_sftp_client):
        """
        Test the download functionality.

        :param mock_sftp_client: A mock SFTP client to test with.
        """

        # Set up the return value for the mocked transport
        mock_transport = MagicMock()
        self.remote_client._ssh_fd.get_transport.return_value = mock_transport
        mock_sftp_client.return_value = self.remote_client._sftp_client

        # Execute the function being tested
        remote_path = '/remote/file.txt'
        local_path = '/local/file.txt'
        self.remote_client.download(remote_path, local_path)

        # Verify the correct calls were made
        self.remote_client._ssh_fd.get_transport.assert_called_once()
        mock_sftp_client.assert_called_once_with(mock_transport)
        self.remote_client.stdio.verbose.assert_called_once_with('Download 127.0.0.1:/remote/file.txt')
        self.remote_client._sftp_client.get.assert_called_once_with(remote_path, local_path, callback=self.remote_client.progress_bar)
        self.remote_client._sftp_client.close.assert_called_once()

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

    @patch('common.ssh_client.remote_client.paramiko.SFTPClient.from_transport')
    def test_upload(self, mock_sftp_client):
        """Tests the file upload functionality.

        This test uses a mocked SFTP client to ensure the `upload` method calls the necessary functions correctly.

        Args:
            mock_sftp_client: A MagicMock object used to simulate the behavior of an SFTP client.
        """

        # Set up the return values for the mock objects
        mock_transport = MagicMock()
        self.remote_client._ssh_fd.get_transport.return_value = mock_transport
        mock_sftp_client.return_value = MagicMock()

        # Call the method under test
        remote_path = '/remote/path/file.txt'
        local_path = '/local/path/file.txt'
        self.remote_client.upload(remote_path, local_path)

        # Assert that methods are called correctly
        self.remote_client._ssh_fd.get_transport.assert_called_once()
        mock_sftp_client.assert_called_once_with(mock_transport)
        mock_sftp_client.return_value.put.assert_called_once_with(local_path, remote_path)
        self.assertIsNotNone(self.remote_client._sftp_client)
        mock_sftp_client.return_value.close.assert_called_once()

    def test_ssh_invoke_shell_switch_user_success(self):
        """
        Test the ssh_invoke_shell_switch_user command successfully and returns standard output.

        This function simulates normal operation scenarios and verifies if the command is executed correctly.
        """

        # Simulate the return values under normal conditions
        self.remote_client._ssh_fd.invoke_shell.return_value.send.return_value = None
        self.remote_client._ssh_fd.invoke_shell.return_value.recv.return_value = b'successful output'

        # Define the test parameters: new user, command, and timeout
        new_user = 'new_user'
        cmd = 'ls'
        time_out = 1

        # Call the function under test
        result = self.remote_client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)

        # Verify that the method is called correctly
        self.assertEqual(result, 'successful output')
        self.remote_client._ssh_fd.invoke_shell.assert_called_once()
        self.remote_client._ssh_fd.invoke_shell.return_value.send.assert_any_call('su {0}\n'.format(new_user))
        self.remote_client._ssh_fd.invoke_shell.return_value.send.assert_any_call('{}\n'.format(cmd))
        self.remote_client._ssh_fd.close.assert_called_once()

    def test_ssh_invoke_shell_switch_user_exception(self):
        """
        Tests the ssh_invoke_shell_switch_user command under exceptional conditions.

        This function sets up an exception to be raised when invoking the shell and verifies
        that the correct exception is thrown and caught, along with the expected error messages.
        """

        # Set up the mock object to raise an SSHException when invoke_shell is called
        self.remote_client._ssh_fd.invoke_shell.side_effect = paramiko.SSHException("SSH error")

        # Define the new user, command, and timeout for testing
        new_user = 'new_user'
        cmd = 'ls'
        time_out = 1

        # Expect an OBDIAGShellCmdException to be raised when calling ssh_invoke_shell_switch_user
        with self.assertRaises(OBDIAGShellCmdException) as context:
            self.remote_client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)

        # Verify the exception message contains the expected error information
        self.assertIn("Execute Shell command on server 127.0.0.1 failed", str(context.exception))
        self.assertIn("command=[ls]", str(context.exception))
        self.assertIn("SSH error", str(context.exception))

    def test_get_name(self):
        """Test the get name functionality.

        This test case verifies the correctness of the remote client's get name method.
        It calls the `get_name` method to retrieve the name,
        and uses the `assertEqual` assertion method to check if the retrieved name matches the expected value.
        """

        # Call the get_name method on the remote client to retrieve the name
        name = self.remote_client.get_name()

        # Assert that the retrieved name matches the expected value "remote_127.0.0.1"
        self.assertEqual(name, "remote_127.0.0.1")


if __name__ == '__main__':
    unittest.main()
