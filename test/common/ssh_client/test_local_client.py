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
@time: 2024/07/22
@file: test_local_client.py
@desc:
"""

import unittest
import subprocess
from unittest.mock import patch, MagicMock
from src.common.ssh_client.local_client import LocalClient
from src.common.context import HandlerContext


class TestLocalClient(unittest.TestCase):
    def test_init_with_context_and_node(self):
        """
        Test the initialization process when passing `context` and `node`.
        """

        # Create an instance of HandlerContext for testing how the `context` parameter is handled during initialization.
        context = HandlerContext()

        # Create an empty dictionary to test how the `node` parameter is handled during initialization.
        node = {}

        # Initialize a LocalClient instance with the provided `context` and `node`.
        client = LocalClient(context=context, node=node)

        # Assert that the `context` attribute of `client` is equal to the passed-in `context`.
        self.assertEqual(client.context, context)

        # Assert that the `node` attribute of `client` is equal to the passed-in `node`.
        self.assertEqual(client.node, node)

    def test_init_with_only_node(self):
        """
        Test the initialization behavior when only providing a node.

        This test case aims to verify that when passing `None` as the context and a node dictionary to `LocalClient`,
        they are correctly assigned to their respective attributes.
        """

        # Initialize an empty dictionary as the node
        node = {}

        # Initialize `LocalClient` with `None` as the context and the previously defined node
        client = LocalClient(context=None, node=node)

        # Verify that the `context` attribute of `client` is `None`
        self.assertIsNone(client.context)

        # Verify that the `node` attribute of `client` matches the passed-in `node`
        self.assertEqual(client.node, node)

    def test_init_with_only_context(self):
        """
        Test initialization when only the context is passed.

        This test case checks if the initialization raises the expected exception when only the context is provided and other necessary parameters are missing.
        It verifies that object creation is correctly prevented when the initialization conditions are not fully met.

        Parameters:
        - context (HandlerContext): An instance of HandlerContext representing the event handling context.

        Returns:
        - No return value, but raises an AttributeError to test the robustness of the initialization process.
        """
        context = HandlerContext()
        self.assertRaises(AttributeError, LocalClient, context, None)

    def test_init_with_no_args(self):
        """Tests initialization without passing any parameters"""
        # Attempt to instantiate LocalClient without arguments to verify if it raises an AttributeError
        self.assertRaises(AttributeError, LocalClient, None, None)

    def setUp(self):
        """
        Set up the environment before executing test cases.

        This method initializes necessary components for test cases by creating an instance of `HandlerContext`,
        an empty node dictionary, and mocking the standard input/output and client of the `LocalClient`.

        :param self: The instance of the class that this method is part of.
        """

        # Create an instance of HandlerContext to simulate the testing environment's context
        context = HandlerContext()

        # Create an empty dictionary as the node object, which will be used to simulate data storage in tests
        node = {}

        # Initialize a LocalClient instance using the context and node, simulating local client operations
        self.local_client = LocalClient(context=context, node=node)

        # Mock the standard input/output of LocalClient to avoid actual I/O operations during tests
        self.local_client.stdio = MagicMock()

        # Mock the client attribute of LocalClient to avoid actual client connections during tests
        self.local_client.client = MagicMock()

    @patch('subprocess.Popen')
    def test_exec_cmd_success(self, mock_popen):
        """
        Test the exec_cmd command successfully and return standard output.

        :param mock_popen: A mocked version of subprocess.Popen for testing purposes.
        """

        # Create a mock process object
        mock_process = MagicMock()

        # Set up the communicate method's return value to simulate stdout and stderr
        mock_process.communicate.return_value = (b"stdout output", b"")

        # Set the return value of the mocked popen to be the mock process
        mock_popen.return_value = mock_process

        # Call the function under test
        result = self.local_client.exec_cmd("echo 'Hello World'")

        # Verify the results of the function call
        # Assert that the returned result matches the expected output
        self.assertEqual(result, "stdout output")

        # Verify that the verbose method was called with the correct logging information
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [echo 'Hello World'] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_failure(self, mock_popen):
        """
        Tests the exec_cmd command when it fails and returns the stderr output.

        This test simulates a failure scenario for the exec_cmd command by mocking the popen object.
        It checks whether the exec_cmd command handles failures correctly and returns the expected error message.

        Parameters:
        - mock_popen: A parameter used to mock the popen object for testing failure scenarios.

        Returns:
        No return value; this method primarily performs assertion checks.
        """

        # Create a mocked popen object to simulate a failed command execution
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"stderr output")
        mock_popen.return_value = mock_process

        # Call the function under test
        result = self.local_client.exec_cmd("exit 1")

        # Verify that the function execution result matches the expected outcome, i.e., the correct error message is returned
        self.assertEqual(result, "stderr output")

        # Verify that the log information was recorded correctly during command execution
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [exit 1] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_exception(self, mock_popen):
        """
        Test the exec_cmd command in exceptional scenarios.

        This test sets up a scenario where the `popen` method raises an exception,
        and checks if `exec_cmd` handles it correctly.

        Parameters:
            - mock_popen: A mock object to simulate the behavior of popen, which will raise an exception.

        Raises:
            Exception: If the `exec_cmd` does not handle the exception properly.
        """

        # Configure the mock_popen to raise an exception when called
        mock_popen.side_effect = Exception("Popen error")

        # Execute the function being tested, expecting it to raise an exception
        with self.assertRaises(Exception) as context:
            self.local_client.exec_cmd("exit 1")

        # Verify the exception message contains the expected text
        self.assertIn("Execute Shell command failed", str(context.exception))

        # Ensure the error log is recorded as expected
        self.local_client.stdio.error.assert_called_with("run cmd = [exit 1] on localhost, Exception = [Popen error]")

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_download_success(self, mock_copy):
        """
        Test the successful scenario of the download command.

        This test case simulates a successful file download and verifies the following:
        - The download method was called.
        - The download method was called correctly once.
        - In the case of a successful download, the error message method was not called.

        Parameters:
        - mock_copy: A mocked copy method used to replace the actual file copying operation in the test.

        Returns:
        None
        """

        # Define remote and local file paths
        remote_path = "/path/to/remote/file"
        local_path = "/path/to/local/file"

        # Call the download method under test
        self.local_client.download(remote_path, local_path)

        # Verify that mock_copy was called correctly once
        mock_copy.assert_called_once_with(remote_path, local_path)

        # Verify that the error message method was not called
        self.local_client.stdio.error.assert_not_called()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_download_failure(self, mock_copy):
        """
        Tests the failure scenario of the download command.

        :param mock_copy: A mock object to simulate the copy operation and its failure.
        """

        # Set up the mock object to raise an exception to simulate a failure during the download process
        mock_copy.side_effect = Exception('copy error')

        # Define the remote and local file paths
        remote_path = "/path/to/remote/file"
        local_path = "/path/to/local/file"

        # Execute the download operation, expecting it to fail and raise an exception
        with self.assertRaises(Exception) as context:
            self.local_client.download(remote_path, local_path)

        # Verify that the exception message contains the expected text
        self.assertTrue("download file from localhost" in str(context.exception))

        # Verify that the error message was recorded correctly
        self.local_client.stdio.error.assert_called_once()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_upload_success(self, mock_copy):
        """
        Tests the successful scenario of the upload command.

        This test case simulates a successful file upload and verifies if the upload process calls methods correctly.

        Parameters:
        - mock_copy: A mock object used to simulate the file copy operation.
        """

        # Define remote and local file paths
        remote_path = '/tmp/remote_file.txt'
        local_path = '/tmp/local_file.txt'

        # Call the function under test for uploading
        self.local_client.upload(remote_path, local_path)

        # Verify if mock_copy was called once with the correct parameters
        mock_copy.assert_called_once_with(local_path, remote_path)

        # Verify if error messages were not called, ensuring no errors occurred during the upload
        self.local_client.stdio.error.assert_not_called()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_upload_failure(self, mock_copy):
        """
        Test the upload command failure.

        :param mock_copy: A mocked copy operation that simulates an upload.
        """

        # Simulate an exception to test the failure scenario of the upload
        mock_copy.side_effect = Exception('copy error')

        # Define remote and local file paths
        remote_path = '/tmp/remote_file.txt'
        local_path = '/tmp/local_file.txt'

        # Call the function under test and expect it to raise an exception
        with self.assertRaises(Exception) as context:
            self.local_client.upload(remote_path, local_path)

        # Verify the exception message matches the expected one
        self.assertIn('upload file to localhost', str(context.exception))

        # Verify that the error message was output through stdio.error
        self.local_client.stdio.error.assert_called_once()

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_success(self, mock_popen):
        """
        Test the ssh_invoke_shell_switch_user command executing successfully and returning standard output.

        Parameters:
            mock_popen: A mocked popen object to simulate the subprocess behavior.

        Returns:
            None
        """

        # Create a mock process object
        mock_process = MagicMock()

        # Set up the communicate method's return value to simulate command execution output
        mock_process.communicate.return_value = (b"successful output", b"")

        # Set up the mock_popen method to return the mock process object
        mock_popen.return_value = mock_process

        # Call the function under test
        result = self.local_client.ssh_invoke_shell_switch_user("new_user", 'echo "Hello World"', 10)

        # Verify if the function was called correctly and the return value matches the expected output
        self.assertEqual(result, "successful output")

        # Verify if stdio.verbose was called once appropriately
        self.local_client.stdio.verbose.assert_called_once()

        # Verify if mock_popen was called with the expected parameters
        mock_popen.assert_called_once_with("su - new_user -c 'echo \"Hello World\"'", stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_failure(self, mock_popen):
        """
        Tests the ssh_invoke_shell_switch_user command failure and returns standard output.

        :param mock_popen: A mocked popen object for testing purposes.
        :return: None
        """

        # Create a mock process object
        mock_process = MagicMock()

        # Set up the communicate method of the mock process to return error output
        mock_process.communicate.return_value = (b"", b"error output")

        # Set up the mock_popen to return the mock process object
        mock_popen.return_value = mock_process

        # Call the function under test
        result = self.local_client.ssh_invoke_shell_switch_user("new_user", 'echo "Hello World"', 10)

        # Verify that the method is called correctly
        self.assertEqual(result, "error output")

        # Verify stdio.verbose was called once
        self.local_client.stdio.verbose.assert_called_once()

        # Verify mock_popen was called with the correct parameters
        mock_popen.assert_called_once_with("su - new_user -c 'echo \"Hello World\"'", stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_exception(self, mock_popen):
        """
        Test the ssh_invoke_shell_switch_user command under exceptional circumstances.

        :param mock_popen: A mock object for the popen method to simulate failure scenarios.
        """

        # Set up the mock_popen to raise an exception, simulating a Popen operation failure.
        mock_popen.side_effect = Exception("Popen error")

        # Call the function under test and expect it to raise an exception.
        with self.assertRaises(Exception) as context:
            self.local_client.ssh_invoke_shell_switch_user("new_user", "echo 'Hello World'", 10)

        # Verify that the exception message contains the expected error message.
        self.assertTrue("the client type is not support ssh invoke shell switch user" in str(context.exception))

        # Ensure that the error logging method was called once.
        self.local_client.stdio.error.assert_called_once()

    def test_get_name(self):
        """Test getting the name of the SSH client."""

        # Retrieve the name by calling the get_name method on self.local_client
        name = self.local_client.get_name()
        # Assert that the method was called correctly and the returned name matches the expected "local"
        self.assertEqual(name, "local")

    def test_get_ip(self):
        """Test the IP retrieval functionality of the SSH client.

        This test case verifies the correctness of the IP address retrieved through the SSH client.
        It sets an expected IP address and then calls the `get_ip` method to obtain the actual IP address,
        comparing it with the expected one. Additionally, it ensures that the `get_ip` method is called
        exactly once.

        Parameters:
            None

        Returns:
            None
        """

        # Set the expected IP address
        expected_ip = '127.0.0.1'

        # Mock the client.get_ip method to return the expected IP address
        self.local_client.client.get_ip.return_value = expected_ip

        # Call the tested function to get the IP
        ip = self.local_client.get_ip()

        # Assert that the retrieved IP matches the expected IP
        self.assertEqual(ip, expected_ip)

        # Assert that the client.get_ip method was called exactly once
        self.local_client.client.get_ip.assert_called_once()


if __name__ == '__main__':
    unittest.main()
