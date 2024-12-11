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
@time: 2024/07/28
@file: test_docker_client.py
@desc:
"""

import unittest
from unittest.mock import patch, MagicMock
from docker import DockerClient as DockerClientSDK
from src.common.ssh_client.docker_client import DockerClient
from src.common.context import HandlerContext
from src.common.obdiag_exception import OBDIAGShellCmdException


class TestDockerClient(unittest.TestCase):

    @patch('common.ssh_client.docker_client.docker.from_env')
    def setUp(self, mock_docker_from_env):
        """
        Configures the mock Docker client and sets up test parameters in a testing environment.

        Parameters:
        - mock_docker_from_env: A Mock object to simulate creating a Docker client from an environment.

        Returns:
        No direct return value, but sets up various mock objects and contexts used during testing.

        Explanation:
        This function is primarily for setting up initialization and mock object configurations before tests run, ensuring controlled test execution.
        """

        # Use MagicMock to simulate a Docker client to avoid actual network operations during tests.
        mock_docker_from_env.return_value = MagicMock(spec_set=DockerClientSDK)

        # Initialize a HandlerContext object to simulate the runtime environment.
        self.context = HandlerContext()

        # Define a node dictionary containing a container name, which will be used during tests.
        self.node_with_container_name = {'container_name': 'test_container'}

        # Define an empty node dictionary for scenarios where no container name is specified.
        self.node_without_container_name = {}

        # Create a DockerClient object with the context and node configuration.
        self.docker_client = DockerClient(self.context, {})

        # Set the node attribute of the DockerClient object to simulate node information.
        self.docker_client.node = {"container_name": "test_container"}

        # Set the container name attribute of the DockerClient object for scenarios where a container name is specified.
        self.docker_client.container_name = "test_container"

        # Use MagicMock to simulate stdio to avoid actual input/output operations.
        self.docker_client.stdio = MagicMock()

        # Use MagicMock to simulate the Docker client object to avoid actual Docker API calls.
        self.docker_client.client = MagicMock()

    @patch('common.ssh_client.docker_client.docker.from_env')
    def test_init_with_valid_node(self, mock_docker_from_env):
        """
        Test the __init__ method with a valid node response.

        This test case ensures that the __init__ method initializes the object correctly when provided with a valid node response.
        It first mocks the creation of a Docker client from an environment, then verifies if the mocked object's method was called correctly,
        and checks if the properties of the initialized object match expectations.

        Parameters:
        - mock_docker_from_env: A mock object used to simulate the creation of a Docker client.
        """

        # Mock returning a DockerClientSDK type object
        mock_docker_from_env.return_value = MagicMock(spec_set=DockerClientSDK)

        # Call the function under test
        docker_client = DockerClient(self.context, self.node_with_container_name)

        # Verify that the method of the mock object was called once
        mock_docker_from_env.assert_called_once()

        # Verify that the container_name attribute of the docker_client object is set correctly
        self.assertEqual(docker_client.container_name, 'test_container')

        # Verify that the client attribute of the docker_client object is of type DockerClientSDK
        self.assertIsInstance(docker_client.client, DockerClientSDK)

    @patch('common.ssh_client.docker_client.docker.from_env')
    def test_init_without_container_name(self, mock_docker_from_env):
        """
        Test the initialization of DockerClient when no container name is provided.

        This test case aims to verify that when initializing the DockerClient without a container name,
        the client can correctly create a Docker client instance using the provided environment,
        and that the container_name attribute is correctly set to None.

        Parameters:
        - mock_docker_from_env: A mock object used to simulate the return value of docker.from_env().

        Returns:
        No return value; this function's purpose is to perform assertion checks.
        """

        # Set the mock object's return value to simulate a Docker client instance
        mock_docker_from_env.return_value = MagicMock(spec_set=DockerClientSDK)

        # Call the function under test to create a DockerClient instance
        docker_client = DockerClient(self.context, self.node_without_container_name)

        # Verify that docker.from_env() was called once correctly
        mock_docker_from_env.assert_called_once()

        # Verify that docker_client's container_name attribute is None
        self.assertIsNone(docker_client.container_name)

        # Verify that docker_client's client attribute is of type DockerClientSDK
        self.assertIsInstance(docker_client.client, DockerClientSDK)

    @patch('common.ssh_client.docker_client.docker.from_env')
    def test_init_with_invalid_context(self, mock_docker_from_env):
        """
        Test the __init__ method with an invalid context.

        This test case ensures that the __init__ method triggers an AttributeError as expected when provided with an invalid context.

        Parameters:
            - mock_docker_from_env: A mock object used to simulate the initialization process of the Docker client SDK.

        Returns:
            No return value; this method is designed to trigger an AttributeError.

        """

        # Set up the mock object to return a MagicMock object with the DockerClientSDK interface.
        mock_docker_from_env.return_value = MagicMock(spec_set=DockerClientSDK)

        # Expect an AttributeError to be raised when initializing DockerClient with invalid context (None).
        # Use assertRaises to verify that the exception is correctly raised.
        with self.assertRaises(AttributeError):
            DockerClient(None, None)

    def test_exec_cmd_success(self):
        """
        Tests the `exec_run` method to simulate successful command execution.

        This test aims to verify whether the `exec_cmd` method can execute commands correctly
        and retrieve the correct output from a simulated container.
        """

        # Create a mock container object for simulating Docker API calls
        mock_container = MagicMock()

        # Set up the mock to return the previously created mock container when containers.get is called
        self.docker_client.client.containers.get.return_value = mock_container

        # Create a mock execution result object to simulate the command execution output and exit code
        mock_exec_result = MagicMock()

        # Set the mock exit code to 0, indicating successful command execution
        mock_exec_result.exit_code = 0

        # Set the mock output as a byte string containing the command execution result
        mock_exec_result.output = b'successful command output'

        # Set up the mock container to return the previously created mock execution result when exec_run is called
        mock_container.exec_run.return_value = mock_exec_result

        # Call the method under test
        result = self.docker_client.exec_cmd("echo 'Hello World'")

        # Verify that the methods are called correctly
        # Assert that containers.get was called once with the correct container name
        self.docker_client.client.containers.get.assert_called_once_with("test_container")

        # Assert that exec_run was called once with the correct parameters
        # This checks the format of the command and related execution options
        mock_container.exec_run.assert_called_once_with(
            cmd=["bash", "-c", "echo 'Hello World'"],
            detach=False,
            stdout=True,
            stderr=True,
        )

        # Compare the method's return value with the expected output
        self.assertEqual(result, 'successful command output')

    def test_exec_cmd_failure(self):
        """
        Test the exec_run method to simulate a failed command execution.

        This function sets up a mock container and a mock execution result to simulate a failure scenario.
        It then calls the method under test and verifies that it behaves as expected.
        """

        # Create a mock container object
        mock_container = MagicMock()

        # Set the return value for getting a container from the Docker client
        self.docker_client.client.containers.get.return_value = mock_container

        # Create a mock execution result object
        mock_exec_result = MagicMock()

        # Set the exit code and output of the mock execution result
        mock_exec_result.exit_code = 1
        mock_exec_result.output = b'command failed output'

        # Set the return value for executing a command on the mock container
        mock_container.exec_run.return_value = mock_exec_result

        # Call the method under test and expect an exception to be raised
        with self.assertRaises(Exception):
            self.docker_client.exec_cmd("exit 1")

        # Verify that the container get method was called correctly
        self.docker_client.client.containers.get.assert_called_once_with("test_container")
        # Verify that the exec_run method was called with the correct parameters
        mock_container.exec_run.assert_called_once_with(
            cmd=["bash", "-c", "exit 1"],
            detach=False,
            stdout=True,
            stderr=True,
        )

        # Check that the expected exception is raised
        self.assertRaises(OBDIAGShellCmdException)

    def test_exec_cmd_exception(self):
        """
        Test if the containers.get method raises an exception.

        This function sets up a side effect for the containers.get method to simulate an error scenario,
        calls the method under test, and verifies if the expected exception is raised.
        """

        # Set up the containers.get method to raise an exception when called
        self.docker_client.client.containers.get.side_effect = Exception('Error', 'Something went wrong')

        # Call the method under test and expect a specific exception to be raised
        with self.assertRaises(Exception) as context:
            self.docker_client.exec_cmd("echo 'Hello World'")

        # Verify that the containers.get method was called exactly once with the correct argument
        self.docker_client.client.containers.get.assert_called_once_with("test_container")

        # Get the exception message and verify it contains the expected information
        exception_message = str(context.exception)
        self.assertIn("sshHelper ssh_exec_cmd docker Exception", exception_message)
        self.assertIn("Something went wrong", exception_message)

    @patch('builtins.open', new_callable=MagicMock)
    def test_download_success(self, mock_open):
        """
        Test the download method with a successful response.

        :param mock_open: A mock object to simulate file operations.
        """

        # Create a list with simulated file content
        fake_data = [b'this is a test file content']

        # Create a fake file status dictionary containing the file size
        fake_stat = {'size': len(fake_data[0])}

        # Set up the mock container get function return value
        self.docker_client.client.containers.get.return_value.get_archive.return_value = (fake_data, fake_stat)

        # Define remote and local file paths
        remote_path = '/path/in/container'
        local_path = '/path/on/host/test_file'

        # Call the function under test
        self.docker_client.download(remote_path, local_path)

        # Verify that the method was called correctly
        self.docker_client.client.containers.get.return_value.get_archive.assert_called_once_with(remote_path)

        # Verify that the local file was opened in binary write mode
        mock_open.assert_called_once_with(local_path, "wb")

        # Get the file handle from the mock_open return value
        handle = mock_open.return_value.__enter__.return_value

        # Verify that the file content was written correctly
        handle.write.assert_called_once_with(fake_data[0])

        # Verify that verbose logging was called
        self.docker_client.stdio.verbose.assert_called_once()

        # Verify that error logging was not called, as no errors are expected
        self.docker_client.stdio.error.assert_not_called()

    def test_download_exception(self):
        """
        Test the download method when it receives an exception response.

        Sets up a side effect to simulate an error when attempting to get a container,
        then calls the download method expecting an exception, and finally verifies
        that the exception message contains the expected text and that the error
        was logged.
        """

        # Set up a side effect for getting containers to raise an exception
        self.docker_client.client.containers.get.side_effect = Exception('Error', 'Message')

        # Define the remote and local paths for the file to be downloaded
        remote_path = '/path/in/container'
        local_path = '/path/on/host/test_file'

        # Call the function under test, expecting an exception
        with self.assertRaises(Exception) as context:
            self.docker_client.download(remote_path, local_path)

        # Verify that the exception message contains the expected text
        self.assertIn("sshHelper download docker Exception", str(context.exception))

        # Verify that the error was logged
        self.docker_client.stdio.error.assert_called_once()

    def test_upload_success(self):
        """Test the upload method and verify a successful response."""

        # Set up a mock container object to simulate Docker client operations
        mock_container = self.docker_client.client.containers.get.return_value

        # Configure the mock container's put_archive method to return None when called
        mock_container.put_archive.return_value = None

        # Call the function under test
        self.docker_client.upload("/remote/path", "/local/path")

        # Verify that the put_archive method was called once with the correct arguments
        mock_container.put_archive.assert_called_once_with("/remote/path", "/local/path")

        # Verify that the stdio verbose method was called once, ensuring proper logging during the upload process
        self.docker_client.stdio.verbose.assert_called_once()

    def test_upload_failure(self):
        """
        Tests the upload method when it receives a failure response.

        This test case simulates an error during the upload process.
        """

        # Set up the mock container object
        mock_container = self.docker_client.client.containers.get.return_value

        # Trigger an exception to simulate a failed upload
        mock_container.put_archive.side_effect = Exception('Error')

        # Call the function under test and expect an exception to be raised
        with self.assertRaises(Exception) as context:
            self.docker_client.upload("/remote/path", "/local/path")

        # Verify the exception message is correct
        self.assertIn("sshHelper upload docker Exception: Error", str(context.exception))

        # Verify the error message is output through the error channel
        self.docker_client.stdio.error.assert_called_once_with("sshHelper upload docker Exception: Error")

    def test_ssh_invoke_shell_switch_user_success(self):
        """
        Test the ssh_invoke_shell_switch_user method with a successful response.

        This test simulates a successful scenario of invoking an SSH shell and switching users within a Docker container.
        It ensures that when the user switch operation in the Docker container is successful, the method correctly calls
        `exec_create` and `exec_start`, and returns the expected response.
        """

        # Set up mock objects for the Docker client's exec_create and exec_start methods
        mock_exec_create = self.docker_client.client.exec_create
        mock_exec_start = self.docker_client.client.exec_start

        # Configure the return values for the mock objects
        mock_exec_create.return_value = {'Id': 'exec_id'}
        mock_exec_start.return_value = b'successful response'

        # Call the method under test
        response = self.docker_client.ssh_invoke_shell_switch_user('new_user', 'ls', 10)

        # Verify that exec_create was called correctly
        mock_exec_create.assert_called_once_with(container='test_container', command=['su', '- new_user'])

        # Verify that exec_start was called with the correct exec_id
        mock_exec_start.assert_called_once_with({'Id': 'exec_id'})

        # Verify that the response matches the expected value
        self.assertEqual(response, b'successful response')

    def test_ssh_invoke_shell_switch_user_exception(self):
        """
        Test the behavior of the ssh_invoke_shell_switch_user method when it encounters an exception.

        This test simulates an exception being thrown during the execution of the `exec_create` method,
        and verifies that the `ssh_invoke_shell_switch_user` method handles this exception correctly.

        Expected outcome: When `exec_create` throws an exception, the `ssh_invoke_shell_switch_user` method
        should catch the exception and include a specific error message in the caught exception.
        """

        # Set up the mock object to simulate the `exec_create` method throwing an exception
        mock_exec_create = self.docker_client.client.exec_create
        mock_exec_create.side_effect = Exception('Error')

        # Call the function under test and expect it to raise an exception
        with self.assertRaises(Exception) as context:
            self.docker_client.ssh_invoke_shell_switch_user('new_user', 'ls', 10)

        # Verify that the raised exception contains the expected error message
        self.assertIn("sshHelper ssh_invoke_shell_switch_user docker Exception: Error", str(context.exception))

    def test_get_name(self):
        """Test the get_name method to ensure it correctly returns the container name.

        This test case verifies that the custom naming convention for containers is implemented correctly.
        It checks the correctness by comparing the expected container name with the actual one obtained.
        """

        # Set a test container name
        self.container_name = "test_container"

        # Assign the test container name to the docker_client object
        self.docker_client.container_name = self.container_name

        # Construct the expected container name in the format "docker_{actual_container_name}"
        expected_name = "docker_{0}".format(self.container_name)

        # Assert that the actual container name matches the expected one
        self.assertEqual(self.docker_client.get_name(), expected_name)

    def test_get_ip(self):
        """Test the test_get_ip method."""

        # Set the expected IP address
        expected_ip = '192.168.1.100'

        # Mock the return value of the Docker client's containers.get method
        # This is to ensure the get_ip method returns the correct IP address
        self.docker_client.client.containers.get.return_value.attrs = {'NetworkSettings': {'Networks': {'bridge': {"IPAddress": expected_ip}}}}

        # Call the function under test
        ip = self.docker_client.get_ip()

        # Verify that the method is called correctly
        # Here we use an assertion to check if the returned IP matches the expected one
        self.assertEqual(ip, expected_ip)

        # Ensure that the containers.get method is called correctly with the right parameters
        self.docker_client.client.containers.get.assert_called_once_with(self.docker_client.node["container_name"])


if __name__ == '__main__':
    unittest.main()
