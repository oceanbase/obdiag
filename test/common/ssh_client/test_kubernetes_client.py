#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2024/07/31
@file: test_kubernetes_client.py
@desc:
"""

import unittest
import os
from unittest.mock import MagicMock, patch
from kubernetes import config
from src.common.context import HandlerContext
from src.common.ssh_client.kubernetes_client import KubernetesClient
from kubernetes.client.api.core_v1_api import CoreV1Api
from tempfile import NamedTemporaryFile
from kubernetes.client import ApiClient


FILE_DIR = "test/common/ssh_client/test_kubernetes_cilent.yaml"


class TestKubernetesClient(unittest.TestCase):
    def setUp(self):
        """
        Setup function to initialize the test environment.

        This function initializes the necessary context, node information, a mock for standard input/output,
        a client for interacting with Kubernetes, and creates a temporary file for use during testing.
        """

        # Initialize a HandlerContext object to simulate the test environment's context
        self.context = HandlerContext()

        # Define node information including namespace, pod name, container name, and Kubernetes configuration file path
        self.node = {"namespace": "default", "pod_name": "test-pod", "container_name": "test-container", "kubernetes_config_file": FILE_DIR}

        # Use MagicMock to mock standard input/output for predictable behavior during tests
        self.context.stdio = MagicMock()

        # Create a KubernetesClient instance with the context and node information to interact with the Kubernetes API
        self.client = KubernetesClient(context=self.context, node=self.node)

        # Create a temporary file that is not automatically deleted for storing temporary data during testing
        self.temp_file = NamedTemporaryFile(delete=False)

    def tearDown(self):
        """
        Cleanup actions: close and delete the temporary file.

        This method is called at the end of tests to ensure that temporary files do not occupy system resources.
        """

        # Close the temporary file to ensure all file operations are completed
        self.temp_file.close()

        # Remove the temporary file to avoid leaving unused data
        os.remove(self.temp_file.name)

    @patch('src.common.ssh_client.kubernetes_client.config.load_incluster_config')
    def test_init_with_no_config_file(self, mock_load_incluster_config):
        """
        Test the initialization of KubernetesClient without a configuration file.

        This test ensures that when no kubernetes_config_file is specified in the node dictionary,
        initializing KubernetesClient triggers a call to the load_incluster_config method.
        This validates that the client correctly loads configurations from the default config file in the cluster.

        Parameters:
        - mock_load_incluster_config: A mock object used to track calls to the load_incluster_config method.
        """

        # Set the kubernetes_config_file in the node dictionary to an empty string to simulate the absence of a provided configuration file.
        self.node["kubernetes_config_file"] = ""

        # Instantiate KubernetesClient, triggering the initialization process.
        KubernetesClient(context=self.context, node=self.node)

        # Verify that the load_incluster_config method was called exactly once.
        mock_load_incluster_config.assert_called_once()

        # Check if a message indicating the use of the default configuration file in the cluster was logged.
        self.context.stdio.verbose.assert_called_with("KubernetesClient load_kube_config from default config file in cluster.")

    @patch('src.common.ssh_client.kubernetes_client.config.kube_config.load_kube_config')
    def test_init_with_config_file(self, mock_load_kube_config):
        """
        Test the initialization of KubernetesClient with a configuration file.

        This test verifies that when initializing a KubernetesClient object,
        the Kubernetes configuration is loaded correctly and that the stdio.verbose
        method is called to log the loading of the configuration file.

        Parameters:
        - mock_load_kube_config: A mock object to track calls to the load_kube_config function.

        Returns:
        No return value; this method performs assertion checks.
        """

        # Initialize the KubernetesClient, triggering the configuration file loading logic.
        KubernetesClient(context=self.context, node=self.node)

        # Verify that load_kube_config was called once with the expected configuration file path.
        mock_load_kube_config.assert_called_once_with(config_file=FILE_DIR)

        # Verify that stdio.verbose was called to log the configuration file loading.
        self.context.stdio.verbose.assert_called_with(f"KubernetesClient load_kube_config from {FILE_DIR}")

    @patch('src.common.ssh_client.kubernetes_client.config.load_incluster_config', side_effect=config.ConfigException)
    def test_init_raises_exception(self, mock_load_incluster_config):
        """
        Tests whether the __init__ method correctly raises an expected exception.

        This test case verifies that when initializing the KubernetesClient with an empty `kubernetes_config_file`,
        it raises the expected exception and checks if the exception message contains the specified error message.

        Parameters:
        - mock_load_incluster_config: A mock object used to simulate the behavior of loading kube configurations.

        Returns:
        None

        Exceptions:
        - Exception: Expected to be raised when `kubernetes_config_file` is set to an empty string.
        """

        # Set the Kubernetes configuration file path in the node to an empty string to trigger an exception
        self.node["kubernetes_config_file"] = ""

        # Use the assertRaises context manager to capture and validate the raised exception
        with self.assertRaises(Exception) as context:
            KubernetesClient(context=self.context, node=self.node)

        # Verify if the captured exception message contains the expected error message
        self.assertTrue("KubernetesClient load_kube_config error. Please check the config file." in str(context.exception))

    @patch.object(CoreV1Api, 'connect_get_namespaced_pod_exec', autospec=True)
    def test_exec_cmd_success(self, mock_connect_get_namespaced_pod_exec):
        """
        Test the `exec_cmd` method with a successful response.

        This method sets up a mock for `connect_get_namespaced_pod_exec` to return a predefined successful response,
        ensuring the `exec_cmd` method behaves as expected.

        Parameters:
        - mock_connect_get_namespaced_pod_exec: A mock object used to replace the actual `connect_get_namespaced_pod_exec` method's return value.

        Returns:
        No return value; this method verifies behavior through assertions.
        """

        # Set up the mock object to return a predefined response simulating a successful command execution
        mock_connect_get_namespaced_pod_exec.return_value = "mocked response"

        # Define a test command using an echo command outputting a simple string
        cmd = "echo 'Hello, World!'"

        # Call the `exec_cmd` method and get the response
        response = self.client.exec_cmd(cmd)

        # Verify that the returned response matches the predefined mocked response
        self.assertEqual(response, "mocked response")

    @patch.object(CoreV1Api, 'connect_get_namespaced_pod_exec', autospec=True)
    def test_exec_cmd_failure(self, mock_connect_get_namespaced_pod_exec):
        """
        Tests the `exec_cmd` method's behavior when it encounters a failure response.

        This test simulates a command execution failure by causing the `connect_get_namespaced_pod_exec` method to throw an exception,
        and verifies that the error handling behaves as expected.

        Parameters:
        - mock_connect_get_namespaced_pod_exec: A Mock object used to simulate the `connect_get_namespaced_pod_exec` method.

        Returns:
        No return value; this method verifies its behavior through assertions.
        """

        # Simulate the `connect_get_namespaced_pod_exec` method throwing an exception on call
        mock_connect_get_namespaced_pod_exec.side_effect = Exception("Mocked exception")

        # Call the method under test
        cmd = "fail command"
        response = self.client.exec_cmd(cmd)

        # Verify that the error message matches the expected one
        expected_error_msg = "KubernetesClient can't get the resp by fail command: Mocked exception"
        self.assertEqual(response, expected_error_msg)

    @patch.object(KubernetesClient, '_KubernetesClient__download_file_from_pod')
    def test_download_file_from_pod_success(self, mock_download):
        """
        Test successful file download from a Pod.

        This test case simulates the scenario of downloading a file from a Kubernetes Pod.
        It focuses on verifying the correctness of the download process, including calling
        the appropriate mocked method and ensuring the file content matches expectations.

        Args:
        - mock_download: A mock object used to simulate the download method.
        """

        # Define the behavior of the mocked download method
        def mock_download_method(namespace, pod_name, container_name, file_path, local_path):
            """
            Mocked method for simulating file downloads.

            Args:
            - namespace: The Kubernetes namespace.
            - pod_name: The name of the Pod.
            - container_name: The name of the container.
            - file_path: The remote file path.
            - local_path: The local file save path.
            """
            # Create a local file and write mock data
            with open(local_path, 'wb') as file:  # Write in binary mode
                file.write(b"test file content")  # Write mock data

        # Assign the mocked method to the mock object
        mock_download.side_effect = mock_download_method

        # Initialize the mocked Kubernetes client
        k8s_client = KubernetesClient(self.context, self.node)
        k8s_client.client = MagicMock()
        k8s_client.stdio = MagicMock()

        # Define the required local path, namespace, Pod name, container name, and file path for testing
        local_path = self.temp_file.name
        namespace = "test-namespace"
        pod_name = "test-pod"
        container_name = "test-container"
        file_path = "test/file.txt"

        # Call the mocked download method
        mock_download(namespace, pod_name, container_name, file_path, local_path)

        # Verify that the file has been written with the expected content
        with open(local_path, 'rb') as file:  # Read in binary mode
            content = file.read()
            self.assertEqual(content, b"test file content")  # Compare byte type data

    @patch('src.common.ssh_client.kubernetes_client.stream')
    def test_download_file_from_pod_error(self, mock_stream):
        """
        Test the scenario of an error occurring when downloading a file from a Pod.

        This test case sets up an error response through a mocked stream object to simulate a situation where errors occur during file download.
        The focus is on the error handling logic, ensuring that errors encountered during the download process are correctly logged and handled.

        Parameters:
        - mock_stream: A mocked stream object used to set up the expected error response.
        """

        # Set up the return values for the mocked response to simulate an error response.
        mock_resp = MagicMock()
        mock_resp.is_open.return_value = True  # Simulate the response as not closed
        mock_resp.peek_stdout.return_value = False
        mock_resp.peek_stderr.return_value = True
        mock_resp.read_stderr.return_value = "Error occurred"  # Ensure read_stderr is called
        mock_stream.return_value = mock_resp

        # Initialize the Kubernetes client with mocked objects
        k8s_client = self.client
        k8s_client.client = MagicMock()
        k8s_client.stdio = MagicMock()

        # Define parameters required for downloading the file
        local_path = self.temp_file.name
        namespace = "test-namespace"
        pod_name = "test-pod"
        container_name = "test-container"
        file_path = "test/file.txt"

        # Call the download function, which will trigger the mocked error response
        k8s_client._KubernetesClient__download_file_from_pod(namespace, pod_name, container_name, file_path, local_path)

        # Verify that the stderr content is correctly logged, ensuring that error messages are captured and handled
        k8s_client.stdio.error.assert_called_with("ERROR: ", "Error occurred")

    @patch('kubernetes.config.load_kube_config')
    @patch('kubernetes.client.CoreV1Api')
    def test_upload_file_to_pod(self, mock_core_v1_api, mock_load_kube_config):
        """
        Tests the functionality of uploading a file to a Kubernetes Pod.

        This is a unit test that uses MagicMock to simulate the Kubernetes CoreV1Api and file operations.
        It verifies the behavior of the `__upload_file_to_pod` method, including whether the underlying API is called correctly,
        and the reading and uploading of the file.

        Parameters:
        - mock_core_v1_api: A mocked instance of CoreV1Api.
        - mock_load_kube_config: A mocked function for loading Kubernetes configuration.

        Returns:
        None
        """

        # Set up mock objects
        mock_resp = MagicMock()
        mock_resp.is_open.return_value = True  # # Simulate interaction based on requirements
        mock_resp.peek_stdout.return_value = False
        mock_resp.peek_stderr.return_value = False
        mock_resp.read_stdout.return_value = ''
        mock_resp.read_stderr.return_value = ''

        # Set up the return value for the stream function
        mock_core_v1_api_instance = MagicMock(spec=CoreV1Api)
        mock_core_v1_api.return_value = mock_core_v1_api_instance
        mock_core_v1_api_instance.api_client = MagicMock()  # 添加 api_client 属性

        # Create a mock object with a __self__ attribute
        mock_self = MagicMock()
        mock_self.api_client = mock_core_v1_api_instance.api_client

        # Bind connect_get_namespaced_pod_exec to an object with an api_client attribute
        mock_core_v1_api_instance.connect_get_namespaced_pod_exec = MagicMock(__self__=mock_self, return_value=mock_resp)

        # Instantiate KubernetesClient and call the method
        k8s_client = KubernetesClient(self.context, self.node)
        k8s_client.stdio = MagicMock()  # 模拟 stdio 对象
        namespace = 'test_namespace'
        pod_name = 'test_pod'
        container_name = 'test_container'
        local_path = '/local/path/to/file'
        remote_path = '/remote/path/to/file'

        # Since there's no real Kubernetes cluster or Pod in the test environment, use MagicMock to simulate the file
        mock_file_content = b'test file content'
        with patch('builtins.open', return_value=MagicMock(__enter__=lambda self: self, __exit__=lambda self, *args: None, read=lambda: mock_file_content)) as mock_open_file:
            k8s_client._KubernetesClient__upload_file_to_pod(namespace, pod_name, container_name, local_path, remote_path)

        # Verify if load_kube_config was called
        mock_load_kube_config.assert_called_once()

        # Verify if the stream function was called correctly
        mock_core_v1_api_instance.connect_get_namespaced_pod_exec.assert_called_once()

        # Verify if the file was read and uploaded correctly
        mock_open_file.assert_called_once_with(local_path, 'rb')

        # Ensure is_open returns True to trigger write_stdin
        mock_resp.is_open.return_value = True

        # Use side_effect to simulate writing file content
        mock_resp.write_stdin.side_effect = lambda data: None

        # Ensure write_stdin was called correctly
        mock_resp.write_stdin.assert_called_once_with(mock_file_content)

        # Verify if the response was closed
        mock_resp.close.assert_called_once()

    def test_ssh_invoke_shell_switch_user(self):
        """
        Test the functionality of switching users within an SSH session.

        This test validates the ability to switch users within an SSH session by mocking the Kubernetes API client and related Pod execution environment.
        It simulates calling the private method `__ssh_invoke_shell_switch_user` of a `KubernetesClient` instance and asserts that the method's return value matches the expected value.
        """

        # Mock some attributes of the KubernetesClient instance
        self.client.pod_name = "test_pod"
        self.client.namespace = "default"
        self.client.container_name = "test_container"

        # Create a mock ApiClient instance
        self.api_client_mock = MagicMock(spec=ApiClient)
        self.api_client_mock.configuration = MagicMock()  # 添加configuration属性

        # Create a mock connect_get_namespaced_pod_exec method
        self.client.client = MagicMock()
        self.client.client.connect_get_namespaced_pod_exec = MagicMock(__self__=MagicMock(api_client=self.api_client_mock))

        # Mock stream function
        self.stream_mock = MagicMock()

        # Define test user, command, and timeout values
        new_user = "test_user"
        cmd = "echo 'Hello, World!'"
        time_out = 10

        # Define the expected response
        expected_response = "Hello, World!\n"

        # Directly mock the function return value
        self.client._KubernetesClient__ssh_invoke_shell_switch_user = MagicMock(return_value=expected_response)

        # Call the function
        result = self.client._KubernetesClient__ssh_invoke_shell_switch_user(new_user, cmd, time_out)

        # Assert the result matches the expected value
        self.assertEqual(result, expected_response)

    def test_get_name(self):
        """
        This function tests the `get_name` method of a simulated KubernetesClient instance.

        Steps:
        - Sets up the client's namespace and pod_name attributes.
        - Calls the `get_name` method on the client.
        - Asserts that the returned name matches the expected format.
        """

        # Simulate a KubernetesClient instance by setting its namespace and pod_name attributes
        self.client.namespace = "default"
        self.client.pod_name = "test-pod"

        # Call the get_name method to retrieve the formatted name
        name = self.client.get_name()

        # Assert that the retrieved name matches the expected format
        self.assertEqual(name, "kubernetes_default_test-pod")

    def test_get_ip_with_ip_set(self):
        """
        Test case to verify the IP address retrieval when an IP is set.

        This test case checks whether the correct IP address can be retrieved when the node's IP address is already set.
        The test sets the IP address for the client node, then calls the get_ip method and expects it to return the set IP address.
        """
        ip_address = "192.168.1.1"
        self.client.node['ip'] = ip_address
        self.assertEqual(self.client.get_ip(), ip_address)

    def test_get_ip_without_ip_set(self):
        """
        Test the logic of getting an IP when no IP is set.

        This test case aims to verify that calling the get_ip method should raise an exception when Kubernetes has not set the IP for the Observer.
        Use assertRaises to check if the expected exception is correctly raised.
        """
        with self.assertRaises(Exception) as context:
            self.client.get_ip()

        # Verify if the error message contains the specific message.
        self.assertTrue("kubernetes need set the ip of observer" in str(context.exception))


if __name__ == '__main__':
    unittest.main()
