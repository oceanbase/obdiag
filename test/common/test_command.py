import unittest
from unittest.mock import Mock, patch
import subprocess
from common.command import LocalClient  # 请替换为实际的模块路径

class TestLocalClient(unittest.TestCase):
    def setUp(self):
        self.stdio = Mock()
        self.local_client = LocalClient(stdio=self.stdio)

    @patch('subprocess.Popen')
    def test_run_success(self, mock_popen):
        # 模拟命令成功执行
        mock_process = Mock()
        mock_process.communicate.return_value = (b'success', None)
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)
        
        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
        
        # 验证结果
        self.assertEqual(result, b'success')

    @patch('subprocess.Popen')
    def test_run_failure(self, mock_popen):
        # 模拟命令执行失败
        mock_process = Mock()
        mock_process.communicate.return_value = (b'', b'error')
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)
        
        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
        
        # 验证错误处理
        self.stdio.error.assert_called_with("run cmd = [echo \"hello\"] on localhost, stderr=[b'error']")
        self.assertEqual(result, b'')

    @patch('subprocess.Popen')
    def test_run_exception(self, mock_popen):
        # 模拟命令执行时抛出异常
        mock_popen.side_effect = Exception('Test exception')

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)
        
        # 验证 verbose 调用和异常处理
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        self.stdio.error.assert_called_with("run cmd = [echo \"hello\"] on localhost")
        self.assertIsNone(result)
        

if __name__ == '__main__':
    unittest.main()
