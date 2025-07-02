from fastmcp import FastMCP
import subprocess


def run_obdiag_command(command: str) -> str:
    """
    运行 obdiag 命令并返回结果
    :param command: 完整的 obdiag 命令
    :return: 指令执行的输出结果
    """
    try:
        # 使用 subprocess 执行命令
        result = subprocess.run(command, shell=True, text=True, capture_output=True)
        # 返回标准输出或错误输出
        return result.stdout if result.returncode == 0 else f"Error: {result.stderr}"
    except Exception as e:
        return f"Exception occurred: {str(e)}"


def register_tools(mcp: FastMCP):
    @mcp.tool()
    async def obdiag_check_run() -> str:
        """
        巡检集群，并返回巡检报告
        :return: 指令执行的输出结果
        """
        return run_obdiag_command("obdiag check run")
