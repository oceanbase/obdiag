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
import pytest


def test_models_import_without_pydantic_ai():
    """models.py must import cleanly — no pydantic_ai dependency."""
    from src.handler.agent.models import AgentDependencies, AgentConfig

    assert AgentDependencies is not None
    assert AgentConfig is not None


def test_agent_config_from_dict():
    from src.handler.agent.models import AgentConfig

    cfg = AgentConfig.from_dict({"ui": {"tool_approval": False}})
    assert cfg.tool_approval is False


def test_create_obdiag_tools_returns_callables():
    """create_obdiag_tools() must return 17 plain callable functions."""
    from src.handler.agent.toolsets.obdiag import create_obdiag_tools

    config_path_ref = {"v": "/dev/null"}
    tools = create_obdiag_tools(lambda: config_path_ref["v"], stdio=None)
    assert len(tools) == 17
    expected_names = {
        "gather_log",
        "gather_obproxy_log",
        "gather_oms_log",
        "gather_sysstat",
        "gather_perf",
        "gather_ash",
        "gather_awr",
        "gather_plan_monitor",
        "analyze_log",
        "check_cluster",
        "check_list",
        "rca_run",
        "rca_list",
        "tool_io_performance",
        "tool_sql_syntax",
        "list_obdiag_clusters",
        "show_current_cluster",
    }
    names = {fn.__name__ for fn in tools}
    assert names == expected_names
    # Must not have pydantic-ai RunContext in any signature
    import inspect

    for fn in tools:
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            assert "RunContext" not in str(param.annotation), f"{fn.__name__} param {param.name} still uses RunContext"


def test_create_db_tools_returns_callables():
    from src.handler.agent.toolsets.database import create_db_tools
    from src.handler.agent.models import AgentDependencies

    deps = AgentDependencies(stdio=None, config_path="")
    tools = create_db_tools(lambda: deps)
    assert len(tools) == 1
    assert tools[0].__name__ == "db_query"
    import inspect

    sig = inspect.signature(tools[0])
    assert "RunContext" not in str(sig)


def test_create_config_gen_tools_returns_callables():
    from src.handler.agent.toolsets.config_gen import create_config_gen_tools

    tools = create_config_gen_tools(stdio=None)
    assert len(tools) >= 1
    import inspect

    for fn in tools:
        sig = inspect.signature(fn)
        assert "RunContext" not in str(sig)


def test_create_knowledge_tools_returns_callables():
    from src.handler.agent.toolsets.knowledge_base import create_knowledge_tools

    tools = create_knowledge_tools(token_getter=lambda: "")
    assert len(tools) == 1
    assert tools[0].__name__ == "query_oceanbase_knowledge_base"
    import inspect

    sig = inspect.signature(tools[0])
    assert "RunContext" not in str(sig)


def test_toolsets_init_exports():
    from src.handler.agent.toolsets import (
        create_obdiag_tools,
        create_db_tools,
        create_config_gen_tools,
        create_knowledge_tools,
    )

    assert callable(create_obdiag_tools)
    assert callable(create_db_tools)
    assert callable(create_config_gen_tools)
    assert callable(create_knowledge_tools)


def test_handler_imports_cleanly():
    """handler.py must import without errors and expose AiAgentHandler."""
    from src.handler.agent.handler import AiAgentHandler

    assert AiAgentHandler is not None


def test_handler_init_no_crash():
    """AiAgentHandler.__init__ must not crash with a minimal mock context."""
    from src.handler.agent.handler import AiAgentHandler

    class FakeStdio:
        def verbose(self, *a):
            pass

        def warn(self, *a):
            pass

        def print(self, *a, **kw):
            pass

    class FakeOptions:
        pass

    class FakeContext:
        stdio = FakeStdio()
        options = FakeOptions()

    handler = AiAgentHandler(FakeContext())
    assert handler.stdio is not None  # context is stored
    assert handler.options is not None
