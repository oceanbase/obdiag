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
@time: 2025/12/08
@file: openai_client.py
@desc: OpenAI API client for AI Assistant with MCP support
"""

import json
import os
import re
from typing import Dict, List, Optional, Any, Generator, Tuple
from openai import OpenAI

from src.handler.ai.mcp_client import MCPClientManager
from src.handler.ai.mcp_server import MCPServer
from src.handler.ai.obi_client import OBIClient
from src.common.ob_connector import OBConnector


class ObdiagAIClient:
    """
    OpenAI API client for obdiag AI Assistant.
    Supports tool calling via MCP protocol.
    """

    SYSTEM_PROMPT = """You are obdiag AI Assistant, an intelligent diagnostic assistant for OceanBase database.

Your capabilities include:
1. Executing obdiag diagnostic commands (gather logs, analyze, check health, RCA)
2. Analyzing diagnostic results and providing insights
3. Recommending diagnostic steps based on user descriptions
4. Explaining OceanBase concepts and troubleshooting procedures
5. Searching OceanBase knowledge base through OBI (OceanBase Intelligence) when available

When users describe problems or ask for diagnostics:
1. First understand what they need
2. If OBI is available and the question relates to OceanBase knowledge, concepts, or troubleshooting, consider searching the knowledge base first
3. Use the appropriate diagnostic tools
4. Analyze the results
5. Provide clear explanations and recommendations

Important guidelines:
- Always confirm before executing potentially long-running operations
- Provide clear, actionable insights from diagnostic results
- Respond in the same language as the user's question
- Format output clearly with proper structure
- When OBI knowledge search is available, use it to enhance your responses with official OceanBase documentation and knowledge
- **CRITICAL**: When OBI search results include reference documents (references), you MUST list all reference document links in your response. Format them clearly at the end of your answer, including both the document title and URL if available. This helps users access the original documentation for more details.

When a tool execution fails, explain the error and suggest alternatives."""

    def __init__(
        self,
        context,
        api_key: str,
        base_url: Optional[str] = None,
        model: str = "gpt-4",
        config_path: Optional[str] = None,
        use_mcp: bool = True,
        mcp_servers: Optional[Dict[str, Dict]] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        system_prompt: Optional[str] = None,
        obi_client: Optional[OBIClient] = None,
    ):
        """
        Initialize the OpenAI client

        Args:
            api_key: OpenAI API key
            base_url: Optional custom API base URL
            model: Model name to use
            config_path: Path to obdiag config file (unused, kept for compatibility)
            use_mcp: Whether to use MCP client (unused, kept for compatibility)
            mcp_servers: MCP servers configuration dict
                {
                    "server_name": {
                        "command": "...",  # for stdio
                        "args": [...],
                        "url": "...",      # for http
                        ...
                    }
                }
            temperature: Model temperature
            max_tokens: Maximum tokens in response
            system_prompt: Custom system prompt (uses default if not provided)
            obi_client: Optional OBI client for knowledge search
        """
        self.context = context
        self.stdio = context.stdio
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        # Use custom system prompt if provided, otherwise use default
        self.system_prompt = system_prompt if system_prompt else self.SYSTEM_PROMPT
        # Store OBI client
        self.obi_client = obi_client

        # Initialize OpenAI client
        client_kwargs = {"api_key": api_key}
        if base_url:
            client_kwargs["base_url"] = base_url
        self.client = OpenAI(**client_kwargs)

        # Initialize MCP - use built-in server by default, or external servers if configured
        self.mcp_client: Optional[MCPClientManager] = None
        self.builtin_mcp_server: Optional[MCPServer] = None
        self.config_path = config_path or os.path.expanduser("~/.obdiag/config.yml")
        self._db_connector: Optional[OBConnector] = None  # Store database connection

        if mcp_servers:
            # Use external MCP servers
            servers_config = self._convert_servers_config(mcp_servers)
            if servers_config:
                self.mcp_client = MCPClientManager(servers_config=servers_config, context=self.context)
                try:
                    self.mcp_client.start()
                    if not self.mcp_client.is_connected():
                        self.stdio.warn("Warning: No external MCP server connected, falling back to built-in server")
                        self.mcp_client = None
                except Exception as e:
                    self.stdio.warn("Warning: External MCP client failed to start ({0}), falling back to built-in server".format(e))
                    self.mcp_client = None

        # If no external MCP client, use built-in MCP server
        if self.mcp_client is None:
            try:
                self.builtin_mcp_server = MCPServer(config_path=self.config_path, stdio=self.stdio, context=self.context)
                self.stdio.verbose("Using built-in MCP server")
            except Exception as e:
                self.stdio.warn("Warning: Failed to initialize built-in MCP server: {0}".format(e))

    def _convert_servers_config(self, mcp_servers: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Convert MCP servers config from JSON format to internal format

        Input format (compatible with Cursor's mcp.json):
            {
                "server_name": {
                    "command": "...",
                    "args": [...],
                    "env": {...},      # optional
                    "url": "...",      # for http
                    "headers": {...}   # optional, for http
                }
            }

        Output format:
            {
                "server_name": {
                    "transport": "stdio" | "http",
                    "command": "...",
                    "args": [...],
                    ...
                }
            }
        """
        result = {}
        for name, config in mcp_servers.items():
            server_config = {}

            if "url" in config:
                # HTTP transport
                server_config["transport"] = "http"
                server_config["url"] = config["url"]
                if "headers" in config:
                    server_config["headers"] = config["headers"]
            elif "command" in config:
                # Stdio transport
                server_config["transport"] = "stdio"
                server_config["command"] = config["command"]
                server_config["args"] = config.get("args", [])
                if "env" in config:
                    server_config["env"] = config["env"]
            else:
                # Skip invalid config
                continue

            result[name] = server_config

        return result

    def _get_tools(self) -> List[Dict]:
        """Get available tools for function calling"""
        tools = []
        
        # Try external MCP client first
        if self.mcp_client and self.mcp_client.is_connected():
            try:
                tools.extend(self.mcp_client.get_tools_for_openai())
            except Exception:
                pass

        # Fall back to built-in MCP server
        if self.builtin_mcp_server:
            try:
                mcp_tools = self.builtin_mcp_server.tools
                tools.extend([
                    {
                        "type": "function",
                        "function": {
                            "name": tool.get("name", ""),
                            "description": tool.get("description", ""),
                            "parameters": tool.get("inputSchema", {"type": "object", "properties": {}}),
                        },
                    }
                    for tool in mcp_tools
                ])
            except Exception:
                pass

        # Add direct database and file tools (use context for connection info)
        tools.extend(self._get_direct_tools())
        
        return tools

    def _get_direct_tools(self) -> List[Dict]:
        """Get direct tools that use context for connection info"""
        return [
            {
                "type": "function",
                "function": {
                    "name": "db_query",
                    "description": "Execute SQL query on the configured OceanBase database. Uses connection information from context (config file). No need to connect first - connection is automatic.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "SQL query to execute"
                            }
                        },
                        "required": ["sql"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "file_write",
                    "description": "Create or write to a local file. Automatically creates directories if they don't exist.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "Path to the file (can be relative or absolute)"
                            },
                            "content": {
                                "type": "string",
                                "description": "Content to write to the file"
                            },
                            "mode": {
                                "type": "string",
                                "description": "File mode: 'w' for write (overwrite), 'a' for append",
                                "default": "w",
                                "enum": ["w", "a"]
                            },
                            "encoding": {
                                "type": "string",
                                "description": "File encoding, default utf-8",
                                "default": "utf-8"
                            }
                        },
                        "required": ["file_path", "content"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "file_read",
                    "description": "Read content from a local file",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "Path to the file (can be relative or absolute)"
                            },
                            "encoding": {
                                "type": "string",
                                "description": "File encoding, default utf-8",
                                "default": "utf-8"
                            }
                        },
                        "required": ["file_path"]
                    }
                }
            }
        ]

    def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """
        Execute a tool and return the result

        Args:
            tool_name: Name of the tool to execute
            arguments: Tool arguments

        Returns:
            Tool execution result as string
        """
        # Handle direct tools that use context
        if tool_name == "db_query":
            return self._execute_db_query(arguments)
        elif tool_name == "file_write":
            return self._execute_file_write(arguments)
        elif tool_name == "file_read":
            return self._execute_file_read(arguments)
        
        # Try external MCP client first
        if self.mcp_client and self.mcp_client.is_connected():
            try:
                result = self.mcp_client.call_tool(tool_name, arguments)
                return self._extract_tool_result(result)
            except Exception as e:
                return "MCP tool execution error: {0}".format(str(e))

        # Fall back to built-in MCP server
        if self.builtin_mcp_server:
            try:
                result = self.builtin_mcp_server._call_tool(tool_name, arguments)
                return self._extract_tool_result_from_builtin(result)
            except Exception as e:
                return "Built-in tool execution error: {0}".format(str(e))

        return "No MCP server available. Please check configuration."

    def _extract_tool_result(self, result: Dict) -> str:
        """Extract text content from MCP client result"""
        if result.get("success"):
            content = result.get("content", [])
            if content:
                texts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        texts.append(item.get("text", ""))
                    elif isinstance(item, str):
                        texts.append(item)
                return "\n".join(texts) if texts else str(content)
            return "Tool executed successfully but returned no content."
        else:
            return "Tool execution failed: {0}".format(result.get('error', 'Unknown error'))

    def _extract_tool_result_from_builtin(self, result: Dict) -> str:
        """Extract text content from built-in MCP server result"""
        content = result.get("content", [])
        is_error = result.get("isError", False)

        if content:
            texts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    texts.append(item.get("text", ""))
                elif isinstance(item, str):
                    texts.append(item)
            result_text = "\n".join(texts) if texts else str(content)
            if is_error:
                return "Tool execution failed: {0}".format(result_text)
            return result_text

        return "Tool executed but returned no content."

    def _validate_sql(self, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL query for safety
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not sql or not sql.strip():
            return False, "Error: SQL query cannot be empty"
        
        # Remove comments and normalize
        sql_normalized = sql.strip()
        
        # Check for multiple statements (count semicolons that are not in strings)
        # Simple check: count semicolons outside of quotes
        semicolon_count = 0
        in_single_quote = False
        in_double_quote = False
        in_backtick = False
        
        for char in sql_normalized:
            if char == "'" and not in_double_quote and not in_backtick:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote and not in_backtick:
                in_double_quote = not in_double_quote
            elif char == '`' and not in_single_quote and not in_double_quote:
                in_backtick = not in_backtick
            elif char == ';' and not in_single_quote and not in_double_quote and not in_backtick:
                semicolon_count += 1
        
        # Allow at most one semicolon at the end
        if semicolon_count > 1:
            return False, "Error: Only one SQL statement is allowed per query. Multiple statements detected."
        
        # Remove trailing semicolon for validation
        sql_for_validation = sql_normalized.rstrip(';').strip()
        
        # Convert to uppercase for keyword checking (but preserve original for execution)
        sql_upper = sql_for_validation.upper().strip()
        
        # Allowed read-only SQL keywords (must start with one of these)
        allowed_keywords = [
            'SELECT',
            'SHOW',
            'DESCRIBE',
            'DESC',
            'EXPLAIN',
            'WITH',  # CTE queries (must be followed by SELECT)
        ]
        
        # Check if SQL starts with an allowed keyword
        sql_starts_with_allowed = False
        starts_with_keyword = None
        for keyword in allowed_keywords:
            if sql_upper.startswith(keyword):
                sql_starts_with_allowed = True
                starts_with_keyword = keyword
                break
        
        if not sql_starts_with_allowed:
            return False, f"Error: Only read-only SQL statements are allowed (SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, WITH). Your query starts with: {sql_for_validation[:50]}"
        
        # Special handling for WITH statements - must be followed by SELECT
        if starts_with_keyword == 'WITH':
            # Check if WITH is followed by SELECT (after CTE definitions)
            # Pattern: WITH ... AS (...) SELECT ...
            # Simple check: ensure SELECT appears after WITH
            if 'SELECT' not in sql_upper:
                return False, "Error: WITH statements must be followed by a SELECT statement"
            # Find the position of first SELECT after WITH
            with_pos = sql_upper.find('WITH')
            select_pos = sql_upper.find('SELECT', with_pos)
            if select_pos == -1:
                return False, "Error: WITH statements must contain a SELECT statement"
        
        # Forbidden keywords (even if starts with allowed keyword, check for dangerous operations)
        forbidden_keywords_pattern = re.compile(
            r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE|COMMIT|ROLLBACK|LOCK|UNLOCK)\b',
            re.IGNORECASE
        )
        
        if forbidden_keywords_pattern.search(sql_for_validation):
            return False, "Error: Dangerous SQL operations are not allowed (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, etc.). Only read-only queries are permitted."
        
        # Additional check: prevent UNION with dangerous operations
        # This is a basic check - more sophisticated parsing would be needed for complete safety
        if 'UNION' in sql_upper:
            # Check if UNION is followed by SELECT (which is safe)
            union_parts = re.split(r'\bUNION\s+(?:ALL\s+)?', sql_upper, flags=re.IGNORECASE)
            for part in union_parts[1:]:  # Skip first part (already checked)
                part_stripped = part.strip()
                if not any(part_stripped.startswith(kw) for kw in allowed_keywords):
                    return False, "Error: UNION queries must only contain SELECT statements"
        
        return True, ""

    def _execute_db_query(self, arguments: Dict[str, Any]) -> str:
        """Execute SQL query using context connection info"""
        try:
            sql = arguments.get("sql")
            if not sql:
                return "Error: SQL query is required"

            # Validate SQL for safety
            is_valid, error_msg = self._validate_sql(sql)
            if not is_valid:
                self.stdio.verbose(f"SQL validation failed: {error_msg}")
                return error_msg

            # Get database connection info from context
            if not self.context or not hasattr(self.context, 'cluster_config'):
                return "Error: Database connection information not available in context. Please ensure cluster is configured."

            cluster_config = self.context.cluster_config
            if not cluster_config:
                return "Error: Cluster configuration not found. Please configure the cluster first."

            db_host = cluster_config.get("db_host")
            db_port = cluster_config.get("db_port")
            tenant_sys = cluster_config.get("tenant_sys", {})
            username = tenant_sys.get("user", "root@sys")
            password = tenant_sys.get("password")

            if not all([db_host, db_port, username, password]):
                return f"Error: Incomplete database configuration. Missing: host={db_host}, port={db_port}, user={username}, password={'***' if password else 'MISSING'}"

            self.stdio.verbose(f"Executing SQL query on {db_host}:{db_port} as {username}: {sql[:100]}...")

            # Create or reuse database connection
            if not self._db_connector:
                self._db_connector = OBConnector(
                    context=self.context,
                    ip=db_host,
                    port=db_port,
                    username=username,
                    password=password,
                    timeout=100,
                )

            # Execute query using dictionary cursor
            cursor = self._db_connector.execute_sql_return_cursor_dictionary(sql)
            results = cursor.fetchall()
            cursor.close()

            # Format results
            if not results:
                return "Query executed successfully. No rows returned."
            else:
                result_text = f"Query executed successfully. Returned {len(results)} row(s):\n\n"
                result_text += json.dumps(results, indent=2, ensure_ascii=False, default=str)
                return result_text

        except Exception as e:
            error_msg = f"SQL query execution failed: {str(e)}"
            self.stdio.verbose(error_msg)
            return error_msg

    def _execute_file_write(self, arguments: Dict[str, Any]) -> str:
        """Create or write to a local file"""
        try:
            file_path = arguments.get("file_path")
            content = arguments.get("content")
            mode = arguments.get("mode", "w")
            encoding = arguments.get("encoding", "utf-8")

            if not file_path or content is None:
                return "Error: Missing required parameters: file_path, content"

            # Convert to absolute path
            abs_path = os.path.abspath(file_path)

            # Create directory if it doesn't exist
            dir_path = os.path.dirname(abs_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                self.stdio.verbose(f"Created directory: {dir_path}")

            # Write file
            with open(abs_path, mode, encoding=encoding) as f:
                f.write(content)

            file_size = os.path.getsize(abs_path)
            success_msg = f"File created successfully: {abs_path}\nFile size: {file_size} bytes"
            self.stdio.verbose(success_msg)
            return success_msg

        except PermissionError as e:
            error_msg = f"Permission denied: {str(e)}"
            self.stdio.verbose(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"File creation failed: {str(e)}"
            self.stdio.verbose(error_msg)
            return error_msg

    def _execute_file_read(self, arguments: Dict[str, Any]) -> str:
        """Read content from a local file"""
        try:
            file_path = arguments.get("file_path")
            encoding = arguments.get("encoding", "utf-8")

            if not file_path:
                return "Error: Missing required parameter: file_path"

            abs_path = os.path.abspath(file_path)

            if not os.path.exists(abs_path):
                return f"Error: File not found: {abs_path}"

            with open(abs_path, "r", encoding=encoding) as f:
                content = f.read()

            result_text = f"File read successfully: {abs_path}\nFile size: {len(content)} characters\n\nContent:\n{content}"
            self.stdio.verbose(f"File read successfully: {abs_path} ({len(content)} characters)")
            return result_text

        except PermissionError as e:
            error_msg = f"Permission denied: {str(e)}"
            self.stdio.verbose(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"File read failed: {str(e)}"
            self.stdio.verbose(error_msg)
            return error_msg

    def chat(self, user_message: str, conversation_history: Optional[List[Dict]] = None) -> str:
        """
        Send a chat message and get a response

        Args:
            user_message: User's message
            conversation_history: Optional conversation history

        Returns:
            AI response as string
        """
        # If OBI is available, try to enhance the message with knowledge search
        obi_context = ""
        if self.obi_client and self.obi_client.is_configured():
            try:
                self.stdio.verbose("Searching OBI knowledge base...")
                # Search OBI knowledge base (with timeout handled by requests library)
                search_result = self.obi_client.search_knowledge(user_message)
                
                if search_result.get("success"):
                    answer = search_result.get("answer", "")
                    references = search_result.get("references", [])
                    if answer:
                        obi_context = f"\n\n[OBI Knowledge Base Search Results]\n{answer}"
                        if references:
                            # Format references with title and URL if available
                            ref_list = []
                            for ref in references[:10]:  # Show up to 10 references
                                title = ref.get('title') or ref.get('name') or 'Unknown Document'
                                url = ref.get('url') or ref.get('link') or ref.get('source_url') or ''
                                if url:
                                    ref_list.append(f"- {title}: {url}")
                                else:
                                    ref_list.append(f"- {title}")
                            if ref_list:
                                obi_context += f"\n\n[Reference Documents - MUST be listed in your response]\n" + "\n".join(ref_list)
                                obi_context += "\n\nIMPORTANT: You must include all these reference document links in your final response to help users access the original documentation."
                        self.stdio.verbose("OBI knowledge search completed successfully: {}".format(search_result))
                    else:
                        self.stdio.verbose("OBI search returned empty answer")
                else:
                    error_msg = search_result.get("error", "Unknown error")
                    self.stdio.verbose(f"OBI search failed: {error_msg}")
            except Exception as e:
                # Ensure we continue even if OBI search fails
                self.stdio.verbose(f"OBI knowledge search exception: {e}")
                # Continue without OBI context

        # Build messages
        messages = [{"role": "system", "content": self.system_prompt}]

        if conversation_history:
            messages.extend(conversation_history)

        # Enhance user message with OBI context if available
        enhanced_message = user_message
        if obi_context:
            enhanced_message = f"{user_message}\n\n{obi_context}"

        messages.append({"role": "user", "content": enhanced_message})

        # Get available tools
        tools = self._get_tools()

        # Make API call
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools if tools else None,
                tool_choice="auto" if tools else None,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )

            # Process response
            assistant_message = response.choices[0].message

            # Check if tool calls are needed
            if assistant_message.tool_calls:
                self.stdio.verbose(f"Tool calls detected: {len(assistant_message.tool_calls)}")
                # Execute tools and collect results
                tool_results = []
                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    self.stdio.verbose(f"Executing tool: {tool_name}")
                    try:
                        arguments = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        arguments = {}
                        self.stdio.verbose(f"Failed to parse tool arguments, using empty dict")

                    try:
                        result = self._execute_tool(tool_name, arguments)
                        tool_results.append(
                            {
                                "tool_call_id": tool_call.id,
                                "role": "tool",
                                "name": tool_name,
                                "content": result,
                            }
                        )
                        self.stdio.verbose(f"Tool {tool_name} executed successfully")
                    except Exception as tool_error:
                        error_msg = f"Tool {tool_name} execution failed: {str(tool_error)}"
                        self.stdio.verbose(error_msg)
                        tool_results.append(
                            {
                                "tool_call_id": tool_call.id,
                                "role": "tool",
                                "name": tool_name,
                                "content": error_msg,
                            }
                        )

                # Add assistant message and tool results to messages
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_message.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                            }
                            for tc in assistant_message.tool_calls
                        ],
                    }
                )
                messages.extend(tool_results)

                # Get final response after tool execution
                try:
                    self.stdio.verbose("Getting final response after tool execution...")
                    final_response = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        temperature=self.temperature,
                        max_tokens=self.max_tokens,
                    )
                    final_content = final_response.choices[0].message.content
                    if not final_content:
                        self.stdio.verbose("Final response is empty, using assistant message content")
                        return assistant_message.content or "No response generated."
                    self.stdio.verbose(f"Final response received ({len(final_content)} characters)")
                    return final_content
                except Exception as e:
                    self.stdio.verbose(f"Error getting final response after tool execution: {e}")
                    # Fallback to assistant message content
                    return assistant_message.content or f"Tool execution completed but failed to get final response: {str(e)}"

            content = assistant_message.content
            if not content:
                self.stdio.verbose("Assistant message content is empty")
                return "No response generated."
            return content

        except Exception as e:
            error_msg = f"API call failed: {str(e)}"
            self.stdio.verbose(error_msg)
            raise RuntimeError(error_msg)

    def chat_stream(self, user_message: str, conversation_history: Optional[List[Dict]] = None) -> Generator[str, None, None]:
        """
        Send a chat message and get a streaming response

        Args:
            user_message: User's message
            conversation_history: Optional conversation history

        Yields:
            Response chunks
        """
        # Build messages
        messages = [{"role": "system", "content": self.system_prompt}]

        if conversation_history:
            messages.extend(conversation_history)

        messages.append({"role": "user", "content": user_message})

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                stream=True,
            )

            for chunk in response:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content

        except Exception as e:
            yield f"Error: {str(e)}"

    def close(self):
        """Clean up resources"""
        # Close database connection if exists
        if self._db_connector and self._db_connector.conn:
            try:
                self._db_connector.conn.close()
                self.stdio.verbose("Database connection closed")
            except Exception:
                pass
            finally:
                self._db_connector = None
        
        if self.mcp_client:
            self.mcp_client.stop()
            self.mcp_client = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False
