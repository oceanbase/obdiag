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
@time: 2025/01/27
@file: obi_client.py
@desc: OBI (OceanBase Intelligence) API client for knowledge search
"""

import time
import requests
from typing import Dict, Optional, Any


class OBIClient:
    """
    OBI (OceanBase Intelligence) API client for knowledge search.
    Provides access to OceanBase knowledge base through OBI API.
    """

    def __init__(
        self,
        base_url: str,
        app_code: str,
        cookie: str,
        enabled: bool = True,
        stdio=None,
    ):
        """
        Initialize OBI client

        Args:
            base_url: OBI API base URL
            app_code: OBI application code
            cookie: Cookie string containing authorization
            enabled: Whether OBI is enabled
            stdio: Standard IO handler for logging
        """
        self.base_url = base_url
        self.app_code = app_code
        self.cookie = cookie
        self.enabled = enabled
        self.stdio = stdio
        self._access_token = None
        self._token_expiry = None

    def _extract_authorization_from_cookie(self) -> str:
        """Extract authorization value from cookie string"""
        if not self.cookie:
            return ""

        # Parse cookie string to find authorization
        cookie_parts = self.cookie.split(';')
        for part in cookie_parts:
            part = part.strip()
            if part.startswith('authorization='):
                return part[len('authorization=') :]

        # Return empty string if authorization not found
        return ""

    def _get_access_token(self) -> Optional[str]:
        """Get or refresh access_token"""
        # If token exists and not expired (5 minutes buffer)
        if self._access_token and self._token_expiry:
            if time.time() < self._token_expiry - 300:  # Refresh 5 minutes early
                return self._access_token

        # Extract authorization from cookie
        authorization = self._extract_authorization_from_cookie()
        if not authorization:
            if self.stdio:
                self.stdio.verbose("Failed to extract authorization from cookie")
            return None

        # Re-acquire token
        try:
            auth_url = f"{self.base_url}/v1/authn/authenticate"
            params = {'app_code': self.app_code, 'authn_type': 'custom'}
            headers = {'Authorization': authorization}

            response = requests.post(auth_url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                auth_data = response.json()
                self._access_token = auth_data.get('access_token')

                # Set token expiry time (assume 30 minutes validity)
                self._token_expiry = time.time() + 1800  # 30 minutes

                if self.stdio:
                    self.stdio.verbose("Successfully acquired new access_token")
                return self._access_token
            else:
                if self.stdio:
                    self.stdio.warn(f"Failed to acquire access_token: {response.status_code}")
                return None

        except Exception as e:
            if self.stdio:
                self.stdio.warn(f"Exception acquiring access_token: {str(e)}")
            return None

    def is_enabled(self) -> bool:
        """Check if OBI is enabled"""
        return self.enabled

    def is_configured(self) -> bool:
        """Check if OBI is properly configured"""
        if not self.enabled:
            return False
        if not self.app_code or not self.cookie:
            return False
        authorization = self._extract_authorization_from_cookie()
        return bool(authorization)

    def test_connection(self) -> Dict[str, Any]:
        """Test OBI connection"""
        if not self.enabled:
            return {'success': False, 'error': 'OBI not enabled in configuration'}

        if not self.app_code or not self.cookie:
            return {'success': False, 'error': 'Missing required configuration parameters'}

        # Check if authorization can be extracted from cookie
        authorization = self._extract_authorization_from_cookie()
        if not authorization:
            return {'success': False, 'error': 'Failed to extract authorization from cookie'}

        try:
            # Test authentication endpoint
            auth_url = f"{self.base_url}/v1/authn/authenticate"
            params = {'app_code': self.app_code, 'authn_type': 'custom'}
            headers = {'Authorization': authorization}

            if self.stdio:
                self.stdio.verbose(f"Testing authentication endpoint: {auth_url}")
            response = requests.post(auth_url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                auth_data = response.json()
                access_token = auth_data.get('access_token')

                if access_token:
                    return {'success': True, 'message': 'Authentication successful', 'access_token': access_token[:20] + '...'}
                else:
                    return {'success': False, 'error': 'Invalid authentication response format', 'details': auth_data}
            else:
                return {'success': False, 'error': f'Authentication failed (HTTP {response.status_code})', 'details': response.text}

        except requests.exceptions.Timeout:
            return {'success': False, 'error': 'Connection timeout', 'details': 'Please check network connection and base_url configuration'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'error': 'Connection failed', 'details': 'Please check network connection and base_url configuration'}
        except Exception as e:
            return {'success': False, 'error': str(e), 'details': 'Unknown error'}

    def search_knowledge(self, query: str, enable_deepthink: int = 0) -> Dict[str, Any]:
        """
        Search OceanBase knowledge base

        Args:
            query: Search query string
            enable_deepthink: Whether to enable deep thinking (0 or 1)

        Returns:
            Dictionary containing search results:
            {
                'success': bool,
                'answer': str,  # Answer text
                'references': list,  # Reference documents
                'error': str,  # Error message if failed
                'details': Any  # Additional details
            }
        """
        if not self.enabled:
            return {'success': False, 'error': 'OBI not enabled in configuration'}

        if not self.app_code or not self.cookie:
            return {'success': False, 'error': 'Missing required configuration parameters'}

        access_token = self._get_access_token()
        if not access_token:
            return {'success': False, 'error': 'Unable to get access_token'}

        try:
            # Call knowledge search API
            chat_url = f"{self.base_url}/api/chat-messages"
            headers = {'Authorization': f'Bearer {access_token}', 'X-App-Code': self.app_code, 'Content-Type': 'application/json'}

            data = {'query': query, 'response_mode': 'blocking', 'inputs': {'enable_deepthink': enable_deepthink}}

            if self.stdio:
                self.stdio.verbose(f"Searching knowledge base: {query}")
            # Use shorter timeout to avoid blocking the main request
            response = requests.post(chat_url, headers=headers, json=data, timeout=300)

            if response.status_code == 200:
                result = response.json()
                if self.stdio:
                    self.stdio.verbose(f"Knowledge search response: {result.get('retriever_resources', [])}")
                return {'success': True, 'answer': result.get('answer', ''), 'references': result.get('retriever_resources', []), 'full_response': result}
            else:
                return {'success': False, 'error': f'Knowledge search failed (HTTP {response.status_code})', 'details': response.text}

        except requests.exceptions.Timeout:
            return {'success': False, 'error': 'Request timeout', 'details': 'The request took too long to complete'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'error': 'Connection failed', 'details': 'Please check network connection'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_config(self) -> Dict[str, Any]:
        """Get OBI configuration"""
        return {'base_url': self.base_url, 'app_code': self.app_code, 'cookie': '***' if self.cookie else '', 'enabled': self.enabled}  # Hide cookie for security
