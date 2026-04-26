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
@time: 2026/03/20
@file: knowledge_base.py
@desc: OceanBase knowledge tools — POST /retrieval on the knowledge gateway.
"""

import json
import logging
import os
import time
from collections.abc import Callable
from typing import Any, Dict, Optional

import requests

_LOG = logging.getLogger(__name__)

# Gateway origin only (no trailing slash). Override with env for staging/self-hosted gateways.
OCEANBASE_KNOWLEDGE_GATEWAY_BASE = (os.environ.get("OCEANBASE_KNOWLEDGE_GATEWAY_BASE") or "https://ai-api.oceanbase.com").strip().rstrip("/") or "https://ai-api.oceanbase.com"
_KNOWLEDGE_RETRIEVAL_PATH = "/gateway/retrieval"
_MAX_QUERY_CHARS = 4096
_MAX_COMPONENT_CHARS = 256
_MAX_VERSION_CHARS = 64
_DEFAULT_COMPONENT = "oceanbase"
_REQUEST_TIMEOUT_SECONDS = 300.0

# Retrieval `component` values accepted by the knowledge gateway (no other strings).
OCEANBASE_KNOWLEDGE_ALLOWED_COMPONENTS = frozenset(
    (
        "oceanbase-standalone",
        "oceanbase-kv",
        "connector-c",
        "connector-j",
        "connector-odbc",
        "ecob",
        "oas",
        "oat",
        "ob-operator",
        "obci",
        "obd",
        "obdiag",
        "ob-loader-dumper",
        "oblogproxy",
        "ocp",
        "odc",
        "odm",
        "odp",
        "oma",
        "oms",
        "tugraph",
        "oceanbase_cloud",
        "oceanbase",
    )
)

# TODO: Document user registration flow to obtain ~/.obdiag/config/agent.toml
#       oceanbase_knowledge.bearer_token (onboarding / portal link).


def oceanbase_knowledge_gateway_ready(config: object) -> bool:
    """True when ``oceanbase_knowledge.enabled`` and a non-empty ``bearer_token`` (real gateway calls)."""
    return bool(getattr(config, "oceanbase_knowledge_enabled", False) and (getattr(config, "oceanbase_knowledge_bearer_token", "") or "").strip())


def _stdio_verbose(deps: Optional[object], msg: str) -> None:
    stdio = getattr(deps, "stdio", None) if deps else None
    if stdio and getattr(stdio, "verbose", None):
        try:
            stdio.verbose(msg)
        except Exception:
            pass
    _LOG.debug("%s", msg)


def _stdio_warn(deps: Optional[object], msg: str) -> None:
    stdio = getattr(deps, "stdio", None) if deps else None
    if stdio and getattr(stdio, "warn", None):
        try:
            stdio.warn(msg)
        except Exception:
            pass
    _LOG.warning("%s", msg)


def _compose_query_with_context(query: str, context_text: Optional[str]) -> str:
    q = (query or "").strip()
    ctx = (context_text or "").strip()
    if not ctx:
        return q
    return f"{q}\n\n{ctx}".strip()


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _format_json_error(status: int, body: str) -> str:
    try:
        data = json.loads(body)
        if isinstance(data, dict):
            msg = data.get("message") or data.get("detail") or data.get("error")
            if msg:
                return f"HTTP {status}: {msg}"
    except Exception:
        pass
    snippet = (body or "").strip()[:2000]
    return f"HTTP {status}: {snippet}" if snippet else f"HTTP {status}: (empty body)"


def _extract_knowledge_answer(data: dict) -> str:
    """Extract and format results from the knowledge gateway response.

    Response structure: data.data.outputs.result[] with {title, content} items.
    Falls back to full JSON dump if the expected path is absent.
    """
    try:
        results = data.get("data", {}).get("outputs", {}).get("result", [])
        if isinstance(results, list) and results:
            parts = []
            for item in results:
                title = (item.get("title") or "").strip()
                content = (item.get("content") or "").strip()
                if title and content:
                    parts.append(f"## {title}\n{content}")
                elif content:
                    parts.append(content)
                elif title:
                    parts.append(title)
            if parts:
                return "\n\n".join(parts)[:8000]
    except Exception:
        pass
    return json.dumps(data, ensure_ascii=False)[:8000]


def create_knowledge_tools(token_getter: Callable[[], str]) -> list:
    """Return list of knowledge base tool functions.

    Args:
        token_getter: Callable that returns the OceanBase knowledge bearer token.
    """

    def query_oceanbase_knowledge_base(
        query: str,
        context_text: Optional[str] = None,
        component: Optional[str] = None,
        version: Optional[str] = None,
    ) -> str:
        """
        Query official OceanBase knowledge via the fixed gateway (POST ``/retrieval``).

        Use for documented information about OceanBase (not live cluster state; use obdiag tools or SQL).

        Args:
            query: Main question string (up to 4096 characters after optional context merge).
                When Observer/OBProxy/etc. versions are known, put them in ``version`` or include
                them in this string so retrieval matches the right doc generation.
            context_text: Optional extra material (logs, errors, long snippets); appended to ``query``
                for the gateway (same field budget as ``query`` overall).
            component: Knowledge slice id; must be one of ``OCEANBASE_KNOWLEDGE_ALLOWED_COMPONENTS``
                (see oceanbase-knowledge SKILL). Default ``oceanbase`` when omitted.
            version: Optional product/doc line version (max 64 chars), e.g. ``4.3.0.0``.

        The tool is registered only when ``oceanbase_knowledge.enabled: true`` in ``agent.toml``.
        Without ``bearer_token``, returns setup instructions for obtaining a token.
        """
        token = (token_getter() or "").strip()

        if not token:
            _stdio_warn(
                None,
                "query_oceanbase_knowledge_base: bearer token empty; set oceanbase_knowledge.bearer_token in agent.toml to reach the gateway",
            )
            return "OceanBase knowledge is not enabled: set oceanbase_knowledge.bearer_token in " "~/.obdiag/config/agent.toml after registration. " "(Registration / token onboarding — see project TODO.)"

        base = OCEANBASE_KNOWLEDGE_GATEWAY_BASE.rstrip("/")

        q = _compose_query_with_context(query or "", context_text)
        if not q.strip():
            _stdio_warn(None, "query_oceanbase_knowledge_base: rejected empty query")
            return "Error: query must be non-empty."
        if len(q) > _MAX_QUERY_CHARS:
            _stdio_warn(None, f"query_oceanbase_knowledge_base: query too long len={len(q)} (max {_MAX_QUERY_CHARS})")
            return f"Error: query (including context_text) must be at most {_MAX_QUERY_CHARS} characters (got {len(q)})."

        comp = (component or "").strip() or _DEFAULT_COMPONENT
        if len(comp) > _MAX_COMPONENT_CHARS:
            _stdio_warn(None, f"query_oceanbase_knowledge_base: component too long len={len(comp)}")
            return f"Error: component must be at most {_MAX_COMPONENT_CHARS} characters (got {len(comp)})."
        if comp not in OCEANBASE_KNOWLEDGE_ALLOWED_COMPONENTS:
            _stdio_warn(None, f"query_oceanbase_knowledge_base: invalid component {comp!r}")
            allowed = ", ".join(sorted(OCEANBASE_KNOWLEDGE_ALLOWED_COMPONENTS))
            return f"Error: component must be exactly one of the gateway allowlist: {allowed}"

        ver = (version or "").strip()
        if len(ver) > _MAX_VERSION_CHARS:
            _stdio_warn(None, f"query_oceanbase_knowledge_base: version too long len={len(ver)}")
            return f"Error: version must be at most {_MAX_VERSION_CHARS} characters (got {len(ver)})."

        body: Dict[str, Any] = {
            "query": q,
            "component": comp,
            "version": ver,
        }

        url = f"{base}{_KNOWLEDGE_RETRIEVAL_PATH}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        preview = _truncate(q, 120)
        ctx_part = f"yes(len={len(context_text)})" if context_text else "no"
        _stdio_verbose(
            None,
            "OceanBase knowledge: POST " f"{url} query_len={len(q)} query_preview={preview!r} " f"context_text={ctx_part} component={comp!r} version={ver!r} " f"bearer_len={len(token)} timeout_s={_REQUEST_TIMEOUT_SECONDS}",
        )
        _LOG.info(
            "OceanBase knowledge request url=%s query_len=%s has_context=%s component=%s version=%s",
            url,
            len(q),
            bool(context_text),
            comp,
            ver or "(empty)",
        )

        t0 = time.monotonic()
        try:
            r = requests.post(url, json=body, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.Timeout:
            elapsed = time.monotonic() - t0
            _stdio_warn(
                None,
                f"OceanBase knowledge: timeout after {elapsed:.2f}s (limit {_REQUEST_TIMEOUT_SECONDS}s) url={url}",
            )
            _LOG.warning("OceanBase knowledge timeout url=%s elapsed_s=%.2f", url, elapsed)
            return f"Error: request timed out after {_REQUEST_TIMEOUT_SECONDS}s (gateway: {base})."
        except requests.exceptions.RequestException as e:
            elapsed = time.monotonic() - t0
            _stdio_warn(
                None,
                f"OceanBase knowledge: request failed url={url} error={type(e).__name__}: {e}",
            )
            _LOG.warning(
                "OceanBase knowledge request error url=%s type=%s err=%s elapsed_s=%.2f",
                url,
                type(e).__name__,
                e,
                elapsed,
            )
            return f"Error: failed to reach knowledge gateway ({base}): {e}"

        elapsed = time.monotonic() - t0
        body_len = len(r.text or "")
        _stdio_verbose(
            None,
            f"OceanBase knowledge: response status={r.status_code} body_len={body_len} elapsed_s={elapsed:.2f}",
        )

        if r.status_code != 200:
            snippet = (r.text or "")[:800]
            _stdio_warn(
                None,
                f"OceanBase knowledge: HTTP {r.status_code} url={url} body_preview={snippet!r}",
            )
            _LOG.warning(
                "OceanBase knowledge HTTP %s url=%s body_len=%s preview=%r",
                r.status_code,
                url,
                body_len,
                snippet[:200],
            )
            return _format_json_error(r.status_code, r.text)

        try:
            data = r.json()
        except Exception as ex:
            raw = (r.text or "").strip()[:800]
            _stdio_warn(
                None,
                f"OceanBase knowledge: JSON parse failed status=200 url={url} err={type(ex).__name__}: {ex} " f"raw_preview={raw!r}",
            )
            _LOG.warning(
                "OceanBase knowledge JSON parse error url=%s err=%s raw_len=%s",
                url,
                ex,
                body_len,
            )
            return (r.text or "").strip()[:8000] or "(empty response)"

        if isinstance(data, dict):
            out = _extract_knowledge_answer(data)
            ans_len = len(out)
            _stdio_verbose(
                None,
                f"OceanBase knowledge: success answer_len={ans_len} elapsed_s={elapsed:.2f}",
            )
            _LOG.info(
                "OceanBase knowledge success url=%s answer_len=%s elapsed_s=%.2f",
                url,
                ans_len,
                elapsed,
            )
            return out.strip()

        out = str(data)[:8000]
        _stdio_verbose(None, f"OceanBase knowledge: success (non-dict JSON) out_len={len(out)} elapsed_s={elapsed:.2f}")
        _LOG.info("OceanBase knowledge success url=%s non_dict_json elapsed_s=%.2f", url, elapsed)
        return out

    return [query_oceanbase_knowledge_base]
