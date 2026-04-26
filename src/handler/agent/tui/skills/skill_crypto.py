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

"""Skill content encryption utilities.

Skills shipped in plugins/agent/skills/ are stored with their body encrypted
to protect SOP content in the repository.  The YAML frontmatter (name,
description) is kept in plaintext so skill discovery works without decryption.

Encryption: Fernet (AES-128-CBC + HMAC-SHA256) with a PBKDF2-derived key,
matching the existing FileEncryptor strategy in src/common/file_crypto/.
"""

from __future__ import annotations

import base64

_MARKER = "OBDIAG_SKILL_ENCRYPTED:"
_SALT = b'obdiag'
_PWD = "********"


def _fernet():
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=_SALT, iterations=100000)
    return Fernet(base64.urlsafe_b64encode(kdf.derive(_PWD.encode())))


def _split_frontmatter(content: str) -> tuple[str, str]:
    """Return (frontmatter_block, body).

    frontmatter_block: everything up to and including the closing ``---`` line.
    body: everything after the closing ``---`` line, preserving the leading ``\\n``.
    """
    if not content.startswith("---"):
        return "", content
    # Locate the closing "---" by finding its character position, not by
    # splitting into lines, so we preserve all newlines exactly.
    # The opening "---" occupies positions 0-2; we search from position 3.
    search_start = 3
    while True:
        pos = content.find("\n---", search_start)
        if pos == -1:
            return "", content
        end = pos + 4  # points just past "\n---"
        # Accept "\n---" only when it is a standalone line
        if end >= len(content) or content[end] in ("\n", "\r"):
            return content[:end], content[end:]
        search_start = pos + 1


def is_encrypted(content: str) -> bool:
    """Return True if the skill body starts with the encrypted marker."""
    _, body = _split_frontmatter(content)
    return body.strip().startswith(_MARKER)


def encrypt_skill_content(content: str) -> str:
    """Encrypt the body of a SKILL.md, keeping frontmatter in plaintext.

    Idempotent: already-encrypted content is returned unchanged.
    """
    if is_encrypted(content):
        return content
    frontmatter, body = _split_frontmatter(content)
    # body already starts with "\n" from _split_frontmatter; encrypt it as-is
    # so decrypt can recover the exact original content via frontmatter + body.
    token = _fernet().encrypt(body.encode()).decode("ascii")
    # Separator "\n" sits between frontmatter's closing "---" and the marker line.
    encrypted_body = "\n" + _MARKER + token + "\n"
    return (frontmatter + encrypted_body) if frontmatter else encrypted_body.lstrip("\n")


def decrypt_skill_content(content: str) -> str:
    """Decrypt the body of a SKILL.md, returning full plaintext.

    Idempotent: plaintext content is returned unchanged.
    """
    if not is_encrypted(content):
        return content
    frontmatter, body = _split_frontmatter(content)
    # body is "\nOBDIAG_SKILL_ENCRYPTED:<token>\n" — strip whitespace to get token
    token = body.strip()[len(_MARKER):]
    decrypted_body = _fernet().decrypt(token.encode()).decode("utf-8")
    # decrypted_body starts with "\n" (the original body separator), so
    # frontmatter + decrypted_body reconstructs the original file exactly.
    return (frontmatter + decrypted_body) if frontmatter else decrypted_body


class DecryptingFilesystemBackend:
    """FilesystemBackend wrapper that transparently decrypts SKILL.md files on read.

    The agent uses ``read_file`` to load skill bodies from their on-disk paths.
    Because skills are stored encrypted, a plain ``FilesystemBackend`` would
    return ciphertext to the agent.  This class intercepts ``read()`` calls for
    ``SKILL.md`` files and decrypts the content before returning it.

    All other operations are forwarded unchanged to the wrapped backend.
    """

    def __init__(self, backend: object) -> None:
        self._backend = backend

    def __getattr__(self, name: str) -> object:
        return getattr(self._backend, name)

    def _decrypt_result(self, result: object, file_path: str, limit: int) -> object:
        """Decrypt ReadResult file_data content if the file is an encrypted SKILL.md."""
        try:
            from deepagents.backends.protocol import FileData, ReadResult

            fd = getattr(result, "file_data", None)
            if fd is None:
                return result
            # file_data may be a dict (TypedDict) or a dataclass-like object
            encoding = fd.get("encoding") if isinstance(fd, dict) else getattr(fd, "encoding", None)
            content = fd.get("content") if isinstance(fd, dict) else getattr(fd, "content", None)
            if encoding != "utf-8" or not content:
                return result
            decrypted = decrypt_skill_content(content)
            if decrypted == content:
                return result
            lines = decrypted.splitlines()
            end_idx = min(limit, len(lines))
            paged = "\n".join(lines[:end_idx])
            return ReadResult(file_data=FileData(content=paged, encoding="utf-8"))
        except Exception:
            return result

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> object:
        result = self._backend.read(file_path, offset=offset, limit=limit)
        # Only decrypt SKILL.md reads starting at offset 0 (the encrypted marker is at the top)
        if offset == 0 and file_path.endswith("SKILL.md"):
            result = self._decrypt_result(result, file_path, limit)
        return result

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> object:
        result = await self._backend.aread(file_path, offset=offset, limit=limit)
        if offset == 0 and file_path.endswith("SKILL.md"):
            result = self._decrypt_result(result, file_path, limit)
        return result
