#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Minimal reproduction of issue #1202 root cause: strict UTF-8 decode on bytes that
contain invalid sequences (same as PyMySQL row decode for TEXT/VARCHAR).

https://github.com/oceanbase/obdiag/issues/1202
"""
from __future__ import print_function


def main():
    payload = (b"a" * 268) + b"\xff"
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as e:
        print("Reproduced (same class as issue #1202):", repr(e))
        print("  reason:", e.reason, "at position", e.start)
        return 0
    raise SystemExit("Expected UnicodeDecodeError")


if __name__ == "__main__":
    raise SystemExit(main())
