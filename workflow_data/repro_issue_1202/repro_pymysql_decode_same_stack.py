#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Trigger the same decode path as PyMySQL connections._read_row_from_packet (line ~1354)
without a database: strict .decode(encoding) on bytes that are not valid UTF-8.

This matches the issue report:
  UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 268
"""
from __future__ import print_function


def pymysql_like_decode(data, encoding):
    """Same as pymysql for TEXT when use_unicode=True: data.decode(encoding)."""
    return data.decode(encoding)


def main():
    raw = (b"a" * 268) + b"\xff"
    # PyMySQL uses connection charset name; Python accepts utf-8 (same as utf8mb4 for decode)
    try:
        pymysql_like_decode(raw, "utf-8")
    except UnicodeDecodeError as e:
        print("Same failure mode as PyMySQL _read_row_from_packet + utf8:")
        print(" ", repr(e))
        return 0
    raise SystemExit("unexpected: decode succeeded")


if __name__ == "__main__":
    raise SystemExit(main())
