#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
"""
@file: pack_discovery.py
@desc: Discover log files from obdiag gather pack directory structure.
       Used by analyze log, analyze memory, rca when --log_dir is specified.
       Supports gather log .tar.gz archives (auto-extract to a cache dir under the pack).
"""
import fnmatch
import hashlib
import os
import shutil
import tarfile

# Standard log file names in OB + OS logs for cluster_down RCA
LOG_NAMES = [
    "observer.log",
    "observer.log.wf",
    "rootservice.log",
    "rootservice.log.wf",
    "election.log",
    "election.log.wf",
    "trace.log",
    "trace.log.wf",
    "dmesg",
    "message.log",
    "syslog",
]
# Glob patterns for rotated logs + coredump
LOG_PATTERNS = [
    "observer.log.*",
    "observer.log.wf.*",
    "rootservice.log.*",
    "rootservice.log.wf.*",
    "election.log.*",
    "election.log.wf.*",
    "trace.log.*",
    "trace.log.wf.*",
    "core*",
    "*.core",
]

# Auto-extract dir under gather pack (skipped when walking for loose logs / tar scan avoids re-entering)
INTERNAL_EXTRACT_DIR = ".obdiag_extracted_for_analyze"


def _stdio_print_progress(stdio, msg):
    if stdio is None or getattr(stdio, "silent", False):
        return
    stdio.print(msg)


def is_ob_log_basename(name):
    """True if basename matches known OB / OS log names (plain file, not archive)."""
    if name in LOG_NAMES:
        return True
    return any(fnmatch.fnmatch(name, pat) for pat in LOG_PATTERNS)


def _filter_safe_tar_members(members):
    out = []
    for m in members:
        if not m.isfile():
            continue
        p = os.path.normpath(m.name)
        if p.startswith("..") or os.path.isabs(p):
            continue
        out.append(m)
    return out


def extract_gather_tarball(tar_path, dest_dir, stdio=None):
    """
    Extract obdiag gather .tar.gz into dest_dir (path traversal safe).
    :return: True on success
    """
    try:
        os.makedirs(dest_dir, exist_ok=True)
        with tarfile.open(tar_path, "r:gz") as tar:
            safe = _filter_safe_tar_members(tar.getmembers())
            tar.extractall(path=dest_dir, members=safe)
        return True
    except Exception as e:
        if stdio:
            stdio.warn("Failed to extract {0}: {1}".format(tar_path, e))
        return False


def discover_log_files(log_dir):
    """
    Discover OceanBase log files from a pack directory (obdiag gather output).
    Supports:
    - obdiag_gather_pack_<timestamp>/ with node subdirs (observer_log_*)
    - Direct log dir with observer.log etc.
    Skips subdirectory named INTERNAL_EXTRACT_DIR (auto-extract cache).
    :param log_dir: root path of pack or log directory
    :return: list of absolute paths to log files
    """
    if not log_dir or not os.path.isdir(log_dir):
        return []
    log_dir = os.path.abspath(log_dir)
    found = []

    def add_file(p):
        if os.path.isfile(p) and p not in found:
            found.append(p)

    def scan_dir(d):
        for name in os.listdir(d):
            if name == INTERNAL_EXTRACT_DIR:
                continue
            full = os.path.join(d, name)
            if os.path.isfile(full):
                if name in LOG_NAMES:
                    add_file(full)
                else:
                    for pat in LOG_PATTERNS:
                        if fnmatch.fnmatch(name, pat):
                            add_file(full)
                            break
            elif os.path.isdir(full):
                scan_dir(full)

    scan_dir(log_dir)
    return sorted(found)


def _iter_gather_tarballs_under(log_dir):
    """Yield .tar.gz files under log_dir, excluding INTERNAL_EXTRACT_DIR."""
    log_dir = os.path.abspath(log_dir)
    for root, dirs, files in os.walk(log_dir):
        dirs[:] = [d for d in dirs if d != INTERNAL_EXTRACT_DIR]
        for f in files:
            if f.endswith(".tar.gz"):
                yield os.path.join(root, f)


def discover_log_files_including_gather_archives(log_dir, stdio=None, extract_root=None):
    """
    discover_log_files plus: extract gather *.tar.gz under the pack and include inner logs.

    Always scans tarballs (in addition to loose logs) so mixed packs work.
    Extract cache: <log_dir>/.obdiag_extracted_for_analyze/<key>/
    """
    log_dir = os.path.abspath(os.path.expanduser(log_dir))
    if not os.path.isdir(log_dir):
        return []

    found = list(discover_log_files(log_dir))
    seen = {os.path.abspath(p) for p in found}

    extract_root = extract_root or os.path.join(log_dir, INTERNAL_EXTRACT_DIR)
    tar_paths = sorted(set(_iter_gather_tarballs_under(log_dir)))
    n_tar = len(tar_paths)
    if n_tar:
        _stdio_print_progress(stdio, "Found {0} gather archive(s) under pack, extracting...".format(n_tar))

    for i, tp in enumerate(tar_paths, 1):
        if n_tar:
            _stdio_print_progress(stdio, "  [extract {0}/{1}] {2}".format(i, n_tar, os.path.basename(tp)))
        key = hashlib.md5(os.path.abspath(tp).encode("utf-8")).hexdigest()[:16]
        sub = os.path.join(extract_root, key)
        if os.path.isdir(sub):
            shutil.rmtree(sub, ignore_errors=True)
        if not extract_gather_tarball(tp, sub, stdio):
            continue
        for inner in discover_log_files(sub):
            ap = os.path.abspath(inner)
            if ap not in seen:
                seen.add(ap)
                found.append(inner)

    return sorted(found, key=lambda x: x)


def path_under_internal_extract_cache(path):
    """True if path is inside a .obdiag_extracted_for_analyze directory (avoid duplicate --files picks)."""
    parts = os.path.normpath(path).split(os.sep)
    return INTERNAL_EXTRACT_DIR in parts


def expand_offline_paths_with_archives(raw_paths, extract_base, stdio=None):
    """
    For analyze log --files: raw_paths are files (e.g. from find_all_file).
    - Keeps paths whose basename is an OB log name.
    - Expands each .tar.gz via extract_gather_tarball under extract_base/<key>/ and adds inner logs.
    Ignores unrelated files (e.g. result_summary.txt).
    Skips paths under INTERNAL_EXTRACT_DIR (stale cache from prior --log_dir runs).
    """
    if not raw_paths:
        return []
    os.makedirs(extract_base, exist_ok=True)
    tar_count = 0
    for p in raw_paths:
        ap0 = os.path.abspath(os.path.expanduser(p))
        if os.path.isfile(ap0) and ap0.endswith(".tar.gz") and not path_under_internal_extract_cache(ap0):
            tar_count += 1
    tar_i = 0
    out = []
    seen = set()
    if tar_count:
        _stdio_print_progress(stdio, "Found {0} gather archive(s) in input, extracting...".format(tar_count))

    for p in raw_paths:
        ap = os.path.abspath(os.path.expanduser(p))
        if not os.path.isfile(ap):
            continue
        if path_under_internal_extract_cache(ap):
            continue
        if ap.endswith(".tar.gz"):
            tar_i += 1
            if tar_count:
                _stdio_print_progress(stdio, "  [extract {0}/{1}] {2}".format(tar_i, tar_count, os.path.basename(ap)))
            key = hashlib.md5(ap.encode("utf-8")).hexdigest()[:16]
            sub = os.path.join(extract_base, key)
            if os.path.isdir(sub):
                shutil.rmtree(sub, ignore_errors=True)
            if not extract_gather_tarball(ap, sub, stdio):
                continue
            for inner in discover_log_files(sub):
                iap = os.path.abspath(inner)
                if iap not in seen:
                    seen.add(iap)
                    out.append(inner)
        elif is_ob_log_basename(os.path.basename(ap)):
            if ap not in seen:
                seen.add(ap)
                out.append(ap)
    return out
