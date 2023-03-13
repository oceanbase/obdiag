#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/21
@file: file_utils.py
@desc:
"""
import functools
import os
import io
import shutil
import threading

import tabulate


def synchronized(wrapped):
    lock = threading.Lock()

    @functools.wraps(wrapped)
    def _wrap(*args, **kwargs):
        with lock:
            return wrapped(*args, **kwargs)

    return _wrap


@synchronized
def mkdir_if_not_exist(target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)


def delete_file_if_exist(file_path):
    if os.path.exists(file_path):
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)


def size_format(num, unit="B", output_str=False):
    """
    transform a large number bytes to unit that the number is smaller 1024
    example: 1048576B -> 1MB
    input: size_format(1025, "KB") if you want to transform 1025KB to 1MB
    :param num: the number of bytes/K/M/...
    :param unit: the unit of your input
    :param output_str: if output_str is true, directly output a string such as "12KB", otherwise output (12, "KB")
    :return: if output_str is True: a tuple(12, 'KB') else a string "12KB"
    """
    if num < 0:
        raise ValueError("num cannot be negative!")
    units = ["B", "K", "M", "G", "T"]
    try:
        unit_idx = units.index(unit)
    except KeyError:
        raise ValueError("unit {0} is illegal!".format(unit))
    new_num = float(num) * (1024 ** unit_idx)
    unit_idx = 0
    while new_num > 1024:
        new_num = float(new_num) / 1024
        unit_idx += 1
    if unit_idx >= len(units):
        raise ValueError("size exceed 1023TB!")
    if output_str:
        return "".join(["%.3f" % new_num, units[unit_idx]])
    return new_num, units[unit_idx]


# if not found, return the idx nearest to the val
def binary_search(val_list, val, key=lambda x: x):
    li_size = len(val_list)
    if li_size == 0:
        return -1
    st, et = 0, li_size - 1
    while et >= st:
        if et == st:
            return et
        mid = (st + et) // 2
        if key(val_list[mid]) == val:
            return mid
        elif key(val_list[mid]) > val:
            et = mid
        else:
            st = mid + 1
    return et


# closed interval, return the range as two sides of the closed interval
# if none, return (x, -1), where x is the last val smaller than min_val, and x in [-1, len(val_list)-1]
# x == -1 means the interval is all smaller than the first element
def binary_range_search(val_list, min_val, max_val, key=lambda x: x):
    """
    range search on a sorted val_list.
    :param val_list: A sorted val_list. From small to large.
    :param min_val: min_val in range constrain.
    :param max_val: max_val in range constrain.
    :param key: sorted key.
    :return: a tuple (x,y).
     if x>=0 and y>=0, indicates that val_list[x:y+1] are all in [min_val, max_val].
     if y==-1, indicates that no element is in the range, and x is the idx of last element that smaller than min_val.
     If x==-1 and y==-1, all elements are larger than min_val and max_val, or val_list is empty.
    """
    if len(val_list) == 0:
        return -1, -1
    if min_val > key(val_list[-1]):
        return len(val_list) - 1, -1
    if max_val < key(val_list[0]):
        return -1, -1
    min_idx = binary_search(val_list, min_val, key=key) + 1
    max_idx = binary_search(val_list, max_val, key=key) - 1
    # the last idx that val[idx] <= max_val
    while max_idx + 1 < len(val_list) and key(val_list[max_idx + 1]) <= max_val:
        max_idx += 1
    # the first idx that val[idx] >= min_val
    while min_idx - 1 >= 0 and key(val_list[min_idx - 1]) >= min_val:
        min_idx -= 1
    if max_idx < min_idx:
        return max_idx, -1
    return min_idx, max_idx


def get_log_dump_pkl_file_name(log_filename):
    return "{0}_logObj.pkl".format(log_filename)


def parse_size(size_str, unit='B'):
    unit_size_dict = {
        "b": 1,
        "B": 1,
        "k": 1024,
        "K": 1024,
        "m": 1024 * 1024,
        "M": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "t": 1024 * 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }
    unit_str = size_str.strip()[-1]
    if unit_str not in unit_size_dict:
        raise ValueError('unit {0} not in {1}'.format(unit_str, unit_size_dict.keys()))
    real_size = float(size_str.strip()[:-1]) * unit_size_dict[unit_str]
    if real_size < 0:
        raise ValueError('size cannot be negative!')
    return real_size / unit_size_dict[unit]


def write_result_append_to_file(filename, result):
    with io.open(filename, 'a', encoding='utf-8') as fileobj:
        fileobj.write(result)


def show_file_size_tabulate(ip, file_size):
    """
        show the size of the file
        :param args: remote host ip, file size
        :return: file info
        """
    format_file_size = size_format(int(file_size), output_str=True)
    summary_tab = []
    field_names = ["Node", "LogSize"]
    summary_tab.append((ip, format_file_size))
    return "\nZipFileInfo:\n" + \
           tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
