#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/21
@file: compressor_utils.py
@desc:
"""
import contextlib
import os
import tarfile
import zipfile
import zstandard as zstd


class Compressor(object):
    def __init__(self):
        self.compressed_path = ""

    def set_compressed_path(self, path):
        self.compressed_path = path

    # interface
    def execute_compress(self, file_path_list):
        """
        :param file_path_list: (file_path, arc_name)
        :return:
        """
        pass

    # interface
    def execute_decompress(self, compressed_file_path, target_path):
        pass


class ZipDeflateCompressor(Compressor):
    def __init__(self):
        super(ZipDeflateCompressor, self).__init__()

    def execute_compress(self, file_path_list):
        with contextlib.closing(zipfile.ZipFile(self.compressed_path, 'w')) as zip_fd:
            for file_path, arc_name in file_path_list:
                zip_fd.write(file_path, arc_name, compress_type=zipfile.ZIP_DEFLATED)
        return self.compressed_path

    def execute_decompress(self, compressed_file_path, target_path):
        with contextlib.closing(zipfile.ZipFile(compressed_file_path)) as zip_fd:
            zip_fd.extractall(target_path)
        return target_path


class ZstdCompressor(Compressor):
    def __init__(self):
        super(ZstdCompressor, self).__init__()

    def execute_compress(self, file_path_list):
        tar_path = self.compressed_path + ".tar"
        with contextlib.closing(tarfile.TarFile(tar_path, "w")) as tar_fd:
            for file_path, arc_name in file_path_list:
                tar_fd.add(file_path, arc_name, recursive=False)
        with open(tar_path, "rb") as tar_fd:
            with open(self.compressed_path, "wb") as zstd_fd:
                cctx = zstd.ZstdCompressor()
                cctx.copy_stream(tar_fd, zstd_fd)
        os.remove(tar_path)
        return self.compressed_path

    def execute_decompress(self, compressed_file_path, target_path):
        tar_path = compressed_file_path+".tar"
        with open(tar_path, 'wb') as tar_fd:
            with open(compressed_file_path, 'rb') as zstd_fd:
                dctx = zstd.ZstdDecompressor()
                dctx.copy_stream(zstd_fd, tar_fd)
        with contextlib.closing(tarfile.TarFile(tar_path, 'rb')) as tar_fd:
            tar_fd.extractall(target_path)
        os.remove(tar_path)
        return target_path
