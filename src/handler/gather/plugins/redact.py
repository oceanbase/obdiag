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
@time: 2024/09/25
@file: redact.py
@desc:
"""
import os
import shutil
import tarfile

from src.common.import_module import import_modules
import multiprocessing as mp


class Redact:
    def __init__(self, context, input_file_dir, output_file_dir):
        self.context = context
        self.stdio = context.stdio
        self.redacts = {}
        self.input_file_dir = input_file_dir
        self.output_file_dir = output_file_dir
        self.stdio.verbose("Redact output_file_dir: {0}".format(self.output_file_dir))
        self.module_dir = os.path.expanduser('~/.obdiag/gather/redact')
        self.inner_config = self.context.inner_config

        # init all redact
        # import all redact module
        # Try to load from plugins directory first, then from user directory
        self.all_redact = {}

        # Try to load from plugins directory
        plugins_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "plugins", "gather", "redact")
        if os.path.exists(plugins_dir):
            try:
                self.stdio.verbose("Trying to import redact modules from plugins directory: {0}".format(plugins_dir))
                plugins_redact = import_modules(plugins_dir, self.stdio)
                if plugins_redact:
                    self.all_redact.update(plugins_redact)
                    self.stdio.verbose("Imported redact modules from plugins: {0}".format(list(plugins_redact.keys())))
            except Exception as e:
                self.stdio.verbose("Failed to import redact modules from plugins directory: {0}".format(str(e)))

        # Try to load from user directory
        try:
            self.stdio.verbose("Trying to import redact modules from user directory: {0}".format(self.module_dir))
            user_redact = import_modules(self.module_dir, self.stdio)
            if user_redact:
                self.all_redact.update(user_redact)
                self.stdio.verbose("Imported redact modules from user directory: {0}".format(list(user_redact.keys())))
        except Exception as e:
            self.stdio.verbose("Failed to import redact modules from user directory: {0}".format(str(e)))

        if not self.all_redact:
            self.stdio.warn("No redact modules found in plugins or user directory")
        else:
            self.stdio.verbose("Total imported redact modules: {0}".format(list(self.all_redact.keys())))

    def check_redact(self, input_redacts):
        for input_redact in input_redacts:
            if not input_redact in self.all_redact:
                self.stdio.error("Redact {0} not found".format(input_redact))
                raise Exception(f"Redact {input_redact} not found")
            else:
                self.stdio.verbose(f"Redact {input_redact} found")
                redact_plugin = self.all_redact[input_redact]
                # Set stdio for plugins that support it
                if hasattr(redact_plugin, 'stdio'):
                    redact_plugin.stdio = self.stdio
                # Reset warn_count for time_jump plugin if present
                if input_redact == "time_jump" and hasattr(redact_plugin, 'warn_count'):
                    redact_plugin.warn_count = 0
                self.redacts[input_redact] = redact_plugin

    def redact_files(self, input_redacts, files_name):
        if len(files_name) == 0:
            self.stdio.warn("No files to redact")
            return True
        self.stdio.verbose("redact_files start")
        self.check_redact(input_redacts)
        # check self.redacts
        if not self.redacts or len(self.redacts) == 0:
            self.stdio.error("No redact found")
            return False

        # create dir to save the files after redact
        if not os.path.exists(self.output_file_dir):
            os.makedirs(self.output_file_dir)
        # gather all files
        self.stdio.verbose("gather_log_files: {0}".format(files_name))
        if len(files_name) == 0:
            self.stdio.warn("No log file found. The redact process will be skipped.")
            return False
        file_queue = []
        max_processes = int(self.inner_config.get('gather').get('redact_processing_num')) or 3
        self.stdio.verbose("max_processes: {0}".format(max_processes))
        semaphore = mp.Semaphore(max_processes)
        for dir_name in files_name:
            for file_name in files_name[dir_name]:
                self.stdio.verbose("inport file name: {0}".format(file_name))
                self.stdio.verbose("output file name: {0}".format(file_name.replace(self.input_file_dir, self.output_file_dir)))
                semaphore.acquire()
                file_thread = mp.Process(target=self.redact_file, args=(file_name, file_name.replace(self.input_file_dir, self.output_file_dir), semaphore))
                file_thread.start()
                file_queue.append(file_thread)
        for file_thread in file_queue:
            file_thread.join()

        # tar the dir by node
        subfolders = [f for f in os.listdir(self.output_file_dir) if os.path.isdir(os.path.join(self.output_file_dir, f))]
        for subfolder in subfolders:
            subfolder_path = os.path.join(self.output_file_dir, subfolder)
            tar_filename = os.path.join(self.output_file_dir, f"{subfolder}.tar.gz")
            with tarfile.open(tar_filename, "w:gz") as tar:
                for root, dirs, files in os.walk(subfolder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        tar.add(file_path, os.path.relpath(file_path, subfolder_path))
            self.stdio.verbose("delete the dir: {0}".format(subfolder_path))
            shutil.rmtree(subfolder_path)
            self.stdio.print(f"{subfolder} is tar on {tar_filename}")
        return True

    def redact_file(self, input_file, output_file, semaphore):
        try:
            input_file = os.path.abspath(input_file)
            output_file = os.path.abspath(output_file)
            dir_path = os.path.dirname(output_file)
            log_content = ""
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            # Read file content
            with open(input_file, 'r', encoding='utf-8', errors='ignore') as file:
                log_content = file.read()

            # Apply all redact plugins in sequence
            # Pass output_file_path as keyword argument for plugins that support it (like time_jump)
            for redact_name in self.redacts:
                redact_plugin = self.redacts[redact_name]
                # Try to call with output_file_path parameter (plugins that don't support it will ignore it)
                try:
                    log_content = redact_plugin.redact(log_content, output_file_path=output_file)
                except TypeError:
                    # If plugin doesn't support output_file_path parameter, call without it
                    log_content = redact_plugin.redact(log_content)

            # Write output file
            with open(output_file, 'w', encoding='utf-8', errors='ignore') as file:
                file.write(log_content)

        except Exception as e:
            self.stdio.error(f"Error redact file {input_file}: {e}")
        finally:
            semaphore.release()
