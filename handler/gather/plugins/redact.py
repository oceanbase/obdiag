import os
import re
import threading
from datetime import datetime

from common.import_module import import_modules


class Redact:
    def __init__(self, context, input_file_dir, output_file_dir):
        self.redacts = None
        self.stdio = context.stdio
        self.context = context
        self.input_file_dir = input_file_dir
        self.output_file_dir = output_file_dir
        self.module_dir = os.path.expanduser('~/.obdiag/redact')

        def find_all_files(target_path):
            # 使用os.walk遍历目标路径及其子目录
            for root, dirs, files in os.walk(target_path):
                # 只返回目标路径下的文件，不包括子目录下的文件
                if root == target_path:
                    return files

        # init all redact
        # import all redact module
        self.all_redact = []
        try:
            self.stdio.info("Importing redact modules...")
            module_files = find_all_files(self.module_dir) or []
            for module_file in module_files:
                if not os.path.isfile(os.path.join(self.module_dir, module_file)):
                    continue
                if not module_file.endswith('.py'):
                    continue
                module_name = os.path.splitext(module_file)[0]
                try:
                    redact_module = import_modules(os.path.join(self.module_dir, module_file), module_name, self.stdio)
                    self.all_redact.append(redact_module)
                except Exception as e:
                    raise Exception(f"Error importing redact module {module_name}: {e}")
        except Exception as e:
            raise e

    def check_redact(self, input_redacts):
        for input_redact in input_redacts:
            if not input_redact in self.all_redact:
                raise Exception(f"Redact {input_redact} not found")
            else:
                self.stdio.verbose(f"Redact {input_redact} found")
                self.redacts.append(input_redact)

    def redact_files(self):
        # check self.redacts
        if not self.redacts or len(self.redacts) == 0:
            self.stdio.error("No redact found")
            return False
        # create dir to save the files after redact
        if not os.path.exists(self.output_file_dir):
            os.makedirs(self.output_file_dir)
        # use threading to redact the files
        files_name = os.listdir(self.input_file_dir)
        file_queue = []
        for file_name in files_name:
            if "result_summary.txt" in file_name:
                continue
            file_path = os.path.join(self.input_file_dir, file_name)
            output_file_dir = os.path.join(self.output_file_dir, file_name)
            file_thread = threading.Thread(target=self.redact_file, args=(file_path, output_file_dir))
            file_thread.start()
            file_queue.append(file_thread)
        for file_thread in file_queue:
            file_thread.join()

    def redact_file(self, file_path, output_file_dir):
        # get all files_name
        with open(file_path, 'rb') as f:
            content = f.read()
            for redact in self.redacts:
                content = redact.redact(content)
            with open(output_file_dir, 'wb') as f_new:
                f_new.write(content)
