import os
import shutil
import tarfile

from common.import_module import import_modules
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
        self.all_redact = []
        try:
            self.stdio.print("Importing redact modules...")
            self.all_redact = import_modules(self.module_dir, self.stdio)
            self.stdio.verbose("Imported redact module {0}".format(self.all_redact))
        except Exception as e:
            self.stdio.error(f"Error importing redact modules: {e}")
            raise e

    def check_redact(self, input_redacts):
        for input_redact in input_redacts:
            if not input_redact in self.all_redact:
                self.stdio.error("Redact {0} not found".format(input_redact))
                raise Exception(f"Redact {input_redact} not found")
            else:
                self.stdio.verbose(f"Redact {input_redact} found")
                self.redacts[input_redact] = self.all_redact[input_redact]

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
            with open(input_file, 'r', encoding='utf-8', errors='ignore') as file:
                log_content = file.read()
            for redact in self.redacts:
                log_content = self.redacts[redact].redact(log_content)
            with open(output_file, 'w', encoding='utf-8', errors='ignore') as file:
                file.write(log_content)
        except Exception as e:
            self.stdio.error(f"Error redact file {input_file}: {e}")
        finally:
            semaphore.release()
