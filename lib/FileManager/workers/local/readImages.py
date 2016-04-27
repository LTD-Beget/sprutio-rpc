from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
from config.main import TMP_DIR
import traceback
import os


class ReadImages(MainWorkerCustomer):
    def __init__(self, paths, *args, **kwargs):
        super(ReadImages, self).__init__(*args, **kwargs)

        self.paths = paths

    def run(self):
        try:
            self.preload()
            self.logger.info("Local ReadImages process run")

            hash_str = self.random_hash()
            self.download_dir = TMP_DIR + '/images/' + self.login + '/' + hash_str + '/'
            success_paths, error_paths = self.copy_files_to_tmp()

            if len(success_paths) == 1:
                one_file = True
            else:
                one_file = False

            if len(error_paths) == 0:  # Значит все хорошо, можно дальше обрабатывать

                file_list = {
                    "succeed": list(os.path.basename(filename) for filename in success_paths),
                    "failed": list(os.path.basename(filename) for filename in error_paths)
                }

                answer = {
                    "success": True,
                    "file_list": file_list,
                    "hash": hash_str,
                    "one_file": one_file,
                    "sid": self.login
                }

                result = {
                    "data": answer
                }

                self.on_success(result)
            else:
                raise Exception("read error")

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    def copy_files_to_tmp(self):
        success_paths = []
        error_paths = []

        for path in self.paths:
            try:
                abs_path = self.get_abs_path(path)
                file_basename = os.path.basename(abs_path)

                if self.ssh_manager.isdir(abs_path):
                    self.ssh_manager.sync_new(path, self.download_dir, direction="rl", create_folder=True)
                else:
                    self.ssh_manager.sync_new(path, os.path.join(self.download_dir, file_basename), direction="rl")

                success_paths.append(os.path.join(self.download_dir, file_basename))

            except Exception as e:
                self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                error_paths.append(path)

        return success_paths, error_paths
