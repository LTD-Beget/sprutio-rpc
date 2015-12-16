from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback
import os
import shutil


class UploadFile(BaseWorkerCustomer):
    def __init__(self, path, file_path, overwrite, *args, **kwargs):
        super(UploadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.file_path = file_path
        self.overwrite = overwrite

    def run(self):
        try:
            self.preload()
            self.logger.info("Local UploadFile process run")

            target_file = os.path.join(self.path, os.path.basename(self.file_path))
            abs_target = self.get_abs_path(target_file)

            if not os.path.exists(abs_target):
                shutil.move(self.file_path, abs_target)
            elif self.overwrite and os.path.exists(abs_target) and not os.path.isdir(abs_target):
                shutil.move(self.file_path, abs_target)
            elif self.overwrite and os.path.isdir(abs_target):
                """
                See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                In case copy file when destination is dir
                """
                shutil.rmtree(abs_target)
                shutil.move(self.file_path, abs_target)
            else:
                pass

            result = {
                "success": True
            }

            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
