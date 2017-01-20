import os
import shutil
import traceback

from lib.FileManager.workers.baseUploadWorker import BaseUploadWorker


class UploadFile(BaseUploadWorker):
    def __init__(self, path, file_path, overwrite, *args, **kwargs):
        super(UploadFile, self).__init__(file_path, *args, **kwargs)

        self.path = path
        self.overwrite = overwrite

    def run(self):
        try:
            # prepare source files before moving to target dir
            self._prepare()

            # drop privileges
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
