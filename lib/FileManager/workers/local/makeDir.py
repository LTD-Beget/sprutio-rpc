import os
import traceback

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class MakeDir(BaseWorkerCustomer):
    def __init__(self, path, *args, **kwargs):
        super(MakeDir, self).__init__(*args, **kwargs)

        self.path = path

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM MakeDir worker run(), abs_path = %s" % abs_path)

            try:
                os.mkdir(abs_path, 0o700)
                info = self._make_file_info(abs_path)
                info["name"] = self.path

                result = {
                    "data": info,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)
            except OSError:
                result = {
                    "error": True,
                    "message": "File path already exists",
                    "traceback": traceback.format_exc()
                }

                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
