from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback
import os


class NewFile(BaseWorkerCustomer):

    def __init__(self, path, *args, **kwargs):
        super(NewFile, self).__init__(*args, **kwargs)

        self.path = path

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM NewFile worker run(), abs_path = %s" % abs_path)

            try:
                if os.path.exists(abs_path):
                    raise OSError("File path already exists")

                fd = os.open(abs_path, os.O_CREAT, 0o600)
                os.close(fd)

                info = self._make_file_info(abs_path)
                info["name"] = abs_path

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
