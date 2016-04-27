from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
import traceback


class NewFile(MainWorkerCustomer):

    def __init__(self, path, *args, **kwargs):
        super(NewFile, self).__init__(*args, **kwargs)

        self.path = path

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM NewFile worker run(), abs_path = %s" % abs_path)

            try:
                sftp = self.conn.open_sftp()

                pid = sftp.open(abs_path, 'w')
                if pid:
                    pid.close()
                    info = self._make_file_info(abs_path)
                else:
                    raise Exception('Cannot write file resource on server')

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
