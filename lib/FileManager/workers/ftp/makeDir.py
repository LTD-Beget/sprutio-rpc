from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
import traceback
import os


class MakeDir(MainWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(MakeDir, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM FTP MakeDir worker run(), abs_path = %s" % abs_path)

            ftp_connection = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)

            try:
                ftp_connection.mkdir(abs_path)
                info = ftp_connection.lstat(abs_path)
                fileinfo = {
                    "name": abs_path,
                    "mode": ftp_connection.getmode(info),
                    "mtime": info.st_mtime
                }

                result = {
                    "data": fileinfo,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)
            except Exception as e:
                result = FTPConnection.get_error(e, "File path already exists")
                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
