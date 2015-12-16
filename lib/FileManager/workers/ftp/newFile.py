from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
import traceback
import os


class NewFile(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(NewFile, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM FTP NewFile worker run(), abs_path = %s" % abs_path)

            ftp_connection = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)

            try:
                if ftp_connection.path.exists(abs_path):
                    raise Exception("File with target name already exists")

                pid = ftp_connection.open(abs_path, 'w')

                if pid:
                    pid.close()
                    info = ftp_connection.lstat(abs_path)
                    result = {
                        "name": abs_path,
                        "mode": ftp_connection.getmode(info),
                        "mtime": info.st_mtime
                    }
                else:
                    raise Exception('Cannot write file resource on FTP server')

                result = {
                    "data": result,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)

            except Exception as e:
                result = FTPConnection.get_error(e, str(e))
                return self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
