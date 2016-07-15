from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
import traceback
import os


class RenameFile(BaseWorkerCustomer):

    def __init__(self, source_path, target_path, session, *args, **kwargs):
        super(RenameFile, self).__init__(*args, **kwargs)

        self.source_path = source_path
        self.target_path = target_path
        self.session = session

    def run(self):
        try:
            self.preload()

            source_abs_path = os.path.abspath(self.source_path)
            target_abs_path = os.path.abspath(self.target_path)

            self.logger.debug("FM FTP NewFile worker run(), source_abs_path = %s" % source_abs_path)
            self.logger.debug("FM FTP NewFile worker run(), target_abs_path = %s" % target_abs_path)

            ftp_connection = self.get_ftp_connection(self.session)

            try:
                source_info = ftp_connection.file_info(source_abs_path)

                ftp_connection.rename(source_abs_path, target_abs_path)
                ftp_connection.clear_cache()
                target_info = ftp_connection.file_info(target_abs_path)

                ftp_result = {
                    "source": source_info,
                    "target": target_info
                }

                result = {
                    "data": ftp_result,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)

            except Exception as e:
                result = FTPConnection.get_error(e, "Unable to rename source element.")
                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
