from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
import traceback


class RenameFile(BaseWorkerCustomer):

    def __init__(self, source_path, target_path, session, *args, **kwargs):
        super(RenameFile, self).__init__(*args, **kwargs)

        self.source_path = source_path
        self.target_path = target_path
        self.session = session

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)

            self.logger.debug("FM NewFile worker run(), source_abs_path = %s" % self.source_path)
            self.logger.debug("FM NewFile worker run(), target_abs_path = %s" % self.target_path)

            try:
                if not sftp.exists(self.source_path):
                    raise OSError("Source file path not exists")

                if sftp.exists(self.target_path):
                    raise OSError("Target file path already exists")

                source = sftp.make_file_info(self.source_path)

                sftp.sftp.rename(self.source_path, self.target_path)
                target = sftp.make_file_info(self.target_path)

                result = {
                    "data": {
                        "source": source,
                        "target": target
                    },
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)

            except OSError as e:
                result = {
                    "error": True,
                    "message": str(e),
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
