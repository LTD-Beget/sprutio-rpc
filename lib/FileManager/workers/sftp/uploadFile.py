import os
import traceback

from lib.FileManager.workers.baseUploadWorker import BaseUploadWorker


class UploadFile(BaseUploadWorker):
    def __init__(self, path, file_path, overwrite, session, *args, **kwargs):
        super(UploadFile, self).__init__(file_path, *args, **kwargs)

        self.path = path
        self.overwrite = overwrite
        self.session = session

    def run(self):
        try:
            # prepare source files before moving to target dir
            self._prepare()

            self.preload()
            sftp = self.get_sftp_connection(self.session)
            self.logger.info("SFTP UploadFile process run")

            target_file = os.path.join(self.path, os.path.basename(self.file_path))
            abs_target = target_file

            if not sftp.exists(abs_target):
                sftp.sftp.put(self.file_path, abs_target)
            elif self.overwrite and sftp.exists(abs_target) and not sftp.isdir(abs_target):
                sftp.sftp.put(self.file_path, abs_target)
            elif self.overwrite and sftp.isdir(abs_target):
                sftp.rmtree(abs_target)
                sftp.sftp.put(self.file_path, abs_target)

            os.remove(self.file_path)

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
