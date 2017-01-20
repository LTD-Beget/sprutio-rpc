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
            self.logger.info("FTP UploadFile process run")

            ftp = self.get_ftp_connection(self.session)

            target_file = ftp.path.join(self.path, os.path.basename(self.file_path))

            if not ftp.path.exists(target_file):
                upload_result = ftp.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
            elif self.overwrite and ftp.path.exists(target_file):
                ftp.remove(target_file)
                upload_result = ftp.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")

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
