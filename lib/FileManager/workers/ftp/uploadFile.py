from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
import traceback
import os


class UploadFile(BaseWorkerCustomer):
    def __init__(self, path, file_path, overwrite, session, *args, **kwargs):
        super(UploadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.file_path = file_path
        self.overwrite = overwrite
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.info("FTP UploadFile process run")

            ftp = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)

            target_file = ftp.path.join(self.path, os.path.basename(self.file_path))

            if not ftp.path.exists(target_file):
                upload_result = ftp.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
                os.remove(self.file_path)
            elif self.overwrite and ftp.path.exists(target_file) and not ftp.path.isdir(target_file):
                upload_result = ftp.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
                os.remove(self.file_path)
            elif self.overwrite and ftp.path.isdir(target_file):
                ftp.remove(target_file)
                upload_result = ftp.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
                os.remove(self.file_path)
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
