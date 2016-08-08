from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
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
            self.logger.info("WebDav UploadFile process run")

            webdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

            target_file = "{0}{1}".format(self.path, os.path.basename(self.file_path))

            if not webdav.exists(target_file):
                upload_result = webdav.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
                os.remove(self.file_path)
            elif self.overwrite and webdav.exists(target_file) and not webdav.isdir(target_file):
                upload_result = webdav.upload(self.file_path, self.path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
                os.remove(self.file_path)
            elif self.overwrite and webdav.isdir(target_file):
                """
                See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                In case copy file when destination is dir
                """
                webdav.remove(target_file)
                upload_result = webdav.upload(self.file_path, self.path)
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

