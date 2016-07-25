from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
import traceback
import os


class UploadFile(BaseWorkerCustomer):
    def __init__(self, path, file_path, overwrite, session, *args, **kwargs):
        super(UploadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.file_path = file_path
        self.overwrite = overwrite
        self.session = session

    def _prepare(self):
        if os.path.islink(self.file_path):
            raise Exception('Symlinks are not allowed!')

        pw = self._get_login_pw()

        # allow writing to parent dir
        os.lchown(os.path.dirname(self.file_path), pw.pw_uid, pw.pw_gid)

        if os.path.isdir(self.file_path):
            for root, dirs, files in os.walk(self.file_path):
                for item in dirs + files:
                    os.lchown(item, pw.pw_uid, pw.pw_gid)
        else:
            os.lchown(self.file_path, pw.pw_uid, pw.pw_gid)

    def run(self):
        try:
            self._prepare()
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            self.logger.info("SFTP UploadFile process run")

            target_file = os.path.join(self.path, os.path.basename(self.file_path))
            abs_target = target_file

            if not sftp.exists(abs_target):
                sftp.sftp.put(self.file_path, abs_target)
                os.remove(self.file_path)
            elif self.overwrite and sftp.exists(abs_target) and not sftp.isdir(abs_target):
                sftp.sftp.put(self.file_path, abs_target)
                os.remove(self.file_path)
            elif self.overwrite and sftp.isdir(abs_target):
                sftp.rmtree(abs_target)
                sftp.sftp.put(self.file_path, abs_target)
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
