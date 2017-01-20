import os

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class BaseUploadWorker(BaseWorkerCustomer):
    def __init__(self, file_path, *args, **kwargs):
        super(BaseUploadWorker, self).__init__(*args, **kwargs)

        self.file_path = file_path

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
        raise NotImplementedError()
