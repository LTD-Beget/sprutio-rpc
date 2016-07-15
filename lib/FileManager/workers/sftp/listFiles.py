from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
import os
import traceback


class ListFiles(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(ListFiles, self).__init__(*args, **kwargs)

        self.path = os.path.normpath(path)
        if self.path == '/':
            self.path = '.'
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.debug("FM ListFiles worker run(), abs_path = %s" % self.path)
            sftp = self.get_sftp_connection(self.session)

            info = sftp.list(path=self.path)

            result = {
                "data": {
                    # 'path': self.path,
                    'is_share': False,
                    'is_share_write': False,
                    # 'items': items
                },
                "error": False,
                "message": None,
                "traceback": None
            }
            result["data"].update(info)

            self.on_success(result)
            return result

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
