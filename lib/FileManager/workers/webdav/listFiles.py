import os
import traceback

from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class ListFiles(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(ListFiles, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM WebDav ListFiles worker run(), abs_path = %s" % abs_path)

            webdav_connection = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)
            listing = webdav_connection.list(path=abs_path)

            result = {
                "data": listing,
                "error": False,
                "message": None,
                "traceback": None
            }

            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
