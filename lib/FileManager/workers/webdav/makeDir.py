from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
import traceback
import os


class MakeDir(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(MakeDir, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM WebDav MakeDir worker run(), abs_path = %s" % abs_path)

            webdav_connection = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

            try:
                webdav_connection.mkdir(abs_path)
                info = webdav_connection.info(abs_path)
                fileinfo = {
                    "name": abs_path,
                    "mode": webdav_connection.getmode(info),
                    "mtime": str(info['modified'])
                }

                result = {
                    "data": fileinfo,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)
            except Exception as e:
                result = WebDavConnection.get_error(e, "File path already exists")
                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

