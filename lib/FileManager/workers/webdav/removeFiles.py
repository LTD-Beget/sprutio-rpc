import time
import traceback

from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class RemoveFiles(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(RemoveFiles, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = session

    def run(self):
        try:
            self.preload()
            success_paths = []
            error_paths = []

            next_tick = time.time() + REQUEST_DELAY
            webdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

            for path in self.paths:
                try:
                    webdav.remove(path)

                    success_paths.append(path)

                    if time.time() > next_tick:
                        progress = {
                            'percent': round(float(len(success_paths)) / float(len(self.paths)), 2),
                            'text': str(int(round(float(len(success_paths)) / float(len(self.paths)), 2) * 100)) + '%'
                        }

                        self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                        next_tick = time.time() + REQUEST_DELAY

                except Exception as e:
                    self.logger.error("Error removing file %s , error %s" % (str(path), str(e)))
                    error_paths.append(path)

            result = {
                "success": success_paths,
                "errors": error_paths
            }

            progress = {
                'percent': round(float(len(success_paths)) / float(len(self.paths)), 2),
                'text': str(int(round(float(len(success_paths)) / float(len(self.paths)), 2) * 100)) + '%'
            }

            self.on_success(self.status_id, data=result, progress=progress, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
