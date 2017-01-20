import fnmatch
import os
import traceback

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class FindFiles(BaseWorkerCustomer):
    def __init__(self, params, session, *args, **kwargs):
        super(FindFiles, self).__init__(*args, **kwargs)

        self.path = params.get('path', '/')
        self.session = session
        self.filename = params.get('filename', None)
        self.file_size = params.get('file_size', 0)
        self.type_dir = params.get('type_dir', True)
        self.type_file = params.get('type_file', True)
        self.size_direction = params.get('size_direction', 'more')

        self.params = params

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            result = []

            abs_path = self.path
            self.logger.debug("FM FindFiles worker run(), abs_path = %s" % abs_path)

            if not sftp.exists(abs_path):
                raise Exception("Provided path not exist")

            self.on_running(self.status_id, pid=self.pid, pname=self.name)

            for current, dirs, files in sftp.walk(abs_path):
                for f in files:
                    try:
                        if fnmatch.fnmatch(f, self.filename) and self.type_file:
                            if self.file_size == 0:
                                self.logger.debug("matched %s current = %s" % (f, current))
                                result.append(sftp.make_file_info(os.path.join(current, f)))
                            else:
                                stat_info = os.lstat(os.path.join(current, f))
                                size = stat_info.st_size
                                if self.size_direction == 'more':
                                    if size >= self.file_size:
                                        result.append(sftp.make_file_info(os.path.join(current, f)))
                                        self.logger.debug(
                                            "matched %s current = %s" % (f, current))
                                else:
                                    if size <= self.file_size:
                                        result.append(sftp.make_file_info(os.path.join(current, f)))
                                        self.logger.debug(
                                            "matched %s current = %s" % (f, current))

                    except UnicodeDecodeError as e:
                        self.logger.error("%s, %s" % (str(e), traceback.format_exc()))

                for d in dirs:
                    try:
                        if fnmatch.fnmatch(d, self.filename) and self.type_dir:
                            result.append(sftp.make_file_info(os.path.join(current, d)))
                            self.logger.debug("matched %s current = %s" % (d, current))
                    except UnicodeDecodeError as e:
                        self.logger.error("%s, %s" % (str(e), traceback.format_exc()))

            self.on_success(self.status_id, data=result, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
