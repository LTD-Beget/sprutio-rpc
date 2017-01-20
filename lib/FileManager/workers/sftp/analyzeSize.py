import os
import re
import traceback

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from misc.helpers import get_util


class AnalyzeSize(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(AnalyzeSize, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            self.logger.debug("FM AnalyzeSize worker run(), abs_path = %s" % self.path)

            if not sftp.exists(self.path):
                raise Exception("Provided path not exist")

            result = []
            file_list = sftp.sftp.listdir(self.path)
            self.logger.debug("file_list = %s" % file_list)

            self.on_running(self.status_id, pid=self.pid, pname=self.name)

            if len(file_list) > 0:

                abs_file_list = []
                for f in file_list:
                    abs_f = os.path.join(self.path, f)
                    abs_file_list.append("'{}'".format(abs_f))

                du = get_util('du')
                command = [du, "-B", "1", "-x", "-P", "-s", "--"] + abs_file_list
                du_regex = re.compile('^([0-9]+)\s+(.*)$', re.UNICODE | re.IGNORECASE)

                full_command = " ".join(command)
                stdout = sftp.run(full_command).stdout
                print("stdout", stdout)

                for line in stdout.decode().split("\n"):
                    try:
                        match = du_regex.match(line)

                        if match is None:
                            continue

                        match_array = match.groups()

                        size = int(match_array[0])
                        path = match_array[1]

                        file_info = sftp.make_file_info(path)
                        file_info['size'] = size

                        result.append(file_info)

                    except Exception as e:
                        self.logger.error("Exception %s, %s" % (str(e), traceback.format_exc()))

            self.on_success(self.status_id, data=result, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
