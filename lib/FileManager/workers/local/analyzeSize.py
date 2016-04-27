from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
from misc.helpers import get_util
from misc.helpers import SubprocessRunner
import traceback
import os
import re


class AnalyzeSize(MainWorkerCustomer):
    def __init__(self, path, *args, **kwargs):
        super(AnalyzeSize, self).__init__(*args, **kwargs)

        self.path = path

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM AnalyzeSize worker run(), abs_path = %s" % abs_path)

            if not self.ssh_manager.exists(abs_path):
                raise Exception("Provided path not exist")

            result = []
            filelist = self.ssh_manager.sftp.listdir(abs_path)
            self.logger.debug("filelist = %s" % filelist)

            self.on_running(self.status_id, pid=self.pid, pname=self.name)

            if len(filelist) > 0:

                abs_filelist = []
                for f in filelist:
                    abs_f = os.path.join(abs_path, f)
                    abs_filelist.append("'{}'".format(abs_f))

                du = get_util('du')
                command = [du, "-B", "1", "-x", "-P", "-s"] + abs_filelist
                du_regex = re.compile('^([0-9]+)\s+(.*)$', re.UNICODE | re.IGNORECASE)

                full_command = " ".join(command)
                stdout = self.ssh_manager.conn.run(full_command).stdout
                print("stdout", stdout)

                for line in stdout.decode().split("\n"):
                    try:
                        match = du_regex.match(line)

                        if match is None:
                            continue

                        match_array = match.groups()

                        size = int(match_array[0])
                        path = match_array[1]

                        fileinfo = self._make_file_info(path)
                        fileinfo['size'] = size

                        result.append(fileinfo)

                    except Exception as e:
                        self.logger.error("Exception %s, %s" % (str(e), traceback.format_exc()))

            self.on_success(self.status_id, data=result, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
