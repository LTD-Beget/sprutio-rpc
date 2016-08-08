from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
import traceback


class NewFile(BaseWorkerCustomer):

    def __init__(self, path, session, *args, **kwargs):
        super(NewFile, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            abs_path = self.path
            self.logger.debug("FM NewFile worker run(), abs_path = %s" % abs_path)

            try:
                if sftp.exists(abs_path):
                    raise OSError("File path already exists")

                fd = sftp.open(abs_path, 'w')
                if fd:
                    fd.close()
                    info = sftp.make_file_info(abs_path)
                    info["name"] = abs_path
                else:
                    raise Exception('Cannot write file resource on server')

                result = {
                    "data": info,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)

            except OSError:
                result = {
                    "error": True,
                    "message": "File path already exists",
                    "traceback": traceback.format_exc()
                }

                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
