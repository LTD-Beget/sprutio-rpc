from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
from misc.helperUnicode import as_bytes
import traceback
import pprint
import random
import os
import shutil


class WriteFile(MainWorkerCustomer):
    def __init__(self, path, content, encoding, *args, **kwargs):
        super(WriteFile, self).__init__(*args, **kwargs)

        self.path = path
        self.content = as_bytes(content)
        self.encoding = encoding

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM WriteFile worker run(), abs_path = %s" % abs_path)

            self.logger.debug("content %s" % pprint.pformat(self.content))
            self.logger.debug("encoding %s" % pprint.pformat(self.encoding))

            try:
                decoded = self.content.decode('utf-8')
                self.logger.debug("DECODED %s" % pprint.pformat(decoded))
            except Exception as e:
                self.logger.error("Error %s , %s" % (str(e), traceback.format_exc()))
                raise e

            try:
                temp_dir = os.path.abspath('/tmp/fm/' + self.random_hash())
                temp_path = temp_dir + "/" + self.random_hash()
                os.makedirs(temp_dir)

                sftp = self.conn.open_sftp()
                # copy remote file localy
                sftp.get(self.path, temp_path)
            except Exception as e:
                result = {"message": "Cant download remote file"}

            if isinstance(self.content, bytes):
                self.content = self.content.decode(self.encoding)

            with sftp.file(self.path, "w") as fd:
                fd.write(self.content)

            file_info = self._make_file_info(self.path)
            shutil.rmtree(temp_dir, True)

            file_result = {
                "encoding": self.encoding,
                "item": file_info
            }

            result = {
                "data": file_result,
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

    @staticmethod
    def random_hash():
        bit_hash = random.getrandbits(128)
        return "%032x" % bit_hash
