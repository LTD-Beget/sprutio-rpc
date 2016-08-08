from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
from misc.helperUnicode import as_bytes
import traceback
import pprint
import random
import os
import shutil


class WriteFile(BaseWorkerCustomer):
    def __init__(self, path, content, encoding, session, *args, **kwargs):
        super(WriteFile, self).__init__(*args, **kwargs)

        self.path = path
        self.content = as_bytes(content)
        self.encoding = encoding
        self.session = session

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            abs_path = self.path
            self.logger.debug("FM WriteFile worker run(), abs_path = %s" % abs_path)

            self.logger.debug("content %s" % pprint.pformat(self.content))
            self.logger.debug("encoding %s" % pprint.pformat(self.encoding))

            try:
                decoded = self.content.decode('utf-8')
                self.logger.debug("DECODED %s" % pprint.pformat(decoded))
            except Exception as e:
                self.logger.error("Error %s , %s" % (str(e), traceback.format_exc()))
                raise e

            temp_dir = os.path.abspath('/tmp/fm/' + self.random_hash())
            os.makedirs(temp_dir)

            try:
                content = decoded.encode(self.encoding)
                f = sftp.open(abs_path, 'wb')
                f.write(content)
                f.close()

            except Exception as e:
                self.logger.error(
                    "Error write file %s , error %s , %s" % (str(abs_path), str(e), traceback.format_exc()))
                raise Exception("Error saving file")

            shutil.rmtree(temp_dir, True)

            answer = {
                "item": sftp.make_file_info(abs_path),
                "encoding": self.encoding
            }

            result = {
                "data": answer,
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
