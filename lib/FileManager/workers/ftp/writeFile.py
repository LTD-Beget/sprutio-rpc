import os
import pprint
import random
import shutil
import traceback

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from misc.helperUnicode import as_bytes


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
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM WriteFile worker run(), abs_path = %s" % abs_path)

            self.logger.debug("content %s" % pprint.pformat(self.content))
            self.logger.debug("encoding %s" % pprint.pformat(self.encoding))

            try:
                decoded = self.content.decode('utf-8')
                self.logger.debug("DECODED %s" % pprint.pformat(decoded))
            except Exception as e:
                self.logger.error("Error %s , %s" % (str(e), traceback.format_exc()))
                raise e

            ftp_connection = self.get_ftp_connection(self.session)

            try:
                temp_dir = os.path.abspath('/tmp/fm/' + self.random_hash())
                os.makedirs(temp_dir)
                ftp_connection.download(abs_path, temp_dir)
            except Exception as e:
                ftp_connection.close()
                result = ftp_connection.get_error(e,
                                                 "Unable to make file backup before saving \"%s\"." % os.path.basename(
                                                     abs_path))
                self.on_error(result)
                return

            try:
                if isinstance(self.content, bytes):
                    self.content = self.content.decode(self.encoding)

                with ftp_connection.open(abs_path, 'w', self.encoding) as fd:
                    fd.write(self.content)

                file_info = ftp_connection.file_info(abs_path)
                ftp_connection.close()

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
                try:
                    ftp_connection.upload(os.path.join(temp_dir, os.path.basename(abs_path)), abs_path, True)
                except Exception as e:
                    ftp_connection.close()
                    result = ftp_connection.get_error(e,
                                                      "Unable to upload tmp file during write error \"%s\"."
                                                      % os.path.basename(abs_path))
                    self.on_error(result)
                    return

                ftp_connection.close()
                result = ftp_connection.get_error(e, "Unable to write file \"%s\"." % os.path.basename(abs_path))
                self.on_error(result)
                return

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
