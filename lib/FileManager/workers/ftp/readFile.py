import os
import traceback

from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from misc.helpers import detect_encoding


class ReadFile(BaseWorkerCustomer):
    def __init__(self, path, encoding, session, *args, **kwargs):
        super(ReadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.encoding = encoding
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = os.path.abspath(self.path)
            self.logger.debug("FM FTP ReadFile worker run(), abs_path = %s" % abs_path)

            ftp_connection = self.get_ftp_connection(self.session)

            try:
                with ftp_connection.open(abs_path) as fd:
                    content = fd.read()

                encoding = detect_encoding(content, abs_path, self.encoding, self.logger)

                answer = {
                    "item": ftp_connection.file_info(abs_path),
                    "content": content,
                    "encoding": encoding
                }

                result = {
                    "data": answer,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)
            except Exception as e:
                result = ftp_connection.get_error(e, "Unable to open file \"%s\"." % os.path.basename(abs_path))
                self.on_error(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
