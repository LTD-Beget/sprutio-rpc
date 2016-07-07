from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback
from misc.helpers import detect_encoding


class ReadFile(BaseWorkerCustomer):
    def __init__(self, path, encoding, session, *args, **kwargs):
        super(ReadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session
        self.encoding = encoding

        # чем больше - тем точнее определяется кодировка, но медленнее, 50000 - выбрано опытным путем
        self.charset_detect_buffer = 50000

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            abs_path = self.path
            self.logger.debug("FM ReadFile worker run(), abs_path = %s" % abs_path)

            if not sftp.exists(abs_path):
                raise OSError("File not exists")

            if sftp.is_binary(abs_path):
                raise OSError("File has binary content")

            with sftp.open(abs_path, 'rb') as fd:
                content = fd.read()

            encoding = detect_encoding(content, abs_path, self.encoding, self.logger)

            answer = {
                "item": sftp.make_file_info(abs_path),
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
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
