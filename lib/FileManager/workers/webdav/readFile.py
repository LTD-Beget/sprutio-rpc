import os
import traceback

from binaryornot.check import is_binary

from config.main import TMP_DIR
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from misc.helpers import detect_encoding


class ReadFile(BaseWorkerCustomer):
    def __init__(self, path, encoding, session, *args, **kwargs):
        super(ReadFile, self).__init__(*args, **kwargs)

        self.path = path
        self.encoding = encoding

        # чем больше - тем точнее определяется кодировка, но медленнее, 50000 - выбрано опытным путем
        self.charset_detect_buffer = 50000
        self.session = session
        self.webdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

    def run(self):
        try:
            self.preload()

            self.logger.debug("FM WebDav ReadFile worker run(), path = %s" % self.path)

            webdav_path = self.webdav.path(self.path)

            hash_str = self.random_hash()
            download_path = TMP_DIR + '/' + self.login + '/' + hash_str + '/'

            download_result = self.download_file_from_webdav(webdav_path, download_path)

            if download_result["success"]:
                filedir = self.webdav.parent(self.path)
                filename = self.path
                if filedir != '/':
                    filename = filename.replace(filedir, "", 1)
                read_path = (download_path + '/' + filename)
                if not os.path.exists(read_path):
                    raise OSError("File not downloaded")

                if is_binary(read_path):
                    raise OSError("File has binary content")

                with open(read_path, 'rb') as fd:
                    content = fd.read()

                encoding = detect_encoding(content, read_path, self.encoding, self.logger)

                answer = {
                    "item": self._make_file_info(read_path),
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

    def download_file_from_webdav(self, abs_path, target_path):
        try:
            if not os.path.exists(target_path):
                os.makedirs(target_path)
            download_result = self.webdav.download(abs_path, target_path)
            if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                raise download_result['error'] if download_result['error'] is not None else Exception("Download error")
        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
            raise e

        return download_result
