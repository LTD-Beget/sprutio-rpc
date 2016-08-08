from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager import FM
from config.main import TMP_DIR
import traceback
import os
from binaryornot.check import is_binary
import chardet
import re


class ReadFile(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(ReadFile, self).__init__(*args, **kwargs)

        self.path = path

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

                # part of file content for charset detection
                part_content = content[0:self.charset_detect_buffer] + content[-self.charset_detect_buffer:]
                chardet_result = chardet.detect(part_content)
                detected = chardet_result["encoding"]
                confidence = chardet_result["confidence"]

                self.logger.debug("Detected encoding = %s (%s), %s" % (detected, confidence, read_path))

                # костыль пока не соберем нормальную версию libmagick >= 5.10
                # https://github.com/ahupp/python-magic/issues/47
                #
                # так же можно собрать uchardet от Mozilla, пока изучаю ее (тоже свои косяки),
                # кстати ее порт на python chardet мы юзаем, а их сайт уже мертв :(
                re_utf8 = re.compile('.*charset\s*=\s*utf\-8.*', re.UNICODE | re.IGNORECASE | re.MULTILINE)
                html_ext = ['htm', 'html', 'phtml', 'php', 'inc', 'tpl', 'xml']
                file_ext = os.path.splitext(read_path)[1][1:].strip().lower()

                if confidence > 0.75 and detected != 'windows-1251' and detected != FM.DEFAULT_ENCODING:
                    if detected == "ISO-8859-7":
                        detected = "windows-1251"

                    if detected == "ISO-8859-2":
                        detected = "utf-8"

                    if detected == "ascii":
                        detected = "utf-8"

                    if detected == "MacCyrillic":
                        detected = "windows-1251"

                    # если все же ошиблись - костыль на указанный в файле charset
                    if detected != FM.DEFAULT_ENCODING and file_ext in html_ext:
                        result_of_search = re_utf8.search(part_content)
                        self.logger.debug(result_of_search)
                        if result_of_search is not None:
                            self.logger.debug("matched utf-8 charset")
                            detected = FM.DEFAULT_ENCODING
                        else:
                            self.logger.debug("not matched utf-8 charset")

                elif confidence > 0.60 and detected != 'windows-1251' and detected != FM.DEFAULT_ENCODING:
                    if detected == "ISO-8859-2":
                        detected = "windows-1251"

                    if detected == "MacCyrillic":
                        detected = "windows-1251"

                    # если все же ошиблись - костыль на указанный в файле charset
                    if detected != FM.DEFAULT_ENCODING and file_ext in html_ext:
                        result_of_search = re_utf8.search(part_content)
                        self.logger.debug(result_of_search)
                        if result_of_search is not None:
                            self.logger.debug("matched utf-8 charset")
                            detected = FM.DEFAULT_ENCODING
                        else:
                            self.logger.debug("not matched utf-8 charset")

                elif detected == 'windows-1251' or detected == FM.DEFAULT_ENCODING:
                    pass
                else:
                    detected = FM.DEFAULT_ENCODING

                encoding = detected if (detected or "").lower() in FM.encodings else FM.DEFAULT_ENCODING
                self.logger.debug("Result encoding = %s, %s" % (encoding, read_path))

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
