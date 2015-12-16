from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager import FM
import traceback
import os
from binaryornot.check import is_binary
import chardet
import re


class ReadFile(BaseWorkerCustomer):
    def __init__(self, path, *args, **kwargs):
        super(ReadFile, self).__init__(*args, **kwargs)

        self.path = path

        # чем больше - тем точнее определяется кодировка, но медленнее, 50000 - выбрано опытным путем
        self.charset_detect_buffer = 50000

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM ReadFile worker run(), abs_path = %s" % abs_path)

            if not os.path.exists(abs_path):
                raise OSError("File not exists")

            if is_binary(abs_path):
                raise OSError("File has binary content")

            with open(abs_path, 'rb') as fd:
                content = fd.read()

            # part of file content for charset detection
            part_content = content[0:self.charset_detect_buffer] + content[-self.charset_detect_buffer:]
            chardet_result = chardet.detect(part_content)
            detected = chardet_result["encoding"]
            confidence = chardet_result["confidence"]

            self.logger.debug("Detected encoding = %s (%s), %s" % (detected, confidence, abs_path))

            # костыль пока не соберем нормальную версию libmagick >= 5.10
            # https://github.com/ahupp/python-magic/issues/47
            #
            # так же можно собрать uchardet от Mozilla, пока изучаю ее (тоже свои косяки),
            # кстати ее порт на python chardet мы юзаем, а их сайт уже мертв :(
            re_utf8 = re.compile('.*charset\s*=\s*utf\-8.*', re.UNICODE | re.IGNORECASE | re.MULTILINE)
            html_ext = ['htm', 'html', 'phtml', 'php', 'inc', 'tpl', 'xml']
            file_ext = os.path.splitext(abs_path)[1][1:].strip().lower()
            self.logger.debug("File ext = %s" % file_ext)

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
            self.logger.debug("Result encoding = %s, %s" % (encoding, abs_path))

            answer = {
                "item": self._make_file_info(abs_path),
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
