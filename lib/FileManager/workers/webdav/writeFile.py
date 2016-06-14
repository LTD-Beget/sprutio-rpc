from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from misc.helperUnicode import as_bytes
import traceback
import pprint
import random
from config.main import TMP_DIR
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

            webdav_connection = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

            try:
                hash_str = self.random_hash()
                temp_dir = os.path.abspath(TMP_DIR + '/' + self.login + '/' + hash_str + '/')
                os.makedirs(temp_dir)
                self.logger.info("abs_path=%s, temp_dir=%s" % (abs_path, temp_dir))
                temp_filename = temp_dir + self.path
                self.logger.info("temp_filename=%s" % temp_filename)
            except Exception as e:
                result = WebDavConnection.get_error(e,
                                                 "Unable to make file backup before saving \"%s\"." % os.path.basename(
                                                     abs_path))
                self.on_error(result)
                return

            try:
                content = decoded.encode(self.encoding)
                self.create_local_file(temp_filename, content)

                self.logger.info("uploading source=%s, target=%s" % (temp_filename, abs_path))
                folder = webdav_connection.parent(self.path)
                webdav_connection.upload(temp_filename, folder, True)
                self.logger.info("webdav upload succeed")

                file_info = webdav_connection.generate_file_info(self.path)
                self.logger.info("getting file info succeed")
                self.logger.info("file_info=%s", file_info)

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
                    webdav_connection.upload(os.path.join(temp_dir, os.path.basename(abs_path)), abs_path, True)
                except Exception as e:
                    webdav_connection.close()
                    result = WebDavConnection.get_error(e,
                                                     "Unable to upload tmp file during write error \"%s\"."
                                                     % os.path.basename(abs_path))
                    self.on_error(result)
                    return

                webdav_connection.close()
                result = WebDavConnection.get_error(e, "Unable to write file \"%s\"." % os.path.basename(abs_path))
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

    def create_local_file(self, filename, content):
        f = open(filename, 'wb')
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
        f.close()

