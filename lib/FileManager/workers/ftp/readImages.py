from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
from config.main import TMP_DIR
import traceback
import os


class ReadImages(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(ReadImages, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.info("FTP ReadImages process run")

            hash_str = self.random_hash()
            download_dir = TMP_DIR + '/images/' + self.login + '/' + hash_str + '/'
            success_paths, error_paths = self.copy_files_to_tmp(download_dir)

            if len(success_paths) == 1:
                one_file = True
            else:
                one_file = False

            if len(error_paths) == 0:  # Значит все хорошо, можно дальше обрабатывать

                file_list = {
                    "succeed": list(os.path.basename(filename) for filename in success_paths),
                    "failed": list(os.path.basename(filename) for filename in error_paths)
                }

                answer = {
                    "success": True,
                    "file_list": file_list,
                    "hash": hash_str,
                    "one_file": one_file,
                    "sid": self.login
                }

                result = {
                    "data": answer
                }

                self.on_success(result)
            else:
                raise Exception("read error")

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    def copy_files_to_tmp(self, target_path):
        if not os.path.exists(target_path):
            os.makedirs(target_path)

        ftp = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)

        success_paths = []
        error_paths = []

        for path in self.paths:
            try:
                abs_path = ftp.path.abspath(path)
                source_path = ftp.path.dirname(path)
                file_basename = ftp.path.basename(abs_path)

                if ftp.isdir(abs_path):
                    destination = os.path.join(target_path, file_basename)

                    if not os.path.exists(destination):
                        os.makedirs(destination)
                    else:
                        raise Exception("destination already exist")

                    for current, dirs, files in ftp.ftp.walk(ftp.to_string(abs_path)):
                        current = current.encode("ISO-8859-1").decode("UTF-8")
                        relative_root = os.path.relpath(current, source_path)

                        for d in dirs:
                            d = d.encode("ISO-8859-1").decode("UTF-8")
                            target_dir = os.path.join(target_path, relative_root, d)
                            if not os.path.exists(target_dir):
                                os.makedirs(target_dir)
                            else:
                                raise Exception("destination dir already exists")

                        for f in files:
                            f = f.encode("ISO-8859-1").decode("UTF-8")
                            source_file = os.path.join(current, f)
                            target_file_path = os.path.join(target_path, relative_root)
                            target_file = os.path.join(target_path, relative_root, f)
                            if not os.path.exists(target_file):
                                download_result = ftp.download(source_file, target_file_path)
                                if not download_result['success'] or len(
                                        download_result['file_list']['failed']) > 0:
                                    raise download_result['error'] if download_result[
                                                                          'error'] is not None else Exception(
                                            "Download error")
                            else:
                                raise Exception("destination file already exists")

                elif ftp.isfile(abs_path):
                    try:
                        target_file = os.path.join(target_path, file_basename)
                        if not os.path.exists(target_file):
                            download_result = ftp.download(abs_path, target_path)
                            if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                                raise download_result['error'] if download_result[
                                                                      'error'] is not None else Exception(
                                        "Download error")
                        else:
                            raise Exception("destination file already exists")

                    except Exception as e:
                        self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                        raise e

                success_paths.append(path)

            except Exception as e:
                self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                error_paths.append(path)

        return success_paths, error_paths
