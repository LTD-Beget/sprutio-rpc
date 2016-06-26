from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FTPConnection import FTPConnection
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.progress_helper import update_progress
import traceback
import threading
import time
import os
import shutil
from config.main import TMP_DIR


class MoveFromWebDavToFtp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(MoveFromWebDavToFtp, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite

    def run(self):
        try:
            self.preload()
            success_paths = []
            error_paths = []

            operation_progress = {
                "total_done": False,
                "total": 0,
                "operation_done": False,
                "processed": 0
            }

            source_path = self.source.get('path')
            target_path = self.target.get('path')
            hash_str = self.random_hash()
            temp_path = TMP_DIR + '/' + self.login + '/' + hash_str + '/'

            if source_path is None:
                raise Exception("Source path empty")

            if target_path is None:
                raise Exception("Target path empty")

            self.logger.info("MoveFromWebDavToFTP process run source = %s , target = %s" % (source_path, target_path))

            source_webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
            target_ftp = FTPConnection.create(self.login, self.target.get('server_id'), self.logger)
            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=update_progress, args=(operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    download_result = self.download_file_from_webdav(path, temp_path, source_webdav)

                    if download_result["success"]:
                        filedir = source_webdav.parent(path)
                        filename = path
                        if source_webdav.isdir(path):
                            filename = path + '/'
                        if filedir != '/':
                            filename = filename.replace(filedir, "", 1)
                        read_path = (temp_path + filename)
                        if not os.path.exists(read_path):
                            raise OSError("File not downloaded")

                        upload_success, upload_error = self.upload_files_recursive_to_ftp(
                            filename, temp_path, target_path, target_ftp, operation_progress)
                        if len(upload_error) == 0:
                            success_paths.append(path)
                            shutil.rmtree(temp_path, True)
                            source_webdav.remove(path)

                except Exception as e:
                    self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                    error_paths.append(path)

            operation_progress["operation_done"] = True

            result = {
                "success": success_paths,
                "errors": error_paths
            }

            # иначе пользователям кажется что скопировалось не полностью )
            progress = {
                'percent': round(float(len(success_paths)) / float(len(self.paths)), 2),
                'text': str(int(round(float(len(success_paths)) / float(len(self.paths)), 2) * 100)) + '%'
            }
            time.sleep(REQUEST_DELAY)
            self.on_success(self.status_id, data=result, progress=progress, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)

    def upload_files_recursive_to_ftp(self, path, read_path, target_path, ftp, operation_progress):
        success_paths = []
        error_paths = []
        upload_result = {}
        try:
            abs_path = self.get_abs_path(path)
            file_basename = os.path.basename(abs_path)
            read_file_path = os.path.join(read_path, file_basename)

            if os.path.isdir(read_file_path):
                destination = ftp.path.join(target_path, file_basename)
                self.make_directory_on_ftp(destination, ftp)
                operation_progress["processed"] += 1

                for current, dirs, files in os.walk(read_file_path):
                    relative_root = os.path.relpath(current, read_path)
                    for d in dirs:
                        next_directory = ftp.path.join(destination, d)
                        self.make_directory_on_ftp(next_directory, ftp)
                    for f in files:
                        source_file = os.path.join(current, f)
                        target_file_path = ftp.path.join(target_path, relative_root)
                        target_file = ftp.path.join(target_file_path, f)
                        self.upload_file_to_ftp(source_file, target_file_path, target_file, ftp, operation_progress)

            elif os.path.isfile(read_file_path):
                target_file = ftp.path.join(target_path, file_basename)
                self.upload_file_to_ftp(read_file_path, target_path, target_file, ftp, operation_progress)

            success_paths.append(path)

        except Exception as e:
            self.logger.error(
                "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
            error_paths.append(path)

        return success_paths, error_paths

    def make_directory_on_ftp(self, destination, ftp):
        if not ftp.exists(destination):
            ftp.mkdir(destination)
        elif self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
            ftp.remove(destination)
            ftp.mkdir(destination)
        elif not self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
            raise Exception("destination is not a dir")
        else:
            pass

    def upload_file_to_ftp(self, read_file_path, target_path, target_file, ftp, operation_progress):
        try:
            if not ftp.exists(target_file):
                upload_result = ftp.upload(read_file_path, target_path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
            elif self.overwrite and ftp.exists(target_file) and not ftp.isdir(target_file):
                ftp.remove(target_file)
                upload_result = ftp.upload(read_file_path, target_path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
            elif self.overwrite and ftp.isdir(target_file):
                ftp.remove(target_file)
                upload_result = ftp.upload(read_file_path, target_path)
                if not upload_result['success'] or len(upload_result['file_list']['failed']) > 0:
                    raise upload_result['error'] if upload_result['error'] is not None else Exception(
                        "Upload error")
            else:
                pass
            operation_progress["processed"] += 1
        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (target_file, str(e)))
            raise e
        finally:
            operation_progress["processed"] += 1

    def download_file_from_webdav(self, abs_path, target_path, webdav):
        try:
            if not os.path.exists(target_path):
                os.makedirs(target_path)
            download_result = webdav.download(abs_path, target_path)
            if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                raise download_result['error'] if download_result['error'] is not None else Exception("Download error")
        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
            raise e

        return download_result

    def get_total(self, progress_object, paths, count_dirs=True, count_files=True):
        self.logger.debug("start get_total() dirs = %s , files = %s" % (count_dirs, count_files))
        source_webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
        for path in paths:
            try:
                abs_path = path
                if count_dirs:
                    progress_object["total"] += 1

                for files in source_webdav.listdir(abs_path):
                    progress_object["total"] += 1
            except Exception as e:
                self.logger.error("Error get_total file %s , error %s" % (str(path), str(e)))
                continue

        progress_object["total_done"] = True
        self.logger.debug("done get_total()")
        return


