from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.progress_helper import update_progress
import os
import traceback
import threading
import shutil
import time


class CopyFromFtp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyFromFtp, self).__init__(*args, **kwargs)

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

            if source_path is None:
                raise Exception("Source path empty")

            if target_path is None:
                raise Exception("Target path empty")

            target_path = self.get_abs_path(target_path)

            self.logger.info("CopyFromFtp process run source = %s , target = %s" % (source_path, target_path))

            ftp = FTPConnection.create(self.login, self.source.get('server_id'), self.logger)
            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=update_progress, args=(operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    abs_path = ftp.path.abspath(path)
                    file_basename = ftp.path.basename(abs_path)

                    if ftp.isdir(abs_path):
                        destination = os.path.join(target_path, file_basename)

                        if not os.path.exists(destination):
                            os.makedirs(destination)
                        elif self.overwrite and os.path.exists(destination) and not os.path.isdir(destination):
                            shutil.rmtree(destination)
                            os.makedirs(destination)
                        elif not self.overwrite and os.path.exists(destination) and not os.path.isdir(destination):
                            raise Exception("destination is not a dir")
                        else:
                            pass

                        operation_progress["processed"] += 1

                        for current, dirs, files in ftp.ftp.walk(ftp.to_string(abs_path)):
                            current = current.encode("ISO-8859-1").decode("UTF-8")
                            relative_root = os.path.relpath(current, source_path)
                            for d in dirs:
                                d = d.encode("ISO-8859-1").decode("UTF-8")
                                target_dir = os.path.join(target_path, relative_root, d)
                                if not os.path.exists(target_dir):
                                    os.makedirs(target_dir)
                                elif self.overwrite and os.path.exists(target_dir) and not os.path.isdir(target_dir):
                                    shutil.rmtree(target_dir)
                                    os.makedirs(target_dir)
                                elif not self.overwrite and os.path.exists(target_dir) and not os.path.isdir(
                                        target_dir):
                                    raise Exception("destination is not a dir")
                                else:
                                    pass
                                operation_progress["processed"] += 1
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
                                elif self.overwrite and os.path.exists(target_file) and not os.path.isdir(target_file):
                                    download_result = ftp.download(source_file, target_file_path)
                                    if not download_result['success'] or len(
                                            download_result['file_list']['failed']) > 0:
                                        raise download_result['error'] if download_result[
                                                                              'error'] is not None else Exception(
                                            "Download error")
                                elif self.overwrite and os.path.isdir(target_file):
                                    """
                                    See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                    In case copy file when destination is dir
                                    """
                                    shutil.rmtree(target_file)
                                    download_result = ftp.download(source_file, target_file_path)
                                    if not download_result['success'] or len(
                                            download_result['file_list']['failed']) > 0:
                                        raise download_result['error'] if download_result[
                                                                              'error'] is not None else Exception(
                                            "Download error")
                                else:
                                    pass
                                operation_progress["processed"] += 1
                    elif ftp.isfile(abs_path):
                        try:
                            target_file = os.path.join(target_path, file_basename)
                            if not os.path.exists(target_file):
                                download_result = ftp.download(abs_path, target_path)
                                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                                    raise download_result['error'] if download_result[
                                                                          'error'] is not None else Exception(
                                        "Download error")
                            elif self.overwrite and os.path.exists(target_file) and not os.path.isdir(target_file):
                                download_result = ftp.download(abs_path, target_path)
                                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                                    raise download_result['error'] if download_result[
                                                                          'error'] is not None else Exception(
                                        "Download error")
                            elif self.overwrite and os.path.isdir(target_file):
                                """
                                See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                In case copy file when destination is dir
                                """
                                shutil.rmtree(target_file)
                                download_result = ftp.download(abs_path, target_path)
                                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                                    raise download_result['error'] if download_result[
                                                                          'error'] is not None else Exception(
                                        "Download error")
                            else:
                                pass

                        except Exception as e:
                            self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                            raise e
                        finally:
                            operation_progress["processed"] += 1

                    success_paths.append(path)

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

    def get_total(self, progress_object, paths, count_dirs=True, count_files=True):
        self.logger.debug("start get_total() dirs = %s , files = %s" % (count_dirs, count_files))
        ftp = FTPConnection.create(self.login, self.source.get('server_id'), self.logger)
        for path in paths:
            try:
                abs_path = ftp.path.abspath(path)
                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in ftp.ftp.walk(ftp.to_string(abs_path)):
                    if count_dirs:
                        progress_object["total"] += len(dirs)
                    if count_files:
                        progress_object["total"] += len(files)
            except Exception as e:
                self.logger.error("Error get_total file %s , error %s" % (str(path), str(e)))
                continue

        progress_object["total_done"] = True
        self.logger.debug("done get_total()")
        return
