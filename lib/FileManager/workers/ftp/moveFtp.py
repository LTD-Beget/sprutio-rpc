from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
from lib.FileManager.workers.progress_helper import update_progress
from lib.FileManager.FM import REQUEST_DELAY
import traceback
import threading
import time


class MoveFtp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(MoveFtp, self).__init__(*args, **kwargs)

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

            self.logger.info("CopyFtp process run source = %s , target = %s" % (source_path, target_path))

            ftp = self.get_ftp_connection(self.target)
            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            # sleep for a while for better total counting
            time.sleep(REQUEST_DELAY)

            t_progress = threading.Thread(target=update_progress, args=(self, operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    abs_path = ftp.path.abspath(path)
                    file_basename = ftp.path.basename(abs_path)

                    if ftp.isdir(abs_path):
                        destination = ftp.path.join(target_path, file_basename)

                        if not ftp.exists(destination):
                            ftp.makedirs(destination)
                        elif self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
                            ftp.remove(destination)
                            ftp.makedirs(destination)
                        elif not self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
                            raise Exception("destination is not a dir")
                        else:
                            pass

                        operation_progress["processed"] += 1

                        for current, dirs, files in ftp.ftp.walk(abs_path):
                            current = current.encode("ISO-8859-1").decode("UTF-8")
                            relative_root = ftp.relative_root(source_path, current)
                            for d in dirs:
                                d = d.encode("ISO-8859-1").decode("UTF-8")
                                target_dir = ftp.path.join(target_path, relative_root, d)
                                if not ftp.exists(target_dir):
                                    ftp.makedirs(target_dir)
                                elif self.overwrite and ftp.exists(target_dir) and not ftp.isdir(target_dir):
                                    ftp.remove(target_dir)
                                    ftp.makedirs(target_dir)
                                elif not self.overwrite and ftp.exists(target_dir) and not ftp.isdir(
                                        target_dir):
                                    raise Exception("destination is not a dir")
                                else:
                                    pass
                                operation_progress["processed"] += 1
                            for f in files:
                                f = f.encode("ISO-8859-1").decode("UTF-8")
                                source_file = ftp.path.join(current, f)
                                target_file = ftp.path.join(target_path, relative_root, f)
                                if not ftp.exists(target_file):
                                    copy_result = ftp.copy_file(source_file, ftp.path.dirname(target_file),
                                                                overwrite=self.overwrite)
                                    if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                        raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                            "Copy error")
                                elif self.overwrite and ftp.exists(target_file) and not ftp.isdir(target_file):
                                    copy_result = ftp.copy_file(source_file, ftp.path.dirname(target_file),
                                                                overwrite=self.overwrite)
                                    if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                        raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                            "Copy error")
                                elif self.overwrite and ftp.isdir(target_file):
                                    """
                                    See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                    In case copy file when destination is dir
                                    """
                                    ftp.remove(target_file)
                                    copy_result = ftp.copy_file(source_file, ftp.path.dirname(target_file),
                                                                overwrite=self.overwrite)
                                    if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                        raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                            "Copy error")
                                else:
                                    pass
                                operation_progress["processed"] += 1
                    elif ftp.isfile(abs_path):
                        try:
                            target_file = ftp.path.join(target_path, file_basename)
                            if not ftp.exists(target_file):
                                copy_result = ftp.copy_file(abs_path, ftp.path.dirname(target_file),
                                                            overwrite=self.overwrite)
                                if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                    raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                        "Copy error")
                            elif self.overwrite and ftp.exists(target_file) and not ftp.isdir(target_file):
                                copy_result = ftp.copy_file(abs_path, ftp.path.dirname(target_file),
                                                            overwrite=self.overwrite)
                                if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                    raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                        "Copy error")
                            elif self.overwrite and ftp.isdir(target_file):
                                """
                                See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                In case copy file when destination is dir
                                """
                                ftp.remove(target_file)
                                copy_result = ftp.copy_file(abs_path, ftp.path.dirname(target_file),
                                                            overwrite=self.overwrite)
                                if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                                    raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                        "Copy error")
                            else:
                                pass
                            operation_progress["processed"] += 1
                        except Exception as e:
                            self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                            raise e
                        finally:
                            operation_progress["processed"] += 1

                    success_paths.append(path)
                    ftp.remove(path)

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
        ftp = self.get_ftp_connection(self.target)
        for path in paths:
            try:
                abs_path = ftp.path.abspath(path)
                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in ftp.ftp.walk(abs_path):
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
