import threading
import time
import traceback

from lib.FTP.FTP import transfer_between_ftp
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.workers.progress_helper import update_progress


class CopyBetweenFtp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyBetweenFtp, self).__init__(*args, **kwargs)

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

            self.logger.info("CopyFromFtp process run source = %s , target = %s" % (source_path, target_path))

            source_ftp = self.get_ftp_connection(self.source)
            target_ftp = self.get_ftp_connection(self.target)
            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=update_progress, args=(self, operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    abs_path = source_ftp.path.abspath(path)
                    file_basename = source_ftp.path.basename(abs_path)

                    if source_ftp.isdir(abs_path):
                        destination = target_ftp.path.join(target_path, file_basename)

                        if not target_ftp.exists(destination):
                            target_ftp.makedirs(destination)
                        elif self.overwrite and target_ftp.exists(destination) and not target_ftp.isdir(destination):
                            target_ftp.remove(destination)
                            target_ftp.makedirs(destination)
                        elif not self.overwrite and target_ftp.exists(destination) and not target_ftp.isdir(
                                destination):
                            raise Exception("destination is not a dir")
                        else:
                            pass

                        operation_progress["processed"] += 1

                        for current, dirs, files in source_ftp.ftp.walk(source_ftp.to_string(abs_path)):
                            current = current.encode("ISO-8859-1").decode("UTF-8")
                            relative_root = target_ftp.relative_root(source_path, current)
                            for d in dirs:
                                d = d.encode("ISO-8859-1").decode("UTF-8")
                                target_dir = target_ftp.path.join(target_path, relative_root, d)
                                if not target_ftp.exists(target_dir):
                                    target_ftp.makedirs(target_dir)
                                elif self.overwrite and target_ftp.exists(target_dir) and not target_ftp.isdir(
                                        target_dir):
                                    target_ftp.remove(target_dir)
                                    target_ftp.makedirs(target_dir)
                                elif not self.overwrite and target_ftp.exists(target_dir) and not target_ftp.isdir(
                                        target_dir):
                                    raise Exception("destination is not a dir")
                                else:
                                    pass
                                operation_progress["processed"] += 1
                            for f in files:
                                f = f.encode("ISO-8859-1").decode("UTF-8")
                                source_file = source_ftp.path.join(current, f)
                                target_file = target_ftp.path.join(target_path, relative_root, f)
                                if not target_ftp.exists(target_file):
                                    transfer_between_ftp(source_ftp, target_ftp, source_file, target_file)
                                elif self.overwrite and target_ftp.exists(target_file) and not target_ftp.isdir(
                                        target_file):
                                    transfer_between_ftp(source_ftp, target_ftp, source_file, target_file)
                                elif self.overwrite and target_ftp.isdir(target_file):
                                    """
                                    See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                    In case copy file when destination is dir
                                    """
                                    target_ftp.remove(target_file)
                                    transfer_between_ftp(source_ftp, target_ftp, source_file, target_file)
                                else:
                                    pass
                                operation_progress["processed"] += 1
                    elif source_ftp.isfile(abs_path):
                        try:
                            target_file = target_ftp.path.join(target_path, file_basename)
                            if not target_ftp.exists(target_file):
                                transfer_between_ftp(source_ftp, target_ftp, abs_path, target_file)
                            elif self.overwrite and target_ftp.exists(target_file) and not target_ftp.isdir(
                                    target_file):
                                transfer_between_ftp(source_ftp, target_ftp, abs_path, target_file)
                            elif self.overwrite and target_ftp.isdir(target_file):
                                """
                                See https://docs.python.org/3.4/library/shutil.html?highlight=shutil#shutil.copy
                                In case copy file when destination is dir
                                """
                                target_ftp.remove(target_file)
                                transfer_between_ftp(source_ftp, target_ftp, abs_path, target_file)
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
        source_ftp = self.get_ftp_connection(self.source)
        for path in paths:
            try:
                abs_path = source_ftp.path.abspath(path)
                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in source_ftp.ftp.walk(source_ftp.to_string(abs_path)):
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
