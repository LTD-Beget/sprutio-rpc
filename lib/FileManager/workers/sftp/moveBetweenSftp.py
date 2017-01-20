import os
import stat
import threading
import time
import traceback

from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.workers.progress_helper import update_progress


class MoveBetweenSftp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(MoveBetweenSftp, self).__init__(*args, **kwargs)

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

            source_sftp = self.get_sftp_connection(self.source)
            target_sftp = self.get_sftp_connection(self.target)
            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=update_progress, args=(self, operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    abs_path = path
                    file_basename = os.path.basename(abs_path)

                    if source_sftp.isdir(abs_path):
                        destination = os.path.join(target_path, file_basename)

                        st = source_sftp.stat(abs_path)
                        if not target_sftp.exists(destination):
                            target_sftp.makedirs(destination, stat.S_IMODE(st.st_mode))
                        elif self.overwrite and target_sftp.exists(destination) and not target_sftp.isdir(destination):
                            target_sftp.remove(destination)
                            target_sftp.makedirs(destination, stat.S_IMODE(st.st_mode))
                        elif self.overwrite and target_sftp.isdir(destination):
                            target_sftp.sftp.chmod(destination, stat.S_IMODE(st.st_mode))
                        elif not self.overwrite and target_sftp.exists(destination) and not target_sftp.isdir(
                                destination):
                            raise Exception("destination is not a dir")
                        else:
                            pass

                        operation_progress["processed"] += 1

                        for current, dirs, files in source_sftp.walk(abs_path):
                            relative_root = os.path.relpath(current, source_path)
                            for d in dirs:
                                target_dir = os.path.join(target_path, relative_root, d)
                                st = source_sftp.stat(os.path.join(current, d))
                                if not target_sftp.exists(target_dir):
                                    target_sftp.makedirs(target_dir, stat.S_IMODE(st.st_mode))
                                elif self.overwrite and target_sftp.exists(target_dir) and not target_sftp.isdir(
                                        target_dir):
                                    target_sftp.remove(target_dir)
                                    target_sftp.makedirs(target_dir, stat.S_IMODE(st.st_mode))
                                elif self.overwrite and target_sftp.isdir(target_dir):
                                    target_sftp.sftp.chmod(target_dir, stat.S_IMODE(st.st_mode))
                                elif not self.overwrite and target_sftp.exists(target_dir) and not target_sftp.isdir(
                                        target_dir):
                                    raise Exception("destination is not a dir")
                                else:
                                    pass
                                operation_progress["processed"] += 1
                            for f in files:
                                source_file = os.path.join(current, f)
                                target_file = os.path.join(target_path, relative_root, f)
                                if not target_sftp.exists(target_file):
                                    self.move_between_sftp(source_sftp, target_sftp, source_file, target_file)
                                elif self.overwrite and target_sftp.exists(target_file) and not target_sftp.isdir(
                                        target_file):
                                    target_sftp.remove(target_file)
                                    self.move_between_sftp(source_sftp, target_sftp, source_file, target_file)
                                elif self.overwrite and target_sftp.isdir(target_file):
                                    target_sftp.rmtree(target_file)
                                    self.move_between_sftp(source_sftp, target_sftp, source_file, target_file)
                                else:
                                    pass
                                operation_progress["processed"] += 1
                    elif source_sftp.isfile(abs_path):
                        try:
                            target_file = os.path.join(target_path, file_basename)
                            if not target_sftp.exists(target_file):
                                self.move_between_sftp(source_sftp, target_sftp, abs_path, target_file)
                            elif self.overwrite and target_sftp.exists(target_file) and not target_sftp.isdir(
                                    target_file):
                                self.move_between_sftp(source_sftp, target_sftp, abs_path, target_file)
                            elif self.overwrite and target_sftp.isdir(target_file):
                                target_sftp.rmtree(target_file)
                                self.move_between_sftp(source_sftp, target_sftp, abs_path, target_file)
                            else:
                                pass

                        except Exception as e:
                            self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                            raise e
                        finally:
                            operation_progress["processed"] += 1

                    success_paths.append(path)
                    source_sftp.rmtree(abs_path)

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
        source_ftp = self.get_sftp_connection(self.source)
        for path in paths:
            try:
                abs_path = os.path.abspath(path)
                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in source_ftp.walk(abs_path):
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

    @staticmethod
    def move_between_sftp(source_sftp, target_sftp, source_path, target_path):
        """
        Скачивает файл с source_sftp/source_path
        закачивает на target_sftp/target_path
        удаляет скачанный файл
        :param SFTP source_sftp:
        :param SFTP target_sftp:
        :param str source_path:
        :param str target_path:
        :return:
        """
        with source_sftp.sftp.open(source_path) as fo:
            target_sftp.sftp.putfo(fo, target_path)
        source_sftp.remove(source_path)
