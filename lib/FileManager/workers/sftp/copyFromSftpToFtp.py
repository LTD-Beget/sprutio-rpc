from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
from lib.FileManager.FTPConnection import FTPConnection
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.progress_helper import update_progress
import os
import traceback
import threading
import time


class CopyFromSftpToFtp(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyFromSftpToFtp, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.source)

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

            self.logger.info("CopyFromSftpToFtp process run source = %s , target = %s" % (source_path, target_path))
            ftp = self.get_ftp_connection(self.target)

            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=update_progress, args=(operation_progress,))
            t_progress.start()

            for path in self.paths:
                try:
                    abs_path = path
                    file_basename = os.path.basename(abs_path)

                    if sftp.isdir(abs_path):
                        destination = ftp.path.join(target_path, file_basename)

                        if not ftp.exists(destination):
                            ftp.mkdir(destination)
                        elif self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
                            ftp.remove(destination)
                            ftp.mkdir(destination)
                        elif not self.overwrite and ftp.exists(destination) and not ftp.isdir(destination):
                            raise Exception("destination is not a dir")
                        else:
                            pass

                        operation_progress["processed"] += 1

                        for current, dirs, files in sftp.walk(abs_path):
                            relative_root = os.path.relpath(current, source_path)
                            for d in dirs:
                                target_dir = ftp.path.join(target_path, relative_root, d)
                                if not ftp.exists(target_dir):
                                    ftp.mkdir(target_dir)
                                elif self.overwrite and ftp.exists(target_dir) and not ftp.isdir(target_dir):
                                    ftp.remove(target_dir)
                                    ftp.mkdir(target_dir)
                                elif not self.overwrite and ftp.exists(target_dir) and not ftp.isdir(
                                        target_dir):
                                    raise Exception("destination is not a dir")
                                else:
                                    pass
                                operation_progress["processed"] += 1

                            for f in files:
                                source_file = os.path.join(current, f)
                                target_file = ftp.path.join(target_path, relative_root, f)
                                if not ftp.exists(target_file):
                                    self.copy_from_sftp_to_ftp(source_file, target_file, sftp, ftp)
                                elif self.overwrite and ftp.exists(target_file) and not ftp.isdir(target_file):
                                    ftp.remove(target_file)
                                    self.copy_from_sftp_to_ftp(source_file, target_file, sftp, ftp)
                                elif self.overwrite and ftp.isdir(target_file):
                                    ftp.remove(target_file)
                                    self.copy_from_sftp_to_ftp(source_file, target_file, sftp, ftp)
                                else:
                                    pass
                                operation_progress["processed"] += 1

                    elif sftp.isfile(abs_path):
                        try:
                            target_file = ftp.path.join(target_path, file_basename)
                            if not ftp.exists(target_file):
                                self.copy_from_sftp_to_ftp(abs_path, target_file, sftp, ftp)
                            elif self.overwrite and ftp.exists(target_file) and not ftp.isdir(target_file):
                                ftp.remove(target_file)
                                self.copy_from_sftp_to_ftp(abs_path, target_file, sftp, ftp)
                            elif self.overwrite and ftp.isdir(target_file):
                                ftp.remove(target_file)
                                self.copy_from_sftp_to_ftp(abs_path, target_file, sftp, ftp)
                            else:
                                pass
                            operation_progress["processed"] += 1
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

        for path in paths:
            try:
                sftp = self.get_sftp_connection(self.source)
                abs_path = path

                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in sftp.walk(abs_path):
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
    def copy_from_sftp_to_ftp(source_path, target_path, sftp, ftp):
        with sftp.open(source_path, 'rb') as source_file_obj:
            with ftp.open(target_path, 'wb') as target_file_obj:
                ftp.ftp.copyfileobj(source_file_obj, target_file_obj)
