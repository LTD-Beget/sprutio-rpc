import os
import shutil
import threading
import time
import traceback

from config.main import TMP_DIR
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.SFTPConnection import SFTPConnection
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class MoveFromSftpToWebDav(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(MoveFromSftpToWebDav, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite
        self.operation_progress = {
            "total_done": False,
            "total": 0,
            "operation_done": False,
            "processed": 0,
            "previous_percent": 0
        }

    def run(self):
        try:
            self.preload()
            success_paths = []
            error_paths = []

            source_path = self.source.get('path')
            target_path = self.target.get('path')
            hash_str = self.random_hash()
            temp_path = TMP_DIR + '/' + self.login + '/' + hash_str + '/'

            if source_path is None:
                raise Exception("Source path empty")

            if target_path is None:
                raise Exception("Target path empty")

            self.logger.info("MoveFromSftpToWebDav process run source = %s , target = %s" % (source_path, target_path))

            target_webdav = WebDavConnection.create(self.login, self.target.get('server_id'), self.logger)
            source_sftp = SFTPConnection.create(self.login, self.source.get('server_id'), self.logger)
            t_total = threading.Thread(target=self.get_total, args=(self.operation_progress, self.paths))
            t_total.start()

            for path in self.paths:
                try:
                    success_paths, error_paths = self.copy_files_to_tmp(temp_path)

                    if len(error_paths) == 0:
                        abs_path = self.get_abs_path(path)
                        file_basename = os.path.basename(abs_path)
                        uploading_path = temp_path + file_basename
                        if os.path.isdir(uploading_path):
                            uploading_path += '/'
                            file_basename += '/'

                        upload_result = target_webdav.upload(uploading_path, target_path, self.overwrite, file_basename,
                                                             self.uploading_progress)

                        if upload_result['success']:
                            success_paths.append(path)
                            shutil.rmtree(temp_path, True)
                            source_sftp.remove(path)

                except Exception as e:
                    self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                    error_paths.append(path)

            self.operation_progress["operation_done"] = True

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

    def copy_files_to_tmp(self, target_path):
        if not os.path.exists(target_path):
            os.makedirs(target_path)

        sftp = SFTPConnection.create(self.login, self.source.get('server_id'), self.logger)

        success_paths = []
        error_paths = []

        for path in self.paths:
            try:
                sftp.rsync_from(path, target_path)
                success_paths.append(path)

            except Exception as e:
                self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                error_paths.append(path)

        return success_paths, error_paths

    def get_total(self, progress_object, paths, count_dirs=True, count_files=True):
        self.logger.debug("start get_total() dirs = %s , files = %s" % (count_dirs, count_files))
        sftp = SFTPConnection.create(self.login, self.source.get('server_id'), self.logger)
        for path in paths:
            try:
                abs_path = path

                for current, dirs, files in sftp.walk(abs_path):
                    if count_files:
                        progress_object["total"] += len(files)

                if sftp.isfile(abs_path):
                    progress_object["total"] += 1
            except Exception as e:
                self.logger.error("Error get_total file %s , error %s" % (str(path), str(e)))
                continue

        progress_object["total_done"] = True
        self.logger.debug("done get_total()")
        return

    def uploading_progress(self, download_t, download_d, upload_t, upload_d):
        try:
            percent_upload = 0
            if upload_t != 0:
                percent_upload = round(float(upload_d) / float(upload_t), 2)

            if percent_upload != self.operation_progress.get("previous_percent"):
                if percent_upload == 0 and self.operation_progress.get("previous_percent") != 0:
                    self.operation_progress["processed"] += 1
                self.operation_progress["previous_percent"] = percent_upload
                total_percent = percent_upload + self.operation_progress.get("processed")

                percent = round(float(total_percent) /
                                float(self.operation_progress.get("total")), 2)
                progress = {
                    'percent': percent,
                    'text': str(int(percent * 100)) + '%'
                }

                self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
        except Exception as ex:
            self.logger.error("Error in MoveFromSftpToWebDav uploading_progress(): %s, traceback = %s" %
                              (str(ex), traceback.format_exc()))
