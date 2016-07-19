from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import os
import traceback
import threading
import time
import shutil


class MoveToWebDav(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(MoveToWebDav, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite
        self.operation_progress = {
            "total_done": False,
            "total": 0,
            "operation_done": False,
            "processed": 0,
            "file_uploading": 0,
            "previous_percent": 0
        }

    def run(self):
        try:
            self.preload()
            success_paths = []
            error_paths = []

            source_path = self.source.get('path')
            target_path = self.target.get('path')

            if source_path is None:
                raise Exception("Source path empty")

            if target_path is None:
                raise Exception("Target path empty")

            source_path = self.get_abs_path(source_path)
            webdav = WebDavConnection.create(self.login, self.target.get('server_id'), self.logger)

            self.logger.info("MoveToWebDav process run source = %s , target = %s" % (source_path, target_path))

            t_total = threading.Thread(target=self.get_total, args=(self.operation_progress, self.paths))
            t_total.start()

            for path in self.paths:
                try:
                    abs_path = self.get_abs_path(path)
                    file_basename = os.path.basename(abs_path)

                    uploading_path = abs_path
                    if os.path.isdir(abs_path):
                        uploading_path += '/'
                        file_basename += '/'

                    self.operation_progress["file_uploading"] += 1

                    result_upload = webdav.upload(uploading_path, target_path, self.overwrite, file_basename,
                                                  self.uploading_progress)

                    if result_upload['success']:
                        self.operation_progress["processed"] += 1
                        success_paths.append(path)
                        if os.path.isfile(abs_path):
                            os.remove(abs_path)
                        elif os.path.islink(abs_path):
                            os.unlink(abs_path)
                        elif os.path.isdir(abs_path):
                            shutil.rmtree(abs_path)
                        else:
                            error_paths.append(abs_path)
                            break

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

    def get_total(self, progress_object, paths, count_dirs=True, count_files=True):

        self.logger.debug("start get_total() dirs = %s , files = %s" % (count_dirs, count_files))

        for path in paths:
            try:
                abs_path = self.get_abs_path(path)

                if count_dirs:
                    progress_object["total"] += 1

                for current, dirs, files in os.walk(abs_path):
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

    def uploading_progress(self, download_t, download_d, upload_t, upload_d):
        percent_upload = 0
        if upload_t != 0:
            percent_upload = round(float(upload_d) / float(upload_t), 2)

        if percent_upload != self.operation_progress.get("previous_percent"):
            self.operation_progress["previous_percent"] = percent_upload
            total_percent = percent_upload + self.operation_progress.get("processed")

            percent = round(float(total_percent) /
                            float(self.operation_progress.get("total")), 2)
            progress = {
                'percent': percent,
                'text': str(int(percent * 100)) + '%'
            }

            self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
