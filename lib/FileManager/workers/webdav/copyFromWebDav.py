from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import os
import traceback
import threading
import shutil
import time


class CopyFromWebDav(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyFromWebDav, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite
        self.webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)

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

            self.logger.info("CopyFromWebDav process run source = %s , target = %s" % (source_path, target_path))

            t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
            t_total.start()

            t_progress = threading.Thread(target=self.update_progress, args=(operation_progress,))
            t_progress.start()

            self.logger.info("paths %s" % self.paths)

            for path in self.paths:
                self.logger.info("path %s" % path)
                try:
                    if self.webdav.isdir(path):
                        path += '/'
                        target_path += path.replace(self.webdav.parent(path), "/", 1)

                    self.download_file_from_webdav(path, target_path, operation_progress)

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

    def download_file_from_webdav(self, path, target_path, operation_progress):
        try:
            target_file = os.path.join(target_path, path)
            download_result = {}
            if not os.path.exists(target_file):
                download_result = self.webdav.download(path, target_path)
                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                    raise download_result['error'] if download_result[
                                                          'error'] is not None else Exception(
                        "Download error")
            elif self.overwrite and os.path.exists(target_file) and not os.path.isdir(target_file):
                download_result = self.webdav.download(path, target_path)
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
                download_result = self.webdav.download(path, target_path)
                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                    raise download_result['error'] if download_result[
                                                          'error'] is not None else Exception(
                        "Download error")
            else:
                pass

        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (path, str(e)))
            raise e
        finally:
            operation_progress["processed"] += 1

    def get_total(self, progress_object, paths, count_dirs=True, count_files=True):
        self.logger.debug("start get_total() dirs = %s , files = %s" % (count_dirs, count_files))
        webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
        for path in paths:
            try:
                if count_dirs:
                    progress_object["total"] += 1

                for file in webdav.listdir(path):
                    if count_dirs and webdav.isdir(file):
                        progress_object["total"] += 1
                    if count_files and webdav.isfile(file):
                        progress_object["total"] += 1
            except Exception as e:
                self.logger.error("Error get_total file %s , error %s" % (str(path), str(e)))
                continue

        progress_object["total_done"] = True
        self.logger.debug("done get_total()")
        return

    def update_progress(self, progress_object):
        self.logger.debug("start update_progress()")
        next_tick = time.time() + REQUEST_DELAY

        self.on_running(self.status_id, pid=self.pid, pname=self.name)

        while not progress_object.get("operation_done"):
            if time.time() > next_tick and progress_object.get("total_done"):
                progress = {
                    'percent': round(float(progress_object.get("processed")) / float(progress_object.get("total")), 2),
                    'text': str(int(round(float(progress_object.get("processed")) / float(progress_object.get("total")),
                                          2) * 100)) + '%'
                }

                self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                next_tick = time.time() + REQUEST_DELAY
                time.sleep(REQUEST_DELAY)
            elif time.time() > next_tick:
                next_tick = time.time() + REQUEST_DELAY
                time.sleep(REQUEST_DELAY)

        self.logger.debug("done update_progress()")
        return

