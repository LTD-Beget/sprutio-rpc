import os
import shutil
import threading
import time
import traceback

from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class CopyFromWebDav(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyFromWebDav, self).__init__(*args, **kwargs)

        self.source = source
        self.target = target
        self.paths = paths
        self.overwrite = overwrite
        self.webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
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

            if source_path is None:
                raise Exception("Source path empty")

            if target_path is None:
                raise Exception("Target path empty")

            target_path = self.get_abs_path(target_path)

            self.logger.info("CopyFromWebDav process run source = %s , target = %s" % (source_path, target_path))

            t_total = threading.Thread(target=self.get_total, args=(self.operation_progress, self.paths))
            t_total.start()

            for path in self.paths:
                try:
                    download_path = target_path
                    if self.webdav.isdir(path):
                        path += '/'
                        download_path += path.replace(self.webdav.parent(path), "/", 1)

                    self.download_file_from_webdav(path, download_path)

                    success_paths.append(path)

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

    def download_file_from_webdav(self, path, target_path):
        try:
            target_file = os.path.join(target_path, path)
            if not os.path.exists(target_file):
                download_result = self.webdav.download(path, target_path, self.downloading_progress)
                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                    raise download_result['error'] if download_result[
                                                          'error'] is not None else Exception(
                        "Download error")
            elif self.overwrite and os.path.exists(target_file) and not os.path.isdir(target_file):
                download_result = self.webdav.download(path, target_path, self.downloading_progress)
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
                download_result = self.webdav.download(path, target_path, self.downloading_progress)
                if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                    raise download_result['error'] if download_result[
                                                          'error'] is not None else Exception(
                        "Download error")
            else:
                pass

        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (path, str(e)))
            raise e

    def get_total(self, progress_object, paths, count_files=True):
        self.logger.debug("start get_total() files = %s" % count_files)
        webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
        for path in paths:
            try:
                self.recursive_total(webdav, path, progress_object)

            except Exception as e:
                self.logger.error("Error get_total file %s , error %s" % (str(path), str(e)))
                continue

        progress_object["total_done"] = True
        self.logger.debug("done get_total(), found %s files" % progress_object.get("total"))
        return

    def recursive_total(self, webdav, path, progress_object):
        if webdav.isfile(path):
            progress_object["total"] += 1
        else:
            for file in webdav.listdir(path):
                self.recursive_total(webdav, file, progress_object)

    def downloading_progress(self, download_t, download_d, upload_t, upload_d):
        try:
            percent_download = 0
            if download_t != 0:
                percent_download = round(float(download_d) / float(download_t), 2)

            if percent_download != self.operation_progress.get("previous_percent"):
                if percent_download == 0 and self.operation_progress.get("previous_percent") != 0:
                    self.operation_progress["processed"] += 1
                self.operation_progress["previous_percent"] = percent_download
                total_percent = percent_download + self.operation_progress.get("processed")

                denominator = 50
                if self.operation_progress.get("total_done"):
                    denominator = self.operation_progress.get("total")
                percent = round(float(total_percent) /
                                float(denominator), 2)
                self.logger.debug("percentage changed to %s" % percent)
                progress = {
                    'percent': percent,
                    'text': str(int(percent * 100)) + '%'
                }

                self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
        except Exception as ex:
            self.logger.error("Error in CopyFromWebDav downloading_progress(): %s, traceback = %s" %
                              (str(ex), traceback.format_exc()))

