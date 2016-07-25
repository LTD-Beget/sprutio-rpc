from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import traceback
import threading
import time
import os
import shutil
from config.main import TMP_DIR


class CopyBetweenWebDav(BaseWorkerCustomer):
    def __init__(self, source, target, paths, overwrite, *args, **kwargs):
        super(CopyBetweenWebDav, self).__init__(*args, **kwargs)

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

            self.logger.info("CopyBetweenWebDav process run source = %s , target = %s" % (source_path, target_path))

            source_webdav = WebDavConnection.create(self.login, self.source.get('server_id'), self.logger)
            target_webdav = WebDavConnection.create(self.login, self.target.get('server_id'), self.logger)
            t_total = threading.Thread(target=self.get_total, args=(self.operation_progress, self.paths))
            t_total.start()

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

                        upload_result = self.upload_file_to_webdav(read_path, target_path, filename,
                                                                   target_webdav)
                        if upload_result['success']:
                            success_paths.append(path)
                            shutil.rmtree(temp_path, True)

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

    def upload_file_to_webdav(self, read_path, write_directory, filename, webdav):
        try:
            upload_result = webdav.upload(read_path, write_directory, self.overwrite, filename,
                                          self.uploading_progress)
        except Exception as e:
            self.logger.info("Cannot copy file %s , %s" % (read_path, str(e)))
            raise e

        return upload_result

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
            self.logger.error("Error in CopyFromFtpToWebDav uploading_progress(): %s, traceback = %s" %
                              (str(ex), traceback.format_exc()))
