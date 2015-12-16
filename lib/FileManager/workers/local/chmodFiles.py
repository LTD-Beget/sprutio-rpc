from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FM import REQUEST_DELAY
import traceback
import threading
import time
import os


class ChmodFiles(BaseWorkerCustomer):
    def __init__(self, params, *args, **kwargs):
        super(ChmodFiles, self).__init__(*args, **kwargs)

        self.params = params

    def run(self):
        try:
            self.preload()
            success_paths = []
            error_paths = []

            paths_list = self.params.get('paths')
            paths = []

            for item in paths_list:
                paths.append(item.get("path"))

            operation_progress = {
                "total_done": False,
                "total": 0,
                "operation_done": False,
                "processed": 0
            }

            recursive = self.params.get("recursive")
            mode = int(self.params.get("code"), 8)

            self.logger.info("ChmodFiles process run recursive = %s , mode = %s" % (recursive, mode))

            recursive_dirs = False
            recursive_files = False

            if recursive:
                recursive_dirs = False if self.params.get("recursive_mode") == 'files' else True
                recursive_files = False if self.params.get("recursive_mode") == 'dirs' else True

                t_total = threading.Thread(target=self.get_total,
                                           args=(operation_progress, paths, recursive_dirs, recursive_files))
                t_total.start()

                t_progress = threading.Thread(target=self.update_progress, args=(operation_progress,))
                t_progress.start()

            for path in paths:

                try:
                    abs_path = self.get_abs_path(path)
                    self.logger.debug("Changing attributes file %s , %s" % (str(abs_path), str(oct(mode))))

                    if recursive:
                        if recursive_dirs:
                            os.chmod(abs_path, mode)
                            operation_progress["processed"] += 1

                        for current, dirs, files in os.walk(abs_path):
                            if recursive_dirs:
                                for d in dirs:
                                    dir_path = os.path.join(current, d)
                                    if os.path.islink(dir_path):
                                        try:
                                            os.chmod(dir_path, mode)
                                        except OSError:
                                            self.logger.info("Cannot change attributes on symlink dir %s , %s" % (
                                                str(dir_path), str(oct(mode))))
                                            pass
                                    else:
                                        os.chmod(dir_path, mode)
                                    operation_progress["processed"] += 1

                            if recursive_files:
                                for f in files:
                                    file_path = os.path.join(current, f)

                                    if os.path.islink(file_path):
                                        try:
                                            os.chmod(file_path, mode)
                                        except OSError:
                                            self.logger.info("Cannot change attributes on symlink file %s , %s" % (
                                                str(file_path), str(oct(mode))))
                                            pass
                                    else:
                                        os.chmod(file_path, mode)
                                    operation_progress["processed"] += 1
                    else:
                        os.chmod(abs_path, mode)

                    success_paths.append(path)

                except Exception as e:
                    self.logger.error(
                        "Error change attributes file %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                    error_paths.append(path)

            operation_progress["operation_done"] = True

            result = {
                "success": success_paths,
                "errors": error_paths
            }

            progress = {
                'percent': round(float(len(success_paths)) / float(len(paths)), 2),
                'text': str(int(round(float(len(success_paths)) / float(len(paths)), 2) * 100)) + '%'
            }

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
                abs_path = os.path.abspath(path)

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
