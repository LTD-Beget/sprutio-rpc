from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FM import REQUEST_DELAY
import os
import traceback
import shutil
import time


class CreateCopy(BaseWorkerCustomer):
    def __init__(self, paths, *args, **kwargs):
        super(CreateCopy, self).__init__(*args, **kwargs)

        self.paths = paths

    def run(self):
        try:
            self.preload()
            self.logger.info("CreateCopy process run")

            # Временная хеш таблица для директорий по которым будем делать листинг
            directories = {}

            for path in self.paths:
                abs_path = self.get_abs_path(path)
                dirname = os.path.dirname(abs_path)

                if dirname not in directories.keys():
                    directories[dirname] = []

                directories[dirname].append(abs_path)

            # Массив хешей source -> target для каждого пути
            copy_paths = []

            # Эта содомия нужна чтобы составтить массив source -> target для создания копии файла с красивым именем
            # с учетом того что могут быть совпадения
            for dirname, dir_paths in directories.items():
                dir_listing = os.listdir(dirname)

                for dir_path in dir_paths:
                    i = 0
                    exist = False

                    if os.path.isdir(dir_path):
                        filename = os.path.basename(dir_path)
                        ext = ''
                    else:
                        filename, file_extension = os.path.splitext(dir_path)
                        ext = file_extension

                    copy_name = filename + ' copy' + ext if i == 0 else filename + ' copy(' + str(i) + ')' + ext

                    for dir_current_path in dir_listing:
                        if copy_name == dir_current_path:
                            exist = True
                            i += 1
                            break

                    if not exist:
                        copy_paths.append({
                            'source': dir_path,
                            'target': os.path.join(dirname, copy_name)
                        })

                    while exist:
                        exist = False

                        if os.path.isdir(dir_path):
                            filename = os.path.basename(dir_path)
                            ext = ''
                        else:
                            filename, file_extension = os.path.splitext(dir_path)
                            ext = file_extension

                        copy_name = filename + ' copy' + ext if i == 0 else filename + ' copy(' + str(i) + ')' + ext

                        for dir_current_path in dir_listing:
                            if copy_name == dir_current_path:
                                exist = True
                                i += 1
                                break

                        if not exist:
                            dir_listing.append(copy_name)
                            copy_paths.append({
                                'source': dir_path,
                                'target': os.path.join(dirname, copy_name)
                            })

            success_paths = []
            error_paths = []
            created_paths = []

            next_tick = time.time() + REQUEST_DELAY

            for copy_path in copy_paths:
                try:
                    source_abs_path = copy_path.get('source')
                    target_abs_path = copy_path.get('target')

                    if os.path.isfile(source_abs_path):
                        shutil.copy(source_abs_path, target_abs_path)
                    elif os.path.islink(source_abs_path):
                        shutil.copy(source_abs_path, target_abs_path)
                    elif os.path.isdir(source_abs_path):
                        shutil.copytree(source_abs_path, target_abs_path, True)
                    else:
                        error_paths.append(source_abs_path)
                        break

                    success_paths.append(self.get_rel_path(source_abs_path))
                    created_paths.append(self._make_file_info(target_abs_path))

                    if time.time() > next_tick:
                        progress = {
                            'percent': round(float(len(success_paths)) / float(len(copy_paths)), 2),
                            'text': str(
                                int(round(float(len(success_paths)) / float(len(copy_paths)), 2) * 100)) + '%'
                        }

                        self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                        next_tick = time.time() + REQUEST_DELAY

                except Exception as e:
                    self.logger.error("Error copy file %s , error %s" % (str(source_abs_path), str(e)))
                    error_paths.append(source_abs_path)

            result = {
                "success": success_paths,
                "errors": error_paths,
                "items": created_paths
            }

            # иначе пользователям кажется что скопировалось не полностью )
            progress = {
                'percent': round(float(len(success_paths)) / float(len(copy_paths)), 2),
                'text': str(int(round(float(len(success_paths)) / float(len(copy_paths)), 2) * 100)) + '%'
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
