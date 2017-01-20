import os
import time
import traceback

from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class CreateCopy(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(CreateCopy, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.info("CreateCopy process run")

            ftp = self.get_ftp_connection(self.session)

            # Временная хеш таблица для директорий по которым будем делать листинг
            directories = {}

            for path in self.paths:
                dirname = ftp.path.dirname(path)

                if dirname not in directories.keys():
                    directories[dirname] = []

                directories[dirname].append(path)

            # Массив хешей source -> target для каждого пути
            copy_paths = []

            # Эта содомия нужна чтобы составтить массив source -> target для создания копии файла с красивым именем
            # с учетом того что могут быть совпадения
            for dirname, dir_paths in directories.items():
                dir_listing = ftp.listdir(dirname)

                for dir_path in dir_paths:
                    i = 0
                    exist = False

                    if ftp.isdir(dir_path):
                        filename = os.path.basename(dir_path)
                        ext = ''
                    else:
                        filename, file_extension = ftp.path.splitext(os.path.basename(dir_path))
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
                            'target': ftp.path.join(dirname, copy_name)
                        })

                    while exist:
                        exist = False

                        if ftp.isdir(dir_path):
                            filename = ftp.path.basename(dir_path)
                            ext = ''
                        else:
                            filename, file_extension = ftp.path.splitext(dir_path)
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
                    source_path = copy_path.get('source')
                    target_path = copy_path.get('target')

                    if ftp.isfile(source_path):
                        copy_result = ftp.copy_file(source_path, ftp.path.dirname(target_path), overwrite=True,
                                                    rename=target_path)
                        if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                            raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                "Upload error")
                    elif ftp.isdir(source_path):
                        copy_result = ftp.copy_dir(source_path, ftp.path.dirname(target_path), overwrite=True,
                                                   rename=target_path)
                        if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                            raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                "Upload error")
                    else:
                        error_paths.append(source_path)
                        break

                    success_paths.append(source_path)
                    created_paths.append(ftp.file_info(target_path))

                    if time.time() > next_tick:
                        progress = {
                            'percent': round(float(len(success_paths)) / float(len(copy_paths)), 2),
                            'text': str(
                                int(round(float(len(success_paths)) / float(len(copy_paths)), 2) * 100)) + '%'
                        }

                        self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                        next_tick = time.time() + REQUEST_DELAY

                except Exception as e:
                    self.logger.error("Error copy file %s , error %s" % (str(source_path), str(e)))
                    error_paths.append(source_path)

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
