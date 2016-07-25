from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.SFTPConnection import SFTPConnection
from lib.FileManager.FM import REQUEST_DELAY

import os
import stat
import traceback
import time


class CreateCopy(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(CreateCopy, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = session

    def run(self):
        try:
            self.preload()
            sftp = self.get_sftp_connection(self.session)
            self.logger.info("CreateCopy sftp process run")

            # Временная хеш таблица для директорий по которым будем делать листинг
            directories = {}

            for path in self.paths:
                dir_name = os.path.dirname(path)

                if dir_name not in directories.keys():
                    directories[dir_name] = []

                directories[dir_name].append(path)

            # Массив хешей source -> target для каждого пути
            copy_paths = []

            # Эта содомия нужна чтобы составтить массив source -> target для создания копии файла с красивым именем
            # с учетом того что могут быть совпадения
            for dir_name, dir_paths in directories.items():
                dir_listing = sftp.sftp.listdir(dir_name)

                for dir_path in dir_paths:
                    if sftp.isdir(dir_path):
                        filename = os.path.basename(dir_path)
                        ext = ''
                    else:
                        filename, file_extension = os.path.splitext(os.path.basename(dir_path))
                        ext = file_extension

                    i = 0
                    exist = True
                    while exist:
                        copy_name = filename + ' copy' + ext if i == 0 else filename + ' copy(' + str(i) + ')' + ext

                        if copy_name in dir_listing:
                            i += 1
                        else:
                            exist = False
                            dir_listing.append(copy_name)
                            copy_paths.append({
                                'source': dir_path,
                                'target': os.path.join(dir_name, copy_name)
                            })

            success_paths = []
            error_paths = []
            created_paths = []

            next_tick = time.time() + REQUEST_DELAY

            for copy_path in copy_paths:
                try:
                    source_abs_path = copy_path.get('source')
                    target_abs_path = copy_path.get('target')

                    if sftp.isfile(source_abs_path):
                        sftp.cp_sftp(source_abs_path, target_abs_path)
                    elif sftp.islink(source_abs_path):
                        sftp.cp_sftp(source_abs_path, target_abs_path)
                    elif sftp.isdir(source_abs_path):
                        for current, dirs, files in sftp.walk(source_abs_path):
                            relative_root = os.path.relpath(current, source_abs_path)
                            target_current_dir = os.path.join(target_abs_path, relative_root)

                            st = sftp.stat(current)
                            sftp.makedirs(target_current_dir, stat.S_IMODE(st.st_mode))
                            # progress["processed"] += 1

                            for f in files:
                                sftp.sftp.chdir(None)
                                target_file = os.path.join(target_abs_path, relative_root, f)
                                source_file = os.path.join(current, f)
                                source_file_obj = sftp.sftp.open(source_file)
                                sftp.sftp.putfo(source_file_obj, target_file)
                                # progress["processed"] += 1
                    else:
                        error_paths.append(source_abs_path)
                        break

                    success_paths.append(source_abs_path)
                    created_paths.append(sftp.make_file_info(target_abs_path))

                    if time.time() > next_tick:
                        progress = {
                            'percent': round(float(len(success_paths)) / float(len(copy_paths)), 2),
                            'text': str(
                                int(round(float(len(success_paths)) / float(len(copy_paths)), 2) * 100)) + '%'
                        }

                        self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                        next_tick = time.time() + REQUEST_DELAY

                except Exception as e:
                    self.logger.error("Error CreateCopy file %s , error %s" % (str(source_abs_path), str(e)))
                    self.logger.debug("\n{}".format(traceback.format_exc()))
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
