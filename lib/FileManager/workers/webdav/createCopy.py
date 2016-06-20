from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import os
from config.main import TMP_DIR
import traceback
import time


class CreateCopy(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(CreateCopy, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = sessionwebdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)


    def run(self):
        try:
            self.preload()
            self.logger.info("CreateCopy process run")

            # Временная хеш таблица для директорий по которым будем делать листинг
            directories = {}

            for path in self.paths:
                dirname = self.webdav.parent(path)

                if dirname not in directories.keys():
                    directories[dirname] = []

                directories[dirname].append(path)

            # Массив хешей source -> target для каждого пути
            copy_paths = []

            # Эта содомия нужна чтобы составтить массив source -> target для создания копии файла с красивым именем
            # с учетом того что могут быть совпадения
            for dirname, dir_paths in directories.items():
                dir_listing = self.webdav.listDir(dirname)

                for dir_path in dir_paths:
                    i = 0
                    exist = False

                    ext = ''
                    divide = dir_path.split('.')
                    if self.webdav.isdir(dir_path):
                        filename = dir_path
                    elif len(divide) > 1:
                        ext = dir_path.split('.')[-1].lower()
                        filename = dir_path.replace(ext, "")

                    copy_name = filename + ' copy' + ext if i == 0 else filename + ' copy(' + str(i) + ')' + ext

                    for dir_current_path in dir_listing:
                        if copy_name == dir_current_path:
                            exist = True
                            i += 1
                            break

                    if not exist:
                        copy_paths.append({
                            'source': dir_path,
                            'target': dirname + '/' + copy_name
                        })

                    while exist:
                        exist = False

                        ext = ''
                        divide = dir_path.split('.')
                        if self.webdav.isdir(dir_path):
                            filename = dir_path
                        elif len(divide) > 1:
                            ext = dir_path.split('.')[-1].lower()
                            filename = dir_path.replace(ext, "")

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

                    if self.webdav.isfile(source_path):
                        copy_result = self.copy_file(source_path, self.webdav.path(target_path), overwrite=True,
                                                    rename=target_path)
                        if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                            raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                "Upload error")
                    elif self.webdav.isdir(source_path):
                        copy_result = self.copy_dir(source_path, self.webdav.parent(target_path), overwrite=True,
                                                   rename=target_path)
                        if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                            raise copy_result['error'] if copy_result['error'] is not None else Exception(
                                "Upload error")
                    else:
                        error_paths.append(source_path)
                        break

                    success_paths.append(source_path)
                    created_paths.append(self.webdav.generate_file_info(target_path))

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

    def copy_file(self, source, target, overwrite=False, rename=None, callback=None):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if self.isfile(source):
                if rename is not None:
                    destination = os.path.join(target, os.path.basename(rename))
                else:
                    destination = os.path.join(target, os.path.basename(source))

                if not overwrite and self.exists(destination):
                    failed.append(source)
                    raise Exception('file exist and cannot be overwritten')
                try:
                    source_file = self.webdavClient.open(self.to_byte(source), "rb")

                except Exception as e:
                    failed.append(source)
                    raise Exception('Cannot open source file %s' % (str(e),))

                try:
                    destination_file = self.ftp.open(self.to_byte(destination), "wb")

                except Exception as e:
                    failed.append(source)
                    raise Exception('Cannot open destination file %s' % (str(e)))

                try:
                    self.ftp.copyfileobj(source_file, destination_file, callback=callback)

                except Exception as e:
                    failed.append(source)
                    raise Exception('Cannot copy file %s' % (e,))

                succeed.append(source)

                source_file.close()
                destination_file.close()

                file_list['succeed'] = succeed
                file_list['failed'] = failed

                result['success'] = True
                result['error'] = None
                result['file_list'] = file_list

                return result
            else:

                failed.append(source)
                raise Exception('This is not file')

        except Exception as e:
            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    def copy_dir(self, source, target, overwrite=False, rename=None):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        target_name = os.path.basename(rename) if rename is not None else os.path.basename(source)

        try:
            if self.isdir(self.to_byte(source)):
                tree = self.webdav.listdir(self.to_byte(source))
                first_level = True

                for current in tree:
                    dirs_succeed = []
                    dirs_failed = []

                    files_suceed = []
                    files_failed = []

                    current = current.encode("ISO-8859-1").decode('utf-8', errors='replace')
                    relative_root = os.path.relpath(current, source)

                    try:
                        if first_level:
                            destination = os.path.join(target, target_name)
                            first_level = False

                            if not self.webdav.exists(destination):
                                self.webdav.mkdir(destination)
                            elif not overwrite and self.exists(destination):
                                failed.append(source)
                                raise Exception("Directory already exists and overwrite not permitted")

                        for f in files:
                            f = f.encode("ISO-8859-1").decode('utf-8', errors='replace')
                            source_filename = os.path.join(current, f)
                            source_file = self.ftp.open(self.to_byte(source_filename), "rb")

                            dest_filename = os.path.abspath(
                                    self.ftp.path.join(os.path.join(target, target_name), relative_root, f))

                            try:
                                if not overwrite and self.ftp.path.exists(dest_filename):
                                    raise Exception("File already exists and overwrite not permitted")

                                dest_file = self.ftp.open(self.to_byte(dest_filename), "wb")
                                self.ftp.copyfileobj(source_file, dest_file)

                                source_file.close()
                                dest_file.close()

                                files_suceed.append(source_filename)

                            except Exception as e:
                                files_failed.append(source_filename)
                                self.logger.error(
                                        "Error in FTP copy_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        for d in dirs:
                            d = d.encode("ISO-8859-1").decode('utf-8', errors='replace')
                            source_dirname = os.path.join(current, d)
                            dest_dirname = os.path.abspath(
                                    self.ftp.path.join(os.path.join(target, target_name), relative_root, d))

                            try:
                                if not overwrite and self.exists(dest_dirname):
                                    raise Exception("Directory already exists and overwrite not permitted")

                                if not self.exists(dest_dirname):
                                    self.mkdir(dest_dirname)

                                dirs_succeed.append(source_dirname)

                            except Exception as e:
                                dirs_failed.append(source_dirname)
                                self.logger.error(
                                        "Error in FTP copy_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        succeed.extend(files_suceed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)

                    except Exception as e:
                        succeed.extend(files_suceed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)
                        self.logger.error(
                                "Error in FTP copy_dir(): %s, traceback = %s" % (str(e), traceback.format_exc()))

                # after for statement
                file_list['succeed'] = succeed
                file_list['failed'] = failed

                if len(failed) == 0:
                    result['success'] = True
                else:
                    result['success'] = False

                result['file_list'] = file_list

                return result

            else:
                failed.append(source)
                raise Exception('This is not dir')

        except Exception as e:
            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

