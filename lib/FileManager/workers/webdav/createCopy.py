from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import traceback
import time


class CreateCopy(BaseWorkerCustomer):
    def __init__(self, paths, session, *args, **kwargs):
        super(CreateCopy, self).__init__(*args, **kwargs)

        self.paths = paths
        self.session = session
        self.webdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

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

                for dir_path in dir_paths:
                    ext = ''
                    divide = dir_path.split('.')
                    if self.webdav.isdir(dir_path):
                        filename = dir_path
                        ext = '/'
                        dir_path += '/'
                    elif len(divide) > 1:
                        ext = '.' + dir_path.split('.')[-1].lower()
                        filename = dir_path.replace(ext, "")

                    i = 1
                    copy_name = filename + ' copy' + ext
                    while self.webdav.exists(copy_name):
                        copy_name = filename + ' copy(' + str(i) + ')' + ext
                        i += 1

                    copy_paths.append({
                        'source': dir_path,
                        'target': copy_name
                    })

            success_paths = []
            error_paths = []
            created_paths = []

            next_tick = time.time() + REQUEST_DELAY

            for copy_path in copy_paths:
                try:
                    source_path = copy_path.get('source')
                    target_path = copy_path.get('target')

                    copy_result = self.webdav.copy_file(source_path, self.webdav.path(target_path), overwrite=True)
                    if not copy_result['success'] or len(copy_result['file_list']['failed']) > 0:
                        raise copy_result['error'] if copy_result['error'] is not None else Exception(
                            "Upload error")

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
            self.logger.info("exception=%s" % str(e))
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
