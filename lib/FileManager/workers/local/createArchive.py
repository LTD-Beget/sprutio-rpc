from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
from lib.FileManager.FM import REQUEST_DELAY
from config.main import TMP_DIR

import traceback
import os
import time
import libarchive


class CreateArchive(MainWorkerCustomer):
    def __init__(self, params, *args, **kwargs):
        super(CreateArchive, self).__init__(*args, **kwargs)

        self.path = params.get('path')
        self.type = params.get('type', 'zip')
        self.file_items = params.get('files', [])

        self.params = params

    def _prepare(self):
        self.download_dir = os.path.join(TMP_DIR, self.login, self.random_hash())
        self.archive_dir = os.path.join(TMP_DIR, self.login, self.random_hash())

        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)

    def run(self):
        try:
            self.preload()
            self._prepare()

            success_paths, error_paths = self.copy_files_to_tmp(self.download_dir)
            print("download_dir", self.download_dir)
            print("success_paths", success_paths)

            abs_archive_path = self.get_abs_path(self.archive_dir)
            dir_name = os.path.dirname(abs_archive_path)

            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            if not os.path.isdir(dir_name):
                raise Exception("Destination path is not a directory")

            if self.type == 'zip':
                archive_type = 'zip'
            elif self.type == 'gzip':
                archive_type = 'tar.gz'
            elif self.type == 'bz2':
                archive_type = 'tar.bz2'
            elif self.type == 'tar':
                archive_type = 'tar'
            else:
                raise Exception("Unknown archive type")

            archive_path = os.path.join(abs_archive_path, self.random_hash())
            archive_path += "." + archive_type
            print("archive_path", archive_path)
            if os.path.exists(archive_path):
                raise Exception("Archive file already exist")
            self.on_running(self.status_id, pid=self.pid, pname=self.name)

            archive = libarchive.Archive(archive_path, "w")
            next_tick = time.time() + REQUEST_DELAY
            i = 0

            fake_file_items = []
            for f in self.file_items:
                name = os.path.basename(f.get('path'))
                fake_file_items.append({
                    'path': os.path.join(self.download_dir, name)
                })

            for file_item in fake_file_items:
                try:
                    abs_path = self.get_abs_path(file_item.get("path"))
                    file_basename = os.path.basename(abs_path)
                    if os.path.isfile(abs_path):
                        self.logger.info("Packing file: %s" % (abs_path,))
                        f = open(abs_path, 'rb')
                        archive.write(self.make_entry(abs_path, file_basename), data=f.read())
                        f.close()
                    elif os.path.isdir(abs_path):
                        self.logger.info("Packing dir: %s" % (abs_path,))
                        for current, dirs, files in os.walk(abs_path):
                            for f in files:
                                file_path = os.path.join(current, f)
                                file_obj = open(file_path, 'rb')
                                rel_path = os.path.relpath(file_path, abs_path)
                                base_path = os.path.join(file_basename, rel_path)
                                archive.write(self.make_entry(file_path, base_path), data=file_obj.read())
                                file_obj.close()

                    i += 1
                    if time.time() > next_tick:
                        progress = {
                            'percent': round(float(i) / float(len(self.file_items)), 2),
                            'text': str(int(round(float(i) / float(len(self.file_items)), 2) * 100)) + '%'
                        }
                        self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                        next_tick = time.time() + REQUEST_DELAY

                except Exception as e:
                    self.logger.error(
                        "Error archive file %s , error %s , %s" % (str(file_item), str(e), traceback.format_exc()))
                    raise e
            archive.close()

            print("archive created:", archive_path)
            local_archive_path = archive_path
            remote_archive_path = self.path + "." + archive_type
            r = self.ssh_manager.sftp.put(local_archive_path, remote_archive_path)
            print("local file info", local_archive_path, os.stat(local_archive_path))
            print("sftp put resutl", local_archive_path, remote_archive_path, r)

            progress = {
                'percent': round(float(i) / float(len(self.file_items)), 2),
                'text': str(int(round(float(i) / float(len(self.file_items)), 2) * 100)) + '%'
            }
            result = {
                "archive": self._make_file_info(remote_archive_path)
            }

            self.on_success(self.status_id, data=result, progress=progress, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)

    @staticmethod
    def make_entry(f, base_path):
        entry = libarchive.Entry(encoding='utf-8')
        st = os.stat(f)
        entry.pathname = base_path
        entry.size = st.st_size
        entry.mtime = st.st_mtime
        entry.mode = st.st_mode

        return entry

    def copy_files_to_tmp(self, target_path):
        if not os.path.exists(target_path):
            os.makedirs(target_path)

        success_paths = []
        error_paths = []

        for file_item in self.file_items:
            path = file_item.get('path')
            is_ok = self.ssh_manager.rsync(path, target_path)

            if is_ok:
                success_paths.append(path)
            else:
                error_paths.append(path)

        return success_paths, error_paths
