from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import TMP_DIR
import traceback
import os
import stat
import shutil
import subprocess


class DownloadFiles(BaseWorkerCustomer):
    def __init__(self, paths, mode, *args, **kwargs):
        super(DownloadFiles, self).__init__(*args, **kwargs)

        self.paths = paths
        self.mode = mode

    def run(self):
        try:
            self.preload()
            self.logger.info("DownloadFiles process run")

            download_dir = TMP_DIR + '/' + self.login + '/' + self.random_hash() + '/'
            success_paths, error_paths = self.copy_files_to_tmp(download_dir)

            if len(success_paths) == 1:
                one_file = True
            else:
                one_file = False

            download_path = None
            mtime = 0
            inode = 0
            size = 0

            if len(error_paths) == 0:  # Значит все хорошо, можно дальше обрабатывать
                if one_file is True:
                    if self.mode == "default":
                        download_path = os.path.join(download_dir, os.path.basename(success_paths[0]))
                    else:
                        download_path = download_dir.rstrip("/")
                        files_path = download_path
                        os.chdir(os.path.dirname(download_path))

                        if self.mode == 'zip':
                            zip_util = self.get_util('zip')
                            download_path = download_path + '.' + self.mode
                            return_code = subprocess.call([zip_util, '-r', download_path, os.path.basename(files_path)])
                            if return_code != 0:
                                raise Exception("Zip Error")
                        if self.mode == 'gzip':
                            tar_util = self.get_util('tar')
                            download_path += '.tar.gz'
                            return_code = subprocess.call(
                                    [tar_util, '-zcvf', download_path, os.path.basename(files_path)])
                            if return_code != 0:
                                raise Exception("Tar Error")

                        if self.mode == 'tar':
                            tar_util = self.get_util('tar')
                            download_path += '.tar'
                            return_code = subprocess.call(
                                    [tar_util, '-cvf', download_path, os.path.basename(files_path)])
                            if return_code != 0:
                                raise Exception("Tar Error")

                        if self.mode == 'bz2':
                            tar_util = self.get_util('tar')
                            download_path += '.bz2'
                            return_code = subprocess.call(
                                    [tar_util, '-jcvf', download_path, os.path.basename(files_path)])
                            if return_code != 0:
                                raise Exception("Tar Error")
                else:
                    download_path = download_dir.rstrip("/")
                    files_path = download_path
                    os.chdir(os.path.dirname(download_path))

                    if self.mode == 'zip' or self.mode == 'default':
                        zip_util = self.get_util('zip')
                        download_path = download_path + '.' + self.mode
                        return_code = subprocess.call([zip_util, '-r', download_path, os.path.basename(files_path)])
                        if return_code != 0:
                            raise Exception("Zip Error")

                    if self.mode == 'gzip':
                        tar_util = self.get_util('tar')
                        download_path += '.tar.gz'
                        return_code = subprocess.call([tar_util, '-zcvf', download_path, os.path.basename(files_path)])
                        if return_code != 0:
                            raise Exception("Tar Error")

                    if self.mode == 'tar':
                        tar_util = self.get_util('tar')
                        download_path += '.tar'
                        return_code = subprocess.call([tar_util, '-cvf', download_path, os.path.basename(files_path)])
                        if return_code != 0:
                            raise Exception("Tar Error")

                    if self.mode == 'bz2':
                        tar_util = self.get_util('tar')
                        download_path += '.bz2'
                        return_code = subprocess.call([tar_util, '-jcvf', download_path, os.path.basename(files_path)])
                        if return_code != 0:
                            raise Exception("Tar Error")

                file_info = os.lstat(download_path)

                mtime = int(file_info.st_ctime)
                inode = int(file_info.st_ino)
                size = int(file_info.st_size)

            result = {
                "success": success_paths,
                "errors": error_paths,
                "download_path": download_path,
                "file_name": os.path.basename(download_path),
                "mtime": mtime,
                "inode": inode,
                "size": size
            }

            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    @staticmethod
    def get_util(name):
        p = os.popen('/bin/which ' + name, mode='r')
        s = p.readline().rstrip("\n")

        p.close()
        return s

    def copy_files_to_tmp(self, target_path):
        if not os.path.exists(target_path):
            os.makedirs(target_path)

        success_paths = []
        error_paths = []

        for path in self.paths:
            try:
                abs_path = self.get_abs_path(path)
                source_path = os.path.dirname(path)
                file_basename = os.path.basename(abs_path)

                if os.path.isdir(abs_path):
                    destination = os.path.join(target_path, file_basename)

                    if not os.path.exists(destination):
                        st = os.stat(abs_path)
                        os.makedirs(destination, stat.S_IMODE(st.st_mode))
                    else:
                        raise Exception("destination already exist")

                    for current, dirs, files in os.walk(abs_path):
                        relative_root = os.path.relpath(current, source_path)
                        for d in dirs:
                            source_dir = os.path.join(current, d)
                            target_dir = os.path.join(target_path, relative_root, d)
                            if not os.path.exists(target_dir):
                                st = os.stat(source_dir)
                                os.makedirs(target_dir, stat.S_IMODE(st.st_mode))
                            else:
                                raise Exception("destination dir already exists")
                        for f in files:
                            source_file = os.path.join(current, f)
                            target_file = os.path.join(target_path, relative_root, f)
                            if not os.path.exists(target_file):
                                shutil.copy(source_file, target_file)
                            else:
                                raise Exception("destination file already exists")
                elif os.path.isfile(abs_path):
                    try:
                        target_file = os.path.join(target_path, file_basename)
                        if not os.path.exists(target_file):
                            shutil.copy(abs_path, target_file)
                        else:
                            raise Exception("destination file already exists")
                    except Exception as e:
                        self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                        raise e

                success_paths.append(path)

            except Exception as e:
                self.logger.error(
                        "Error copy %s , error %s , %s" % (str(path), str(e), traceback.format_exc()))
                error_paths.append(path)

        return success_paths, error_paths
