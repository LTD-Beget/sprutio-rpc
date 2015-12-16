from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FTPConnection import FTPConnection
from config.main import TMP_DIR
import traceback
import os
import subprocess


class DownloadFiles(BaseWorkerCustomer):
    def __init__(self, paths, mode, session, *args, **kwargs):
        super(DownloadFiles, self).__init__(*args, **kwargs)

        self.paths = paths
        self.mode = mode
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.info("FTP DownloadFiles process run")

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

        ftp = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)

        success_paths = []
        error_paths = []

        for path in self.paths:
            try:
                abs_path = ftp.path.abspath(path)
                source_path = ftp.path.dirname(path)
                file_basename = ftp.path.basename(abs_path)

                if ftp.isdir(abs_path):
                    destination = os.path.join(target_path, file_basename)

                    if not os.path.exists(destination):
                        os.makedirs(destination)
                    else:
                        raise Exception("destination already exist")

                    for current, dirs, files in ftp.ftp.walk(ftp.to_string(abs_path)):
                        current = current.encode("ISO-8859-1").decode("UTF-8")
                        relative_root = os.path.relpath(current, source_path)

                        for d in dirs:
                            d = d.encode("ISO-8859-1").decode("UTF-8")
                            target_dir = os.path.join(target_path, relative_root, d)
                            if not os.path.exists(target_dir):
                                os.makedirs(target_dir)
                            else:
                                raise Exception("destination dir already exists")

                        for f in files:
                            f = f.encode("ISO-8859-1").decode("UTF-8")
                            source_file = os.path.join(current, f)
                            target_file_path = os.path.join(target_path, relative_root)
                            target_file = os.path.join(target_path, relative_root, f)
                            if not os.path.exists(target_file):
                                download_result = ftp.download(source_file, target_file_path)
                                if not download_result['success'] or len(
                                        download_result['file_list']['failed']) > 0:
                                    raise download_result['error'] if download_result[
                                                                          'error'] is not None else Exception(
                                        "Download error")
                            else:
                                raise Exception("destination file already exists")

                elif ftp.isfile(abs_path):
                    try:
                        target_file = os.path.join(target_path, file_basename)
                        if not os.path.exists(target_file):
                            download_result = ftp.download(abs_path, target_path)
                            if not download_result['success'] or len(download_result['file_list']['failed']) > 0:
                                raise download_result['error'] if download_result[
                                                                      'error'] is not None else Exception(
                                    "Download error")
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
