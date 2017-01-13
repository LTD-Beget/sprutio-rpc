import os
import stat
import uuid
import codecs
import ftplib
import pprint
import traceback
import ftputil
import datetime
from ftputil import file_transfer


def transfer_between_ftp(source_ftp, target_ftp, source_path, target_path):
    """
    Копирует файл между FTP соединенеиями
    :param FTP source_ftp:
    :param FTP target_ftp:
    :param str source_path:
    :param str target_path:
    """
    source_file = file_transfer.RemoteFile(source_ftp.ftp, source_ftp.to_byte(source_path), "rb")
    target_file = file_transfer.RemoteFile(target_ftp, target_ftp.to_byte(target_path), "wb")

    source_fobj = source_file.fobj()
    try:
        target_fobj = target_file.fobj()
        try:
            file_transfer.copyfileobj(source_fobj, target_fobj)
        finally:
            target_fobj.close()
    finally:
        source_fobj.close()


class TimeZoneMSK(datetime.tzinfo):
    def dst(self, dt):
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        return datetime.timedelta(hours=3)

    def tzname(self, dt):
        return 'GMT'


class FTPSession(ftplib.FTP):
    def __init__(self, host, user, passwd, port, timeout):
        ftplib.FTP.__init__(self)
        self.set_debuglevel(2)
        # self.set_debuglevel(0)
        self.connect(host, port, timeout)
        self.login(user, passwd)
        self.set_pasv(True)


class FTP:
    def __init__(self, host, user, passwd, port=21, timeout=-999, logger=None):
        self.fp = dict()

        ftp_host = host

        self.ftp_host = ftp_host
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port

        self.ftp = ftputil.FTPHost(ftp_host,
                                   user,
                                   passwd,
                                   port=port,
                                   timeout=timeout,
                                   session_factory=FTPSession)

        self.ftp.stat_cache.enable()
        self.ftp.stat_cache.max_age = 1800
        self.ftp.keep_alive()

        self.logger = logger
        self._tzinfo = TimeZoneMSK()

    @property
    def path(self):
        return self.ftp.path

    def getcwd(self):
        return self.ftp.getcwd()

    def close(self):
        self.ftp.close()

    def chdir(self, path):
        self.ftp.chdir(self.to_byte(path))

    def _make_file_info(self, file_path):

        info = self.lstat(file_path)

        is_dir = False
        is_link = False

        if stat.S_ISDIR(info.st_mode):
            is_dir = True
        elif stat.S_ISLNK(info.st_mode):
            is_link = True
        elif stat.S_ISREG(info.st_mode):
            pass
        else:
            pass

        file_name = os.path.basename(file_path)
        file_dir = self.ftp.path.dirname(file_path)

        if is_dir:
            ext = b''
        else:
            ext = self.path.splitext(file_name)[1][1:].lower()

        mtime = info.st_mtime

        file_info = {
            "is_dir": is_dir,
            "is_link": is_link,
            "name": file_name.decode("utf-8", errors="replace"),
            "ext": ext.decode("utf-8", errors="replace"),
            "path": file_dir.decode("utf-8", errors="replace"),
            "owner": self.getowner(info),
            "mode": self.getmode(info),
            "size": info.st_size if not is_dir else 0,
            "mtime": mtime,
            'mtime_str': datetime.datetime.fromtimestamp(mtime, self._tzinfo).strftime('%d.%m.%Y %H:%M:%S'),
        }

        return file_info

    @staticmethod
    def to_byte(value):
        if isinstance(value, str):
            try:
                value = value.encode("utf-8")
            except UnicodeDecodeError:
                value = value.encode("ISO-8859-1")
        return value

    @staticmethod
    def to_string(value):
        if isinstance(value, str):
            try:
                value = value.encode("utf-8")
            except UnicodeDecodeError:
                value = value.encode("ISO-8859-1")

        if isinstance(value, bytes):
            try:
                value = value.decode("ISO-8859-1")
            except UnicodeDecodeError:
                try:
                    value = value.decode("ISO-8859-1")
                except UnicodeDecodeError:
                    value = value.decode("utf-8", errors="replace"),

        return value

    def size(self, path):
        try:
            return self.ftp.path.getsize(self.to_byte(path))
        except Exception as e:
            self.logger.error("Error in FTP size(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            return 0

    def lstat(self, path):
        return self.ftp.lstat(self.to_byte(path))

    def exists(self, path):
        return self.ftp.path.exists(self.to_byte(path))

    def isdir(self, path):
        return self.ftp.path.isdir(self.to_byte(path))

    def isfile(self, path):
        return self.ftp.path.isfile(self.to_byte(path))

    def islink(self, path):
        return self.ftp.path.islink(self.to_byte(path))

    def clear_cache(self):
        return self.ftp.stat_cache.clear()

    def getmode(self, info):
        try:
            mode = stat.S_IMODE(info.st_mode)
            mode = oct(int(mode))
            mode = mode[2:].zfill(3)  # for python 3 0o  as octal prefix
            return mode
        except Exception as e:
            self.logger.error("Error in FTP getmode(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception

    @staticmethod
    def getowner(info):
        return info[4]

    def list(self, path):
        byte_path = self.to_byte(path)
        byte_path = self.ftp.path.abspath(byte_path)

        self.chdir(byte_path)  # to surf from removed folder

        flist = {
            "path": path,
            "items": []
        }

        listdir = self.ftp.listdir(self.to_byte(path))

        for name in listdir:
            if isinstance(name, str):
                name = name.encode("ISO-8859-1")
            item_path = self.ftp.path.join(byte_path, name)
            flist["items"].append(self._make_file_info(item_path))

        return flist

    def listdir(self, path):
        byte_path = self.to_byte(path)
        byte_path = self.ftp.path.abspath(byte_path)

        self.chdir(path)

        listdir = self.ftp.listdir(self.to_byte(path))

        listing = []
        for name in listdir:
            if isinstance(name, str):
                name = name.encode("ISO-8859-1")
            item_path = self.ftp.path.join(byte_path, name)
            listing.append(item_path)
        return listing

    def file_info(self, path):

        byte_path = self.to_byte(path)
        byte_path = self.ftp.path.abspath(byte_path)

        file_info = self._make_file_info(byte_path)
        return file_info

    def rename(self, source, target):

        byte_source = self.to_byte(source)
        byte_source = self.ftp.path.abspath(byte_source)

        byte_target = self.to_byte(target)
        byte_target = self.ftp.path.abspath(byte_target)

        if not self.path.exists(byte_source):
            raise Exception("Entry with source name not exists")

        if self.path.exists(byte_target):
            raise Exception("Entry with target name already exists")

        self.ftp.rename(byte_source, byte_target)

    def remove(self, target):
        byte_target = self.to_byte(target)

        if self.isdir(target):
            try:
                self.ftp.rmtree(byte_target, True)
            except Exception as e:
                self.logger.error("Error in FTP dir remove(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception
        else:
            try:
                self.ftp.remove(byte_target)
            except Exception as e:
                self.logger.error("Error in FTP file remove(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception

    def file(self, target, mode):
        return self.ftp.open(self.to_byte(target), mode)

    def mkdir(self, path):
        return self.ftp.mkdir(self.to_byte(path))

    def makedirs(self, path):
        return self.ftp.makedirs(self.to_byte(path))

    def download(self, source, target):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if not self.isfile(source):
                failed.append(source)
                raise Exception("Source is not a file")

            target_path = os.path.join(target, os.path.basename(source))

            try:
                self.ftp.download(self.to_byte(source), target_path)
            except Exception as e:
                failed.append(source)
                self.logger.error("Error in FTP download(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception("Error during file download")

            succeed.append(source)

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = True
            result['error'] = None
            result['file_list'] = file_list

            return result

        except Exception as e:
            self.logger.error("Error in FTP download(): %s, traceback = %s" % (str(e), traceback.format_exc()))

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    def upload(self, source, target, overwrite=False, rename=None):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            byte_source = self.to_byte(source)
            target = self.to_byte(target)

            if rename is not None:
                rename = self.to_byte(rename)
                target_path = os.path.join(target, os.path.basename(rename))
            else:
                target_path = os.path.join(target, os.path.basename(byte_source))

            if not overwrite and self.ftp.path.exists(target_path):
                failed.append(source)
                raise Exception("File already exists and overwrite not permitted")

            try:
                self.ftp.upload(source, self.to_string(target_path))
            except Exception as e:
                failed.append(source)
                self.logger.error("Error in FTP upload(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception("Error during file uploading %s" % traceback.format_exc())

            succeed.append(source)

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = True
            result['error'] = None
            result['file_list'] = file_list

            return result

        except Exception as e:
            self.logger.error("Error in FTP upload(): %s, traceback = %s" % (str(e), traceback.format_exc()))

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    @staticmethod
    def relative_root(relative_root, path):

        relpath = os.path.relpath(path, relative_root)
        return relpath

    def download_dir(self, source, target, overwrite=False):

        result = {}
        file_list = {}

        succeed = []
        failed = []

        source = self.to_byte(source)
        target = self.to_byte(target)

        try:
            if self.ftp.path.isdir(source):

                tree = self.ftp.walk(source)
                source_root = os.path.dirname(source)

                first_level = True

                for root, dirs, files in tree:
                    dirs_succeed = []
                    dirs_failed = []

                    files_succeed = []
                    files_failed = []

                    try:
                        root = root.encode("ISO-8859-1")
                        if first_level:
                            current_dir = source
                            destination = os.path.join(target, os.path.basename(root))
                            first_level = False

                            try:
                                if not os.path.exists(destination):
                                    os.mkdir(destination)
                                    dirs_succeed.append(source)
                            except Exception as e:
                                dirs_failed.append(source)
                                self.logger.error(
                                        "Error in FTP download_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        else:
                            rel_path = self.relative_root(source_root, root)
                            destination = os.path.join(target, rel_path)

                            try:
                                if not os.path.exists(destination):
                                    os.mkdir(destination)
                            except Exception as e:
                                dirs_failed.append(source)
                                self.logger.error(
                                        "Error in FTP download_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                            current_dir = os.path.join(root)

                        for name in files:
                            name = name.encode("ISO-8859-1")
                            source_filename = os.path.join(current_dir, name)

                            if self.ftp.path.islink(source_filename):
                                continue

                            dest_filename = os.path.join(destination, os.path.basename(source_filename))

                            try:
                                if not overwrite and os.path.exists(dest_filename):
                                    raise Exception("File already exists and overwrite not permitted")

                                self.ftp.download(source_filename, dest_filename)
                                files_succeed.append(source_filename)

                            except Exception as e:
                                self.logger.error("Error in FTP download_dir(): %s, traceback = %s" % (
                                    str(e), traceback.format_exc()))
                                files_failed.append(source_filename)

                        for d in dirs:
                            d = d.encode("ISO-8859-1")
                            source_dirname = os.path.join(current_dir, d)
                            dest_dirname = os.path.join(destination, os.path.basename(source_dirname))

                            try:
                                if not overwrite and os.path.exists(dest_dirname):
                                    raise Exception("Directory already exists and overwrite not permitted")

                                if not os.path.exists(dest_dirname):
                                    os.mkdir(dest_dirname)

                                dirs_succeed.append(source_dirname)

                            except Exception as e:
                                dirs_failed.append(source_dirname)
                                self.logger.error(
                                        "Error in FTP download_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        succeed.extend(files_succeed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)

                    except Exception as e:
                        succeed.extend(files_succeed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)
                        self.logger.error(
                                "Error in FTP download_dir(): %s, traceback = %s" % (
                                    str(e), traceback.format_exc()))

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
            self.logger.error("Error in FTP download_dir(): %s, traceback = %s" % (str(e), traceback.format_exc()))

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    def chmod(self, target, mode):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            target = self.to_byte(target)

            if self.ftp.path.exists(target):
                try:
                    self.ftp.chmod(target, mode)
                    succeed.append(self.to_string(target))

                except Exception as e:
                    self.logger.error("Error in FTP chmod(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                    failed.append(self.to_string(target))

                # after for statement
                file_list['succeed'] = succeed
                file_list['failed'] = failed

                if len(failed) == 0:
                    result['success'] = True
                else:
                    result['success'] = False

                result['file_list'] = file_list
                self.logger.info("return 1 result = %s", pprint.pformat(result))
                return result

            else:
                failed.append(self.to_string(target))
                raise Exception('Target path is not exists')

        except Exception as e:
            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            self.logger.info("return 2 result = %s", pprint.pformat(result))
            return result

    def chmod_dir(self, target, mode, recursive=False, recursive_mode='all'):

        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            target = self.to_byte(target)

            if recursive is False:
                recursive_mode = 'none'

            if self.ftp.path.isdir(target):
                tree = self.ftp.walk(target)
                first_level = True

                dirs_succeed = []
                dirs_failed = []

                files_succeed = []
                files_failed = []

                if recursive is False:
                    self.ftp.chmod(target, mode)
                    dirs_succeed.append(self.to_string(target))

                for root, dirs, files in tree:
                    try:
                        if first_level:
                            current_dir = target

                            if not self.ftp.path.exists(current_dir):
                                failed.append(self.to_string(target))
                                raise Exception("Directory is not exist")

                            if self.ftp.path.exists(current_dir):
                                self.ftp.chmod(current_dir, mode)
                                dirs_succeed.append(self.to_string(target))

                            first_level = False
                        else:
                            current_dir = os.path.join(root)

                            if not self.ftp.path.exists(current_dir):
                                failed.append(self.to_string(target))
                                raise Exception("Directory is not exist")

                            if self.ftp.path.exists(current_dir):
                                if recursive_mode == 'all' or recursive_mode == 'dirs':
                                    self.ftp.chmod(current_dir, mode)
                                    dirs_succeed.append(self.to_string(target))

                        for f in files:
                            dest_filename = self.ftp.path.join(current_dir, f)

                            if not self.ftp.path.exists(dest_filename):
                                files_failed.append(self.to_string(target))
                                raise Exception("File not exists")

                            if recursive_mode == 'all' or recursive_mode == 'files':
                                self.ftp.chmod(dest_filename, mode)
                                files_succeed.append(self.to_string(target))

                        for d in dirs:
                            dest_dirname = self.ftp.path.join(current_dir, d)

                            if not self.ftp.path.exists(dest_dirname):
                                dirs_failed.append(self.to_string(target))
                                raise Exception("Directory not exists")

                            if recursive_mode == 'all' or recursive_mode == 'dirs':
                                self.ftp.chmod(dest_dirname, mode)
                                dirs_succeed.append(self.to_string(target))

                    except Exception as e:
                        self.logger.error('error = %s , trace = %s' % (str(e), traceback.format_exc()))
                        pass

                succeed.extend(files_succeed)
                succeed.extend(dirs_succeed)
                failed.extend(files_failed)
                failed.extend(dirs_failed)

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
                failed.append(self.to_string(target))
                raise Exception('This is not dir')

        except Exception as e:
            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list
            return result

    def upload_dir(self, source, target, overwrite=False, rename=None):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if os.path.isdir(source):

                tree = os.walk(source)
                first_level = True
                target = target.encode('utf-8')

                for root, dirs, files in tree:
                    dirs_succeed = []
                    dirs_failed = []

                    files_succeed = []
                    files_failed = []

                    try:
                        if first_level:
                            current_dir = source

                            if rename is not None:
                                destination = os.path.join(target, os.path.basename(rename))
                            else:
                                destination = os.path.join(target, os.path.basename(source))

                            first_level = False

                            if not overwrite and self.ftp.path.exists(destination):

                                failed.append(source)
                                raise Exception("Directory already exists and overwrite not permitted")
                            else:
                                succeed.append(source)

                            if not self.ftp.path.exists(destination):
                                self.ftp.mkdir(destination)

                        else:

                            if rename is not None:
                                rel_path = self.relative_root(os.path.dirname(source), root)

                                chunks = rel_path.split('/', 1)
                                chunks[0] = os.path.basename(rename)
                                rel_path = '/'.join(chunks)

                            else:
                                rel_path = self.relative_root(os.path.dirname(source), root)

                            destination = os.path.join(target, rel_path)
                            current_dir = os.path.join(root)

                        for name in files:

                            source_filename = os.path.join(current_dir, name)
                            dest_filename = os.path.join(destination, os.path.basename(source_filename))

                            try:
                                if not overwrite and self.ftp.path.exists(dest_filename):
                                    raise Exception("File already exists and overwrite not permitted")

                                self.ftp.upload(source_filename, dest_filename, 'b')
                                files_succeed.append(source_filename)

                            except Exception as e:
                                files_failed.append(source_filename)
                                self.logger.error(
                                        "Error in FTP upload_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        for d in dirs:
                            source_dirname = os.path.join(current_dir, d)
                            dest_dirname = os.path.join(destination, os.path.basename(source_dirname))

                            try:
                                if not overwrite and self.ftp.path.exists(dest_dirname):
                                    raise Exception("Directory already exists and overwrite not permitted")

                                if not self.ftp.path.exists(dest_dirname):
                                    self.ftp.mkdir(dest_dirname)

                                dirs_succeed.append(source_dirname)

                            except Exception as e:
                                dirs_failed.append(source_dirname)
                                self.logger.error(
                                        "Error in FTP upload_dir(): %s, traceback = %s" % (
                                            str(e), traceback.format_exc()))

                        succeed.extend(files_succeed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)

                    except Exception as e:
                        succeed.extend(files_succeed)
                        succeed.extend(dirs_succeed)
                        failed.extend(files_failed)
                        failed.extend(dirs_failed)
                        self.logger.error(
                                "Error in FTP upload_dir(): %s, traceback = %s" % (str(e), traceback.format_exc()))

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

    def fopen(self, path, pid=None, mode="rb"):
        if pid:
            self.fclose(pid)
        else:
            pid = str(uuid.uuid4())

        self.fp[pid] = self.ftp.open(path, mode)
        return pid

    def open(self, path, mode="rb", encoding=None, errors=None):
        byte_path = self.to_byte(path)
        return self.ftp.open(path=byte_path, mode=mode, encoding=encoding, errors=errors)

    def fread(self, pid, block=None):
        if block is not None:
            block = int(block)

        return self.fp[pid].read(block)

    @staticmethod
    def fwrite(fd, content, encoding):
        if isinstance(content, bytes):
            content = content.decode("utf-8")

        codecs.getwriter(encoding)(fd, errors="replace").write(content)

    def fclose(self, pid):
        try:
            self.fp[pid].close()
            del self.fp[pid]
        except Exception as e:
            self.logger.error("Error in FTP fclose(): %s, traceback = %s" % (str(e), traceback.format_exc()))

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
                    source_file = self.ftp.open(self.to_byte(source), "rb")

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
            if self.ftp.path.isdir(self.to_byte(source)):
                tree = self.ftp.walk(self.to_byte(source))
                first_level = True

                for current, dirs, files in tree:
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

                            if not self.exists(destination):
                                self.mkdir(destination)
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

    def walk(self, path):
        return self.ftp.walk(path)
