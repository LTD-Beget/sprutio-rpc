import os
import stat
import uuid
import codecs
import ftplib
import pprint
import traceback
import webdav.client as wc
import webdav.urn as urn
import datetime
import time
import psutil
import signal
from misc.helpers import kill
from multiprocessing import Process, JoinableQueue, Queue
from lib.FileManager.FM import REQUEST_DELAY

TIMEOUT_LIMIT = 10

def transfer_from_webdav_to_webdav(source_ftp, target_webdav, source_path, target_path):
    """
    Копирует файл между WebDav соединенеиями
    :param FTP source_ftp:
    :param WebDav target_webdav:
    :param str source_path:
    :param str target_path:
    """
    #source_file = file_transfer.RemoteFile(source_ftp.ftp, source_ftp.to_byte(source_path), "rb")
    #target_file = file_transfer.RemoteFile(target_webdav, target_webdav.to_byte(target_path), "wb")

    #source_fobj = source_file.fobj()
    #try:
        #target_fobj = target_file.fobj()
        #try:
            #file_transfer.copyfileobj(source_fobj, target_fobj)
        #finally:
            #target_fobj.close()
    #finally:
        #source_fobj.close()
    pass


class TimeZoneMSK(datetime.tzinfo):
    def dst(self, dt):
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        return datetime.timedelta(hours=3)

    def tzname(self, dt):
        return 'GMT'


class WebDavSession(ftplib.FTP):
    def __init__(self, host, user, passwd, timeout):
        #ftplib.FTP.__init__(self)
        self.set_debuglevel(2)
        # self.set_debuglevel(0)
        self.connect(host, timeout)
        self.login(user, passwd)
        self.set_pasv(True)


class WebDav:
    NUM_WORKING_PROCESSES = 5

    def __init__(self, host, user, passwd, timeout=-999, logger=None):
        self.fp = dict()

        webdav_host = host

        self.webdav_host = webdav_host
        self.host = host
        self.user = user
        self.passwd = passwd
        self.processes = []
        self.file_queue = JoinableQueue(maxsize=0)
        self.result_queue = Queue(maxsize=0)

        self.is_alive = {
            "status": True
        }

        options = {
            'webdav_hostname': self.webdav_host,
            'webdav_login': self.user,
            'webdav_password': self.passwd
        }

        self.webdavClient = wc.Client(options)

        self.logger = logger
        self._tzinfo = TimeZoneMSK()

    def parent(self, path):
        return urn.Urn(path).parent()

    def path(self, path):
        return urn.Urn(path).path()

    def close(self):
        pass
        #self.webdav

    def generate_file_info(self, file_path):
        info = self.webdavClient.info(file_path)

        is_dir = False
        is_link = False

        if self.webdavClient.is_dir(file_path):
            is_dir = True
        else:
            pass

        file_name = urn.Urn(file_path).filename().replace("/", "")
        file_dir = urn.Urn(file_path).parent()

        ext = ''
        divide = file_name.split('.')
        if len(divide) > 1:
            ext = file_name.split('.')[-1].lower()

        mtime = info['modified']

        file_info = {
            "is_dir": is_dir,
            "is_link": is_link,
            "name": file_name,
            "ext": ext,
            "path": file_dir,
            "owner": self.user,
            "mode": "600",
            "size": info['size'] if not is_dir else 0,
            "mtime": mtime,
            'mtime_str': str(mtime),
        }
        return file_info

    def _make_file_info(self, file_queue, result_queue, logger, timeout):
        while int(time.time()) < timeout:
            if file_queue.empty() is not True:
                file_path = file_queue.get()
                try:
                    file_info = self.generate_file_info(file_path)
                    result_queue.put(file_info)
                except UnicodeDecodeError as unicode_e:
                    logger.error(
                        "UnicodeDecodeError %s, %s" % (str(unicode_e), traceback.format_exc()))

                except IOError as io_e:
                    logger.error("IOError %s, %s" % (str(io_e), traceback.format_exc()))

                except Exception as other_e:
                    logger.error("Exception %s, %s" % (str(other_e), traceback.format_exc()))
                finally:
                    file_queue.task_done()
            else:
                time.sleep(REQUEST_DELAY)

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
            return self.webdavClient.info(path)['size']
        except Exception as e:
            self.logger.error("Error in WebDav size(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            return 0

    def info(self, path):
        return self.webdavClient.info(self.to_byte(path))

    def exists(self, path):
        return self.webdavClient.check(path)

    def isdir(self, path):
        return self.webdavClient.is_dir(path)

    def isfile(self, path):
        return not self.webdavClient.is_dir(self.to_byte(path))

    def list(self, path):
        flist = {
            "path": path,
            "items": []
        }

        listdir = self.webdavClient.list(self.to_byte(path))
        self.logger.info("listdir=%s", listdir)

        start_time = time.time()
        time_limit = int(time.time()) + TIMEOUT_LIMIT

        self.file_queue = JoinableQueue(maxsize=0)
        self.result_queue = Queue(maxsize=0)

        for i in range(self.NUM_WORKING_PROCESSES):
            p = Process(target=self._make_file_info, args=(self.file_queue, self.result_queue, self.logger, time_limit))
            p.start()
            proc = psutil.Process(p.pid)
            proc.ionice(psutil.IOPRIO_CLASS_IDLE)
            proc.nice(20)
            self.logger.debug(
                    "ListDir worker #%s, set ionice = idle and nice = 20 for pid %s" % (
                        str(i), str(p.pid)))
            self.processes.append(p)

        for name in listdir:
            try:
                item_path = '{0}/{1}'.format(path, name)
                self.file_queue.put(item_path)
            except UnicodeDecodeError as e:
                self.logger.error(
                    "UnicodeDecodeError %s, %s" % (str(e), traceback.format_exc()))

            except IOError as e:
                self.logger.error("IOError %s, %s" % (str(e), traceback.format_exc()))

            except Exception as e:
                self.logger.error(
                    "Exception %s, %s" % (str(e), traceback.format_exc()))

        while int(time.time()) <= time_limit:
            self.logger.debug("file_queue size = %s , empty = %s (timeout: %s/%s)" % (
                self.file_queue.qsize(), self.file_queue.empty(), str(int(time.time())), time_limit))
            if self.file_queue.empty():
                self.logger.debug("join() file_queue until workers done jobs")
                self.file_queue.join()
                break
            else:
                time.sleep(REQUEST_DELAY)

        for p in self.processes:
            try:
                self.logger.debug("WebDav ListDir terminate worker process, pid = %s" % p.pid)
                kill(p.pid, signal.SIGKILL, self.logger)
            except OSError:
                self.logger.error(
                    "ListDir unable to terminate worker process, pid = %s" % p.pid)

        if self.is_alive['status'] is True:
            while not self.result_queue.empty():
                file_info = self.result_queue.get()
                flist["items"].append(file_info)

        return flist

    def listdir(self, path):
        listdir = self.webdavClient.list(path)

        listing = []
        for name in listdir:
            item_path = '{0}/{1}'.format(path, name)
            listing.append(item_path)
        return listing

    def file_info(self, path):

        byte_path = self.to_byte(path)

        file_info = self._make_file_info(byte_path)
        return file_info

    def rename(self, source, target):
        if not self.exists(source):
            raise Exception("Entry with source name not exists")

        if self.exists(target):
            raise Exception("Entry with target name already exists")

        self.webdavClient.move(source, target)

    def remove(self, target):
        byte_target = self.to_byte(target)

        try:
            self.webdavClient.clean(byte_target)
        except Exception as e:
            self.logger.error("Error in WebDav dir remove(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception

    def file(self, target, mode):
        return self.webdavClient.open(self.to_byte(target), mode)

    def mkdir(self, path):
        return self.webdavClient.mkdir(self.to_byte(path))

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

            if not overwrite and self.webdavClient.check(target_path):
                failed.append(source)
                raise Exception("File already exists and overwrite not permitted")

            try:
                self.webdavClient.upload(self.to_string(target_path), source)
            except Exception as e:
                failed.append(source)
                self.logger.error("Error in WebDav upload(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception("Error during file uploading %s" % traceback.format_exc())

            succeed.append(source)

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = True
            result['error'] = None
            result['file_list'] = file_list

            return result

        except Exception as e:
            self.logger.error("Error in WebDav upload(): %s, traceback = %s" % (str(e), traceback.format_exc()))

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    def download(self, source, target):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            target_path = os.path.join(target, os.path.basename(source))

            try:
                self.webdavClient.download(self.to_byte(source), target_path)
            except Exception as e:
                failed.append(source)
                self.logger.error("Error in WebDav download(): %s, traceback = %s" % (str(e), traceback.format_exc()))
                raise Exception("Error during file download")

            succeed.append(source)

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = True
            result['error'] = None
            result['file_list'] = file_list

            return result

        except Exception as e:
            self.logger.error("Error in WebDav download(): %s, traceback = %s" % (str(e), traceback.format_exc()))

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = False
            result['error'] = e
            result['file_list'] = file_list

            return result

    def copy_file(self, source, target, overwrite=False):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if not overwrite and self.exists(target):
                failed.append(source)
                raise Exception('file exist and cannot be overwritten')

            try:
                self.webdavClient.copy(source, target)

            except Exception as e:
                failed.append(source)
                raise Exception('Cannot copy file %s' % (e,))

            succeed.append(source)

            file_list['succeed'] = succeed
            file_list['failed'] = failed

            result['success'] = True
            result['error'] = None
            result['file_list'] = file_list

            return result

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
