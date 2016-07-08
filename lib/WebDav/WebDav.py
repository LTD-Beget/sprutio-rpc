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

        try:
            self.webdavClient.check('/')
        except Exception:
            raise Exception("Error during establishing webdav connection")

        listdir = self.webdavClient.list(self.to_byte(path))
        self.logger.info("listdir=%s", listdir)

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

        while not self.file_queue.empty():
            self.logger.debug("file_queue size = %s , empty = %s (timeout: %s/%s)" % (
                self.file_queue.qsize(), self.file_queue.empty(), str(int(time.time())), time_limit))
            time.sleep(REQUEST_DELAY)

        if self.file_queue.empty():
            self.logger.debug("join() file_queue until workers done jobs")
            self.file_queue.join()

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

    def remove(self, target):
        try:
            self.logger.debug("Removing target=%s" % target)
            if self.isdir(target):
                target += '/'
            self.webdavClient.unpublish(target)
            self.webdavClient.clean(target)
        except Exception as e:
            self.logger.error("Error in WebDav dir remove(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception

    def mkdir(self, path):
        self.logger.debug("Creating directory=%s" % path)
        return self.webdavClient.mkdir(path)

    def upload(self, source, target, overwrite=False, rename=None, operation_progress=None):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if rename is not None:
                target_path = os.path.join(target, rename)
            else:
                target_path = os.path.join(target, source)

            if not overwrite and self.exists(target_path):
                failed.append(source)
                raise Exception("File '%s' already exists and overwrite not permitted" % target_path)

            try:
                self.logger.debug("Uploading target_path=%s, source=%s" % (target_path, source))
                self.webdavClient.upload(self.to_string(target_path), source, operation_progress)
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
                self.logger.debug("Downloading source=%s, target_path=%s" % (source, target_path))
                self.webdavClient.download(source, target_path)
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
                self.logger.debug("Copying file source=%s, target=%s" % (source, target))
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

    def move_file(self, source, target, overwrite=False):
        result = {}
        file_list = {}

        succeed = []
        failed = []

        try:
            if not overwrite and self.exists(target):
                failed.append(source)
                raise Exception('file exist and cannot be overwritten')

            try:
                self.logger.debug("Moving file source=%s, target=%s" % (source, target))
                self.webdavClient.move(source, target)

            except Exception as e:
                failed.append(source)
                raise Exception('Cannot move file %s' % (e,))

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

    def make_destination_dir(self, destination, overwrite):
        self.logger.info("making destination %s" % destination)
        if not self.exists(destination):
            self.mkdir(destination)
        elif overwrite and self.exists(destination) and not self.isdir(destination):
            self.remove(destination)
            self.mkdir(destination)
        elif not overwrite and self.exists(destination) and not self.isdir(destination):
            raise Exception("destination is not a dir")
        else:
            pass