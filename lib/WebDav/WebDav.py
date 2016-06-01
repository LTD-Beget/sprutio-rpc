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


def transfer_from_ftp_to_webdav(source_ftp, target_webdav, source_path, target_path):
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
    def __init__(self, host, user, passwd, timeout=-999, logger=None):
        self.fp = dict()

        webdav_host = host

        self.webdav_host = webdav_host
        self.host = host
        self.user = user
        self.passwd = passwd

        options = {
            'webdav_hostname': self.webdav_host,
            'webdav_login': self.user,
            'webdav_password': self.passwd
        }

        self.webdavClient = wc.Client(options)
        self.resource = urn.Urn('/', True)

        self.logger = logger
        self._tzinfo = TimeZoneMSK()

    def parent(self, path):
        return urn.Urn(path).parent()

    def path(self, path):
        return urn.Urn(path).path()

    def getcwd(self):
        return self.resource.parent()

    def close(self):
        pass
        #self.webdav

    def _make_file_info(self, file_path):
        self.logger.info("Make file info %s" % file_path)

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
        if is_dir:
            ext = b''
        else:
            ext = file_name.split('.')[1].lower()

        mtime = info['modified']

        file_info = {
            "is_dir": is_dir,
            "is_link": is_link,
            "name": file_name,
            "ext": ext,
            "path": file_dir,
            "owner": self.user,
            "mode": self.getmode(info),
            "size": info['size'] if not is_dir else 0,
            "mtime": mtime,
            'mtime_str': str(mtime),
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

    def clear_cache(self):
        return self.webdavClient.stat_cache.clear()

    def getmode(self, info):
        try:
            return 'my mode'
        except Exception as e:
            self.logger.error("Error in WebDav getmode(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception

    def list(self, path):
        self.logger.info("List %s" % path)

        flist = {
            "path": path,
            "items": []
        }

        listdir = self.webdavClient.list(self.to_byte(path))

        for name in listdir:
            item_path = '{0}/{1}'.format(path, name)
            flist["items"].append(self._make_file_info(item_path))

        return flist

    def listdir(self, path):
        byte_path = self.to_byte(path)

        listdir = self.webdavClient.list(byte_path)

        listing = []
        for name in listdir:
            if isinstance(name, str):
                name = name.encode("ISO-8859-1")
            item_path = self.resource.path().join(byte_path, name)
            listing.append(item_path)
        return listing

    def file_info(self, path):

        byte_path = self.to_byte(path)

        file_info = self._make_file_info(byte_path)
        return file_info

    def rename(self, source, target):

        byte_source = self.to_byte(source)

        byte_target = self.to_byte(target)

        if not self.path.exists(byte_source):
            raise Exception("Entry with source name not exists")

        if self.path.exists(byte_target):
            raise Exception("Entry with target name already exists")

        resource = self.webdavClient.resource(byte_source)
        resource.rename(byte_target)

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
            if not self.isfile(source):
                failed.append(source)
                raise Exception("Source is not a file")

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







