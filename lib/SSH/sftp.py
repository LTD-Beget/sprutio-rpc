# -*- coding: utf-8 -*-
import traceback
import os
import stat
import shutil
import datetime
import hashlib
import base64
from binaryornot.helpers import is_binary_string

from lib.SSH.ssh import SSH


class TimeZone(datetime.tzinfo):
    def dst(self, dt):
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        return datetime.timedelta(hours=5)

    def tzname(self, dt):
        return 'GMT'


class SFTP(SSH):
    def __init__(self, *args, **kwargs):
        super(SFTP, self).__init__(*args, **kwargs)
        self.sftp = self.open_sftp()

        self._tzinfo = TimeZone()

    def open(self, filename, mode='r', bufsize=-1):
        """
        :param str filename: name of the file to open
        :param str mode: mode (Python-style) to open in
        :param int bufsize: desired buffering (-1 = default buffer size)
        :return: an `.SFTPFile` object representing the open file

        :raises IOError: if the file could not be opened.
        """
        return self.sftp.open(filename, mode, bufsize)

    def list(self, path):

        flist = {
            "path": path,
            "items": []
        }

        for name in self.listdir(path):
            flist["items"].append(self.make_file_info(name))

        return flist

    def listdir(self, path):
        listing = []
        for name in self.sftp.listdir(path):
            item_path = os.path.join(path, name)
            listing.append(item_path)
        return listing

    def make_file_info(self, file_path):
        item_name = os.path.basename(file_path)
        info = self.sftp.lstat(file_path)

        is_dir = 0
        is_link = 0

        if stat.S_ISDIR(info.st_mode):
            is_dir = 1
        elif stat.S_ISLNK(info.st_mode):
            is_link = 1
        elif stat.S_ISREG(info.st_mode):
            pass
        else:
            pass

        file_name = os.path.basename(file_path)
        file_dir = os.path.dirname(file_path)

        if is_dir:
            ext = ''
        else:
            ext = os.path.splitext(file_name)[1][1:].lower()

        mtime = info.st_mtime

        preview_types = ["jpg", "jpeg", "png", "gif", "tif", "tiff"]
        preview_hash = ''

        if ext in preview_types:
            preview_hash = hashlib.sha1(bytes(item_name, 'utf-8')).hexdigest()

        file_info = {
            "is_dir": is_dir,
            "is_link": is_link,
            'is_share': 0,
            'is_share_write': 0,

            "name": file_name,
            "ext": ext,
            "path": file_dir,
            'base64': base64.urlsafe_b64encode(bytes(item_name, 'utf-8')).decode('UTF-8'),

            "owner": self.getowner(info.st_uid),
            "mode": self.getmode(info),
            "size": info.st_size if not is_dir else 0,
            "mtime": mtime,
            'mtime_str': datetime.datetime.fromtimestamp(mtime, self._tzinfo).strftime('%d.%m.%Y %H:%M:%S'),
            'hash': preview_hash
        }

        return file_info

    def get_home_dir(self, cache={}):
        """
        return home directory of user
        :param cache: mutable, save value to don't run ssh command too often
        :return:
        :rtype: str
        """
        id = self.hostname + self.username
        if id not in cache:
            res = self.run('getent passwd "$USER" | cut -d: -f6')
            cache[id] = res.stdout.decode().split(":", 1)[0]
            return res.stdout.decode().split(":", 1)[0]
        else:
            return cache[id]

    def get_current_dir(self):
        res = self.run("pwd")
        return res.stdout.decode().split(":", 1)[0]

    def getowner(self, uid, cache={}):
        """
        return username by uid
        :param uid:
        :param cache: mutable, save value to don't run ssh command too often
        :return:
        :rtype: str
        """
        if uid not in cache:
            res = self.run("getent passwd {}".format(uid))
            cache[uid] = res.stdout.decode().split(":", 1)[0]
            return res.stdout.decode().split(":", 1)[0]
        else:
            return cache[uid]

    def getmode(self, info):
        try:
            mode = stat.S_IMODE(info.st_mode)
            mode = oct(int(mode))
            mode = mode[2:].zfill(3)  # for python 3 0o  as octal prefix
            return mode
        except Exception as e:
            self.logger.error("Error in SFTP getmode(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception

    def stat(self, path):
        return self.sftp.lstat(path)

    def exists(self, path):
        try:
            self.stat(path)
        except IOError:
            return False
        else:
            return True

    def isdir(self, path):
        try:
            info = self.stat(path)
            return stat.S_ISDIR(info.st_mode)
        except IOError:
            return False

    def isfile(self, path):
        try:
            info = self.stat(path)
            return stat.S_ISREG(info.st_mode)
        except IOError:
            return False

    def islink(self, path):
        try:
            info = self.stat(path)
            return stat.S_ISLNK(info.st_mode)
        except IOError:
            return False

    def mkdir(self, path, mode=0o777):
        try:
            self.sftp.chdir(path)
        except IOError:
            self.sftp.mkdir(path, mode)

    def makedirs(self, path, mode=0o777):
        if self.isdir(path):
            return

        rest_path, folder = os.path.split(path)
        if rest_path and not self.isdir(rest_path):
            self.makedirs(rest_path, mode)

        if folder:
            self.mkdir(path, mode)

    def rmtree(self, path):
        """
        Runs 'rm -rf path'
        :param path:
        :return: RunStatus
        """
        path = self._escape_single_quote(path)
        return self.run("rm -rf -- '{}'".format(path))

    def move(self, from_path, to_path):
        """
        Sounds like rename, but moves like move)
        :param from_path: str
        :param to_path: str
        :return:
        """
        self.sftp.rename(from_path, to_path)

    def remove(self, path):
        """
        Remove file
        :param str path:
        :return:
        """
        self.sftp.remove(path)

    def cp_sftp(self, from_path, to_path):
        """
        Runs 'cp from_path to_path'
        :param from_path: str
        :param to_path: str
        :return:
        """
        with self.sftp.open(from_path) as from_file_obj:
            self.sftp.putfo(from_file_obj, to_path)

    def mv_sftp(self, from_path, to_path):
        """
        Runs 'mv from_path to_path'
        :param from_path: str
        :param to_path: str
        :return:
        """
        with self.sftp.open(from_path) as from_file_obj:
            self.sftp.putfo(from_file_obj, to_path)
        self.sftp.remove(from_path)

    @staticmethod
    def _escape_single_quote(s):
        """
         Replace single quotes with '"'"'
         which means "close single, open double, actually print single, close double, again open single"
        :param str s:
        :return:
        :rtype: str
        """
        return s.replace("'", """'"'"'""")

    def is_binary(self, file_path):
        # sftp аналог `from binaryornot.check import is_binary`
        if file_path[-3:] == "pyc":
            return False
        with self.sftp.file(file_path) as f:
            chunk = f.read(1024)
            return is_binary_string(chunk)

    def walk(self, remote_path):
        path = remote_path
        files = []
        folders = []
        for f in self.sftp.listdir_attr(remote_path):
            if stat.S_ISDIR(f.st_mode):
                folders.append(f.filename)
            else:
                files.append(f.filename)

        yield path, folders, files
        for folder in folders:
            new_path = os.path.join(remote_path, folder)
            for x in self.walk(new_path):
                yield x

    def rsync_from(self, remote_path, local_path, overwrite=True, progress=None):
        """
        Копирует содержимое удаленной папки в локальную папку
        Пути должны быть абсолютными
        :param str remote_path:
        :param str local_path:
        :param bool overwrite:
        :param progress:
        :return:
        :rtype: bool
        """
        self.logger.info("rsync_from, remote_path={} local_path={} overwrite={}".format(
                                remote_path, local_path, overwrite))
        if progress is None:
            progress = {"processed": 0}
        try:
            file_basename = os.path.basename(remote_path)
            dir_path = os.path.dirname(remote_path)

            if self.isdir(remote_path):
                for current, dirs, files in self.walk(remote_path):
                    relative_root = os.path.relpath(current, dir_path)

                    local_dir = os.path.join(local_path, relative_root)
                    if not os.path.isdir(local_dir):
                        if os.path.exists(local_dir):
                            if overwrite:
                                shutil.rmtree(local_dir)
                            else:
                                raise Exception("local_dir is not a dir")
                        # если не папка и не существует/удалена, создаём папку
                        # st = self.sftp.stat(os.path.join(current))
                        os.makedirs(local_dir)
                    progress["processed"] += 1

                    for f in files:
                        target_file = os.path.join(local_path, relative_root, f)
                        source_file = os.path.join(current, f)
                        if not os.path.exists(target_file):
                            self.logger.info('self.sftp.get 317 {} {}'.format(source_file, target_file))
                            self.sftp.get(source_file, target_file)
                        elif overwrite:
                            if os.path.isdir(target_file):
                                shutil.rmtree(target_file)
                            else:
                                os.remove(target_file)
                            self.sftp.get(source_file, target_file)
                        else:
                            pass
                        progress["processed"] += 1

            elif self.isfile(remote_path):
                try:
                    target_file = os.path.join(local_path, file_basename)
                    if not os.path.exists(target_file):
                        self.sftp.get(remote_path, target_file)
                    elif overwrite:
                        if os.path.isdir(target_file):
                            shutil.rmtree(target_file)
                        else:
                            os.remove(target_file)
                        self.sftp.get(remote_path, target_file)
                    progress["processed"] += 1
                except Exception as e:
                    self.logger.info("Cannot copy file %s , %s" % (remote_path, str(e)))
                    raise e

            return True

        except Exception as e:
            self.logger.error(
                "Error rsync_from %s , error %s , %s" % (str(local_path), str(e), traceback.format_exc()))
            return False

    def rsync_to(self, local_path, remote_path, overwrite=True, progress=None):
        """
        Копирует содержимое локальной папки в удалённую папку
        Пути должны быть абсолютными
        :param str local_path:
        :param str remote_path:
        :param bool overwrite:
        :param progress:
        :return:
        :rtype: bool
        """
        self.logger.info(
            "rsync_to, local_path {} remote_path {} overwrite {}".format(
                local_path, remote_path, overwrite
            )
        )
        if progress is None:
            progress = {"processed": 0}
        try:
            file_basename = os.path.basename(local_path)
            dir_path = os.path.dirname(local_path)

            if os.path.isdir(local_path):
                for current, dirs, files in os.walk(local_path):
                    relative_root = os.path.relpath(current, dir_path)

                    for d in dirs:
                        target_dir = os.path.join(remote_path, relative_root, d)

                        if not self.isdir(target_dir):
                            if self.exists(target_dir):
                                if overwrite:
                                    self.rmtree(target_dir)
                                else:
                                    raise Exception("target_dir is not a dir")
                            # если не папка и не существует/удалена, создаём папку
                            st = os.stat(os.path.join(current, d))
                            self.makedirs(target_dir, stat.S_IMODE(st.st_mode))
                        progress["processed"] += 1

                    for f in files:
                        target_file = os.path.join(remote_path, relative_root, f)
                        source_file = os.path.join(current, f)
                        if not self.exists(target_file):
                            self.sftp.put(source_file, target_file)
                        elif overwrite:
                            if self.isdir(target_file):
                                self.rmtree(target_file)
                            else:
                                self.sftp.remove(target_file)
                            self.sftp.put(source_file, target_file)
                        else:
                            pass
                        progress["processed"] += 1

            elif os.path.isfile(local_path):
                try:
                    target_file = os.path.join(remote_path, file_basename)
                    if not self.exists(target_file):
                        self.sftp.put(local_path, target_file)
                    elif overwrite:
                        if self.isdir(target_file):
                            self.rmtree(target_file)
                        else:
                            self.sftp.remove(target_file)
                        self.sftp.put(local_path, target_file)
                    progress["processed"] += 1
                except Exception as e:
                    self.logger.info("Cannot copy file %s , %s" % (local_path, str(e)))
                    raise e

            return True

        except Exception as e:
            self.logger.error(
                "Error rsync_to %s , error %s , %s" % (str(local_path), str(e), traceback.format_exc()))
            return False
