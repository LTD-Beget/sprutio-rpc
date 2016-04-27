# -*- coding: utf-8 -*-
import sqlite3
from config.main import DB_FILE

from lib.SSH.ssh import SSHConnection
from lib.SSH.lsparser import parse as parse_ls_output
from lib.SSH.statparser import parse as parse_stat_output

import traceback
import os
import stat
import logging
import shutil
import datetime
import hashlib
import base64

from binaryornot.helpers import is_binary_string


class TimeZoneEKB(datetime.tzinfo):
    def dst(self, dt):
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        return datetime.timedelta(hours=5)

    def tzname(self, dt):
        return 'GMT'


class SSHConnectionManager(object):

    def __init__(self, server, login, password=None, pkey=None, port=22):
        self.conn = SSHConnection(hostname=server, username=login, password=password, pkey=pkey, port=port)
        self._sftp = None

        self.logger = logging.getLogger("SSHConnectionManager")
        self._tzinfo = TimeZoneEKB()

    @property
    def sftp(self):
        if not self._sftp:
            self._sftp = self.conn.open_sftp()
        return self._sftp

    def list(self, path):

        flist = {
            "path": path,
            "items": []
        }

        for name in self.listdir(path):
            item_path = os.path.join(path, name)
            flist["items"].append(self._make_file_info(item_path))

        return flist

    def listdir(self, path):
        byte_path = path

        self.sftp.chdir(path)

        listing = []
        for name in self.sftp.listdir(path):
            item_path = os.path.join(byte_path, name)
            listing.append(item_path)
        return listing

    def _make_file_info(self, file_path):

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

    def getowner(self, id_number):
        res = self.conn.run("getent passwd {}".format(id_number))
        print("getowner", id_number, res.stdout)
        return res.stdout.decode().split(":", 1)[0]

    def getmode(self, info):
        try:
            mode = stat.S_IMODE(info.st_mode)
            mode = oct(int(mode))
            mode = mode[2:].zfill(3)  # for python 3 0o  as octal prefix
            return mode
        except Exception as e:
            self.logger.error("Error in FTP getmode(): %s, traceback = %s" % (str(e), traceback.format_exc()))
            raise Exception


    def stat(self, path):
        res = self.conn.run("stat {}".format(path))
        return parse_stat_output(res.stdout, path)

    def do_whoami(self):
        res = self.conn.run("whoami")
        return res.succeeded

    def get_stat(self, path):
        return self.sftp.lstat(path)

    def exists(self, path):
        try:
            self.get_stat(path)
        except IOError:
            return False
        else:
            return True

    def isdir(self, path):
        try:
            info = self.get_stat(path)
            return stat.S_ISDIR(info.st_mode)
        except:
            return False

    def mkdir(self, path):
        try:
            self.sftp.listdir(path)
        except IOError:
            self.sftp.mkdir(path)

    def makedirs(self, path):
        # TODO: write this method
        if self.isdir(path):
            return

        rest_path, folder = os.path.split(path)
        if rest_path and not self.isdir(rest_path):
            self.makedirs(rest_path)

        if folder:
            self.mkdir(path)

    def isfile(self, path):
        return not (self.isdir(path) or self.islink(path))

    def islink(self, path):
        info = self.get_stat(path)
        return stat.S_ISLNK(info.st_mode)

    def rmtree(self, path):
        self.conn.run("rm -rf '{}'".format(path))

    def isbinary(self, file_path):
        # аналог `from binaryornot.check import is_binary`
        # для sftp
        _, ext = os.path.splitext(file_path)
        if ext == "pyc":
            return False

        with self.sftp.file(file_path) as f:
            chunk = f.read(1024)
            return is_binary_string(chunk)


    def walk(self, remotepath):
        path = remotepath
        files = []
        folders = []
        for f in self.sftp.listdir_attr(remotepath):
            if stat.S_ISDIR(f.st_mode):
                folders.append(f.filename)
            else:
                files.append(f.filename)

        yield path, folders, files
        for folder in folders:
            new_path = os.path.join(remotepath,folder)
            for x in self.walk(new_path):
                yield x

    def rsync(self, remote_path, local_path, inside=False):
        """
        Копирует содержимое удаленной папки в локальную папку
        """
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        print("rsync, remote path", remote_path, "local_path", local_path, "inside", inside)

        try:
            abs_path = remote_path
            source_path = os.path.dirname(abs_path)
            file_basename = os.path.basename(abs_path)

            if self.isdir(abs_path):
                if not inside:
                    destination = os.path.join(local_path, file_basename)

                    if not os.path.exists(destination):
                        os.makedirs(destination)

                for current, dirs, files in self.walk(abs_path):
                    relative_root = os.path.relpath(current, source_path) if not inside else ""

                    for d in dirs:
                        target_dir = os.path.join(local_path, relative_root, d)
                        if not os.path.exists(target_dir):
                            os.makedirs(target_dir)

                    for f in files:
                        source_file = os.path.join(current, f)
                        target_file = os.path.join(local_path, relative_root, f)
                        self.sftp.get(source_file, target_file)

                        print("Copy (rsync) %s" % source_file)

            elif self.isfile(abs_path):
                try:
                    target_file = os.path.join(local_path, file_basename)
                    self.sftp.get(abs_path, target_file)
                except Exception as e:
                    self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                    raise e

            return True

        except Exception as e:
            self.logger.error(
                    "Error rsync %s , error %s , %s" % (str(remote_path), str(e), traceback.format_exc()))
            return False

    def remote_copy_new(self, from_path, to_path, temp_dir=None, create_folder=False):
        import uuid

        if not temp_dir:
            temp_dir = os.path.join("/tmp", str(uuid.uuid4()))

        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        try:
            if self.isdir(from_path):
                self.sync_new(from_path, temp_dir, direction="rl", create_folder=create_folder)
                #self.sync_new(temp_dir, to_path, direction="lr", create_folder=create_folder)
                self.sync_new(temp_dir, to_path, direction="lr")
            else:
                temp_file = os.path.join(temp_dir, os.path.basename(from_path))

                self.sync_new(from_path, temp_file, direction="rl")
                self.sync_new(temp_file, to_path, direction="lr")
        finally:
            shutil.rmtree(temp_dir)

    def sync_new(self, from_path, to_path, direction="lr", temp_dir=None, create_folder=False):
        """
        direction:
          #ll local->-local
          lr local->-remote
          rl remote->-local
          rr remote->-remote
        """

        if direction == "lr":
            # local->-remote
            walk = os.walk
            put = self.sftp.put
            mkdir = self.mkdir
            exists = self.exists
            isdir = os.path.isdir
        elif direction == "rl":
            # rl remote->-local
            walk = self.walk
            put = self.sftp.get
            mkdir = os.mkdir
            exists = os.path.exists
            isdir = self.isdir

        print("SYNCNEW", from_path, to_path)

        if isdir(from_path):
            if not exists(to_path):
                mkdir(to_path)

            # for folder creation
            dirname = os.path.basename(from_path)
            dest_base_folder = None
            if create_folder:
                dest_base_folder = os.path.join(to_path, dirname)
                if not exists(dest_base_folder):
                    mkdir(dest_base_folder)

            for current_dir, dirs, files in walk(from_path):
                print("cur_dir", current_dir)
                print("dirs", dirs)
                print("files", files)
                print

                # rel_path = os.path.relpath(from_path, current_dir)
                rel_path = current_dir.replace(from_path, "")
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]

                if create_folder:
                    if rel_path:
                        rel_path  = dirname + "/" + rel_path
                    else:
                        rel_path  = dirname + rel_path

                print("rel_path", rel_path)

                # make dirs
                for d in dirs:
                    destination_dir_path = os.path.join(to_path, rel_path, d)
                    print("destination_dir_path", destination_dir_path)
                    mkdir(destination_dir_path)

                # sync files
                for f in files:
                    from_file = os.path.join(current_dir, f)
                    destination_file_path = os.path.join(to_path, rel_path, f)
                    print("destination_file_path", destination_file_path)
                    put(from_file, destination_file_path)

        else:
            # a file
            if direction == "rl":
                dest_folder = os.path.dirname(to_path)
                if not exists(dest_folder):
                    os.makedirs(dest_folder)

            put(from_path, to_path)

    def sync(self, local_path, remote_path, inside=False, by_folder=False):
        """
        Копирует содержимое локальной папки в удаленную
        """
        print("SYNC", local_path, remote_path, inside)
        try:
            self.sftp.listdir(remote_path)
        except IOError:
            self.sftp.mkdir(remote_path)

        try:
            abs_path = local_path
            source_path = os.path.dirname(abs_path)
            file_basename = os.path.basename(abs_path)

            if os.path.isdir(abs_path):
                if not inside and not by_folder:
                    destination = os.path.join(remote_path, file_basename)

                    try:
                        self.sftp.listdir(destination)
                    except IOError:
                        self.sftp.mkdir(destination)


                for current, dirs, files in os.walk(abs_path):
                    print("current", current, "dirs", dirs, "files", files, "source_path", source_path)
                    relative_root = os.path.relpath(current, source_path) if not inside else ""
                    print("relative_root", relative_root)

                    remote_relative_root = relative_root
                    if by_folder:
                        tmp_dirname = os.path.basename(local_path)
                        print("local_path", local_path)
                        remote_relative_root = os.path.relpath(remote_relative_root, tmp_dirname)


                    for d in dirs:

                        target_dir = os.path.join(remote_path, remote_relative_root, d)
                        print("sync (mkdir)", target_dir, remote_relative_root, d)


                        try:
                            self.sftp.listdir(target_dir)
                        except IOError:
                            self.sftp.mkdir(target_dir)

                    for f in files:
                        source_file = os.path.join(current, f)
                        target_file = os.path.join(remote_path, remote_relative_root, f)
                        self.sftp.put(source_file, target_file)

                        print("sync (put file) %s" % source_file, target_file)

            elif os.path.isfile(abs_path):
                try:
                    target_file = os.path.join(remote_path, file_basename)
                    self.sftp.put(target_file, abs_path)
                except Exception as e:
                    self.logger.info("Cannot copy file %s , %s" % (abs_path, str(e)))
                    raise e

            return True

        except Exception as e:
            self.logger.error(
                    "Error sync %s , error %s , %s" % (str(remote_path), str(e), traceback.format_exc()))
            return False


    def remote_copy(self, remote_source_path, remote_target_path, temp_folder, rsync_inside=True, sync_inside=True, by_folder=False):
        """
        Создает копию файла/папки на удаленном сервере
        """
        # копируем источник локально
        print("remote_copy", remote_source_path, remote_target_path, temp_folder)
        self.rsync(remote_source_path, temp_folder, inside=rsync_inside)
        self.sync(temp_folder, remote_target_path, inside=sync_inside, by_folder=by_folder)
        #shutil.rmtree(temp_folder)

    def get_server_data(self, login, server_id):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM ssh_servers WHERE fm_login = ? AND id = ?", (login, server_id))
            result = cursor.fetchone()

            if result is None:
                raise Exception("FTP Connection not found")

            ftp_session = {
                'id': result[0],
                'host': result[2],
                'port': result[3],
                'user': result[4],
                'password': result[5]
            }
            return ftp_session

        except Exception as e:
            raise e
        finally:
            db.close()
