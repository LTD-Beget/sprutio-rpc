import pwd
import grp
import os
import setproctitle
import signal
import sys
import pam
import json
from misc.helpers import kill
from misc.RedisConnector import RedisConnector
from config.main import ROOT_MOUNT


from base.exc import Error
from multiprocessing import Process
from misc.logger import DummyLogger
from config.main import ROOT_MOUNT

import os
import pwd
import grp
import datetime
import base64
import stat
import hashlib
import traceback
import random
from tzlocal import get_localzone

from lib.FileManager.SSHConnection import SSHConnectionManager



class BaseMainWorker(Process):
    def __init__(self, on_success=None, on_error=None, on_running=None, on_abort=None,
                 status_id=None, logger=None, **kwargs):
        super(BaseMainWorker, self).__init__(**kwargs)

        self.logger = logger or DummyLogger()
        self._tzinfo = get_localzone()

        def empty_fn(*args2, **kwargs2):
            return None

        self.on_success = on_success or empty_fn
        self.on_running = on_running or empty_fn
        self.on_abort = on_abort or empty_fn
        self.on_error = on_error or empty_fn

        self.status_id = status_id

        self.PWS = {}
        self.GRP = {}

        self.conn = None

    def run(self):
        raise NotImplementedError()

    def _get_login_pw(self):
        return None

    def _get_login_grp(self):
        return None

    def get_login_grp(self):
        group = self._get_login_grp()

        if group is not None:
            if int(group.gr_gid) > 999:
                return group.gr_gid

        return None

    def get_login_uid(self):
        pw = self._get_login_pw()

        if pw is not None:
            if int(pw.pw_uid) > 999:
                return pw.pw_uid

        return None

    #@staticmethod
    #def random_hash():
    #    hash_str = random.getrandbits(128)
    #    return "%032x" % hash_str

    @staticmethod
    def random_hash():
        #hash_str = random.getrandbits(128)
        #return "%032x" % hash_str
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def get_rel_path(path, root_path=ROOT_MOUNT):
        #if not root_path:
        #    raise Error("You must specify root path or return pw!")
        #relpath = os.path.relpath(path, root_path)
        #relative_path = '/' + ('' if relpath == '.' else relpath)
        #print("REL PATH %s , %s, %s" % (root_path, relpath, relative_path))
        #return relative_path
        return path

    def _make_file_info(self, target_path):
        return self.ssh_manager._make_file_info(target_path)


    def _make_file_info_old(self, target_path):
        item_dir = self.get_rel_path(os.path.dirname(target_path))
        item_name = os.path.basename(target_path)

        #stat_info = os.lstat(target_path)
        sftp = self.conn.open_sftp()
        stat_info = sftp.lstat(target_path)

        mtime = stat_info.st_mtime
        size = stat_info.st_size
        mode = ('%o' % (stat.S_IMODE(stat_info.st_mode))).zfill(3)

        is_link = 0
        if stat.S_ISLNK(stat_info.st_mode):
            is_link = 1

        is_share_write = 0
        is_share = 0
        is_dir = 0

        if stat.S_ISDIR(stat_info.st_mode):
            is_dir = 1
            ext = ''
        else:
            ext = os.path.splitext(item_name)[1][1:].lower()
        try:
            owner = self._get_pw_by_uid(stat_info.st_uid).pw_name
            group = self._get_grp_by_gid(stat_info.st_gid).gr_name
        except Exception as e:
            owner = None
            group = None
            self.logger.debug("Error in get owner file %s , trace = %s" % (str(e), traceback.format_exc()))
            pass

        preview_types = ["jpg", "jpeg", "png", "gif", "tif", "tiff"]
        preview_hash = ''

        if ext in preview_types:
            preview_hash = hashlib.sha1(bytes(item_name, 'utf-8')).hexdigest()

        result = {
            'is_link': is_link,
            'is_dir': is_dir,
            'is_share': is_share,
            'is_share_write': is_share_write,
            'name': item_name,
            'ext': ext,
            'path': item_dir,
            'base64': base64.urlsafe_b64encode(bytes(item_name, 'utf-8')).decode('UTF-8'),
            'mode': mode,
            'size': size,
            'mtime': mtime,
            'mtime_str': datetime.datetime.fromtimestamp(mtime, tz=self._tzinfo).strftime('%d.%m.%Y %H:%M:%S'),
            'owner': owner,
            'group': group,
            'hash': preview_hash
        }

        return result

    def _get_pw_by_uid(self, uid):
        if uid not in self.PWS:
            self.PWS[uid] = pwd.getpwuid(uid)

        return self.PWS[uid]

    def _get_grp_by_gid(self, gid):
        if gid not in self.GRP:
            self.GRP[gid] = grp.getgrgid(gid)

        return self.GRP[gid]


class MainWorkerCustomer(BaseMainWorker):

    def __init__(self, login, password, *args, **kwargs):
        super(MainWorkerCustomer, self).__init__(*args, **kwargs)

        self.login = login
        self.password = password
        self.token = self.password

        # all forks processes here
        self.processes = []

        self._login_pw = None
        self._login_grp = None

        self.params = None

    def run(self):
        raise NotImplementedError

    def preload(self, root=False):
        redis = RedisConnector()
        if not redis.exists(self.token):
            raise Exception("Bad login/password")

        params = redis.get(self.token)
        if not params:
            raise Exception("Bad login/password")

        params = json.loads(params.decode('utf-8'))
        print("Mainworker: [PRELOAD]", params)
        self.params = params

        self.ssh_manager = SSHConnectionManager(
            server=params['ssh_server'], login=params['ssh_login'], password=None, pkey=params['ssh_key'],
            port=int(params['ssh_port'])
        )
        self.conn = self.ssh_manager.conn

        self.set_customer_uid()
        signal.signal(signal.SIGTERM, self._on_sigterm)

        # Изменяем имя процесса для мониторинга
        process_title = ' - ' + self.name + ' ' + setproctitle.getproctitle()
        setproctitle.setproctitle(process_title)

        self.logger.info("%s process started PID = %s , Title = %s" % (str(self.__class__.__name__),
                                                                       str(self.pid), str(process_title)))
        self.on_running(self.status_id, pid=self.pid, pname=self.name)


    def preload_ssh_password(self, root=False):
        self.ssh_manager = SSHConnectionManager(self.USER_SERVER, self.login, self.password)
        self.conn = self.ssh_manager.conn

        if not self.ssh_manager.do_whoami():
            raise Exception("Bad login/password")

        self.set_customer_uid()

        signal.signal(signal.SIGTERM, self._on_sigterm)

        # Изменяем имя процесса для мониторинга
        process_title = ' - ' + self.name + ' ' + setproctitle.getproctitle()
        setproctitle.setproctitle(process_title)

        self.logger.info("%s process started PID = %s , Title = %s" % (str(self.__class__.__name__),
                                                                       str(self.pid), str(process_title)))
        self.on_running(self.status_id, pid=self.pid, pname=self.name)

    def _get_login_pw(self):
        if self._login_pw is not None:
            return self._login_pw

        try:
            self._login_pw = pwd.getpwnam(self.login)

        except KeyError:
            return None

        return self._login_pw

    def _get_login_grp(self):
        if self._login_grp is not None:
            return self._login_grp

        try:
            self._login_grp = grp.getgrnam(self.login)

        except KeyError:
            return None

        return self._login_grp

    def get_home_dir(self):
        return "/home/" + self.login

    @staticmethod
    def get_abs_path(path=''):
        """
        Возвращает абсолютный путь относительно точки монтирования в контейнере

        :param path:
        :return: string
        """
        #abs_path = os.path.join(ROOT_MOUNT, MainWorkerCustomer.normalize_path(path))
        #return abs_path
        return path

    @staticmethod
    def normalize_path(path):
        """
        Нормазизует иходный путь, удаляя начальный и конечный слэш и преобразует нормализованный вид (без точек)
        :param path:
        :return:
        """

        path = os.path.normpath(path)
        path = path.strip("/")

        return path

    def set_customer_uid(self):
        pw = self._get_login_pw()
        self.logger.info("Set customer uid %s" % str(pw))

        if pw is not None:
            os.setgid(pw.pw_gid)
            os.setuid(pw.pw_uid)
        return False

    def _on_sigterm(self, num, stackframe):
        try:
            # self.logger.debug("GOT %s, %s FM worker killed! (pid = %s)" % (num, self.__class__.__name__, self.pid))
            if self.pid is not None:
                kill(self.pid, signal.SIGKILL, self.logger)
            for p in self.processes:
                try:
                    # no logs here during deadlock
                    kill(p.pid, signal.SIGKILL, self.logger)
                except OSError:
                    pass
        except Exception:
            # self.logger.error("Error on_sigterm() %s , error %s" % (str(e), traceback.format_exc()))
            sys.exit(1)

    def close_ssh_connection(self):
        if self.conn:
            self.conn.close()
