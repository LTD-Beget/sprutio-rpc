from lib.FileManager.workers.baseWorker import BaseWorker

import pwd
import grp
import os
import setproctitle
import signal
import sys
import pam
from misc.helpers import kill
from config.main import ROOT_MOUNT


class BaseWorkerCustomer(BaseWorker):
    def __init__(self, login, password, *args, **kwargs):
        super(BaseWorkerCustomer, self).__init__(*args, **kwargs)

        self.login = login
        self.password = password

        # all forks processes here
        self.processes = []

        self._login_pw = None
        self._login_grp = None

    def run(self):
        raise NotImplementedError

    def preload(self, root=False):
        if not root:
            p = pam.pam()
            if not p.authenticate(self.login, self.password):
                raise Exception('Not Authenticated - %s (%s)' % (p.code, p.reason))

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
        pw = self._get_login_pw()
        print('HOME DIR = %s' % (pw.pw_dir,))
        if pw is not None:
            return pw.pw_dir
        return False

    @staticmethod
    def get_abs_path(path=''):
        """
        Возвращает абсолютный путь относительно точки монтирования в контейнере

        :param path:
        :return: string
        """
        abs_path = os.path.join(ROOT_MOUNT, BaseWorkerCustomer.normalize_path(path))
        return abs_path

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
