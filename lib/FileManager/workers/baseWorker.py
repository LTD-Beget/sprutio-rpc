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


class BaseWorker(Process):
    def __init__(self, on_success=None, on_error=None, on_running=None, on_abort=None,
                 status_id=None, logger=None, **kwargs):
        super(BaseWorker, self).__init__(**kwargs)

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

    @staticmethod
    def random_hash():
        #hash_str = random.getrandbits(128)
        #return #"%032x" % hash_str
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def get_rel_path(path, root_path=ROOT_MOUNT):
        if not root_path:
            raise Error("You must specify root path or return pw!")
        relpath = os.path.relpath(path, root_path)
        relative_path = '/' + ('' if relpath == '.' else relpath)
        print("REL PATH %s , %s, %s" % (root_path, relpath, relative_path))
        return relative_path

    def _make_file_info(self, target_path):
        item_dir = self.get_rel_path(os.path.dirname(target_path))
        item_name = os.path.basename(target_path)

        stat_info = os.lstat(target_path)

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
