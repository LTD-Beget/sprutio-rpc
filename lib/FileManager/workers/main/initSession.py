from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FM import Module, Action
from lib.FileManager.FTPConnection import FTPConnection
from lib.FileManager.SFTPConnection import SFTPConnection
import traceback
import threading
import os


class InitSession(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(InitSession, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session
        self.session_type = self.session.get("type", None)

    def run(self):
        try:
            self.preload()
            self.logger.info("After preload")
            result = {
                "data": {
                    "actions": self.get_allowed_actions(),
                    "listing": self.make_listing(),
                    "session": self.session
                },
                "error": False,
                "message": None,
                "traceback": None
            }
            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    def get_allowed_actions(self):
        self.logger.info("Getting Allowed Actions for session_type={}".format(self.session_type))

        if self.session_type == Module.HOME:
            actions = {
                Action.ANALYZE_SIZE: True,
                Action.CHMOD: True,
                Action.COPY: True,
                Action.COPY_ENTRY: True,
                Action.COPY_PATH: True,
                Action.CREATE_ARCHIVE: True,
                Action.CREATE_COPY: True,
                Action.DOWNLOAD_ARCHIVE: True,
                Action.DOWNLOAD_BZ2: True,
                Action.DOWNLOAD_BASIC: True,
                Action.DOWNLOAD_GZIP: True,
                Action.DOWNLOAD_TAR: True,
                Action.DOWNLOAD_ZIP: True,
                Action.EDIT: True,
                Action.EXTRACT_ARCHIVE: True,
                Action.HELP: True,
                Action.HOME: True,
                Action.HTPASSWD: False,
                Action.IP_BLOCK: True,
                Action.LOCAL: False,
                Action.LOGOUT: False,
                Action.MOVE: True,
                Action.NAVIGATE: True,
                Action.NEW_FILE: True,
                Action.NEW_FOLDER: True,
                Action.OPEN_DIRECTORY: True,
                Action.REFRESH: True,
                Action.REMOVE: True,
                Action.RENAME: True,
                Action.ROOT: True,
                Action.SEARCH_FILES: True,
                Action.SEARCH_TEXT: True,
                Action.SETTINGS: True,
                Action.SHARE_ACCESS: False,
                Action.SITE_LIST: False,
                Action.UP: True,
                Action.UPLOAD: True,
                Action.VIEW: True
            }

            return actions

        if self.session_type == Module.PUBLIC_FTP:
            self.logger.info("FTP Actions preload")
            actions = {
                Action.ANALYZE_SIZE: False,
                Action.CHMOD: True,
                Action.COPY: True,
                Action.COPY_ENTRY: True,
                Action.COPY_PATH: True,
                Action.CREATE_ARCHIVE: False,
                Action.CREATE_COPY: True,
                Action.DOWNLOAD_ARCHIVE: True,
                Action.DOWNLOAD_BZ2: True,
                Action.DOWNLOAD_BASIC: True,
                Action.DOWNLOAD_GZIP: True,
                Action.DOWNLOAD_TAR: True,
                Action.DOWNLOAD_ZIP: True,
                Action.EDIT: True,
                Action.HELP: True,
                Action.HOME: True,
                Action.HTPASSWD: False,
                Action.IP_BLOCK: True,
                Action.LOCAL: False,
                Action.LOGOUT: False,
                Action.MOVE: True,
                Action.NAVIGATE: True,
                Action.NEW_FILE: True,
                Action.NEW_FOLDER: True,
                Action.OPEN_DIRECTORY: True,
                Action.REFRESH: True,
                Action.REMOVE: True,
                Action.RENAME: True,
                Action.ROOT: True,
                Action.SEARCH_FILES: False,
                Action.SEARCH_TEXT: False,
                Action.SETTINGS: True,
                Action.SHARE_ACCESS: False,
                Action.SITE_LIST: False,
                Action.UP: True,
                Action.UPLOAD: True,
                Action.VIEW: True
            }

            return actions

        if self.session_type == Module.SFTP:
            self.logger.info("SFTP Actions preload")
            actions = {
                Action.ANALYZE_SIZE: True,
                Action.CHMOD: True,
                Action.COPY: True,
                Action.COPY_ENTRY: True,
                Action.COPY_PATH: True,
                Action.CREATE_ARCHIVE: True,
                Action.CREATE_COPY: True,
                Action.DOWNLOAD_ARCHIVE: True,
                Action.DOWNLOAD_BZ2: True,
                Action.DOWNLOAD_BASIC: True,
                Action.DOWNLOAD_GZIP: True,
                Action.DOWNLOAD_TAR: True,
                Action.DOWNLOAD_ZIP: True,
                Action.EDIT: True,
                Action.EXTRACT_ARCHIVE: True,
                Action.HOME: True,
                Action.HTPASSWD: False,
                Action.IP_BLOCK: False,
                Action.HELP: True,
                Action.LOCAL: False,
                Action.LOGOUT: False,
                Action.MOVE: True,
                Action.NAVIGATE: True,
                Action.NEW_FILE: True,
                Action.NEW_FOLDER: True,
                Action.OPEN_DIRECTORY: True,
                Action.REFRESH: True,
                Action.REMOVE: True,
                Action.RENAME: True,
                Action.ROOT: True,
                Action.SEARCH_FILES: True,
                Action.SEARCH_TEXT: True,
                Action.SETTINGS: True,
                Action.SHARE_ACCESS: False,
                Action.SITE_LIST: False,
                Action.UP: True,
                Action.UPLOAD: True,
                Action.VIEW: True
                }

            return actions

        raise Exception("Unknown session type '{}'".format(self.session_type))

    def make_listing(self):

        if self.session_type == Module.HOME:
            path = self.path if self.path is not None else self.get_home_dir()
            abs_path = self.get_abs_path(path)
            items = []
            self.__list_recursive(abs_path, items, 1)
            result = {
                'path': self.get_rel_path(abs_path),
                'items': items
            }

            return result

        if self.session_type == Module.PUBLIC_FTP:
            self.logger.info("FTP Listing preload")
            path = self.path if self.path is not None else '/'
            abs_path = os.path.abspath(path)
            ftp_connection = FTPConnection.create(self.login, self.session.get('server_id'), self.logger)
            listing = ftp_connection.list(path=abs_path)

            return listing

        if self.session_type == Module.SFTP:
            self.logger.info("SFTP Listing preload")
            path = self.path if self.path is not None else '.'
            sftp_connection = SFTPConnection.create(self.login, self.session.get('server_id'), self.logger)
            listing = sftp_connection.list(path=path)

            return listing

        raise Exception("Unknown session type")

    def __list_recursive(self, path, items, depth):
        if depth == 0:
            return

        threads = []

        for item in os.listdir(path):
            item_info = self._make_file_info(os.path.join(path, item))

            items.append(item_info)

            if item_info['is_dir']:
                if depth > 1:
                    item_info['items'] = [{'is_dir': 1, 'name': '..'}, ]
                    t = threading.Thread(target=self.__list_recursive,
                                         args=(os.path.join(path, item), item_info['items'], depth - 1))
                    t.start()
                    threads.append(t)

        for thread in threads:
            thread.join()

        return
