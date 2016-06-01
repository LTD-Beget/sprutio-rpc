# -*- coding: utf-8 -*-
import sqlite3
import traceback

from config.main import DB_FILE
from lib.SSH.sftp import SFTP


class SFTPConnection(object):
    CONNECTION_TIMEOUT = 600
    DEBUG = True

    @staticmethod
    def create(login, server_id, logger=None):
        """
        Создает SFTP соединение
        :param login:
        :param server_id:
        :param logger:
        :return: connection
        :rtype: SFTP
        """
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        logger.info("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM sftp_servers WHERE fm_login = ? AND id = ?", (login, server_id))
            result = cursor.fetchone()

            if result is None:
                raise Exception("SFTP Connection not found")

            params = {
                'id': result[0],
                'host': result[2],
                'port': int(result[3]),
                'user': result[4],
                'password': result[5]
            }
            # TODO add pbkey support
            connection = SFTP(hostname=params['host'], username=params['user'],
                             password=params['password'], pkey=None, port=params['port'],
                             logger=logger)
            return connection

        except Exception as e:
            raise e
        finally:
            db.close()

    @staticmethod
    def get_error(e, msg="", logger=None):
        if logger is not None:
            logger.error("Error in FTP: %s, %s, traceback = %s" % (msg, str(e), traceback.format_exc()))

        result = {
            "error": True,
            "message": msg,
        }

        if SFTPConnection.DEBUG:
            result['traceback'] = traceback.format_exc()
            result['message'] += ' ' + str(e)

        return result
