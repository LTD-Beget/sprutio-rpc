import sqlite3
from config.main import DB_FILE
from lib.WebDav.WebDav import WebDav
import traceback


class WebDavConnection(object):
    CONNECTION_TIMEOUT = 600
    DEBUG = True

    @staticmethod
    def create(login, server_id, logger=None):
        """
        Создает WebDav соединение
        :param login:
        :param server_id:
        :param logger:
        :return: WebDav
        """
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM webdav_servers WHERE fm_login = ? AND id = ?", (login, server_id))
            result = cursor.fetchone()

            if result is None:
                raise Exception("WebDav Connection not found")

            webdav_session = {
                'id': result[0],
                'host': result[2],
                'user': result[3],
                'password': result[4]
            }
            logger.info("WebDav session creating %s" % (webdav_session,))
            connection = WebDav(host=webdav_session.get('host'), user=webdav_session.get('user'),
                             passwd=webdav_session.get('password'),
                             timeout=WebDavConnection.CONNECTION_TIMEOUT, logger=logger)
            return connection

        except Exception as e:
            raise e
        finally:
            db.close()

    @staticmethod
    def get_error(e, msg="", logger=None):
        if logger is not None:
            logger.error("Error in WebDav: %s, %s, traceback = %s" % (msg, str(e), traceback.format_exc()))

        result = {
            "error": True,
            "message": msg,
        }

        if WebDavConnection.DEBUG:
            result['traceback'] = traceback.format_exc()
            result['message'] += ' ' + str(e)

        return result
