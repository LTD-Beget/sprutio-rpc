import sqlite3
from config.main import DB_FILE
from lib.FTP.FTP import FTP
import traceback


class FTPConnection(object):
    CONNECTION_TIMEOUT = 600
    DEBUG = True

    @staticmethod
    def create(login, server_id, logger=None):
        """
        Создает FTP соединение
        :param login:
        :param server_id:
        :param logger:
        :return: FTP
        """
        db = sqlite3.connect(DB_FILE)
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM ftp_servers WHERE fm_login = ? AND id = ?", (login, server_id))
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
            logger.info("FTP session creating %s" % (ftp_session,))
            connection = FTP(host=ftp_session.get('host'), user=ftp_session.get('user'),
                             passwd=ftp_session.get('password'), port=ftp_session.get('port'),
                             timeout=FTPConnection.CONNECTION_TIMEOUT, logger=logger)
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

        if FTPConnection.DEBUG:
            result['traceback'] = traceback.format_exc()
            result['message'] += ' ' + str(e)

        return result
