from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class UpdateConnection(BaseWorkerCustomer):
    def __init__(self, connection_id, host, ftp_user, ftp_password, *args, **kwargs):
        super(UpdateConnection, self).__init__(*args, **kwargs)

        self.connection_id = connection_id
        self.host = host
        self.ftp_user = ftp_user
        self.ftp_password = ftp_password

    def run(self):
        try:
            self.preload(root=True)
            ftp_connection = self.update_ftp_connection()

            result = {
                "data": ftp_connection,
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

    def update_ftp_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute('''UPDATE ftp_servers SET
                                host = ?,
                                port = ?,
                                user = ?,
                                password = ?
                              WHERE id = ? AND fm_login = ?
                           ''', (self.host, 21, self.ftp_user, self.ftp_password, self.connection_id, self.login))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("FTP connection update failed")

            connection = {
                'id': self.connection_id,
                'host': self.host,
                'port': 21,
                'user': self.ftp_user,
                'decryptedPassword': self.ftp_password
            }
            return connection
        except Exception as e:
            raise e
        finally:
            db.close()
