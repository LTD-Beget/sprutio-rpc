from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class UpdateConnection(BaseWorkerCustomer):
    def __init__(self, connection_id, host, port, sftp_user, sftp_password, *args, **kwargs):
        super(UpdateConnection, self).__init__(*args, **kwargs)

        self.connection_id = connection_id
        self.host = host
        self.port = port
        self.sftp_user = sftp_user
        self.sftp_password = sftp_password

    def run(self):
        try:
            self.preload(root=True)
            sftp_connection = self.update_sftp_connection()

            result = {
                "data": sftp_connection,
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

    def update_sftp_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute('''UPDATE sftp_servers SET
                                host = ?,
                                port = ?,
                                user = ?,
                                password = ?
                              WHERE id = ? AND fm_login = ?
                           ''', (self.host, self.port, self.sftp_user, self.sftp_password, self.connection_id, self.login))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("SFTP connection update failed")

            connection = {
                'id': self.connection_id,
                'host': self.host,
                'port': self.port,
                'user': self.sftp_user,
                'decryptedPassword': self.sftp_password
            }
            return connection
        except Exception as e:
            raise e
        finally:
            db.close()
