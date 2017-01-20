import sqlite3
import traceback

from config.main import DB_FILE
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class CreateConnection(BaseWorkerCustomer):
    def __init__(self, host, port, ftp_user, ftp_password, *args, **kwargs):
        super(CreateConnection, self).__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.ftp_user = ftp_user
        self.ftp_password = ftp_password

    def run(self):
        try:
            self.preload(root=True)
            new_connection = self.create_ftp_connection()

            result = {
                "data": new_connection,
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

    def create_ftp_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("INSERT INTO ftp_servers (fm_login, host, port, user, password) VALUES (?,?,?,?,?)",
                           (self.login, self.host, self.port, self.ftp_user, self.ftp_password))

            db.commit()
            connection = {
                'id': cursor.lastrowid,
                'host': self.host,
                'port': self.port,
                'user': self.ftp_user,
                'decryptedPassword': self.ftp_password
            }
            return connection
        except Exception as e:
            raise e
        finally:
            db.close()
