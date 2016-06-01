from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class CreateConnection(BaseWorkerCustomer):
    def __init__(self, host, port, sftp_user, sftp_password, *args, **kwargs):
        super(CreateConnection, self).__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.sftp_user = sftp_user
        self.sftp_password = sftp_password

    def run(self):
        try:
            self.preload(root=True)
            new_connection = self.create_sftp_connection()

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

    def create_sftp_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("INSERT INTO sftp_servers (fm_login, host, port, user, password) VALUES (?,?,?,?,?)",
                           (self.login, self.host, self.port, self.sftp_user, self.sftp_password))

            db.commit()
            connection = {
                'id': cursor.lastrowid,
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
