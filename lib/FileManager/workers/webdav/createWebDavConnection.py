from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class CreateWebDavConnection(BaseWorkerCustomer):
    def __init__(self, host, webdav_user, webdav_password, *args, **kwargs):
        super(CreateWebDavConnection, self).__init__(*args, **kwargs)
        self.logger.info("called create webdav connection")

        self.host = host
        self.webdav_user = webdav_user
        self.webdav_password = webdav_password

    def run(self):
        try:
            self.preload(root=True)
            new_connection = self.create_webdav_connection()

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

    def create_webdav_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("INSERT INTO webdav_servers (fm_login, host, user, password) VALUES (?,?,?,?)",
                           (self.login, self.host, self.webdav_user, self.webdav_password))

            db.commit()
            connection = {
                'id': cursor.lastrowid,
                'host': self.host,
                'user': self.webdav_user,
                'decryptedPassword': self.webdav_password
            }
            return connection
        except Exception as e:
            raise e
        finally:
            db.close()

