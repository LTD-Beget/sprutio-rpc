from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class UpdateWebDavConnection(BaseWorkerCustomer):
    def __init__(self, connection_id, host, webdav_user, webdav_password, *args, **kwargs):
        super(UpdateWebDavConnection, self).__init__(*args, **kwargs)

        self.connection_id = connection_id
        self.host = host
        self.webdav_user = webdav_user
        self.webdav_password = webdav_password

    def run(self):
        try:
            self.preload(root=True)
            ftp_connection = self.update_webdav_connection()

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

    def update_webdav_connection(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute('''UPDATE webdav_servers SET
                                host = ?,
                                user = ?,
                                password = ?
                              WHERE id = ? AND fm_login = ?
                           ''', (self.host, self.webdav_user, self.webdav_password, self.connection_id, self.login))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("WebDav connection update failed")

            connection = {
                'id': self.connection_id,
                'host': self.host,
                'user': self.webdav_user,
                'decryptedPassword': self.webdav_password
            }
            return connection
        except Exception as e:
            raise e
        finally:
            db.close()

