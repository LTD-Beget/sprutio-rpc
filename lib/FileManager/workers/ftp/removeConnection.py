from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class RemoveConnection(BaseWorkerCustomer):
    def __init__(self, connection_id, *args, **kwargs):
        super(RemoveConnection, self).__init__(*args, **kwargs)

        self.connection_id = connection_id

    def run(self):
        try:
            self.preload(root=True)
            status = self.remove_ftp_connection()

            result = {
                "data": status,
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

    def remove_ftp_connection(self):
        db = sqlite3.connect(DB_FILE)
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("DELETE FROM ftp_servers WHERE id = ? AND fm_login = ?", (self.connection_id, self.login))

            status = {
                "status": True
            }
            return status
        except Exception as e:
            raise e
        finally:
            db.close()
