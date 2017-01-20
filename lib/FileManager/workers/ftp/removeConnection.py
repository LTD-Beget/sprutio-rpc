import sqlite3
import traceback

from config.main import DB_FILE
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


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
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            self.logger.info("Removing connection with id %s by user %s" % (self.connection_id, self.login))

            cursor.execute("DELETE FROM ftp_servers WHERE id = ? AND fm_login = ?", (self.connection_id, self.login))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("FTP connection deleting failed")

            status = {
                "status": True
            }
            return status
        except Exception as e:
            raise e
        finally:
            db.close()
