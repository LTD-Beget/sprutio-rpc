from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from config.main import DB_FILE
import traceback
import sqlite3


class InitCallback(BaseWorkerCustomer):
    def __init__(self, *args, **kwargs):
        super(InitCallback, self).__init__(*args, **kwargs)

    def run(self):
        try:
            self.preload()
            result = {
                "data": {
                    "account": {
                        "login": self.login,
                        "server": "localhost"
                    },
                    "ftp_connections": self.get_ftp_connections(),
                },
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

    def get_ftp_connections(self):
        db = sqlite3.connect(DB_FILE)
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT * FROM ftp_servers WHERE fm_login = ?", (self.login,))
            results = cursor.fetchall()

            connections = []
            for result in results:
                connections.append({
                    'id': result[0],
                    'host': result[2],
                    'port': result[3],
                    'user': result[4],
                    'decryptedPassword': result[5]
                })
            return connections
        except Exception as e:
            raise e
        finally:
            db.close()
