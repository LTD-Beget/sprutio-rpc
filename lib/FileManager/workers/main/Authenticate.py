from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
import traceback


class Authenticate(MainWorkerCustomer):
    def __init__(self, *args, **kwargs):
        super(Authenticate, self).__init__(*args, **kwargs)

    def run(self):
        try:
            self.preload()
            result = {
                "data": {
                    "status": True
                },
                "error": False,
                "message": None,
                "traceback": None
            }

            self.on_success(result)
            return result

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            self.on_error(result)
