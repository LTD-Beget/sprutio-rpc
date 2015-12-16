from beget_msgpack import Controller
import pprint
import select
from multiprocessing import Pipe
from lib.FileManager.workers.main.loadSettings import LoadSettings
from lib.FileManager.workers.main.saveSettings import SaveSettings
from lib.FileManager.workers.main.initSession import InitSession
from lib.FileManager.workers.main.initCallback import InitCallback
from lib.FileManager.workers.main.cancelOperation import CancelOperation
from lib.FileManager.workers.main.Authenticate import Authenticate
from lib.FileManager.OperationStatus import OperationStatus
from misc.helpers import byte_to_unicode_dict


class MainController(Controller):
    def action_authenticate(self, login, password):
        return self.get_process_data(Authenticate, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8')
        })

    def action_cancel_operation(self, login, password, status_id):
        operation = OperationStatus.load(status_id.decode('UTF-8'))

        return self.get_process_data(CancelOperation, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "pid": operation.pid,
            "pname": operation.pname
        })

    def action_load_settings(self, login, password):
        return self.get_process_data(LoadSettings, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8')
        })

    def action_save_settings(self, login, password, params):
        return self.get_process_data(SaveSettings, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "params": byte_to_unicode_dict(params)
        })

    def action_init_session(self, login, password, path, session):
        return self.get_process_data(InitSession, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode('UTF-8') if path is not None else None,
            "session": byte_to_unicode_dict(session)
        })

    def action_init_callback(self, login, password):
        return self.get_process_data(InitCallback, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8')
        })

    def get_process_data(self, process_object, process_kwargs):

        print(" === before start queue === ")
        parent_conn, child_conn = Pipe()
        logger = self.logger

        def on_success(data=None):
            logger.debug("Process on_success()")
            child_conn.send(data)

        def on_error(data=None):
            logger.debug("Process on_error() data: %s" % pprint.pformat(data))
            child_conn.send(data)

        kwargs = {
            "logger": self.logger,
            "on_error": on_error,
            "on_success": on_success
        }

        kwargs.update(process_kwargs)

        process = process_object(**kwargs)
        process.start()
        ready = select.select([parent_conn], [], [], 30)  # timeout 30sec
        if ready[0]:
            result = parent_conn.recv()
            process.join()
        else:
            result = {
                "error": True,
                "message": "Request timeout"
            }
            if process.is_alive():
                self.logger.error('Terminate child by timeout with pid: %s', process.pid)
                process.terminate()

        return self.on_finish(process, result)

    def on_finish(self, process, data=None):
        self.logger.info("Process on_finish()")
        self.logger.info("Process exit code %s info = %s" % (str(process.exitcode), pprint.pformat(process)))

        if process.exitcode is None:
            raise Exception("Process finish with errors, by timeout")
        elif process.exitcode < 0:
            raise Exception("Process aborted with exitcode = %s" % str(process.exitcode))
        elif process.exitcode > 0:
            raise Exception("Process finish with errors, exitcode = %s" % str(process.exitcode))

        return data
