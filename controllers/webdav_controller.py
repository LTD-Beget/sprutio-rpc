from beget_msgpack import Controller
import pprint
import select
from lib.FileManager.workers.webdav.createWebDavConnection import CreateWebDavConnection
from lib.FileManager.workers.webdav.removeWebDavConnection import RemoveWebDavConnection
from lib.FileManager.workers.webdav.updateWebDavConnection import UpdateWebDavConnection

from base.exc import Error
from lib.FileManager import FM
from multiprocessing import Pipe, Process
import traceback


class WebdavController(Controller):

    def action_create_connection(self, login, password, host, webdav_user, webdav_password):

        return self.get_process_data(CreateWebDavConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "host": host.decode('UTF-8'),
            "webdav_user": webdav_user.decode('UTF-8'),
            "webdav_password": webdav_password.decode('UTF-8')
        })

    def action_edit_connection(self, login, password, connection_id, host, webdav_user, webdav_password):

        return self.get_process_data(UpdateWebDavConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "connection_id": connection_id,
            "host": host.decode('UTF-8'),
            "webdav_user": webdav_user.decode('UTF-8'),
            "webdav_password": webdav_password.decode('UTF-8')
        })

    def action_remove_connection(self, login, password, connection_id):

        return self.get_process_data(RemoveWebDavConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "connection_id": connection_id
        })

    def get_process_data(self, process_object, process_kwargs, timeout=30):

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
        ready = select.select([parent_conn], [], [], timeout)  # timeout 30sec
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

        if process.exitcode < 0:
            raise Exception("Process aborted with exitcode = %s" % str(process.exitcode))

        elif process.exitcode > 0:
            raise Exception("Process finish with errors, exitcode = %s" % str(process.exitcode))

        return data
