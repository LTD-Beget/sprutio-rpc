import pprint
import select
from multiprocessing import Pipe

from beget_msgpack import Controller

from lib.FileManager import FM
from lib.FileManager.workers.htaccess.readRulesFtp import ReadRulesFtp
from lib.FileManager.workers.htaccess.readRulesLocal import ReadRulesLocal
from lib.FileManager.workers.htaccess.readRulesSftp import ReadRulesSftp
from lib.FileManager.workers.htaccess.readRulesWebDav import ReadRulesWebDav
from lib.FileManager.workers.htaccess.saveRulesFtp import SaveRulesFtp
from lib.FileManager.workers.htaccess.saveRulesLocal import SaveRulesLocal
from lib.FileManager.workers.htaccess.saveRulesSftp import SaveRulesSftp
from lib.FileManager.workers.htaccess.saveRulesWebDav import SaveRulesWebDav
from misc.helpers import byte_to_unicode_dict


class HtaccessController(Controller):
    def action_read_rules(self, login, password, path, session):

        session = byte_to_unicode_dict(session)

        if session.get('type') == FM.Module.HOME:
            return self.get_process_data(ReadRulesLocal, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "session": session
            })
        elif session.get('type') == FM.Module.FTP:
            return self.get_process_data(ReadRulesFtp, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "session": session
            })
        elif session.get('type') == FM.Module.SFTP:
            return self.get_process_data(ReadRulesSftp, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "session": session
            })
        else:
            return self.get_process_data(ReadRulesWebDav, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "session": session
            })

    def action_save_rules(self, login, password, path, params, session):

        session = byte_to_unicode_dict(session)

        if session.get('type') == FM.Module.HOME:
            return self.get_process_data(SaveRulesLocal, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "params": byte_to_unicode_dict(params),
                "session": session
            })
        elif session.get('type') == FM.Module.FTP:
            return self.get_process_data(SaveRulesFtp, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "params": byte_to_unicode_dict(params),
                "session": session
            })
        elif session.get('type') == FM.Module.SFTP:
            return self.get_process_data(SaveRulesSftp, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "params": byte_to_unicode_dict(params),
                "session": session
            })
        else:
            return self.get_process_data(SaveRulesWebDav, {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "path": path.decode("UTF-8"),
                "params": byte_to_unicode_dict(params),
                "session": session
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
