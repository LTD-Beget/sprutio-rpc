from beget_msgpack import Controller
import pprint
import select
from lib.FileManager.workers.webdav.createWebDavConnection import CreateWebDavConnection
from lib.FileManager.workers.webdav.removeWebDavConnection import RemoveWebDavConnection
from lib.FileManager.workers.webdav.updateWebDavConnection import UpdateWebDavConnection

from lib.FileManager.workers.webdav.listFiles import ListFiles
from lib.FileManager.workers.webdav.makeDir import MakeDir
from lib.FileManager.workers.webdav.removeFiles import RemoveFiles
from lib.FileManager.workers.webdav.downloadFiles import DownloadFiles
from lib.FileManager.workers.webdav.uploadFile import UploadFile
from lib.FileManager.workers.webdav.renameFile import RenameFile
from lib.FileManager.workers.webdav.readImages import ReadImages
from lib.FileManager.workers.webdav.readFile import ReadFile
from lib.FileManager.workers.webdav.writeFile import WriteFile
from lib.FileManager.workers.webdav.copyWebDav import CopyWebDav
from lib.FileManager.workers.webdav.createCopy import CreateCopy
from lib.FileManager.workers.webdav.copyFromWebDav import CopyFromWebDav
from lib.FileManager.workers.webdav.moveFromWebDav import MoveFromWebDav
from lib.FileManager.workers.webdav.moveWebDav import MoveWebDav
from lib.FileManager.workers.webdav.copyBetweenWebDav import CopyBetweenWebDav
from lib.FileManager.workers.webdav.moveBetweenWebDav import MoveBetweenWebDav

from base.exc import Error
from lib.FileManager import FM
from multiprocessing import Pipe, Process
from lib.FileManager.OperationStatus import OperationStatus
import traceback
from misc.helpers import byte_to_unicode_dict, byte_to_unicode_list


class WebdavController(Controller):
    def action_list_files(self, login, password, path, session):
        return self.get_process_data(ListFiles, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_make_dir(self, login, password, path, session):

        return self.get_process_data(MakeDir, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_rename_file(self, login, password, source_path, target_path, session):

        return self.get_process_data(RenameFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "source_path": source_path.decode("UTF-8"),
            "target_path": target_path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_read_file(self, login, password, path, session):

        return self.get_process_data(ReadFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_create_connection(self, login, password, host, webdav_user, webdav_password):

        return self.get_process_data(CreateWebDavConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "host": host.decode('UTF-8'),
            "webdav_user": webdav_user.decode('UTF-8'),
            "webdav_password": webdav_password.decode('UTF-8')
        })

    def action_download_files(self, login, password, paths, mode, session):

        return self.get_process_data(DownloadFiles, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "paths": byte_to_unicode_list(paths),
            "mode": mode.decode('UTF-8'),
            "session": byte_to_unicode_dict(session)
        }, timeout=7200)

    def action_read_images(self, login, password, paths, session):

        return self.get_process_data(ReadImages, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "paths": byte_to_unicode_list(paths),
            "session": byte_to_unicode_dict(session)
        }, timeout=7200)

    def action_upload_file(self, login, password, path, file_path, overwrite, session):

        return self.get_process_data(UploadFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode('UTF-8'),
            "file_path": file_path.decode('UTF-8'),
            "overwrite": overwrite,
            "session": byte_to_unicode_dict(session)
        }, timeout=7200)

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

    @staticmethod
    def run_subprocess(logger, worker_object, status_id, name, params):
        logger.info(
                "FM call WebDav long action %s %s %s" % (
                    name, pprint.pformat(status_id), pprint.pformat(params.get("login"))))

        def async_check_operation(op_status_id):
            operation = OperationStatus.load(op_status_id)
            logger.info("Operation id='%s' status is '%s'" % (str(status_id), operation.status))
            if operation.status != OperationStatus.STATUS_WAIT:
                raise Error("Operation status is not wait - aborting")

        def async_on_error(op_status_id, data=None, progress=None, pid=None, pname=None):
            logger.info("Process on_error()")
            operation = OperationStatus.load(op_status_id)
            data = {
                'id': status_id,
                'status': 'error',
                'data': data,
                'progress': progress,
                'pid': pid,
                'pname': pname
            }
            operation.set_attributes(data)
            operation.save()

        def async_on_success(op_status_id, data=None, progress=None, pid=None, pname=None):
            logger.info("Process on_success()")
            operation = OperationStatus.load(op_status_id)
            data = {
                'id': op_status_id,
                'status': OperationStatus.STATUS_SUCCESS,
                'data': data,
                'progress': progress,
                'pid': pid,
                'pname': pname
            }
            operation.set_attributes(data)
            operation.save()

        def async_on_running(op_status_id, data=None, progress=None, pid=None, pname=None):
            logger.info("Process on_running()")
            operation = OperationStatus.load(op_status_id)
            data = {
                'id': op_status_id,
                'status': OperationStatus.STATUS_RUNNING,
                'data': data,
                'progress': progress,
                'pid': pid,
                'pname': pname
            }
            operation.set_attributes(data)
            operation.save()

        def async_on_abort(op_status_id, data=None, progress=None, pid=None, pname=None):
            logger.info("Process on_abort()")
            operation = OperationStatus.load(op_status_id)
            data = {
                'id': op_status_id,
                'status': OperationStatus.STATUS_ABORT,
                'data': data,
                'progress': progress,
                'pid': pid,
                'pname': pname
            }
            operation.set_attributes(data)
            operation.save()

        def async_on_finish(worker_process, op_status_id, pid=None, pname=None):
            logger.info("Process on_finish()")
            logger.info("Process exit code %s info = %s", str(process.exitcode), pprint.pformat(process))

            if worker_process.exitcode < 0:
                async_on_abort(status_id, pid=pid, pname=pname)
            elif worker_process.exitcode > 0:
                async_on_error(op_status_id, pid=pid, pname=pname)

        try:
            async_check_operation(status_id)
            kwargs = {
                "name": name,
                "status_id": status_id,
                "logger": logger,
                "on_running": async_on_running,
                "on_abort": async_on_abort,
                "on_error": async_on_error,
                "on_success": async_on_success
            }

            kwargs.update(params)

            process = worker_object(**kwargs)
            process.start()
            process.join()
            async_on_finish(process, status_id, pid=process.pid, pname=process.name)

        except Exception as e:
            result = {
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            async_on_error(status_id, result)

    def action_write_file(self, login, password, path, content, encoding, session):
        return self.get_process_data(WriteFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "content": content.decode('UTF-8'),
            "encoding": encoding.decode('UTF-8'),
            "session": byte_to_unicode_dict(session)
        })

    def action_remove_files(self, login, password, status_id, paths, session):
        try:
            self.logger.info("FM starting subprocess worker remove_files %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, RemoveFiles, status_id.decode('UTF-8'), FM.Action.REMOVE, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "paths": byte_to_unicode_list(paths),
                            "session": byte_to_unicode_dict(session)
                        }))
            p.start()
            return {"error": False}
        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            return result

    def action_copy_files(self, login, password, status_id, source, target, paths, overwrite):
        try:
            self.logger.info("FM starting subprocess worker copy_files %s %s source=%s target=%", pprint.pformat(status_id),
                             pprint.pformat(login))

            self.logger.info("source before %s" % source)

            source = byte_to_unicode_dict(source)
            target = byte_to_unicode_dict(target)

            self.logger.info("source after %s" % source)

            params = {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "source": source,
                "target": target,
                "paths": byte_to_unicode_list(paths),
                "overwrite": overwrite
            }

            if source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.HOME:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyFromWebDav, status_id.decode('UTF-8'), FM.Action.COPY, params))
            elif (source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.PUBLIC_WEBDAV) and (
                        source.get('server_id') == target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyWebDav, status_id.decode('UTF-8'), FM.Action.COPY, params))
            elif (source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.PUBLIC_WEBDAV) and (
                        source.get('server_id') != target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyBetweenWebDav, status_id.decode('UTF-8'), FM.Action.COPY, params))
            else:
                raise Exception("Unable to get worker for these source and target")

            p.start()
            return {"error": False}

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            return result

    def action_move_files(self, login, password, status_id, source, target, paths, overwrite):
        try:
            self.logger.info("FM starting subprocess worker move_files %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            source = byte_to_unicode_dict(source)
            target = byte_to_unicode_dict(target)

            params = {
                "login": login.decode('UTF-8'),
                "password": password.decode('UTF-8'),
                "source": source,
                "target": target,
                "paths": byte_to_unicode_list(paths),
                "overwrite": overwrite
            }

            if source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.HOME:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveFromWebDav, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            elif (source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.PUBLIC_WEBDAV) and (
                        source.get('server_id') == target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveWebDav, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            elif (source.get('type') == FM.Module.PUBLIC_WEBDAV and target.get('type') == FM.Module.PUBLIC_WEBDAV) and (
                        source.get('server_id') != target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveBetweenWebDav, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            else:
                raise Exception("Unable to get worker for these source and target")

            p.start()
            return {"error": False}

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            return result

    def action_create_copy(self, login, password, status_id, paths, session):
        try:
            self.logger.info("FM starting subprocess worker create_copy %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, CreateCopy, status_id.decode('UTF-8'), FM.Action.CREATE_COPY, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "paths": byte_to_unicode_list(paths),
                            "session": byte_to_unicode_dict(session)
                        }))

            p.start()
            return {"error": False}

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            return result

    def on_finish(self, process, data=None):
        self.logger.info("Process on_finish()")
        self.logger.info("Process exit code %s info = %s" % (str(process.exitcode), pprint.pformat(process)))

        if process.exitcode < 0:
            raise Exception("Process aborted with exitcode = %s" % str(process.exitcode))

        elif process.exitcode > 0:
            raise Exception("Process finish with errors, exitcode = %s" % str(process.exitcode))

        return data
