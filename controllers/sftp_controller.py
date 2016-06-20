from beget_msgpack import Controller

from lib.FileManager.workers.sftp.analyzeSize import AnalyzeSize
from lib.FileManager.workers.sftp.chmodFiles import ChmodFiles
from lib.FileManager.workers.sftp.createArchive import CreateArchive
from lib.FileManager.workers.sftp.createConnection import CreateConnection
from lib.FileManager.workers.sftp.createCopy import CreateCopy
from lib.FileManager.workers.sftp.extractArchive import ExtractArchive
from lib.FileManager.workers.sftp.findFiles import FindFiles
from lib.FileManager.workers.sftp.findText import FindText
from lib.FileManager.workers.sftp.listFiles import ListFiles
from lib.FileManager.workers.sftp.makeDir import MakeDir
from lib.FileManager.workers.sftp.newFile import NewFile
from lib.FileManager.workers.sftp.readFile import ReadFile
from lib.FileManager.workers.sftp.removeConnection import RemoveConnection
from lib.FileManager.workers.sftp.removeFiles import RemoveFiles
from lib.FileManager.workers.sftp.renameFile import RenameFile
from lib.FileManager.workers.sftp.updateConnection import UpdateConnection
from lib.FileManager.workers.sftp.writeFile import WriteFile
from lib.FileManager.workers.sftp.copySftp import CopySftp
from lib.FileManager.workers.sftp.copyBetweenSftp import CopyBetweenSftp
from lib.FileManager.workers.sftp.copyFromSftp import CopyFromSftp
from lib.FileManager.workers.sftp.copyFromSftpToFtp import CopyFromSftpToFtp
from lib.FileManager.workers.sftp.moveSftp import MoveSftp
from lib.FileManager.workers.sftp.moveBetweenSftp import MoveBetweenSftp
from lib.FileManager.workers.sftp.moveFromSftp import MoveFromSftp
from lib.FileManager.workers.sftp.moveFromSftpToFtp import MoveFromSftpToFtp
from lib.FileManager.workers.sftp.downloadFiles import DownloadFiles
from lib.FileManager.workers.sftp.readImages import ReadImages
from lib.FileManager.workers.sftp.uploadFile import UploadFile
from lib.FileManager import FM
from multiprocessing import Pipe, Process
from base.exc import Error
from misc.helpers import byte_to_unicode_list, byte_to_unicode_dict
import pprint
import select
import traceback
from lib.FileManager.OperationStatus import OperationStatus


class SftpController(Controller):
    def action_create_connection(self, login, password, host, port, sftp_user, sftp_password):

        return self.get_process_data(CreateConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "host": host.decode('UTF-8'),
            "port": port,
            "sftp_user": sftp_user.decode('UTF-8'),
            "sftp_password": sftp_password.decode('UTF-8')
        })

    def action_edit_connection(self, login, password, connection_id, host, port, sftp_user, sftp_password):

        return self.get_process_data(UpdateConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "connection_id": connection_id,
            "host": host.decode('UTF-8'),
            "port": port,
            "sftp_user": sftp_user.decode('UTF-8'),
            "sftp_password": sftp_password.decode('UTF-8')
        })

    def action_remove_connection(self, login, password, connection_id):

        return self.get_process_data(RemoveConnection, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "connection_id": connection_id
        })

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

    def action_new_file(self, login, password, path, session):

        return self.get_process_data(NewFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_read_file(self, login, password, path, session):

        return self.get_process_data(ReadFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "session": byte_to_unicode_dict(session)
        })

    def action_write_file(self, login, password, path, content, encoding, session):

        return self.get_process_data(WriteFile, {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode("UTF-8"),
            "content": content.decode('UTF-8'),
            "encoding": encoding.decode('UTF-8'),
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

        params = {
            "login": login.decode('UTF-8'),
            "password": password.decode('UTF-8'),
            "path": path.decode('UTF-8'),
            "file_path": file_path.decode('UTF-8'),
            "overwrite": overwrite,
            "session": byte_to_unicode_dict(session),
        }

        return self.get_process_data(UploadFile, params, timeout=7200)

    @staticmethod
    def run_subprocess(logger, worker_object, status_id, name, params):
        logger.info("FM call SFTP long action %s %s %s" % (name, pprint.pformat(status_id), pprint.pformat(params.get("login"))))

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

    def action_analyze_size(self, login, password, status_id, path, session):
        try:
            self.logger.info("FM starting subprocess worker analyze_size %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, AnalyzeSize, status_id.decode('UTF-8'), FM.Action.ANALYZE_SIZE, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "path": path.decode('UTF-8'),
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

    def action_chmod_files(self, login, password, status_id, params, session):
        try:
            self.logger.info("FM starting subprocess worker chmod_files %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, ChmodFiles, status_id.decode('UTF-8'), FM.Action.CHMOD, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "params": byte_to_unicode_dict(params),
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

    def action_find_text(self, login, password, status_id, params, session):
        try:
            self.logger.info("FM starting subprocess worker find_text %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, FindText, status_id.decode('UTF-8'), FM.Action.SEARCH_TEXT, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "params": byte_to_unicode_dict(params),
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

    def action_find_files(self, login, password, status_id, params, session):
        try:
            self.logger.info("FM starting subprocess worker find_files %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, FindFiles, status_id.decode('UTF-8'), FM.Action.SEARCH_FILES, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "params": byte_to_unicode_dict(params),
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

    def action_create_archive(self, login, password, status_id, params, session):
        try:
            self.logger.info("FM starting subprocess worker create_archive %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, CreateArchive, status_id.decode('UTF-8'), FM.Action.CREATE_ARCHIVE, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "params": byte_to_unicode_dict(params),
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

    def action_extract_archive(self, login, password, status_id, params, session):
        try:
            self.logger.info("FM starting subprocess worker extract_archive %s %s", pprint.pformat(status_id),
                             pprint.pformat(login))

            p = Process(target=self.run_subprocess,
                        args=(self.logger, ExtractArchive, status_id.decode('UTF-8'), FM.Action.EXTRACT_ARCHIVE, {
                            "login": login.decode('UTF-8'),
                            "password": password.decode('UTF-8'),
                            "params": byte_to_unicode_dict(params),
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
            self.logger.info("FM starting subprocess worker copy_files %s %s", pprint.pformat(status_id),
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

            if source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.HOME:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyFromSftp, status_id.decode('UTF-8'), FM.Action.COPY, params))
            elif source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.PUBLIC_FTP:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyFromSftpToFtp, status_id.decode('UTF-8'), FM.Action.COPY, params))
                self.logger.info('###################################################3333333')
                self.logger.info(p)
                self.logger.info('###################################################3333333')
            elif (source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.SFTP) and (
                        source.get('server_id') == target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopySftp, status_id.decode('UTF-8'), FM.Action.COPY, params))
            elif source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.SFTP:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, CopyBetweenSftp, status_id.decode('UTF-8'), FM.Action.COPY, params))
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

            if source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.HOME:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveFromSftp, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            elif source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.PUBLIC_FTP:
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveFromSftpToFtp, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            elif (source.get('type') == FM.Module.SFTP and target.get('type') == FM.Module.SFTP) and (
                        source.get('server_id') == target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveSftp, status_id.decode('UTF-8'), FM.Action.MOVE, params))
            elif (source.get('type') == FM.Module.PUBLIC_FTP and target.get('type') == FM.Module.PUBLIC_FTP) and (
                        source.get('server_id') != target.get('server_id')):
                p = Process(target=self.run_subprocess,
                            args=(self.logger, MoveBetweenSftp, status_id.decode('UTF-8'), FM.Action.MOVE, params))
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

        if process.exitcode is None:
            raise Exception("Process finish with errors, by timeout")
        elif process.exitcode < 0:
            raise Exception("Process aborted with exitcode = %s" % str(process.exitcode))
        elif process.exitcode > 0:
            raise Exception("Process finish with errors, exitcode = %s" % str(process.exitcode))

        return data
