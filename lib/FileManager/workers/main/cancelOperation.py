from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from misc.helpers import kill
import traceback
import signal
import psutil
import pprint
import pam


class CancelOperation(BaseWorkerCustomer):
    def __init__(self, pid, pname, *args, **kwargs):
        super(CancelOperation, self).__init__(*args, **kwargs)

        self.operation_pid = int(pid) if pid is not None else None
        self.operation_pname = pname

    def run(self):
        p = pam.pam()
        if not p.authenticate(self.login, self.password):
            raise Exception('Not Authenticated')

        self.logger.info("CancelOperation process started PID = %s", str(self.pid))

        try:
            if self.operation_pid is None:
                self.on_error({
                    "error": True,
                    "message": "Operation pid not provided",
                    "status": False
                })
                return

            if self.operation_pname is None:
                self.on_error({
                    "error": True,
                    "message": "Operation pname not provided",
                    "status": False
                })
                return

            try:
                proc = psutil.Process(self.operation_pid)

                self.logger.info("PROC!!!!!!!!!!!! %s", proc)

                self.logger.info(
                    "Process object = %s name = %s , cmd = %s" % (pprint.pformat(proc),
                                                                  proc.name(), pprint.pformat(proc.cmdline())))

                self.logger.info(
                    "check = %s  ,  (%s)" % (str(self.operation_pname in proc.cmdline()), self.operation_pname))

                if self.operation_pname in str(proc.cmdline()):
                    self.logger.info('==== MATCHED ====')
                    kill(self.operation_pid, signal.SIGTERM, self.logger)
                    self.on_success({
                        "status": True
                    })
                    return

            except psutil.NoSuchProcess:
                self.on_error({
                    "error": True,
                    "message": "process not found",
                    "status": False
                })
                return

            except Exception as e:
                self.on_error({
                    "error": True,
                    "message": "%s %s" % (str(e), traceback.format_exc()),
                    "status": False
                })

            self.on_error({
                "error": True,
                "message": "process not killed",
                "status": False
            })

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)
