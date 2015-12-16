from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.FM import REQUEST_DELAY
from binaryornot.check import is_binary
from misc.helperUnicode import as_unicode
from misc.helpers import kill
import traceback
import os
import signal
import re
import fnmatch
import chardet
import psutil
import mimetypes
from multiprocessing import Process, JoinableQueue, Queue
import time

TIMEOUT_LIMIT = 3600


class FindText(BaseWorkerCustomer):
    NUM_WORKING_PROCESSES = 2

    def __init__(self, params, *args, **kwargs):
        super(FindText, self).__init__(*args, **kwargs)

        self.path = params.get('path', '/')
        self.text = params.get('text', '')

        self.params = params

        # file queue to be processed by many threads
        self.file_queue = JoinableQueue(maxsize=0)
        self.result_queue = Queue(maxsize=0)
        self.result = []

        self.is_alive = {
            "status": True
        }

        self.re_text = re.compile('.*' + fnmatch.translate(self.text)[:-7] + '.*',
                                  re.UNICODE | re.IGNORECASE)
        # remove \Z(?ms) from end of result expression

    def run(self):
        try:
            self.preload()
        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
            return

        def worker(re_text, file_queue, result_queue, logger, timeout):
            while int(time.time()) < timeout:
                if file_queue.empty() is not True:
                    f_path = file_queue.get()
                    try:
                        if not is_binary(f_path):
                            mime = mimetypes.guess_type(f_path)[0]

                            # исключаем некоторые mime типы из поиска
                            if mime not in ['application/pdf', 'application/rar']:
                                with open(f_path, 'rb') as fp:
                                    for line in fp:
                                        try:
                                            line = as_unicode(line)
                                        except UnicodeDecodeError:
                                            charset = chardet.detect(line)
                                            if charset.get('encoding') in ['MacCyrillic']:
                                                detected = 'windows-1251'
                                            else:
                                                detected = charset.get('encoding')

                                            if detected is None:
                                                break
                                            try:
                                                line = str(line, detected, "replace")
                                            except LookupError:
                                                pass

                                        if re_text.match(line) is not None:
                                            result_queue.put(f_path)
                                            # logger.debug("matched file = %s " % f_path)
                                            break

                    except UnicodeDecodeError as unicode_e:
                        logger.error(
                            "UnicodeDecodeError %s, %s" % (str(unicode_e), traceback.format_exc()))

                    except IOError as io_e:
                        logger.error("IOError %s, %s" % (str(io_e), traceback.format_exc()))

                    except Exception as other_e:
                        logger.error("Exception %s, %s" % (str(other_e), traceback.format_exc()))
                    finally:
                        file_queue.task_done()
                else:
                    time.sleep(REQUEST_DELAY)

        try:
            self.logger.debug("findText started with timeout = %s" % TIMEOUT_LIMIT)
            time_limit = int(time.time()) + TIMEOUT_LIMIT
            # Launches a number of worker threads to perform operations using the queue of inputs
            for i in range(self.NUM_WORKING_PROCESSES):
                p = Process(target=worker,
                            args=(self.re_text, self.file_queue, self.result_queue, self.logger, time_limit))
                p.start()
                proc = psutil.Process(p.pid)
                proc.ionice(psutil.IOPRIO_CLASS_IDLE)
                proc.nice(20)
                self.logger.debug(
                    "Search worker #%s, set ionice = idle and nice = 20 for pid %s" % (
                        str(i), str(p.pid)))
                self.processes.append(p)

            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM FindText worker run(), abs_path = %s" % abs_path)

            if not os.path.exists(abs_path):
                raise Exception("Provided path not exist")

            self.on_running(self.status_id, pid=self.pid, pname=self.name)
            for current, dirs, files in os.walk(abs_path):
                for f in files:
                    try:
                        file_path = os.path.join(current, f)
                        self.file_queue.put(file_path)

                    except UnicodeDecodeError as e:
                        self.logger.error(
                            "UnicodeDecodeError %s, %s" % (str(e), traceback.format_exc()))

                    except IOError as e:
                        self.logger.error("IOError %s, %s" % (str(e), traceback.format_exc()))

                    except Exception as e:
                        self.logger.error(
                            "Exception %s, %s" % (str(e), traceback.format_exc()))

            while int(time.time()) <= time_limit:
                self.logger.debug("file_queue size = %s , empty = %s (timeout: %s/%s)" % (
                    self.file_queue.qsize(), self.file_queue.empty(), str(int(time.time())), time_limit))
                if self.file_queue.empty():
                    self.logger.debug("join() file_queue until workers done jobs")
                    self.file_queue.join()
                    break
                else:
                    time.sleep(REQUEST_DELAY)

            if int(time.time()) > time_limit:
                self.is_alive['status'] = False

            for p in self.processes:
                try:
                    self.logger.debug("FM FindText terminate worker process, pid = %s" % p.pid)
                    kill(p.pid, signal.SIGKILL, self.logger)
                except OSError:
                    self.logger.error(
                        "FindText unable to terminate worker process, pid = %s" % p.pid)

            if self.is_alive['status'] is True:
                while not self.result_queue.empty():
                    file_path = self.result_queue.get()
                    self.result.append(self._make_file_info(file_path))

                self.on_success(self.status_id, data=self.result, pid=self.pid, pname=self.name)
            else:
                result = {
                    "error": True,
                    "message": "Operation timeout exceeded",
                    "traceback": ""
                }
                self.on_error(self.status_id, result, pid=self.pid, pname=self.name)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)
