import psutil
import socket
import os
import subprocess
import pwd
import time
import signal
from queue import Queue
from misc.helperUnicode import as_default_string, as_unicode
from base.exc import Error
from threading import Thread


def get_hostname():
    """
    :return:
    """
    hostname_parts = socket.gethostname().split(".")
    if len(hostname_parts) > 0:
        return hostname_parts[0]
    else:
        raise EnvironmentError("Cannot get hostname")


def get_util(name):
    command = ["/bin/which", name]

    env = {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
    p = SubprocessRunner(command=command, env=env)
    p.run()

    return as_unicode(p.wait()).rstrip("\n")


def kill(pid, sig=0, logger=None):
    if logger:
        p = psutil.Process(pid)
        logger.debug("Sending signal %d to process with pid %d (%s)", sig, pid, ' '.join(p.cmdline()))
    os.kill(pid, sig)


def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    p = psutil.Process(parent_pid)
    child_pid = p.children(recursive=True)
    for pid in child_pid:
        os.kill(pid.pid, sig)


def microtime(get_as_float=False):
    if get_as_float:
        return time.time()
    else:
        return '%d' % time.time()


def get_user_quota_info(login, custom_path="/home"):
    command = [get_util("quota"), "--hide-device", "--show-mntpoint", "-v", "-l", "-w", "-u", login]
    p = SubprocessRunner(command=command)
    p.run()

    out, err, returncode = p.wait(extended_return=True)

    if returncode not in [0, 1]:
        raise Error("Failed to get quota info: %s %s %s" % (out, err, returncode))

    out_lines = out.split("\n")

    fields = ['', '0', '0', '0', '0', '0', '0']
    mntpoint = find_mount_point(custom_path)

    for line in out_lines:
        _fields = line.split()

        if len(_fields) > 0 and _fields[0] == mntpoint:
            fields = _fields
            break

    return {
        "BlockUsed": fields[1].replace("*", ""),
        "BlockSoft": fields[2],
        "BlockHard": fields[3],
        "FileUsed": fields[4].replace("*", ""),
        "FileSoft": fields[5],
        "FileHard": fields[6]
    }


def get_all_quota_info(custom_path="/home"):
    command = [get_util("repquota"), "-v", "-c", "-u", custom_path]
    p = SubprocessRunner(command=command)
    p.run()

    out, err, returncode = p.wait(extended_return=True)

    if returncode not in [0, 1]:
        raise Error("Failed to get repquota info: %s %s %s" % (out, err, returncode))

    out_lines = out.split("\n")
    return_array = {}

    for line in out_lines:
        _fields = line.split()
        if len(_fields) > 1 and (_fields[1] == '--' or _fields[1] == '-+'):
            try:
                return_array[_fields[0]] = {
                    "BlockUsed": _fields[2].replace("*", ""),
                    "BlockSoft": _fields[3],
                    "BlockHard": _fields[4],
                    "FileUsed": _fields[5].replace("*", ""),
                    "FileSoft": _fields[6],
                    "FileHard": _fields[7]}
            except:
                pass

    return return_array


def find_mount_point(path):
    path = os.path.abspath(path)

    while not os.path.ismount(path):
        path = os.path.dirname(path)

    return path


def byte_to_unicode_dict(answer):
    decoded = {}
    for key in answer:
        if isinstance(key, bytes):
            unicode_key = key.decode("utf-8")
        else:
            unicode_key = key
        if isinstance(answer[key], dict):
            decoded[unicode_key] = byte_to_unicode_dict(answer[key])
        elif isinstance(answer[key], list):
            decoded[unicode_key] = byte_to_unicode_list(answer[key])
        elif isinstance(answer[key], int):
            decoded[unicode_key] = answer[key]
        elif isinstance(answer[key], float):
            decoded[unicode_key] = answer[key]
        elif answer[key] is None:
            decoded[unicode_key] = answer[key]
        else:
            try:
                decoded[unicode_key] = answer[key].decode("utf-8")
            except UnicodeDecodeError:
                # Костыль для кракозябр
                decoded[unicode_key] = answer[key].decode("ISO-8859-1")
    return decoded


def byte_to_unicode_dict_only_keys(answer):
    decoded = {}
    for key in answer:
        if isinstance(key, bytes):
            unicode_key = key.decode("utf-8")
        else:
            unicode_key = key
        if isinstance(answer[key], dict):
            decoded[unicode_key] = byte_to_unicode_dict_only_keys(answer[key])
        else:
            decoded[unicode_key] = answer[key]
    return decoded


def byte_to_unicode_list(answer):
    decoded = []
    for item in answer:
        if isinstance(item, dict):
            decoded_item = byte_to_unicode_dict(item)
            decoded.append(decoded_item)
        elif isinstance(item, list):
            decoded_item = byte_to_unicode_list(item)
            decoded.append(decoded_item)
        elif isinstance(item, int):
            decoded.append(item)
        elif isinstance(item, float):
            decoded.append(item)
        elif item is None:
            decoded.append(item)
        else:
            try:
                decoded_item = item.decode("utf-8")
            except UnicodeDecodeError:
                # Костыль для кракозябр
                decoded_item = item.decode("ISO-8859-1")
            decoded.append(decoded_item)
    return decoded


class SubprocessRunner(object):
    def __init__(self, command, logger=None, nice=19, log_prefix="subprocess", **process_options):
        self.command = command
        self.nice = nice
        self.logger = logger
        self.process = None
        self.log_prefix = log_prefix

        self.process_options = self._extend_options(process_options)

    def run(self):
        try:
            if self.logger:
                self.logger.debug("%s : execute command %s" % (as_unicode(self.log_prefix), as_unicode(self.command)))
        except:
            # на случай если в комманде возникнет UNICODE/DECODE error
            # может быть в случае передачи русских символов например в пути
            if self.logger:
                self.logger.error("%s : Error when write log" % (as_unicode(self.log_prefix)))

        command = [as_default_string(item) for item in self.command]
        self.process = subprocess.Popen(command, **self.process_options)

    def wait(self, extended_return=False, write_output_in_log=True):
        out, err = self.process.communicate()

        try:
            if err != "":
                if self.logger:
                    self.logger.error("%s : Error: %s" % (as_unicode(self.log_prefix), as_unicode(err)))

            if write_output_in_log and self.logger:
                self.logger.debug("%s : command output: %s" % (as_unicode(self.log_prefix), as_unicode(out)))

        except:
            if self.logger:
                self.logger.error("%s : Error when write log" % (as_unicode(self.log_prefix)))

        return (out, err, self.process.returncode) if extended_return else out

    def iterate(self):
        try:
            if self.logger:
                self.logger.debug("%s : iterate command %s" % (as_unicode(self.log_prefix), as_unicode(self.command)))
        except Exception as e:
            # на случай если в комманде возникнет UNICODE/DECODE error
            # может быть в случае передачи русских символов например в пути
            if self.logger:
                self.logger.error("%s : Error when write log: %s" % (as_unicode(self.log_prefix), str(e)))

        def enqueue_output(out, queue):
            for line in iter(out.readline, ""):
                queue.put(as_unicode(line).rstrip("\n"))
            out.close()

        def enqueue_errors(err, queue):
            for line in iter(err.readline, ""):
                queue.put(as_unicode(line).rstrip('\n'))
            err.close()

        command = [as_default_string(item) for item in self.command]
        self.process = subprocess.Popen(command, **self.process_options)

        q_out = Queue()
        q_err = Queue()

        t_out = Thread(target=enqueue_output, args=(self.process.stdout, q_out))
        t_out.daemon = True
        t_out.start()

        t_err = Thread(target=enqueue_errors, args=(self.process.stderr, q_err))
        t_err.daemon = True
        t_err.start()

        while True:
            if not q_err.empty():
                err_output = q_err.get()
            else:
                err_output = ""

            if err_output != "":
                if self.logger:
                    self.logger.error("%s : Error: %s" % (as_unicode(self.log_prefix), as_unicode(err_output)))
                raise Exception(err_output)

            if not q_out.empty():
                line_output = q_out.get()
            else:
                line_output = ""

            code = self.process.poll()

            if self.logger and line_output != "":
                try:
                    self.logger.debug(
                        "%s : command iterate: %s" % (as_unicode(self.log_prefix), as_unicode(line_output)))
                except Exception as e:
                    # на случай если в комманде возникнет UNICODE/DECODE error
                    # может быть в случае передачи русских символов например в пути
                    self.logger.error(
                        "%s : Error when write command iterate log: %s" % (as_unicode(self.log_prefix), str(e)))

            if line_output == "":
                if code is not None:
                    self.logger.debug("%s : command iterate end" % as_unicode(self.log_prefix))
                    break

            yield line_output

    def _extend_options(self, options):
        options['cwd'] = options.get('cwd', None)
        options['preexec_fn'] = options.get('preexec_fn', self.pre_exec)
        options['stderr'] = options.get('stderr', subprocess.PIPE)
        options['stdout'] = options.get('stdout', subprocess.PIPE)

        return options

    def pre_exec(self):
        """
            sets nice, ionice to process
        """
        os.nice(self.nice)

        p = psutil.Process(os.getpid())
        p.ionice(psutil.IOPRIO_CLASS_IDLE)


class PwRepository(object):
    pws = {}

    @staticmethod
    def get(login):
        if login in PwRepository.pws:
            return PwRepository.pws[login]
        else:
            password_base = pwd.getpwnam(login)
            PwRepository.pws[login] = password_base
            return password_base
