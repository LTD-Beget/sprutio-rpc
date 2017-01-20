#!/usr/bin/env python
import asynchat
import asyncore
import os
import platform
import sys
import threading
import time
import traceback
from distutils.version import LooseVersion

from misc import helpers
from misc import logger

RPC_SENDFILE_HOST = os.getenv('FM_RPC_SENDFILE_HOST', 'localhost')
RPC_SENDFILE_PORT = int(os.getenv('FM_RPC_SENDFILE_PORT', 51600))
RPC_SENDFILE_BIGFILE_SIZE = 2500000000  # > 2GB file (2GB = 2147483648 bytes)
RPC_SENDFILE_BUFFER_LEN = 4096
RPC_SENDFILE_PROGRAM_NAME = os.getenv("FM_RPC_SENDFILE_PROGRAM_NAME", 'fm-rpc-sendfile')
RPC_SENDFILE_DEFAULT_LOGFILE = os.getenv("FM_RPC_SENDFILE_LOGFILE", '../logs/%s.log' % RPC_SENDFILE_PROGRAM_NAME)
SOCKET_SIZE_LENGTH = 36
SOCKET_HEADER_LENGTH = 2

PY3 = sys.version_info >= (3,)


def b(x):
    if PY3:
        return bytes(x, 'ascii')
    return x


if LooseVersion(platform.release()) >= LooseVersion('3.9'):
    import socket

    if not hasattr(socket, 'SO_REUSEPORT'):
        # We have REUSEPORT in linux kernel, but not compile in lib
        socket.SO_REUSEPORT = 15


class Handler(asynchat.async_chat):
    ac_in_buffer_size = RPC_SENDFILE_BUFFER_LEN
    ac_out_buffer_size = RPC_SENDFILE_BUFFER_LEN

    def __init__(self, conn):
        asynchat.async_chat.__init__(self, conn)
        self.in_buffer = []
        self.closed = False
        self.push(b("220 ready\r\n"))
        self.logger = logger.get_logger()
        self.logger.info("Handler __init__()")

    def handle_read(self):
        self.logger.info("Handler handle_read()")
        data = self.recv(RPC_SENDFILE_BUFFER_LEN)
        self.in_buffer.append(data)

    def get_data(self):
        self.logger.info("Handler get_data()")
        return b('').join(self.in_buffer)

    def handle_close(self):
        self.logger.info("Handler handle_close()")
        self.close()

    def close(self):
        self.logger.info("Handler close()")
        try:
            asynchat.async_chat.close(self)
        except Exception as e:
            self.logger.error("Got an exception: %s" % str(e))
            self.logger.error(traceback.format_exc())
        self.closed = True

    def handle_error(self):
        self.logger.error("Handler handle_error()")
        raise Exception("Sendfile handle handler exception")


class NoMemoryHandler(Handler):
    # same as above but doesn't store received data in memory
    ac_in_buffer_size = 65536

    def __init__(self, conn):
        Handler.__init__(self, conn)
        self.logger.info('NoMemoryHandler init()')
        self.in_buffer_len = 0

    def handle_read(self):
        self.logger.info("handle_read()")
        data = self.recv(self.ac_in_buffer_size)
        self.in_buffer_len += len(data)

    def get_data(self):
        self.logger.info("get_data()")
        raise NotImplementedError


class FileStreamHandler(Handler):
    # same as above but doesn't store received data in memory
    ac_in_buffer_size = 65536

    def __init__(self, conn):
        Handler.__init__(self, conn)
        self.logger.info('FileStreamHandler init()')
        self.in_buffer_len = 0
        self.fd = None
        self.size = None
        self.current_size = 0
        """:type: io.BufferedWriter"""

    def handle_read(self):
        self.logger.info("FileStreamHandler handle_read()")

        # размер получаемого файла нужен для того,
        # чтобы закрыть файл после получения всех данных,
        # но до получения EOF
        # это позволяет исключить состояние гонки при записи
        if self.size is None:
            data = self.recv(SOCKET_SIZE_LENGTH)
            self.size = int.from_bytes(data, byteorder='big')

        if self.fd is None:
            header_len_bytes = self.recv(SOCKET_HEADER_LENGTH)
            header_len = int.from_bytes(header_len_bytes, byteorder='big')

            self.current_size += SOCKET_HEADER_LENGTH + header_len

            file_path_bytes = self.recv(header_len)
            file_path = file_path_bytes.decode("utf-8")
            self.open_file(file_path)

        data = self.recv(self.ac_in_buffer_size)

        if not self.closed:
            self.fd.write(data)

            self.current_size += sys.getsizeof(data)
            if self.current_size >= self.size:
                self.close_file()

        self.logger.info("FileStreamHandler handle_read() done")

    def open_file(self, path):
        self.logger.info("FileStreamHandler open_file(), path = %s" % (path,))
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        self.fd = open(path, 'wb')

    def close_file(self):
        self.logger.info("FileStreamHandler close_file()")
        self.fd.close()

    def handle_close(self):
        self.logger.info("FileStreamHandler handle_close()")
        self.close()


class Server(asyncore.dispatcher, threading.Thread):
    handler = Handler

    def __init__(self, address):
        threading.Thread.__init__(self)
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(address)
        self.listen(5)
        self.host, self.port = self.socket.getsockname()[:2]
        self.handler_instance = None
        self._active = False
        self._active_lock = threading.Lock()
        self.logger = logger.get_logger()

    # --- public API

    @property
    def running(self):
        return self._active

    def start(self):
        assert not self.running
        self.__flag = threading.Event()
        threading.Thread.start(self)
        self.__flag.wait()

    def stop(self):
        assert self.running
        self._active = False
        self.join()
        assert not asyncore.socket_map, asyncore.socket_map

    def wait(self):
        # wait for handler connection to be closed, then stop the server
        while not getattr(self.handler_instance, "closed", True):
            time.sleep(0.001)
        self.stop()

    # --- internals

    def run(self):
        self._active = True
        self.__flag.set()
        while self._active and asyncore.socket_map:
            self._active_lock.acquire()
            asyncore.loop(timeout=0.001, count=1)
            self._active_lock.release()
        asyncore.close_all()

    def handle_accept(self):
        self.logger.info("Server handle_accept()")
        conn, addr = self.accept()
        self.handler_instance = self.handler(conn)

    def handle_connect(self):
        self.logger.info("Server handle_connect()")
        self.close()

    handle_read = handle_connect

    def writable(self):
        return 0

    def handle_error(self):
        raise Exception("Sendfile handle server exception")


if __name__ == "__main__":
    print("FM back-end RPC server")
    print("--------------------------")

    hostname = helpers.get_hostname()
    print("HOSTNAME: %s" % hostname)

    try:
        logger.setup_logger(RPC_SENDFILE_DEFAULT_LOGFILE)

        print("LOGFILE: %s" % os.path.realpath(RPC_SENDFILE_DEFAULT_LOGFILE))
        logger.get_logger().info("Starting server")
        server = Server((RPC_SENDFILE_HOST, RPC_SENDFILE_PORT))
        server.handler = FileStreamHandler
        listen_kwargs = {
            "host": server.host,
            "port": server.port,
            "logger_name": RPC_SENDFILE_DEFAULT_LOGFILE,
        }

        server.start()
        logger.get_logger().info("LISTEN %s" % listen_kwargs)

        sys.stdout.write("\nstarting transfer:\n")
        sys.stdout.flush()

    except Exception as e:
        print("Got an exception: %s" % str(e))
        print(traceback.format_exc())
