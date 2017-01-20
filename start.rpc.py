#!/usr/bin/env python
import argparse
import platform
import sys
import traceback
import os
from distutils.version import LooseVersion

import beget_msgpack

from config.main import DEFAULT_LOGGER
from config.msgpack import MSGPACK_CONTROLLERS
from controllers import *
from misc import helpers
from misc import logger

if LooseVersion(platform.release()) >= LooseVersion('3.9'):
    import socket

    if not hasattr(socket, 'SO_REUSEPORT'):
        # We have REUSEPORT in linux kernel, but not compile in lib
        socket.SO_REUSEPORT = 15

PROGRAM_NAME = os.getenv("FM_RPC_PROGRAM_NAME", 'fm-rpc')
DEFAULT_HOST = os.getenv("FM_RPC_HOST", '127.0.0.1')
DEFAULT_PORT = os.getenv("FM_RPC_PORT", 8400)
DEFAULT_LOGFILE = os.getenv("FM_RPC_LOGFILE", '../logs/%s.log' % PROGRAM_NAME)
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser()

parser.add_argument('--debug', action='store_true')
parser.add_argument('--host', action='store', default=DEFAULT_HOST)
parser.add_argument('--port', action='store', default=DEFAULT_PORT)
parser.add_argument('--logfile', action='store', default=DEFAULT_LOGFILE)
parser.add_argument('--syslog', action='store_true', default=False)

# get args if any
args = parser.parse_args()

# define logfile
logfile = args.logfile

# syslog
if args.syslog:
    logfile = '/dev/log'

# sys.path mangling
if args.debug:
    sys.path.insert(0, CURRENT_DIR)


def setup_logger(syslog=args.syslog):
    logger.setup_logger(logfile, syslog)


if __name__ == "__main__":
    print("FM back-end RPC server")
    print("--------------------------")

    hostname = helpers.get_hostname()
    print("HOSTNAME: %s" % hostname)

    try:
        setup_logger(args.syslog)

        print("LOGFILE: %s" % os.path.realpath(logfile))

        listen_kwargs = {
            "host": args.host,
            "port": args.port,
            "controllers_prefix": MSGPACK_CONTROLLERS,
            "logger_name": DEFAULT_LOGGER,
        }

        print("LISTEN %s" % listen_kwargs)
        beget_msgpack.Server(**listen_kwargs).start()

    except Exception as e:
        print("Got an exception: %s" % str(e))
        print(traceback.format_exc())
