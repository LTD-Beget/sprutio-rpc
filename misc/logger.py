import logging
import logging.handlers
import os

from config.main import DEFAULT_LOGGER


class DummyLogger(object):
    def __getattr__(self):
        return lambda *x: None


def get_logger():
    return logging.getLogger(DEFAULT_LOGGER) or DummyLogger()


def setup_logger(logfile, syslog=False):
    logdir = os.path.dirname(logfile)

    if not os.path.isdir(logdir):
        os.makedirs(logdir, mode=0o700)

    logger = logging.getLogger(DEFAULT_LOGGER)
    logger.setLevel(logging.DEBUG)

    if syslog:
        fh = logging.handlers.SysLogHandler(logfile)
        # fh = logging.StreamHandler(sys.stdout)
    else:
        fh = logging.handlers.WatchedFileHandler(logfile)

    formatter = logging.Formatter(
        "pyportal: %(filename)-20s#%(lineno)-4d: %(name)s:%(levelname)-6s [%(asctime)s][%(process)d]  %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)
