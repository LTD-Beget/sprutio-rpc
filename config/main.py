import os

# Default logger
DEFAULT_LOGGER = "fm-rpc"

APP_PATH = os.path.dirname(os.path.dirname(__file__))
DB_FILE = os.getenv("FM_RPC_SETTINGS_DB_PATH", os.path.join(APP_PATH, 'fm.db'))
ROOT_MOUNT = os.getenv("FM_RPC_ROOT_MOUNT_POINT", '/')
TMP_DIR = os.getenv("FM_RPC_TMP_DIR", '/tmp/fm')
