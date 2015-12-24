#!/usr/bin/with-contenv bash
set -e

cd /rpc
python db_init.py

chmod 0777 ${FM_RPC_SETTINGS_DB_PATH:-fm.db}
