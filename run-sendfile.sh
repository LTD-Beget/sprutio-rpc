#!/usr/bin/with-contenv bash
set -e
cd /rpc
exec python start.sendfile.py
