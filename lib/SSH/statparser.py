# -*- coding: utf-8 -*-
from lib.SSH.lsparser import parse_permission


def parse(text, path=None):
    text = text.decode()
    lines = text.splitlines()

    text_mode = lines[3].split()[1].split("/")[1][1:-1]
    text_mtime = lines[5].split(" ", 1)[1]

    return {
        "name": path,
        "mode": parse_permission(text_mode),
        "mtime": text_mtime
    }
