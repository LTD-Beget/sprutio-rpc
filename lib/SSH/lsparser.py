# -*- coding: utf-8 -*-
import os


def parse(text, path):
    path = os.path.dirname(path)
    text = text.decode()
    result = []
    for line in text.splitlines():
        if line.startswith('total ') or line.endswith((' .', ' ..')):
            continue

        result.append(parse_line(line, path))

    return result


def parse_line(line, path):
    parts = line.split()
    mode, _c, owner, group, size = parts[:5]
    dt = parts[5:-1]
    name = parts[-1]

    result = {
        "name": name,
        "mtime_str": " ".join([str(d) for d in dt]),
        "path": path,
        "mtime": 1456871166.6181855,
        "owner": owner,
        "mode": parse_permission(mode[1:]),
        "size": int(size),
        "ext": name.rsplit(".", 1)[-1] if "." in name else "",

        "is_dir": 1 if mode[0] == "d" else 0,
        "is_link": 0,
        "is_share": 0,
        "is_share_write": 0,
        "base64": ""
    }
    return result


def parse_permission(s):
    """
    in: "rwxr--r--" (str)
    out: 744 (int)
    """
    translation = {
        'r' : 4,
        'w' : 2,
        'x' : 1,
        '-' : 0
    }

    result = []
    for idx in range(3):
        part = s[idx * 3 : idx + 3]
        result.append(
            str(sum([translation[c] for c in part]))
        )

    return "".join(result)
