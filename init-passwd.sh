#!/usr/bin/with-contenv bash
set -e

# use passwd|group|shadow from host
PREFIX=${FM_RPC_ROOT_MOUNT_POINT:-/mnt}/etc

for FILE in passwd group shadow; do
    if [[ -f ${PREFIX}/$FILE && -f /etc/$FILE ]]; then
        mv /etc/$FILE /etc/${FILE}.orig
        ln -s $PREFIX/$FILE /etc/$FILE
    fi
done

# disable pam securetty to allow root logins
sed -e '/pam_securetty\.so/s/^/## disabled by fm ##/' -i /etc/pam.d/login
