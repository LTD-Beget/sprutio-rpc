# Copyright (c) 2011, SmartFile <btimby@smartfile.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the organization nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import stat
import time
import warnings

from libarchive import EOF
from libarchive import _libarchive

# Suggested block size for libarchive. Libarchive may adjust it.
BLOCK_SIZE = 10240

MTIME_FORMAT = ''

# Default encoding scheme.
ENCODING = 'utf-8'

# Functions to initialize read/write for various libarchive supported formats and filters.
FORMATS = {
    None: (_libarchive.archive_read_support_format_all, None),
    'tar': (_libarchive.archive_read_support_format_tar, _libarchive.archive_write_set_format_ustar),
    'pax': (_libarchive.archive_read_support_format_tar, _libarchive.archive_write_set_format_pax),
    'gnu': (_libarchive.archive_read_support_format_gnutar, _libarchive.archive_write_set_format_gnutar),
    'zip': (_libarchive.archive_read_support_format_zip, _libarchive.archive_write_set_format_zip),
    'rar': (_libarchive.archive_read_support_format_rar, None),
    '7zip': (_libarchive.archive_read_support_format_7zip, None),
    'ar': (_libarchive.archive_read_support_format_ar, None),
    'cab': (_libarchive.archive_read_support_format_cab, None),
    'cpio': (_libarchive.archive_read_support_format_cpio, _libarchive.archive_write_set_format_cpio_newc),
    'iso': (_libarchive.archive_read_support_format_iso9660, _libarchive.archive_write_set_format_iso9660),
    'lha': (_libarchive.archive_read_support_format_lha, None),
    'xar': (_libarchive.archive_read_support_format_xar, _libarchive.archive_write_set_format_xar),
}

FILTERS = {
    None: (_libarchive.archive_read_support_filter_all, _libarchive.archive_write_add_filter_none),
    'gz': (_libarchive.archive_read_support_filter_gzip, _libarchive.archive_write_add_filter_gzip),
    'bz2': (_libarchive.archive_read_support_filter_bzip2, _libarchive.archive_write_add_filter_bzip2),
}

# Map file extensions to formats and filters. To support quick detection.
FORMAT_EXTENSIONS = {
    '.tar': 'tar',
    '.zip': 'zip',
    '.rar': 'rar',
    '.7z': '7zip',
    '.ar': 'ar',
    '.cab': 'cab',
    '.rpm': 'cpio',
    '.cpio': 'cpio',
    '.iso': 'iso',
    '.lha': 'lha',
    '.xar': 'xar',
}
FILTER_EXTENSIONS = {
    '.gz': 'gz',
    '.bz2': 'bz2',
}

def get_error(archive):
    '''Retrieves the last error description for the given archive instance.'''
    return _libarchive.archive_error_string(archive)


def call_and_check(func, archive, *args):
    '''Executes a libarchive function and raises an exception when appropriate.'''
    ret = func(*args)
    if ret == _libarchive.ARCHIVE_OK:
        return
    elif ret == _libarchive.ARCHIVE_WARN:
        warnings.warn('Warning executing function: %s.' % get_error(archive), RuntimeWarning)
    elif ret == _libarchive.ARCHIVE_EOF:
        raise EOF()
    else:
        raise Exception('Fatal error executing function, message is: %s.' % get_error(archive))


def get_func(name, items, index):
    item = items.get(name, None)
    if item is None:
        return None
    return item[index]


def guess_format(filename):
    filename, ext = os.path.splitext(filename)
    filter = FILTER_EXTENSIONS.get(ext)
    if filter:
        filename, ext = os.path.splitext(filename)
    format = FORMAT_EXTENSIONS.get(ext)
    return format, filter


def is_archive_name(filename, formats=None):
    '''Quick check to see if the given file has an extension indiciating that it is
    an archive. The format parameter can be used to limit what archive format is acceptable.
    If omitted, all supported archive formats will be checked.

    This function will return the name of the most likely archive format, None if the file is
    unlikely to be an archive.'''
    if formats is None:
        formats = list(FORMAT_EXTENSIONS.values())
    format, filter = guess_format(filename)
    if format in formats:
        return format


def is_archive(f, formats=(None, ), filters=(None, )):
    '''Check to see if the given file is actually an archive. The format parameter
    can be used to specify which archive format is acceptable. If ommitted, all supported
    archive formats will be checked. It opens the file using libarchive. If no error is
    received, the file was successfully detected by the libarchive bidding process.

    This procedure is quite costly, so you should avoid calling it unless you are reasonably
    sure that the given file is an archive. In other words, you may wish to filter large
    numbers of file names using is_archive_name() before double-checking the positives with
    this function.

    This function will return True if the file can be opened as an archive using the given
    format(s)/filter(s).'''
    if isinstance(f, str):
        f = open(f, 'r')
    a = _libarchive.archive_read_new()
    for format in formats:
        format = get_func(format, FORMATS, 0)
        if format is None:
            return False
        format(a)
    for filter in filters:
        filter = get_func(filter, FILTERS, 0)
        if filter is None:
            return False
        filter(a)
    try:
        try:
            call_and_check(_libarchive.archive_read_open_fd, a, a, f.fileno(), BLOCK_SIZE)
            return True
        except:
            return False
    finally:
        _libarchive.archive_read_close(a)
        _libarchive.archive_read_free(a)


class Entry(object):
    '''An entry within an archive. Represents the header data and it's location within the archive.'''
    def __init__(self, pathname=None, size=None, mtime=None, mode=None, hpos=None, encoding=ENCODING):
        self.pathname = pathname
        self.size = size
        self.mtime = mtime
        self.mode = mode
        self.hpos = hpos
        self.encoding = encoding

    @property
    def header_position(self):
        return self.hpos

    @classmethod
    def from_archive(cls, archive, encoding=ENCODING):
        '''Instantiates an Entry class and sets all the properties from an archive header.'''
        e = _libarchive.archive_entry_new()
        try:
            call_and_check(_libarchive.archive_read_next_header2, archive._a, archive._a, e)
            mode = _libarchive.archive_entry_filetype(e)
            mode |= _libarchive.archive_entry_perm(e)
            entry = cls(
                pathname=_libarchive.archive_entry_pathname(e),
                size=_libarchive.archive_entry_size(e),
                mtime=_libarchive.archive_entry_mtime(e),
                mode=mode,
                hpos=archive.header_position,
            )
        finally:
            _libarchive.archive_entry_free(e)
        return entry

    @classmethod
    def from_file(cls, f, entry=None, encoding=ENCODING):
        '''Instantiates an Entry class and sets all the properties from a file on the file system.
        f can be a file-like object or a path.'''
        if entry is None:
            entry = cls(encoding=encoding)
        if entry.pathname is None:
            if isinstance(f, str):
                st = os.stat(f)
                entry.pathname = f
                entry.size = st.st_size
                entry.mtime = st.st_mtime
                entry.mode = st.st_mode
            elif hasattr(f, 'fileno'):
                st = os.fstat(f.fileno())
                entry.pathname = getattr(f, 'name', None)
                entry.size = st.st_size
                entry.mtime = st.st_mtime
                entry.mode = st.st_mode
            else:
                entry.pathname = getattr(f, 'pathname', None)
                entry.size = getattr(f, 'size', 0)
                entry.mtime = getattr(f, 'mtime', time.time())
                entry.mode = getattr(f, 'mode', stat.S_IFREG)
        return entry

    def to_archive(self, archive):
        '''Creates an archive header and writes it to the given archive.'''
        e = _libarchive.archive_entry_new()
        try:
            _libarchive.archive_entry_set_pathname(e, self.pathname.encode(self.encoding))
            _libarchive.archive_entry_set_filetype(e, stat.S_IFMT(self.mode))
            _libarchive.archive_entry_set_perm(e, stat.S_IMODE(self.mode))
            _libarchive.archive_entry_set_size(e, self.size)
            _libarchive.archive_entry_set_mtime(e, self.mtime, 0)
            call_and_check(_libarchive.archive_write_header, archive._a, archive._a, e)
            #self.hpos = archive.header_position
        finally:
            _libarchive.archive_entry_free(e)

    def isdir(self):
        return stat.S_ISDIR(self.mode)

    def isfile(self):
        return stat.S_ISREG(self.mode)

    def issym(self):
        return stat.S_ISLNK(self.mode)

    def isfifo(self):
        return stat.S_ISFIFO(self.mode)

    def ischr(self):
        return stat.S_ISCHR(self.mode)

    def isblk(self):
        return stat.S_ISBLK(self.mode)
