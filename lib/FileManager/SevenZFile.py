import os

import py7zlib


# Wrapper on py7zlib
class SevenZFile(object):
    @classmethod
    def is_7zfile(cls, filepath):
        fp = None
        try:
            fp = open(filepath, 'rb')
            archive = py7zlib.Archive7z(fp)
            archive.getnames()
            is7z = True
        except Exception:
            is7z = False
        finally:
            if fp:
                fp.close()

        return is7z

    def __init__(self, filepath):
        fp = open(filepath, 'rb')
        self.archive = py7zlib.Archive7z(fp)

    def infolist(self):
        return self.archive.getnames()

    def namelist(self):
        """Return a list of file names in the archive."""
        l = []
        for data in self.infolist():
            l.append(data)
        return l

    def extractall(self, path):
        for name in self.archive.getnames():
            outfilename = os.path.join(path, name)
            outdir = os.path.dirname(outfilename)
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            outfile = open(outfilename, 'wb')
            outfile.write(self.archive.getmember(name).read())
            outfile.close()

    def read(self, name):
        return self.archive.getmember(name).read()
