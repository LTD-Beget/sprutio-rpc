import gzip
import os
import pprint
import shutil
import threading
import time
import traceback

import libarchive
import pyinotify
import rarfile

from config.main import TMP_DIR
from lib.FileManager.FM import REQUEST_DELAY
from lib.FileManager.LibArchiveEntry import Entry
from lib.FileManager.SevenZFile import SevenZFile
from lib.FileManager.ZipFile import ZipFile, is_zipfile
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class ExtractArchive(BaseWorkerCustomer):
    def __init__(self, params, session, *args, **kwargs):
        super(ExtractArchive, self).__init__(*args, **kwargs)

        self.file = params.get('file')
        self.extract_path = params.get('extract_path')

        self.params = params
        self.session = session
        self.NUM_WORKING_THREADS = 48

        self.extracted_files = {
            "count": 0,
            "done": False
        }

        self.folder_for_archive = os.path.join(TMP_DIR, self.login, self.random_hash())
        self.tmp_dir = os.path.join(TMP_DIR, self.login, self.random_hash())

    def run(self):
        try:
            self.preload()

            # prepare download dir strictly after dropping privileges
            if not os.path.exists(self.folder_for_archive):
                os.makedirs(self.folder_for_archive)
            if not os.path.exists(self.tmp_dir):
                os.makedirs(self.tmp_dir)

            sftp = self.get_sftp_connection(self.session)

            abs_extract_path = self.extract_path

            if not sftp.exists(abs_extract_path):
                try:
                    sftp.makedirs(abs_extract_path)
                except Exception as e:
                    self.logger.error("Cannot create extract path %s. %s" % (str(e), traceback.format_exc()))
                    raise Exception("Cannot create extract path")
            elif sftp.isfile(abs_extract_path):
                raise Exception("Extract path incorrect - file exists")

            abs_archive_path = self.file.get("path")
            archive_name = os.path.basename(abs_archive_path)
            # copy archive to local fs
            synced_archive_filename = os.path.join(self.folder_for_archive, archive_name)
            sftp.rsync_from(abs_archive_path, self.folder_for_archive)

            if not os.path.exists(synced_archive_filename):
                raise Exception("Archive file is not exist")

            self.on_running(self.status_id, pid=self.pid, pname=self.name)
            self.logger.debug("Start extracting %s", abs_archive_path)

            # for rar and zip same algorithm
            if is_zipfile(synced_archive_filename) or\
                    rarfile.is_rarfile(synced_archive_filename) or\
                    SevenZFile.is_7zfile(synced_archive_filename):

                if is_zipfile(synced_archive_filename):
                    self.logger.info("Archive ZIP type, using zipfile (beget)")
                    a = ZipFile(synced_archive_filename)
                elif rarfile.is_rarfile(synced_archive_filename):
                    self.logger.info("Archive RAR type, using rarfile")
                    a = rarfile.RarFile(synced_archive_filename)
                else:
                    self.logger.info("Archive 7Zip type, using py7zlib")
                    a = SevenZFile(synced_archive_filename)

                    # extract Empty Files first
                    for fileinfo in a.archive.header.files.files:
                        if not fileinfo['emptystream']:
                            continue

                        name = fileinfo['filename']
                        try:
                            unicode_name = name.encode('UTF-8').decode('UTF-8')
                        except UnicodeDecodeError:
                            unicode_name = name.encode('cp866').decode('UTF-8')

                        unicode_name = unicode_name.replace('\\', '/')  # For windows name in rar etc.

                        file_name = os.path.join(self.tmp_dir, unicode_name)
                        dir_name = os.path.dirname(file_name)

                        if not os.path.exists(dir_name):
                            os.makedirs(dir_name)
                        if os.path.exists(dir_name) and not os.path.isdir(dir_name):
                            os.remove(dir_name)
                            os.makedirs(dir_name)
                        if os.path.isdir(file_name):
                            continue

                        f = open(file_name, 'w')
                        f.close()

                infolist = a.infolist()

                not_ascii = False

                # checking ascii names
                try:
                    self.tmp_dir.encode('utf-8').decode('ascii')
                    for name in a.namelist():
                        name.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    not_ascii = True
                except UnicodeEncodeError:
                    not_ascii = True

                t = threading.Thread(target=self.progress, args=(infolist, self.extracted_files, abs_extract_path))
                t.daemon = True
                t.start()

                try:
                    if not_ascii:
                        for name in a.namelist():
                            try:
                                unicode_name = name.encode('UTF-8').decode('UTF-8')
                            except UnicodeDecodeError:
                                unicode_name = name.encode('cp866').decode('UTF-8')

                            unicode_name = unicode_name.replace('\\', '/')  # For windows name in rar etc.

                            file_name = os.path.join(self.tmp_dir, unicode_name)
                            dir_name = os.path.dirname(file_name)

                            if not os.path.exists(dir_name):
                                os.makedirs(dir_name)
                            if os.path.exists(dir_name) and not os.path.isdir(dir_name):
                                os.remove(dir_name)
                                os.makedirs(dir_name)
                            if os.path.isdir(file_name):
                                continue

                            f = open(file_name, 'wb')
                            try:
                                data = a.read(name)
                                f.write(data)
                                f.close()
                            except TypeError:
                                # pass for directories its make recursively for files
                                f.close()
                                os.remove(file_name)

                    else:
                        self.logger.info("EXTRACT ALL to %s , encoded = %s" % (
                            pprint.pformat(self.tmp_dir), pprint.pformat(self.tmp_dir)))
                        a.extractall(self.tmp_dir)  # Not working with non-ascii windows folders
                except Exception as e:
                    self.logger.error("Error extract path %s. %s" % (str(e), traceback.format_exc()))
                    raise e
                finally:
                    self.extracted_files["done"] = True
                    t.join()

            elif libarchive.is_archive(synced_archive_filename):
                self.logger.info("Archive other type, using libarchive")

                next_tick = time.time() + REQUEST_DELAY
                print(pprint.pformat("Clock = %s ,  tick = %s" % (str(time.time()), str(next_tick))))

                infolist = []
                with libarchive.Archive(synced_archive_filename, entry_class=Entry) as a:
                    for entry in a:
                        infolist.append(entry)

                with libarchive.Archive(synced_archive_filename, entry_class=Entry) as a:
                    for entry in a:
                        entry_path = os.path.join(self.tmp_dir, entry.pathname)
                        self.logger.debug("Entry pathname %s - %s", entry.pathname, entry.size)

                        if time.time() > next_tick:
                            progress = {
                                'percent': round(float(self.extracted_files["count"]) / float(len(infolist)), 2),
                                'text': str(int(
                                    round(float(self.extracted_files["count"]) / float(len(infolist)), 2) * 100)) + '%'
                            }

                            self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
                            next_tick = time.time() + REQUEST_DELAY

                        self.extracted_files["count"] += 1
                        dir_name = os.path.dirname(entry_path)

                        if not os.path.exists(dir_name):
                            os.makedirs(dir_name)
                        if os.path.exists(dir_name) and not os.path.isdir(dir_name):
                            os.remove(dir_name)
                            os.makedirs(dir_name)
                        if os.path.isdir(entry_path):
                            continue

                        f = open(entry_path, 'w')
                        a.readpath(f)

            elif abs_archive_path[-3:] == ".gz":
                self.logger.info("gz file type, using gzip")
                try:
                    # if its just a gz file
                    a = gzip.open(synced_archive_filename)
                    file_content = a.read()
                    a.close()

                    file_name = os.path.splitext(os.path.basename(synced_archive_filename))[0]
                    file_path = os.path.join(self.tmp_dir, file_name)
                    infolist = [file_name]
                    dir_name = os.path.dirname(file_path)

                    if not os.path.exists(dir_name):
                        os.makedirs(dir_name)

                    extracted = open(file_path, 'wb')
                    extracted.write(file_content)
                    extracted.close()
                except Exception as e:
                    raise e
                finally:
                    self.extracted_files["done"] = True
            else:
                raise Exception("Archive file has unknown format")

            sftp.rsync_to(self.tmp_dir + '/.', abs_extract_path)
            os.remove(synced_archive_filename)
            shutil.rmtree(self.tmp_dir)

            progress = {
                'percent': round(float(self.extracted_files["count"]) / float(len(infolist)), 2),
                'text': str(int(round(float(self.extracted_files["count"]) / float(len(infolist)), 2) * 100)) + '%'
            }

            result = {}
            time.sleep(REQUEST_DELAY)
            self.on_success(self.status_id, progress=progress, data=result, pid=self.pid, pname=self.name)

        except Exception as e:
            self.extracted_files["done"] = True
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error("SFTP ExtractArchive Error = {}".format(result))

            self.on_error(self.status_id, result, pid=self.pid, pname=self.name)

        finally:
            if os.path.exists(self.tmp_dir):
                shutil.rmtree(self.tmp_dir)

            if os.path.exists(self.folder_for_archive):
                shutil.rmtree(self.folder_for_archive)

    def progress(self, infolist, progress, extract_path):
        self.logger.debug("extract thread progress() start")
        next_tick = time.time() + REQUEST_DELAY
        # print pprint.pformat("Clock = %s ,  tick = %s" % (str(time.time()), str(next_tick)))
        progress["count"] = 0

        class Identity(pyinotify.ProcessEvent):
            def process_default(self, event):
                progress["count"] += 1
                # print("Has event %s progress %s" % (repr(event), pprint.pformat(progress)))

        wm1 = pyinotify.WatchManager()
        wm1.add_watch(extract_path, pyinotify.IN_CREATE, rec=True, auto_add=True)

        s1 = pyinotify.Stats()  # Stats is a subclass of ProcessEvent
        notifier1 = pyinotify.ThreadedNotifier(wm1, default_proc_fun=Identity(s1))
        notifier1.start()

        total = float(len(infolist))

        while not progress["done"]:
            if time.time() > next_tick:
                # print("Tick progress %s / %s" % (pprint.pformat(progress), str(total)))
                count = float(progress["count"]) * 1.5

                if count <= total:
                    op_progress = {
                        'percent': round(count / total, 2),
                        'text': str(int(round(count / total, 2) * 100)) + '%'
                    }
                else:
                    op_progress = {
                        'percent': round(99, 2),
                        'text': '99%'
                    }

                self.on_running(self.status_id, progress=op_progress, pid=self.pid, pname=self.name)
                next_tick = time.time() + REQUEST_DELAY
                time.sleep(REQUEST_DELAY)

        # иначе пользователям кажется что распаковалось не полностью
        op_progress = {
            'percent': round(99, 2),
            'text': '99%'
        }
        self.on_running(self.status_id, progress=op_progress, pid=self.pid, pname=self.name)
        time.sleep(REQUEST_DELAY)

        notifier1.stop()
