from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.WebDavConnection import WebDavConnection
from lib.FileManager.FM import REQUEST_DELAY
import traceback
import threading
import time


# class CopyWebDav(BaseWorkerCustomer):
#     def __init__(self, source, target, paths, overwrite, *args, **kwargs):
#         super(CopyWebDav, self).__init__(*args, **kwargs)
#
#         self.source = source
#         self.target = target
#         self.paths = paths
#         self.overwrite = overwrite
#
#     def run(self):
#         try:
#             self.preload()
#             success_paths = []
#             error_paths = []
#
#             operation_progress = {
#                 "total_done": False,
#                 "total": 0,
#                 "operation_done": False,
#                 "processed": 0
#             }
#
#             source_path = self.source.get('path')
#             target_path = self.target.get('path')
#
#             if source_path is None:
#                 raise Exception("Source path empty")
#
#             if target_path is None:
#                 raise Exception("Target path empty")
#
#             self.logger.info("CopyWebDav process run source = %s , target = %s" % (source_path, target_path))
#
#             webdav = WebDavConnection.create(self.login, self.target.get('server_id'), self.logger)
#             t_total = threading.Thread(target=self.get_total, args=(operation_progress, self.paths))
#             t_total.start()
#
#             t_progress = threading.Thread(target=self.uodate_progress, args=(operation_progress,))
#             t_progress.start()
#
#             for path in self.paths:
#                 try:
#                     abs_path = webdav.path.abspath(path)
#                     file_basename = webdav.path.basename(abs_path)
#
#                     if webdav.is_dir(abs_path):
#                         destination = webdav.path.join(target_path, file_basename)
#
#                         if not webdav.check(destination):
#                             webdav.mkdir(destination)
#                         elif self.overwrite and webdav.check(destination) and not webdav.is_dir(destination):
#                             webdav.clean(destination)
#                             webdav.mkdir(destination)
#                         elif not self.overwrite and webdav.check(destination) and not webdav.is_dir(destination):
#                             raise Exception("destination is not a dir")
#                         else:
#                             pass
#
#                         operation_progress["processed"] += 1





