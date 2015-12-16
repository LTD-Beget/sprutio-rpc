from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback
import os
import threading


class ListFiles(BaseWorkerCustomer):
    def __init__(self, path, *args, **kwargs):
        super(ListFiles, self).__init__(*args, **kwargs)

        self.path = path

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM ListFiles worker run(), abs_path = %s" % abs_path)

            items = []
            self.__list_recursive(abs_path, items, 1)
            info = self._make_file_info(abs_path)
            result = {
                "data": {
                    'path': self.path,
                    'is_share': info['is_share'],
                    'is_share_write': info['is_share_write'],
                    'items': items
                },
                "error": False,
                "message": None,
                "traceback": None
            }

            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    def __list_recursive(self, path, items, depth):
        if depth == 0:
            return

        threads = []

        for item in os.listdir(path):
            item_info = self._make_file_info(os.path.join(path, item))

            items.append(item_info)

            if item_info['is_dir']:
                if depth > 1:
                    item_info['items'] = [{'is_dir': 1, 'name': '..'}, ]
                    t = threading.Thread(target=self.__list_recursive,
                                         args=(os.path.join(path, item), item_info['items'], depth - 1))
                    t.start()
                    threads.append(t)

        for thread in threads:
            thread.join()

        return
