import time

from lib.FileManager.FM import REQUEST_DELAY


def update_progress(self, progress_object):
    self.logger.debug("start update_progress()")
    next_tick = time.time() + REQUEST_DELAY

    self.on_running(self.status_id, pid=self.pid, pname=self.name)

    while not progress_object.get("operation_done"):
        if time.time() > next_tick and progress_object.get("total_done"):
            progress = {
                'percent': round(float(progress_object.get("processed")) / float(progress_object.get("total")), 2),
                'text': str(int(round(float(progress_object.get("processed")) / float(progress_object.get("total")),
                                      2) * 100)) + '%'
            }

            self.on_running(self.status_id, progress=progress, pid=self.pid, pname=self.name)
            next_tick = time.time() + REQUEST_DELAY
            time.sleep(REQUEST_DELAY)
        elif time.time() > next_tick:
            next_tick = time.time() + REQUEST_DELAY
            time.sleep(REQUEST_DELAY)

    self.logger.debug("done update_progress()")
    return
