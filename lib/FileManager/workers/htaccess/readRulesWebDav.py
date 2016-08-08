from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
from lib.FileManager.HtAccess import HtAccess
from lib.FileManager.WebDavConnection import WebDavConnection
import traceback
import os


class ReadRulesWebDav(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(ReadRulesWebDav, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            abs_path = self.get_abs_path(self.path)
            self.logger.debug("FM ReadRulesWebDav worker run(), abs_path = %s" % abs_path)

            webdav = WebDavConnection.create(self.login, self.session.get('server_id'), self.logger)

            htaccess_path = os.path.join(self.path, '.htaccess')

            if not webdav.exists(htaccess_path):
                default_rules = {
                    'allow_all': True,
                    'deny_all': False,
                    'order': 'Allow,Deny',
                    'denied': [],
                    'allowed': []
                }

                result = {
                    "data": default_rules,
                    "error": False,
                    "message": None,
                    "traceback": None
                }

                self.on_success(result)
                return

            with webdav.open(abs_path) as fd:
                content = fd.read()

            htaccess = HtAccess(content, self.logger)

            answer = htaccess.parse_file_content()

            answer['allowed'] = htaccess.get_htaccess_allowed_ip()
            answer['denied'] = htaccess.get_htaccess_denied_ip()

            result = {
                "data": answer,
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
