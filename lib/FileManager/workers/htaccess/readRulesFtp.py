import os
import traceback

from lib.FileManager.HtAccess import HtAccess
from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer


class ReadRulesFtp(BaseWorkerCustomer):
    def __init__(self, path, session, *args, **kwargs):
        super(ReadRulesFtp, self).__init__(*args, **kwargs)

        self.path = path
        self.session = session

    def run(self):
        try:
            self.preload()
            self.logger.debug("FM ReadRulesFtp worker run(), abs_path = %s" % self.path)

            ftp = self.get_ftp_connection(self.session)

            htaccess_path = os.path.join(self.path, '.htaccess')

            if not ftp.exists(htaccess_path):
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

            with ftp.open(htaccess_path, 'r') as fd:
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
