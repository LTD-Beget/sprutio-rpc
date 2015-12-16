import re


class HtAccess(object):

    def __init__(self, content, logger):
        self.content = content
        self.logger = logger

    @staticmethod
    def htaccess_is_comment(string):
        return re.match('^#(.*)', string)

    @staticmethod
    def htaccess_open_directive(string):
        if re.match('^(</)(.*)>$', string) is True:
            return False

        return re.match('^<(.*)>$', string)

    @staticmethod
    def htaccess_close_directive(string):
        return re.match('^(</)(.*)>$', string)

    @staticmethod
    def htaccess_is_order_option(string):
        return re.match('^(order)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_authname_option(string):
        return re.match('^(authname)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_authtype_option(string):
        return re.match('^(authtype)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_authuserfile_option(string):
        return re.match('^(authuserfile)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_require_option(string):
        return re.match('^(require)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_allowed_option(string):
        return re.match('^(allow from)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_denied_option(string):
        return re.match('^(deny from)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_fm_comment_ip(string):
        if string == '# FileManager IP rules':
            return True

        return None

    @staticmethod
    def htaccess_is_fm_comment_auth(string):
        if string == '# FileManager Auth rules':
            return True

        return None

    @staticmethod
    def htaccess_is_allowed_all(string):
        return re.match('^(allow from all)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_is_denied_all(string):
        return re.match('^(deny from all)(.*)', string, re.IGNORECASE)

    @staticmethod
    def htaccess_get_order(string):
        if re.match('^(order)\s(allow,deny|deny,allow)$', string, re.IGNORECASE) is None:
            return ''

        group = re.match('^(order)\s(allow,deny|deny,allow)$', string, re.IGNORECASE).groups()
        return group[1]

    @staticmethod
    def htaccess_get_allowed_array(string):

        ip_array = []

        if re.match('^(allow from)\s([0-9\.]+)\s#(.*)', string, re.IGNORECASE) is not None:
            group = re.match('^(allow from)\s([0-9\.]+)\s#(.*)', string, re.IGNORECASE).groups()
            ip_array.append({'ip': group[1], 'comment': group[2]})
        else:
            chunks = string.split(" ")
            for chunk in chunks:
                chunk = chunk.strip()
                if re.match('^[0-9\.]+', chunk) is not None:
                    ip_array.append({'ip': chunk, 'comment': ''})

        return ip_array

    @staticmethod
    def htaccess_get_denied_array(string):

        ip_array = []

        if re.match('^(deny from)\s([0-9\.]+)\s#(.*)', string, re.IGNORECASE) is not None:
            group = re.match('^(deny from)\s([0-9\.]+)\s#(.*)', string, re.IGNORECASE).groups()
            ip_array.append({'ip': group[1], 'comment': group[2]})
        else:
            chunks = string.split(" ")
            for chunk in chunks:
                chunk = chunk.strip()
                if re.match('^[0-9\.]+', chunk) is not None:
                    ip_array.append({'ip': chunk, 'comment': ''})

        return ip_array

    def parse_file_content(self):
        order = 'Allow,Deny'
        allowed_all = True
        denied_all = False

        strings = self.content.split('\n')

        inner_directive = False
        inner_level = 0

        for string in strings:
            trimmed_string = string.strip()

            if trimmed_string == '':
                continue

            if self.htaccess_is_comment(trimmed_string) is not None:
                continue

            if self.htaccess_open_directive(trimmed_string) is not None:
                inner_directive = True
                inner_level += 1
                continue

            if self.htaccess_close_directive(trimmed_string) is not None:
                inner_level -= trimmed_string
                if inner_level == 0:
                    inner_directive = False

                continue

            if inner_directive is False:
                if self.htaccess_is_order_option(trimmed_string) is not None:
                    order = self.htaccess_get_order(trimmed_string)
                    continue

                if self.htaccess_is_allowed_option(trimmed_string) is not None:
                    if self.htaccess_is_allowed_all(trimmed_string) is not None:
                        allowed_all = True
                        continue

                    allowed_all = False
                    continue

                if self.htaccess_is_denied_option(trimmed_string) is not None:
                    if self.htaccess_is_denied_all(trimmed_string) is not None:
                        denied_all = True
                        continue

                    denied_all = False
                    continue

        data = {'order': order, 'allow_all': allowed_all, 'deny_all': denied_all}
        return data

    def write_htaccess_file(self, settings):

        strings = self.content.split('\n')

        inner_directive = False
        inner_level = 0

        new_file = []

        for string in strings:

            trimmed_string = string.strip()

            if trimmed_string == '':
                new_file.append(string)
                continue

            if self.htaccess_is_comment(trimmed_string) is not None:
                if self.htaccess_is_fm_comment_ip(trimmed_string) is not None:
                    continue

                new_file.append(string)
                continue

            if self.htaccess_open_directive(trimmed_string) is not None:
                inner_directive = True
                inner_level += 1
                new_file.append(string)
                continue

            if self.htaccess_close_directive(trimmed_string) is not None:
                inner_level -= 1
                if inner_level == 0:
                    inner_directive = False
                new_file.append(string)
                continue

            if inner_directive is False:

                if self.htaccess_is_order_option(trimmed_string) is not None:
                    continue

                if self.htaccess_is_allowed_option(trimmed_string) is not None:
                    continue

                if self.htaccess_is_denied_option(trimmed_string) is not None:
                    continue

                new_file.append(string)

        new_file.append('# FileManager IP rules')
        new_file.append('Order ' + settings['order'])

        allowed_ip = settings['allowed']

        for ip in allowed_ip:
            new_file.append('Allow from ' + ip['ip'] + ' #' + ip['comment'])

        if settings['allow_all'] is True:
            new_file.append('Allow from all')

        denied_ip = settings['denied']

        for ip in denied_ip:
            new_file.append('Deny from ' + ip['ip'] + ' #' + ip['comment'])

        if settings['deny_all'] is True:
            new_file.append('Deny from all')

        end_content = "\n".join(new_file)
        return end_content

    def get_htaccess_allowed_ip(self):

        allowed_ip = []
        strings = self.content.split('\n')

        inner_directive = False
        inner_level = 0

        for string in strings:
            trimmed_string = string.strip()

            if trimmed_string == '':
                continue

            if self.htaccess_is_comment(trimmed_string) is not None:
                continue

            if self.htaccess_open_directive(trimmed_string) is not None:
                inner_directive = True
                inner_level += 1
                continue

            if self.htaccess_close_directive(trimmed_string) is not None:
                inner_level -= 1
                if inner_level == 0:
                    inner_directive = False

                continue

            if inner_directive is False:
                if self.htaccess_is_order_option(trimmed_string) is not None:
                    continue

                if self.htaccess_is_allowed_option(trimmed_string) is not None:
                    if self.htaccess_is_allowed_all(trimmed_string) is not None:
                        continue

                    allowed_ip += self.htaccess_get_allowed_array(trimmed_string)
                    continue

                if self.htaccess_is_denied_option(trimmed_string) is not None:
                    continue

        i = 0
        ip_array = []

        for ip in allowed_ip:
            ip_array.append({"id": i, "ip": ip['ip'], "comment": ip['comment']})
            i += 1

        return ip_array

    def get_htaccess_denied_ip(self):

        denied_ip = []
        strings = self.content.split('\n')

        inner_directive = False
        inner_level = 0

        for string in strings:
            trimmed_string = string.strip()

            if trimmed_string == '':
                continue

            if self.htaccess_is_comment(trimmed_string) is not None:
                continue

            if self.htaccess_open_directive(trimmed_string) is not None:
                inner_directive = True
                inner_level += 1
                continue

            if self.htaccess_close_directive(trimmed_string) is not None:
                inner_level -= 1
                if inner_level == 0:
                    inner_directive = False

                continue

            if inner_directive is False:

                if self.htaccess_is_order_option(trimmed_string) is not None:
                    continue

                if self.htaccess_is_allowed_option(trimmed_string) is not None:
                    continue

                if self.htaccess_is_denied_option(trimmed_string) is not None:

                    if self.htaccess_is_denied_all(trimmed_string) is not None:
                        continue

                    denied_ip += self.htaccess_get_denied_array(trimmed_string)
                    continue

        i = 0
        ip_array = []

        for ip in denied_ip:
            ip_array.append({"id": i, "ip": ip['ip'], "comment": ip['comment']})
            i += 1

        return ip_array
