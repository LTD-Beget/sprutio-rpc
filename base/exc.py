class Error(Exception):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code

    def __str__(self):
        return "error_message(%s), error_code(%s)" % (self.message, self.code)


class AccessError(Exception):
    pass
