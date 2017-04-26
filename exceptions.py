class JsonError(Exception):

    def __init__(self, message, status):
        self.message = message
        self.status = status
        super().__init__(message, status)


class MultiTaskError(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
