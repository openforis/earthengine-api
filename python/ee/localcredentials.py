from threading import local

_LOCAL = local()

class ThreadLocalCredentials:
    def add(self, credentials):
        _LOCAL.credentials = credentials

    def authorize(self, http):
        return _LOCAL.credentials.authorize(http)