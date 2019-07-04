from threading import local
from google.auth.credentials import Credentials

_LOCAL = local()


class ThreadLocalCredentials(Credentials):
    def get(self):
        return getattr(_LOCAL, 'credentials', None)

    def set(self, credentials):
        _LOCAL.credentials = credentials

    @property
    def expired(self):
        return _LOCAL.credentials.expired()

    @property
    def valid(self):
        return _LOCAL.credentials.valid()

    def apply(self, headers, token=None):
        _LOCAL.credentials.apply(headers, token)

    def before_request(self, request, method, url, headers):
        _LOCAL.credentials.before_request(request, method, url, headers)

    def authorize(self, http):
        return _LOCAL.credentials.authorize(http)

    def refresh(self, request):
        _LOCAL.credentials.refresh(request)


