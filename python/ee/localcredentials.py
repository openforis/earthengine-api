from threading import local
from google.auth.credentials import Credentials

_LOCAL = local()


def _credentials():
    credentials =  getattr(_LOCAL, 'credentials', None)
    if not credentials:
        raise Exception('ee.InitializeThread() not called for current thread.')
    return credentials


class ThreadLocalCredentials(Credentials):
    def get(self):
        return getattr(_LOCAL, 'credentials', None)

    def set(self, credentials):
        if type(credentials) == ThreadLocalCredentials:
            credentials = credentials.get()
        _LOCAL.credentials = credentials

    @property
    def expired(self):
        return _credentials().expired()

    @property
    def valid(self):
        return _credentials().valid()

    def apply(self, headers, token=None):
        _credentials().apply(headers, token)

    def before_request(self, request, method, url, headers):
        _credentials().before_request(request, method, url, headers)

    def authorize(self, http):
        return _credentials().authorize(http)

    def refresh(self, request):
        _credentials().refresh(request)

    def __str__(self):
        return 'ThreadLocalCredentials({})'.format(getattr(_LOCAL, 'credentials', None))

