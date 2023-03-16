import requests


class HttpProxy(object):
    def __init__(self, endpoint: str, reuse=5):
        self.reuse = reuse
        self.baseurl = endpoint
        self._proxy = None
        self.__count = 0

    def __call__(self):
        return self.get()

    def get(self):
        if self._proxy is None or self.__count > self.reuse:
            self.__count = 0
            resp = requests.get(self.baseurl)
            resp.raise_for_status()
            self._proxy = resp.json()

        self.__count += 1
        return self._proxy
