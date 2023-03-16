import socket
from requests import Session
from requests.sessions import HTTPAdapter
from requests.adapters import PoolManager
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry


class SockOpsAdapter(HTTPAdapter):
    def __init__(self, **kwargs):
        self.options = HTTPConnection.default_socket_options + [
            (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
        ]
        super(SockOpsAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            socket_options=self.options,
        )


DEFAULT_TIMEOUT = 5  # seconds


# copy from https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def make_retryable_session(
    pool_connections=200,
    pool_maxsize=1000,
    retry_total=10,
    timeout=DEFAULT_TIMEOUT,
):
    # ref: https://stackoverflow.com/a/35504626/2298986
    session = Session()

    # set backoff_factor to 2 sconds, sleep as below:
    # - 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
    retries = Retry(
        total=retry_total,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST", "GET"],
        respect_retry_after_header=False,
    )

    # JSONRPC is in POST request
    # By default the `method_whitelist` includes all HTTP methods except POST
    adapter = TimeoutHTTPAdapter(
        pool_connections, pool_maxsize, max_retries=retries, timeout=timeout
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
