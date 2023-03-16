import hashlib
import requests

from requests.sessions import HTTPAdapter

_session_cache = {}


def _get_session(endpoint_uri):
    cache_key = hashlib.md5(endpoint_uri.encode("utf-8")).hexdigest()
    if cache_key not in _session_cache:
        session = requests.Session()
        session.mount("http://", HTTPAdapter(pool_connections=50, pool_maxsize=200))
        session.mount("https://", HTTPAdapter(pool_connections=50, pool_maxsize=200))
        _session_cache[cache_key] = session
    return _session_cache[cache_key]


def make_jsonrpc_request(endpoint_uri, data, *args, **kwargs):
    kwargs.setdefault("timeout", 10)
    session = _get_session(endpoint_uri)
    response = session.post(endpoint_uri, json=data, *args, **kwargs)
    response.raise_for_status()

    return response.content
