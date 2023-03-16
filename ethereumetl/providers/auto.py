from urllib.parse import urlparse
from typing import Union

from web3 import IPCProvider, HTTPProvider, Web3
from web3.middleware.geth_poa import geth_poa_middleware

from blockchainetl import env
from blockchainetl.enumeration.chain import Chain
from ethereumetl.providers.ipc import BatchIPCProvider
from ethereumetl.providers.rpc import BatchHTTPProvider

DEFAULT_TIMEOUT = env.REQUEST_TIMEOUT_SECONDS


def get_provider_from_uri(uri_string, timeout=DEFAULT_TIMEOUT, batch=False):
    uri = urlparse(uri_string)
    if uri.scheme == "file":
        if batch:
            return BatchIPCProvider(uri.path, timeout=timeout)
        else:
            return IPCProvider(uri.path, timeout=timeout)
    elif uri.scheme == "http" or uri.scheme == "https":
        # FIXME: don't use requests.Session, else SOCKET leaks
        # see #65 for more detail
        # ref https://github.com/psf/requests/issues/2766
        # requests.Session is not thread safe, don't use across threads.

        request_kwargs = {"timeout": timeout}
        if batch:
            return BatchHTTPProvider(uri_string, request_kwargs=request_kwargs)
        else:
            return HTTPProvider(uri_string, request_kwargs=request_kwargs)
    else:
        raise ValueError("Unknown uri scheme {}".format(uri_string))


def new_web3_provider(
    provider: Union[str, HTTPProvider], chain: str = Chain.ETHEREUM
) -> Web3:
    if isinstance(provider, str):
        provider = HTTPProvider(provider)

    web3 = Web3(provider)

    # skip block.ExtraData vailidte
    if chain != Chain.ETHEREUM:
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return web3
