from urllib.parse import urlparse
from typing import Union

from web3 import HTTPProvider, Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.middleware import validation

from blockchainetl import env
from blockchainetl.enumeration.chain import Chain
from ethereumetl.providers.rpc import BatchHTTPProvider

DEFAULT_TIMEOUT = env.REQUEST_TIMEOUT_SECONDS

# in our usecase, we don't need to validate the chain_id
validation.METHODS_TO_VALIDATE = []


def get_provider_from_uri(uri_string, timeout=DEFAULT_TIMEOUT, batch=False):
    uri = urlparse(uri_string)
    if uri.scheme == "http" or uri.scheme == "https":
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
        web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return web3
