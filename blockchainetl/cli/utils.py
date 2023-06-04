import click
import random
import functools
from typing import Optional, Dict

from blockchainetl.enumeration.chain import Chain


# register global options
def global_click_options(func):
    @click.option(
        "-c",
        "--chain",
        required=True,
        show_default=True,
        type=click.Choice(Chain.ALL_FOR_ETL),
        help="The chain network to connect to.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def evm_chain_options(func):
    @click.option(
        "-c",
        "--chain",
        required=True,
        show_default=True,
        type=click.Choice(Chain.ALL_ETHEREUM_FORKS),
        help="The chain network to connect to.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def str2bool(val: Optional[str]) -> bool:
    _bool_strs = ("yes", "true", "y", "t", "1")
    return val.lower() in _bool_strs if val is not None else False


def pick_random_provider_uri(provider_uri: str) -> str:
    provider_uris = [uri.strip() for uri in provider_uri.split(",")]
    return random.choice(provider_uris)


# extract the redundant command line arguments into kwargs
def extract_cmdline_kwargs(ctx) -> Dict:
    kwargs = dict()
    args = [x.lstrip("--") for sub in [e.split("=") for e in ctx.args] for x in sub]
    if len(args) % 2 == 0:
        kwargs = {
            args[i].replace("-", "_"): args[i + 1] for i in range(0, len(args), 2)
        }
    return kwargs
