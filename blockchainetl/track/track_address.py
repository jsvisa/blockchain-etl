from typing import NamedTuple, Dict, Optional
from blockchainetl.enumeration.chain import Chain
from bitcoinetl.btc_utils import is_valid_bitcoin_address
from ethereumetl.utils import is_valid_ethereum_address, to_normalized_address


class TrackAddress(NamedTuple):
    address: str
    label: str
    start_block: int
    description: str
    tokens: Optional[Dict[str, str]] = None


def normalize_address(chain: str, address: str):
    if chain in Chain.ALL_ETHEREUM_FORKS:
        assert is_valid_ethereum_address(
            address
        ), f"{address} is not a valid Ethereum address"
        return to_normalized_address(address)
    elif chain in Chain.ALL_BITCOIN_FORKS:
        assert is_valid_bitcoin_address(
            address
        ), f"{address} is not a valid Bitcoin address"
        return address
    else:
        return address
