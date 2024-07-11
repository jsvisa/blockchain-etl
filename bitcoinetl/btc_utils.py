import math
from decimal import Decimal
from typing import Optional, Union, Dict

from . import basic_addr
from . import segwit_addr


def bitcoin_to_satoshi(bitcoin_value: Optional[Union[int, Decimal]]) -> Optional[int]:
    if bitcoin_value is None:
        value = bitcoin_value
    elif isinstance(bitcoin_value, Decimal):
        value = int(bitcoin_value * (Decimal(10) ** 8).to_integral_value())
    else:
        value = int(bitcoin_value * math.pow(10, 8))

    return value


def is_valid_bitcoin_address(address: str) -> bool:
    fn = segwit_addr.validate if address.startswith("bc") else basic_addr.validate
    return fn(address)


def get_address(d: Dict):
    # in bitcoin < 22, returns `addresses` field, which is a list of address
    if "addresses" in d:
        return d["addresses"]
    # in bitcoin >= 22, returns `address` field, which is a string address
    if "address" in d:
        return d["address"]
    return None
