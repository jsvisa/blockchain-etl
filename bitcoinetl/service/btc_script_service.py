import hashlib
from typing import Optional
from btcpy.structs.crypto import PublicKey


def script_hex_to_non_standard_address(
    script_hex: Optional[str], is_mainnet: bool = True
) -> str:
    if script_hex is None:
        script_hex = ""

    # After analysis of 3.7 million UTXO with nonstandard address format
    # (where isin = false and value > 0 and vout_type = 'nonstandard')
    #
    # and we got a statistic that looks like this
    # +--------+---------+
    # | length | count   |
    # |--------+---------|
    # | 70     | 2553332 | -- bitcoin
    # | 134    | 888236  | -- bitcoin
    # | 142    | 170132  | -- canot decoded, btc.com mabye omni?
    # | 402    | 16710   | -- canot decoded, btc.com
    # | 206    | 15666   |
    # ......................
    # If most of it can be parsed, that's fine
    hexlen = len(script_hex)
    if is_mainnet is True and hexlen in (70, 134):
        return str(PublicKey.unhexlify(script_hex[2:-2]).to_address(is_mainnet))

    script_bytes = bytearray.fromhex(script_hex)
    script_hash = hashlib.sha256(script_bytes).hexdigest()[:40]
    address = "nonstandard" + script_hash
    return address
