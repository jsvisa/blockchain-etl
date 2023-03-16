# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from typing import Optional
import warnings
from eth_utils.address import to_checksum_address
from blockchainetl.utils import chunk_string


def is_valid_ethereum_address(address: str) -> bool:
    try:
        to_checksum_address(address)
    except Exception:
        return False
    return True


def check_classic_provider_uri(chain, provider_uri):
    if chain == "classic" and provider_uri == "https://mainnet.infura.io":
        warnings.warn(
            "ETC Chain not supported on Infura.io. Using https://ethereumclassic.network instead"
        )
        return "https://ethereumclassic.network"
    return provider_uri


def to_normalized_address(address: Optional[str]) -> Optional[str]:
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: "0x" + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address("0x" + param[-40:])
    else:
        return to_normalized_address(param)
