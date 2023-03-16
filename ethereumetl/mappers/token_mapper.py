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

from typing import Dict, Union

from ethereumetl.domain.token import EthToken


class EthTokenMapper(object):
    def token_to_dict(self, token: EthToken) -> Dict[str, Union[int, str, None]]:
        base = {
            "type": "token",
            "address": token.address,
            "symbol": token.symbol,
            "name": token.name,
            "decimals": token.decimals,
            "total_supply": token.total_supply,
            "block_number": token.block_number,
            "transaction_hash": token.transaction_hash,
            "transaction_index": token.transaction_index,
            "trace_address": token.trace_address,
            "is_erc20": token.is_erc20,
            "is_erc721": token.is_erc721,
        }

        # used by daily contract -> token extracter
        if token._st is not None:
            base["_st"] = token._st
        if token._st_day is not None:
            base["_st_day"] = token._st_day

        return base
