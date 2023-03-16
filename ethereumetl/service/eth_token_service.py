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
import os
import logging

from typing import Dict

import diskcache as dc
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from web3.contract import Contract
from cachetools import cached, LRUCache
from threading import Lock, get_native_id

from blockchainetl.service.token_service import TokenService
from blockchainetl.enumeration.chain import Chain
from ethereumetl.domain.token import EthToken
from ethereumetl.erc20_abi import ERC20_ABI, ERC20_ABI_ALTERNATIVE_1
from ethereumetl.misc.constant import DEFAULT_TOKEN_ETH

logger = logging.getLogger("eth_token_service")


class EthTokenService(TokenService):
    def __init__(
        self, web3: Web3, function_call_result_transformer=None, cache_path=None
    ):
        self._web3 = web3
        self._function_call_result_transformer = function_call_result_transformer
        self._cache = None
        if cache_path is not None:
            os.makedirs(cache_path, exist_ok=True)
            self._cache = dc.Cache(cache_path)

    def token_contract(self, token_address: str, abi: Dict = ERC20_ABI) -> Contract:
        checksum_address = self._web3.toChecksumAddress(token_address)
        return self._web3.eth.contract(address=checksum_address, abi=abi)

    @cached(cache=LRUCache(maxsize=1024000), lock=Lock())
    def get_token(
        self,
        token_address: str,
        chain: str = Chain.ETHEREUM,
        block_number="latest",
    ) -> EthToken:
        if self._cache is not None:
            token = self._cache.get(token_address)
            if token is not None:
                return token

        logger.info(
            f"[PID: {get_native_id()}] token cache missed, read {token_address} from upstream"
        )
        block_number = block_number or "latest"

        token = EthToken()
        if token_address == DEFAULT_TOKEN_ETH:
            token.address = token_address
            token.symbol = Chain.symbol(chain)
            token.name = "Ether"
            token.decimals = 18
            return token

        contract = self.token_contract(token_address)
        alternative = self.token_contract(token_address, ERC20_ABI_ALTERNATIVE_1)

        symbol = self._get_first_result(
            contract.functions.symbol(),
            contract.functions.SYMBOL(),
            alternative.functions.symbol(),
            alternative.functions.SYMBOL(),
            block_number=block_number,
        )
        if isinstance(symbol, bytes):
            symbol = self._bytes_to_string(symbol)

        name = self._get_first_result(
            contract.functions.name(),
            contract.functions.NAME(),
            alternative.functions.name(),
            alternative.functions.NAME(),
            block_number=block_number,
        )
        if isinstance(name, bytes):
            name = self._bytes_to_string(name)

        decimals = self._get_first_result(
            contract.functions.decimals(),
            contract.functions.DECIMALS(),
            block_number=block_number,
        )
        total_supply = self._get_first_result(
            contract.functions.totalSupply(),
            block_number=block_number,
        )

        token.address = token_address
        token.symbol = self._clean_string(symbol)
        token.name = self._clean_string(name)
        token.decimals = decimals
        token.total_supply = total_supply

        if self._cache is not None:
            self._cache.set(token_address, token)

        return token

    def _get_first_result(self, *funcs, block_number="latest"):
        for func in funcs:
            result = self._call_contract_function(func, block_number)
            if result is not None:
                return result
        return None

    def _call_contract_function(self, func, block_number="latest"):
        # BadFunctionCallOutput exception happens
        #   if the token doesn't implement a particular function
        #   or was self-destructed
        #   OverflowError exception happens
        #   if the return type of the function doesn't match the expected type
        # ContractLogicError exception happens
        #   if the token doesn't implement a particular variable
        #   such as this token doesn't has `decimals` variable
        # https://etherscan.io/token/0xf156d40a3d2c9e014484795e9b76d4b0cf0ce6d1
        result = call_contract_function(
            func=func,
            ignore_errors=(
                BadFunctionCallOutput,
                ContractLogicError,
                OverflowError,
                ValueError,
            ),
            default_value=None,
            block_number=block_number,
        )

        if self._function_call_result_transformer is not None:
            return self._function_call_result_transformer(result)
        else:
            return result

    def _bytes_to_string(self, b, ignore_errors=True):
        if b is None:
            return b

        try:
            b = b.decode("utf-8")
        except UnicodeDecodeError as e:
            if ignore_errors:
                logger.debug(
                    "A UnicodeDecodeError exception occurred "
                    "while trying to decode bytes to string",
                    exc_info=True,
                )
                b = None
            else:
                raise e

        if self._function_call_result_transformer is not None:
            b = self._function_call_result_transformer(b)
        return b

    def _clean_string(self, b):
        if isinstance(b, str):
            return b.replace("\x00", "").replace("\r", "").replace("\n", "")
        return b


def call_contract_function(
    func, ignore_errors, default_value=None, block_number="latest"
):
    try:
        result = func.call(block_identifier=block_number)
        return result
    except Exception as ex:
        msg = "An exception({}) occurred in function '{}' of contract {}.".format(
            type(ex), func.fn_name, func.address
        )
        if type(ex) in ignore_errors:
            logger.debug(msg + " This exception can be safely ignored.", exc_info=True)
            return default_value
        else:
            raise Exception(msg) from ex
