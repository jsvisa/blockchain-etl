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

from typing import Dict, Union, List

from ethereumetl.domain.contract import EthContract
from ethereumetl.service.eth_contract_service import EthContractService


class EthContractMapper(object):
    def __init__(self):
        self.contract_service = EthContractService()

    def rpc_result_to_contract(
        self, contract_address: str, rpc_result: str
    ) -> EthContract:
        contract = EthContract()
        contract.address = contract_address
        contract.bytecode = rpc_result

        return contract

    def contract_to_dict(
        self, contract: EthContract
    ) -> Dict[str, Union[int, str, None, List]]:
        base = {
            "type": "contract",
            "address": contract.address,
            "creater": contract.creater,
            "initcode": contract.initcode,
            "bytecode": contract.bytecode,
            "function_sighashes": contract.function_sighashes,
            "is_erc20": contract.is_erc20,
            "is_erc721": contract.is_erc721,
            "block_number": contract.block_number,
            "transaction_hash": contract.transaction_hash,
            "transaction_index": contract.transaction_index,
            "trace_type": contract.trace_type,
            "trace_address": contract.trace_address,
        }

        if contract._st is not None:
            base["_st"] = contract._st
        if contract._st_day is not None:
            base["_st_day"] = contract._st_day
        return base
