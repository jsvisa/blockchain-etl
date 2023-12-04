# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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

from typing import Dict, Optional, Any, List, Union, Tuple

from blockchainetl.env import GETH_TRACE_IGNORE_GASUSED_ERROR
from blockchainetl.utils import hex_to_dec
from ethereumetl.utils import to_normalized_address
from ethereumetl.domain.trace import EthTrace
from ethereumetl.domain.geth_trace import EthGethTrace
from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ethereumetl.misc.geth_error_convert import geth_error_to_parity
from ethereumetl.misc.geth_precompiled_contract import GETH_PRECOMPILED_CONTRACT_RANGE


class EthTraceMapper(object):
    def json_dict_to_trace(self, json_dict: Dict[str, Any]) -> EthTrace:
        trace = EthTrace()

        trace.block_number = json_dict.get("blockNumber")
        trace.transaction_hash = json_dict.get("transactionHash")
        trace.transaction_index = json_dict.get("transactionPosition")
        trace.subtraces = json_dict.get("subtraces", 0)
        trace.trace_address = json_dict.get("traceAddress", [])

        error = json_dict.get("error")
        if error:
            trace.error = error

        action = json_dict.get("action")
        if action is None:
            action = {}
        result = json_dict.get("result")
        if result is None:
            result = {}

        trace_type = json_dict.get("type")
        trace.trace_type = trace_type

        # common fields in call/create
        # TL'DR:
        # there is no create2 type in OpenEthereum/Parity, but we will add it for compatibility
        if trace_type in ("call", "create", "create2"):
            trace.from_address = to_normalized_address(action.get("from"))
            trace.value = hex_to_dec(action.get("value"))
            trace.gas = hex_to_dec(action.get("gas"))
            trace.gas_used = hex_to_dec(result.get("gasUsed"))

        # process different trace types
        if trace_type == "call":
            trace.call_type = action.get("callType")
            trace.to_address = to_normalized_address(action.get("to"))
            trace.input = action.get("input")
            trace.output = result.get("output")
        elif trace_type in ("create", "create2"):
            trace.to_address = result.get("address")
            trace.input = action.get("init")
            trace.output = result.get("code")
        elif trace_type in ("selfdestruct", "suicide"):
            trace.from_address = to_normalized_address(action.get("address"))
            trace.to_address = to_normalized_address(action.get("refundAddress"))
            trace.value = hex_to_dec(action.get("balance"))
        elif trace_type == "reward":
            trace.to_address = to_normalized_address(action.get("author"))
            trace.value = hex_to_dec(action.get("value"))
            trace.reward_type = action.get("rewardType")

        return trace

    def geth_trace_to_traces(
        self, geth_trace: EthGethTrace, retain_precompiled_calls: bool = True
    ) -> List[EthTrace]:
        block_number = geth_trace.block_number
        tx_traces = geth_trace.tx_traces
        tx_hashes = geth_trace.tx_hashes

        traces = []

        for tx_index, tx_trace in enumerate(tx_traces):
            traces.extend(
                self._iterate_geth_trace(
                    block_number,
                    tx_index,
                    tx_trace,
                    retain_precompiled_calls=retain_precompiled_calls,
                    tx_hashes=tx_hashes,
                )
            )

        return traces

    def genesis_alloc_to_trace(self, allocation: Tuple[str, int]) -> EthTrace:
        address = allocation[0]
        value = allocation[1]

        trace = EthTrace()

        trace.block_number = 0
        trace.transaction_hash = (
            "0x000000000000000000000000000000000000000000000000000000000genesis"
        )
        trace.to_address = address
        trace.value = value
        trace.trace_type = "genesis"
        trace.status = 1

        return trace

    def daofork_state_change_to_trace(
        self, state_change: Tuple[str, str, int]
    ) -> EthTrace:
        from_address = state_change[0]
        to_address = state_change[1]
        value = state_change[2]

        trace = EthTrace()

        trace.block_number = DAOFORK_BLOCK_NUMBER
        trace.transaction_hash = (
            "0x000000000000000000000000000000000000000000000000000000000daofork"
        )
        trace.from_address = from_address
        trace.to_address = to_address
        trace.value = value
        trace.trace_type = "daofork"
        trace.status = 1

        return trace

    def _iterate_geth_trace(
        self,
        block_number: Optional[int],
        tx_index: int,
        tx_trace: Dict[str, Any],
        trace_address: List[int] = [],
        parent_trace: Optional[EthTrace] = None,
        retain_precompiled_calls: bool = True,
        tx_hashes: Dict[int, str] = dict(),
    ) -> List[EthTrace]:
        trace = EthTrace()

        trace.block_number = block_number
        trace.transaction_hash = tx_hashes.get(tx_index)
        trace.transaction_index = tx_index

        trace.from_address = to_normalized_address(tx_trace.get("from"))
        trace.to_address = to_normalized_address(tx_trace.get("to"))

        trace.input = tx_trace.get("input")
        trace.output = tx_trace.get("output")

        # FIXME, geth may return the error gas/gas_used, such as this tx:
        # $ curl -H "Content-Type: application/json" -d '
        # {
        #    "id": 1, "method": "debug_traceTransaction",
        #   "params": [
        #      "0x93548db5e90957f64b673820186fc26b08fb43530d96ed07da4aa02ea78b97dc",
        #      {"tracer": "callTracer", "timeout": "20s"}
        #   ]
        # }' localhost:8545 | jq
        #
        # {
        #   "jsonrpc": "2.0",
        #   "id": 1,
        #   "result": {
        #     "type": "CALL",
        #     "from": "0x6666662ac054fed267a5818001104eb0b5e8bab3",
        #     "to": "0x999999c60566e0a78df17f71886333e1dace0bae",
        #     "value": "0x0",
        #     "gas": "0x9a10",
        #     "gasUsed": "0x59dc",
        #     "input": "0xca722cdc2f7ef1c309698b25705ac2b2c8a61c96dd1080ecfd01e22a5b6e991218...",
        #     "output": "0x",
        #     "time": "5.779963ms",
        #     "calls": [
        #       {
        #         "type": "CALL",
        #         "from": "0x999999c60566e0a78df17f71886333e1dace0bae",
        #         "to": "0x2fcc9629560db6bc32c5ee6e0fea0ecd90c89949",
        #         "value": "0x1b74b05167a20000",
        #         "input": "0x",
        #         "output": "0x"
        #       }
        #     ]
        #   }
        # }
        trace.gas = hex_to_dec(tx_trace.get("gas"))
        trace.gas_used = hex_to_dec(tx_trace.get("gasUsed"))

        # Compatible with Parity, set gas to 0 if None
        if trace.gas is None and trace.gas_used is None:
            trace.gas_used = 0

        # Celo's gas_used may returns invalid Integer, eg:
        # https://celoscan.io/tx/0xb3b844f064dbd3a10e254d567978a64193cead7953b6c1845abdb5c34c012d51
        # returns the gasUsed: 0x-2bc
        if GETH_TRACE_IGNORE_GASUSED_ERROR and not isinstance(trace.gas_used, int):
            trace.gas_used = -1

        # a simple summary of 100 block, whose's gas is None
        # input output trace_type   gas     count
        # 0x    0x      call        2300    11108
        # NULL  NULL    suicide     NULL    6191
        # 0x    0x      call        47992   171
        # 0x    0x      call        71895   100
        # 0x    0x      call        46901   76
        # 0x    0x      call        70821   24
        # 0x    0x      call        100957  20
        # 0x    0x      call        109157  20
        # 0x    0x      call        91981   20
        # 0x    0x      call        108381  20
        # if trace.input == trace.output == "0x":
        #     trace.gas = 2300

        # map the geth's error into Parity's style
        error = tx_trace.get("error")
        if error:
            trace.error = geth_error_to_parity(error)

        # lowercase for compatibility with parity traces
        trace.trace_type = tx_trace.get("type", "").lower()

        if trace.trace_type == "selfdestruct":
            # rename to suicide for compatibility with parity traces
            trace.trace_type = "suicide"
        elif trace.trace_type in ("call", "callcode", "delegatecall", "staticcall"):
            trace.call_type = trace.trace_type
            trace.trace_type = "call"

        # delegatecall,callcode should inherit the value field from the parent trace
        # https://docs.soliditylang.org/en/latest/introduction-to-smart-contracts.html#delegatecall-callcode-and-libraries
        if trace.call_type in ("delegatecall", "callcode"):
            if parent_trace is None:
                raise ValueError(
                    f"tx_trace: {tx_trace} is {trace.trace_type}, but missing parent trace"
                )
            # here we use parent EthTrace, not json_dict trace
            # So if you look at the following scenario, the parent trace is staticcall,
            # and this trace is delegatecall, which would be failed if you used the latter
            # tx: 0xb2d196cf6bd01a0c1b0b3bef891bb0d8b61919f3840084f06d237b3a2e9ed702
            # staticcall ->
            #       delegatecall
            trace.value = parent_trace.value
        elif trace.call_type == "staticcall":
            # static call is pure or view function, state is not modified
            # https://medium.com/blockchannel/state-specifiers-and-staticcall-d50d5b2e4920
            trace.value = 0
        else:
            trace.value = hex_to_dec(tx_trace.get("value"))

        result = [trace]

        calls = tx_trace.get("calls", [])
        if retain_precompiled_calls is False and len(calls) > 0:
            calls = [
                sub
                for sub in calls
                if not (
                    sub.get("type", "").lower()
                    in ("call", "callcode", "delegatecall", "staticcall")
                    and int(sub.get("to", "0x0"), 16) in GETH_PRECOMPILED_CONTRACT_RANGE
                )
            ]

        trace.subtraces = len(calls)
        trace.trace_address = trace_address
        trace.logs = tx_trace.get("logs")

        for call_index, call_trace in enumerate(calls):
            result.extend(
                self._iterate_geth_trace(
                    block_number,
                    tx_index,
                    call_trace,
                    trace_address + [call_index],
                    trace,
                    retain_precompiled_calls=retain_precompiled_calls,
                    tx_hashes=tx_hashes,
                )
            )

        return result

    def trace_to_dict(self, trace: EthTrace) -> Dict[str, Union[str, int, None, List]]:
        return {
            "type": "trace",
            "block_number": trace.block_number,
            "transaction_hash": trace.transaction_hash,
            "transaction_index": trace.transaction_index,
            "from_address": trace.from_address,
            "to_address": trace.to_address,
            "value": trace.value,
            "input": trace.input,
            "output": trace.output,
            "trace_type": trace.trace_type,
            "call_type": trace.call_type,
            "reward_type": trace.reward_type,
            "gas": trace.gas,
            "gas_used": trace.gas_used,
            "subtraces": trace.subtraces,
            "trace_address": trace.trace_address,
            "error": trace.error,
            "status": trace.status,
            "trace_id": trace.trace_id,
            "logs": trace.logs,
        }
