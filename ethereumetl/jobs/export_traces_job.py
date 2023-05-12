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

import json
import logging
from collections import defaultdict

from web3 import Web3
from typing import List, Dict, Optional, Generator, Tuple
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.utils import (
    validate_range,
    rpc_response_to_result,
    rpc_response_batch_to_results,
)

from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ethereumetl.mappers.trace_mapper import EthTraceMapper
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.domain.trace import EthTrace
from ethereumetl.service.eth_special_trace_service import EthSpecialTraceService
from ethereumetl.json_rpc_requests import (
    generate_trace_block_by_number_json_rpc,
    generate_trace_transaction_json_rpc,
    generate_arbtrace_block_by_number_json_rpc,
    generate_parity_trace_block_by_number_json_rpc,
)
from ethereumetl.service.trace_id_calculator import calculate_trace_ids
from ethereumetl.service.trace_status_calculator import calculate_trace_statuses
from blockchainetl import env


class ExportTracesJob(BaseJob):
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int,
        web3: Web3,
        batch_web3_provider,
        item_exporter,
        max_workers: int,
        include_genesis_traces=False,
        include_daofork_traces=False,
        is_geth_provider=True,
        retain_precompiled_calls=True,
        txhash_iterable: Optional[Dict[int, Dict[int, str]]] = None,
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block
        self.txhash_iterable = txhash_iterable or dict()

        self.web3 = web3
        self.batch_web3_provider = batch_web3_provider
        self.is_geth_provider = is_geth_provider
        self.retain_precompiled_calls = retain_precompiled_calls

        # Geth's trace performs very poorly, forcing requests one block at a time
        if is_geth_provider:
            assert (
                len(txhash_iterable) > 0
            ), "geth tracer should contains the detailed transaction hashes"
            # TODO: use batch_size in Parity/OpenEthereum when this issue is fixed
            # https://github.com/paritytech/parity-ethereum/issues/9822
            batch_size = 1
        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(
            batch_size, max_workers, max_retries=3
        )
        self.item_exporter = item_exporter

        self.trace_mapper = EthTraceMapper()
        self.geth_trace_mapper = EthGethTraceMapper()

        self.special_trace_service = EthSpecialTraceService()
        self.include_genesis_traces = include_genesis_traces
        self.include_daofork_traces = include_daofork_traces

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
        )

    def _export_batch(self, block_number_batch: List[int]):
        assert len(block_number_batch) > 0

        all_traces = []

        if self.include_genesis_traces and 0 in block_number_batch:
            genesis_traces = self.special_trace_service.get_genesis_traces()
            all_traces.extend(genesis_traces)

        if self.include_daofork_traces and DAOFORK_BLOCK_NUMBER in block_number_batch:
            daofork_traces = self.special_trace_service.get_daofork_traces()
            all_traces.extend(daofork_traces)

        if env.IS_FLATCALL_TRACE is True:
            traces = self._export_batch_flatcall(block_number_batch)
        elif env.IS_ARBITRUM_TRACE is True:
            traces = self._export_batch_arbitrum(block_number_batch)
        elif self.is_geth_provider is True:
            traces = self._export_batch_geth(block_number_batch)
        else:
            traces = self._export_batch_parity(block_number_batch)

        all_traces.extend(traces)

        calculate_trace_statuses(all_traces)
        calculate_trace_ids(all_traces)

        for trace in all_traces:
            self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))

    def _export_batch_parity(self, block_number_batch: List[int]) -> List[EthTrace]:
        trace_block_rpc = list(
            generate_parity_trace_block_by_number_json_rpc(block_number_batch)
        )
        if self.batch_size == 1:
            trace_block_rpc = trace_block_rpc[0]
        response = self.batch_web3_provider.make_batch_request(
            json.dumps(trace_block_rpc)
        )

        if self.batch_size == 1:
            response = response[0]

        traces = []
        for block_trace in response:
            for trace in block_trace["result"]:
                traces.append(self.trace_mapper.json_dict_to_trace(trace))

        return traces

    def _export_batch_geth(self, block_number_batch: List[int]) -> List[EthTrace]:
        json_traces: List[EthTrace] = []

        def tracer():
            if env.GETH_TRACE_BY_TRANSACTION_HASH is True:
                return self._export_batch_geth_by_txhash(block_number_batch)
            else:
                return self._export_batch_geth_by_block(block_number_batch)

        for blknum, tx_traces in tracer():
            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(
                {
                    "block_number": blknum,
                    "tx_traces": tx_traces,
                    "tx_hashes": self.txhash_iterable.get(blknum, {}),
                }
            )

            json_traces.extend(
                self.trace_mapper.geth_trace_to_traces(
                    geth_trace, retain_precompiled_calls=self.retain_precompiled_calls
                )
            )

        # Cronos's traces maybe return empty results:
        # eg:
        # {
        #     "type": "trace",
        #     "block_number": 2105043,
        #     "transaction_hash": None,
        #     "transaction_index": 108,
        #     "from_address": "",
        #     "to_address": None,
        #     "value": None,
        #     "input": "",
        #     "output": None,
        #     "trace_type": "",
        #     "call_type": None,
        #     "reward_type": None,
        #     "gas": "",
        #     "gas_used": "",
        #     "subtraces": 0,
        #     "trace_address": [],
        #     "error": None,
        #     "status": None,
        #     "trace_id": None,
        # }

        if env.GETH_TRACE_IGNORE_ERROR_EMPTY_TRACE is True:
            json_traces = [
                e
                for e in json_traces
                if (
                    e.transaction_hash is not None
                    and e.transaction_index is not None
                    and e.value is not None
                    and e.gas is not None
                )
            ]

        return json_traces

    def _export_batch_flatcall(self, block_number_batch: List[int]) -> List[EthTrace]:
        trace_block_rpc = list(
            generate_trace_block_by_number_json_rpc(
                block_number_batch,
                tracer="flatCallTracer",
                tracer_config={
                    "convertParityErrors": True,
                    "includePrecompiles": self.retain_precompiled_calls,
                },
            )
        )
        if self.batch_size == 1:
            trace_block_rpc = trace_block_rpc[0]
        response = self.batch_web3_provider.make_batch_request(
            json.dumps(trace_block_rpc)
        )
        if self.batch_size == 1:
            response = response[0]

        traces = []
        for block_traces in rpc_response_batch_to_results(response):
            for tx_traces in block_traces:
                for tx_trace in tx_traces["result"]:
                    trace = self.trace_mapper.json_dict_to_trace(tx_trace)
                    traces.append(trace)

        return traces

    def _export_batch_arbitrum(self, block_number_batch: List[int]) -> List[EthTrace]:
        trace_block_rpc = list(
            generate_arbtrace_block_by_number_json_rpc(block_number_batch)
        )
        if self.batch_size == 1:
            trace_block_rpc = trace_block_rpc[0]
        response = self.batch_web3_provider.make_batch_request(
            json.dumps(trace_block_rpc)
        )

        # flatten block results
        # Arbitrum's JSONRPC is not standard, it's result is List(standard is Dict)
        if self.batch_size == 1:
            json_traces: List[Dict] = rpc_response_to_result(response)
        else:
            json_traces = []
            for r in response:
                json_traces.extend(rpc_response_to_result(r))

        return [
            self.trace_mapper.json_dict_to_trace(json_trace)
            for json_trace in json_traces
        ]

    def _export_batch_geth_by_txhash(
        self, block_number_batch: List[int]
    ) -> Generator[Tuple[int, Dict], None, None]:
        block_traces = defaultdict(list)
        txhashes = dict()
        for blknum in block_number_batch:
            txs = self.txhash_iterable[blknum]
            block_traces[blknum] = [None] * len(txs)
            for txpos, txhash in txs.items():
                txhashes[txhash] = (blknum, txpos)

        # some blocks maybe empty
        # and empty batch request maybe return error result:
        # `[]` -> { "jsonrpc": "2.0", "id": null, "error": { "code": -32600, "message": "empty batch" } } # noqa
        if len(txhashes) == 0:
            return

        trace_hash_rpc = list(
            generate_trace_transaction_json_rpc(list(txhashes.keys()))
        )
        response = self.batch_web3_provider.make_batch_request(
            json.dumps(trace_hash_rpc)
        )
        ignore_error = env.GETH_TRACE_TRANSACTION_IGNORE_ERROR
        for response_item in response:
            txhash = response_item.get("id")
            # ignore cronos trace error:
            # {
            #     "code": -32000,
            #     "message": "ethereum tx not found in msgs: 0xd889167d920f6746276d5852f905f05b2acad01219584020ac897a3b139f2ee9", # noqa: E501
            # }
            result = rpc_response_to_result(response_item, ignore_error=ignore_error)
            if result is None and ignore_error is True:
                continue
            blknum, txpos = txhashes[txhash]
            block_traces[blknum][txpos] = result

        for blknum, tx_traces in block_traces.items():
            if ignore_error is True:
                # remove the None items
                yield blknum, [e for e in tx_traces if e]
            else:
                yield blknum, tx_traces

    def _export_batch_geth_by_block(
        self, block_number_batch: List[int]
    ) -> Generator[Tuple[int, Dict], None, None]:
        trace_block_rpc = list(
            generate_trace_block_by_number_json_rpc(block_number_batch)
        )
        if self.batch_size == 1:
            trace_block_rpc = trace_block_rpc[0]
        response = self.batch_web3_provider.make_batch_request(
            json.dumps(trace_block_rpc)
        )
        if self.batch_size == 1:
            response = [response]

        for response_item in response:
            blknum = response_item.get("id")
            result = rpc_response_to_result(response_item)
            tx_traces = []
            for tx_index, tx_trace in enumerate(result):
                trace = tx_trace.get("result")
                if trace is None:
                    msg = (
                        f"Geth trace for block:{blknum} txpos:{tx_index} is nil, "
                        f"maybe timeout, raw message: {tx_trace}"
                    )
                    # Optimistic's trace block 0x3D9 returns error:
                    # TypeError: cannot read property 'toString' of undefined in server-side tracer function 'result' # noqa
                    if env.IGNORE_TRACE_ERROR:
                        logging.warning(msg)
                        continue
                    else:
                        raise ValueError(msg)
                tx_traces.append(trace)
            yield blknum, tx_traces

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
