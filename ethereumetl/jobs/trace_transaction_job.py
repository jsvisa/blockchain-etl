import json
from typing import List
from web3 import Web3

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.utils import rpc_response_to_result
from ethereumetl.mappers.trace_mapper import EthTraceMapper
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.domain.trace import EthTrace
from ethereumetl.json_rpc_requests import generate_trace_transaction_json_rpc
from ethereumetl.service.trace_status_calculator import calculate_trace_statuses
from ethereumetl.providers.auto import get_provider_from_uri


class TraceTransactionJob(BaseJob):
    def __init__(
        self,
        txhash,
        provider_uri,
        item_exporter,
        is_geth_provider=True,
        retain_precompiled_calls=True,
    ):
        self.txhash = txhash
        self.is_geth_provider = is_geth_provider
        self.retain_precompiled_calls = retain_precompiled_calls
        self.batch_web3_provider = ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        )
        self.web3 = Web3(self.batch_web3_provider)

        self.item_exporter = item_exporter
        self.trace_mapper = EthTraceMapper()
        self.geth_trace_mapper = EthGethTraceMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):

        if self.is_geth_provider is True:
            traces = self._export_geth()
        else:
            traces = self._export_parity()

        calculate_trace_statuses(traces)

        for trace in traces:
            self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))

    def _export_parity(self) -> List[EthTrace]:
        json_traces = self.web3.parity.trace_transaction(self.txhash)

        if json_traces is None:
            raise ValueError(
                "Response from the node for txhash: {} is None. Is the node fully synced?".format(
                    self.txhash
                )
            )

        return [
            self.trace_mapper.json_dict_to_trace(json_trace)
            for json_trace in json_traces
        ]

    def _export_geth(self) -> List[EthTrace]:
        trace_tx_rpc = list(generate_trace_transaction_json_rpc([self.txhash]))[0]
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_tx_rpc))

        result = rpc_response_to_result(response)
        tx_traces = [result]

        # geth_trace_to_traces will iterator traces by index,
        # so the txpos is not used here, we pass the fake block_number/txpos
        geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(
            {
                "block_number": 0,
                "tx_traces": tx_traces,
                "tx_hashes": {0: self.txhash},
            }
        )

        traces = self.trace_mapper.geth_trace_to_traces(
            geth_trace,
            retain_precompiled_calls=self.retain_precompiled_calls,
        )

        tx = self.web3.eth.get_transaction(self.txhash)
        for trace in traces:
            trace.block_number = tx.blockNumber
            trace.transaction_index = tx.transactionIndex
        return traces

    def _end(self):
        self.item_exporter.close()
