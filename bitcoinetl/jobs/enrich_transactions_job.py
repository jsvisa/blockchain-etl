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

from typing import List, Dict
from blockchainetl.enumeration.chain import Chain
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.utils import dynamic_batch_iterator

from bitcoinetl.domain.transaction import BtcTransaction
from bitcoinetl.domain.transaction_input import BtcTransactionInput
from bitcoinetl.mappers.transaction_mapper import BtcTransactionMapper
from bitcoinetl.service.btc_service import BtcService


# Add required_signatures, type, addresses, and value to transaction inputs
class EnrichTransactionsJob(BaseJob):
    def __init__(
        self,
        transactions_iterable,
        batch_size,
        bitcoin_rpc,
        max_workers,
        item_exporter,
        chain=Chain.BITCOIN,
    ):
        self.transactions_iterable = transactions_iterable
        self.btc_service = BtcService(bitcoin_rpc, chain)

        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(
            batch_size, max_workers, max_retries=3
        )
        self.item_exporter = item_exporter

        self.transaction_mapper = BtcTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.transactions_iterable, self._enrich_transactions
        )

    def _enrich_transactions(self, transactions: List[Dict]):
        transactions: List[BtcTransaction] = [
            self.transaction_mapper.dict_to_transaction(transaction)
            for transaction in transactions
        ]

        all_inputs = [transaction.inputs for transaction in transactions]
        flat_inputs = [input for inputs in all_inputs for input in inputs]

        for batch in dynamic_batch_iterator(flat_inputs, lambda: self.batch_size):
            input_txs = self._get_input_transactions_as_map(batch)
            for input in batch:
                if input.spent_transaction_hash is None:
                    continue

                # fill in the missing fields from previous output
                # TODO: better integrate inside input
                output_tx = self._get_output_for_input(input, input_txs)
                output = output_tx.outputs[input.spent_output_index or 0]
                if output is not None:
                    input.req_sigs = output.req_sigs
                    input.type = output.type
                    input.addresses = output.addresses
                    input.value = output.value
                    input.spent_output_count = len(output_tx.outputs)

        for transaction in transactions:
            self.item_exporter.export_item(
                self.transaction_mapper.transaction_to_dict(transaction)
            )

    def _get_input_transactions_as_map(
        self, tx_inputs: List[BtcTransactionInput]
    ) -> Dict[str, BtcTransaction]:
        tx_hashes = [
            input.spent_transaction_hash
            for input in tx_inputs
            if input.spent_transaction_hash is not None
        ]

        tx_hashes = set(tx_hashes)
        if len(tx_hashes) > 0:
            transactions = self.btc_service.get_transactions_by_hashes(tx_hashes)
            return {transaction.hash: transaction for transaction in transactions}
        else:
            return {}

    def _get_output_for_input(
        self,
        transaction_input: BtcTransactionInput,
        input_transactions_map: Dict[str, BtcTransaction],
    ) -> BtcTransaction:
        spent_transaction_hash = transaction_input.spent_transaction_hash
        input_transaction = input_transactions_map.get(spent_transaction_hash)
        if input_transaction is None:
            raise ValueError(
                "Input transaction with hash {} not found".format(
                    spent_transaction_hash
                )
            )

        spent_output_index = transaction_input.spent_output_index or 0
        if input_transaction.outputs is None or len(input_transaction.outputs) < (
            spent_output_index + 1
        ):
            raise ValueError(
                "There is no output with index {} in transaction with hash {}".format(
                    spent_output_index, spent_transaction_hash
                )
            )

        return input_transaction

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
