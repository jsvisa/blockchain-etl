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


import json
import logging
from typing import Iterable, Optional, Dict, Tuple, List

from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.utils import rpc_response_batch_to_results
from ethereumetl.domain.receipt import EthReceipt
from ethereumetl.json_rpc_requests import generate_get_receipt_json_rpc
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.mappers.receipt_mapper import EthReceiptMapper

logger = logging.getLogger(__name__)


# Exports receipts and logs
class ExportReceiptsJob(BaseJob):
    def __init__(
        self,
        transaction_hashes_iterable: Iterable[str],
        batch_size: int,
        batch_web3_provider,
        max_workers: int,
        item_exporter,
        export_receipts=True,
        export_logs=True,
        check_consistency=False,
        transaction_blocks: Optional[Dict[str, List[Tuple[int, int]]]] = None,
        ignore_error: bool = False,
    ):
        self.batch_web3_provider = batch_web3_provider

        # transaction hashes maybe duplicated,
        # eg: in Cronos, the same transaction maybe included in twice blocks
        # let's redup it if consistency is set.
        self.transaction_hashes_iterable = set(list(transaction_hashes_iterable))

        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_receipts = export_receipts
        self.export_logs = export_logs
        if not self.export_receipts and not self.export_logs:
            raise ValueError(
                "At least one of export_receipts or export_logs must be True"
            )

        # transaction hash collision,
        # ref https://github.com/crypto-org-chain/cronos/issues/462
        self.check_consistency = check_consistency
        self.transaction_blocks = transaction_blocks or dict()
        self.ignore_error = ignore_error

        self.receipt_mapper = EthReceiptMapper()
        self.log_mapper = EthLogMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.transaction_hashes_iterable, self._export_receipts
        )

    def _export_receipts(self, transaction_hashes: Iterable[str]):
        receipts_rpc = list(generate_get_receipt_json_rpc(transaction_hashes))
        if self.batch_size == 1:
            receipts_rpc = receipts_rpc[0]
        response = self.batch_web3_provider.make_batch_request(json.dumps(receipts_rpc))
        results = rpc_response_batch_to_results(
            response, ignore_error=self.ignore_error
        )
        receipts = [
            self.receipt_mapper.json_dict_to_receipt(result)
            for result in results
            if result is not None
        ]
        for receipt in receipts:
            if self.check_consistency is False:
                self._export_receipt(receipt)
                continue

            for (tx_blknum, txpos) in self.transaction_blocks.get(
                receipt.transaction_hash, []
            ):
                if tx_blknum != receipt.block_number:
                    # set status to failed, reset block number and txpos
                    receipt.status = 0
                    receipt.block_number = tx_blknum
                    receipt.transaction_index = txpos

                    # and reset other fields
                    receipt.cumulative_gas_used = None
                    receipt.gas_used = None
                    receipt.contract_address = None
                    receipt.logs = []
                    receipt.root = None
                    receipt.effective_gas_price = None
                self._export_receipt(receipt)
            del self.transaction_blocks[receipt.transaction_hash]

    def _export_receipt(self, receipt: EthReceipt):
        if self.export_receipts:
            self.item_exporter.export_item(self.receipt_mapper.receipt_to_dict(receipt))
        if self.export_logs:
            for log in receipt.logs:
                self.item_exporter.export_item(self.log_mapper.log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
