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


import logging
from time import time

from blockchainetl.utils import time_elapsed
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.jobs.enrich_transactions_job import EnrichTransactionsJob
from bitcoinetl.jobs.extract_traces_job import ExtractTracesJob
from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
from bitcoinetl.streaming.btc_item_id_calculator import BtcItemIdCalculator
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter


class BtcStreamerAdapter:
    def __init__(
        self,
        bitcoin_rpc: BitcoinRpc,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.BITCOIN,
        batch_size=2,
        enable_enrich=True,
        max_workers=5,
        entity_types=tuple(EntityType.ALL_FOR_ETL),
        cache_path=None,
    ):
        self.bitcoin_rpc = bitcoin_rpc
        self.chain = chain
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.enable_enrich = enable_enrich
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = BtcItemIdCalculator()
        self.cache_path = cache_path

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self) -> int:
        current_block = self.bitcoin_rpc.getblockcount()
        if current_block is None:
            raise ValueError("current block is none")
        return current_block

    def export_all(self, start_block: int, end_block: int):
        st0 = time()
        blocks, transactions = self._export_blocks(start_block, end_block)

        st1 = time()
        if self.enable_enrich:
            transactions = self._enrich_transactions(transactions)

        traces = []
        if self._should_export(EntityType.TRACE):
            traces = self._extract_traces(transactions)

        # keep empty if don't need to export
        if not self._should_export(EntityType.BLOCK):
            blocks = []
        if not self._should_export(EntityType.TRANSACTION):
            transactions = []

        all_items = blocks + transactions + traces

        st2 = time()
        self.calculate_item_ids(all_items)

        self.item_exporter.export_items(all_items)
        if len(all_items) > 1024:
            st3 = time()
            logging.info(
                f"PERF export blocks=({start_block}, {end_block}) size={len(all_items)} "
                f"total-elapsed={time_elapsed(st0, st3)} extract-elapsed={time_elapsed(st1, st2)} "
                f"export-elapsed={time_elapsed(st2, st3)}"
            )

    def _export_blocks(self, start_block: int, end_block: int):
        # Export blocks and transactions
        exporter = InMemoryItemExporter(
            item_types=[EntityType.BLOCK, EntityType.TRANSACTION]
        )

        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            bitcoin_rpc=self.bitcoin_rpc,
            max_workers=self.max_workers,
            item_exporter=exporter,
            chain=self.chain,
            export_blocks=True,
            export_transactions=True,
        )
        job.run()

        blocks = exporter.get_items(EntityType.BLOCK)
        transactions = exporter.get_items(EntityType.TRANSACTION)
        return blocks, transactions

    def _enrich_transactions(self, transactions):
        exporter = InMemoryItemExporter(item_types=[EntityType.TRANSACTION])

        job = EnrichTransactionsJob(
            transactions_iterable=transactions,
            batch_size=self.batch_size,
            bitcoin_rpc=self.bitcoin_rpc,
            max_workers=self.max_workers,
            item_exporter=exporter,
            chain=self.chain,
        )
        job.run()
        enriched_transactions = exporter.get_items(EntityType.TRANSACTION)
        if len(enriched_transactions) != len(transactions):
            raise ValueError("The number of transactions is wrong " + str(transactions))
        return enriched_transactions

    def _extract_traces(self, transactions):
        exporter = InMemoryItemExporter(item_types=[EntityType.TRACE])

        job = ExtractTracesJob(
            transactions_iterable=transactions,
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items(EntityType.TRACE)
        return traces

    def _should_export(self, entity_type) -> bool:
        return entity_type in self.entity_types

    def calculate_item_ids(self, items):
        for item in items:
            item["item_id"] = self.item_id_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()
