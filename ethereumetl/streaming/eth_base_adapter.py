from typing import Tuple, List, Dict, Optional, Union
from cachetools import cached, TTLCache

from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.providers.auto import new_web3_provider
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_logs_job import ExportLogsJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob


class EthBaseAdapter:
    def __init__(
        self,
        chain: str,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        batch_size=5,
        max_workers=5,
    ):
        self.chain = chain
        self.web3 = new_web3_provider(batch_web3_provider, chain)
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers

    def open(self):
        self._open()
        self.item_exporter.open()

    def _open(self):
        pass

    def close(self):
        self.item_exporter.close()
        self._close()

    def _close(self):
        pass

    # TODO: make this configure able
    # cache for 10s
    @cached(cache=TTLCache(maxsize=16, ttl=10))
    def get_current_block_number(self) -> Tuple[int, int]:
        block = self.web3.eth.get_block("latest")
        return (block.number, block.timestamp)

    def export_blocks_and_transactions(self, start_block, end_block, blocks=None):
        exporter = InMemoryItemExporter(
            item_types=[EntityType.BLOCK, EntityType.TRANSACTION]
        )
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_blocks=True,
            export_transactions=True,
            blocks=blocks,
        )
        job.run()
        blocks = exporter.get_items(EntityType.BLOCK)
        transactions = exporter.get_items(EntityType.TRANSACTION)
        return blocks, transactions

    def export_blocks(self, start_block, end_block, blocks=None) -> List[Dict]:
        exporter = InMemoryItemExporter(item_types=[EntityType.BLOCK])
        job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_blocks=True,
            export_transactions=False,
            blocks=blocks,
        )
        job.run()
        return exporter.get_items(EntityType.BLOCK)

    def export_logs(
        self,
        start_block,
        end_block,
        topics: Optional[List[str]] = None,
        address: Optional[Union[str, List[str]]] = None,
        blocks=None,
    ) -> List[Dict]:
        exporter = InMemoryItemExporter(item_types=[EntityType.LOG])
        job = ExportLogsJob(
            start_block,
            end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            topics=topics,
            address=address,
            item_exporter=exporter,
            blocks=blocks,
        )
        job.run()
        logs = exporter.get_items(EntityType.LOG)
        return logs

    def export_receipts(self, transactions: List[Dict]) -> List[Dict]:
        exporter = InMemoryItemExporter(item_types=[EntityType.RECEIPT])

        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["hash"] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=True,
            export_logs=False,
        )
        job.run()
        receipts = exporter.get_items(EntityType.RECEIPT)
        return receipts
