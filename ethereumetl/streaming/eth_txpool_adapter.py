import logging

from time import time
from datetime import datetime
from blockchainetl.utils import time_elapsed
from blockchainetl.enumeration.chain import Chain
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_txpool_job import ExportTxpoolJob
from ethereumetl.providers.auto import new_web3_provider
from .eth_item_id_calculator import EthItemIdCalculator


class EthTxpoolAdapter:
    def __init__(
        self,
        provider_uri,
        batch_web3_provider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        max_workers=5,
    ):
        self.chain = chain
        self.web3 = new_web3_provider(provider_uri, chain)
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self) -> int:
        return self.web3.eth.get_block("latest").number

    def export_all(self, _, end_block):
        st0 = time()
        now = datetime.utcnow()
        pools = self._export_tx_pools(end_block, now)
        self.calculate_item_ids(pools)
        st1 = time()
        self.item_exporter.export_items(pools)
        st2 = time()

        logging.info(
            f"Export Txpool #items={len(pools)} "
            f"elapsed @total={time_elapsed(st0, st2)} "
            f"@rpc_txpoolContent={time_elapsed(st0, st1)} "
            f"@export={time_elapsed(st1, st2)}"
        )

    def _export_tx_pools(self, end_block, block_timestamp):
        exporter = InMemoryItemExporter(item_types=[EntityType.TXPOOL])
        job = ExportTxpoolJob(provider=self.batch_web3_provider, item_exporter=exporter)
        job.run()
        items = exporter.get_items(EntityType.TXPOOL)
        for item in items:
            item["blknum"] = end_block
            item["block_timestamp"] = block_timestamp
        return items

    def calculate_item_ids(self, items):
        for item in items:
            item["item_id"] = self.item_id_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()
