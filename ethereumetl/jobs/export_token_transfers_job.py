import logging
from time import time
from typing import Dict, Callable, Optional, List

from eth_utils.address import to_checksum_address
from blockchainetl.utils import time_elapsed
from blockchainetl.utils import validate_range
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.providers.auto import new_web3_provider
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_logs_job import ExportLogsJob
from ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ethereumetl.mappers.log_mapper import EthLogMapper
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.service.token_transfer_extractor import (
    EthTokenTransferExtractor,
    TRANSFER_EVENT_TOPIC,
    DEPOSIT_EVENT_TOPIC,
    WITHDRAWAL_EVENT_TOPIC,
)


class ExportTokenTransfersJob(BaseJob):
    def __init__(
        self,
        chain,
        start_block,
        end_block,
        batch_size,
        batch_web3_provider,
        item_exporter,
        max_workers,
        tokens: Optional[List[str]] = None,
        item_converter: Optional[Callable[..., Dict]] = None,
        enable_enrich=True,
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider
        self.web3 = new_web3_provider(batch_web3_provider, chain)
        self.item_exporter = item_exporter
        if tokens is not None:
            tokens = list(set([to_checksum_address(e) for e in tokens]))
        self.tokens = tokens
        self.item_converter = item_converter

        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self.log_mapper = EthLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.extractor = EthTokenTransferExtractor(chain)
        self.enable_enrich = enable_enrich

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
        )

    def _export_batch(self, block_number_batch):
        assert len(block_number_batch) > 0
        start_block, end_block = block_number_batch[0], block_number_batch[-1]

        st0 = time()
        dict_logs = self._get_block_logs(start_block, end_block)
        st1 = time()

        if len(dict_logs) == 0 or self.enable_enrich is False:
            block_timestamps = dict()
        else:
            block_timestamps = self._get_block_timestamps(
                start_block, end_block, set(e["block_number"] for e in dict_logs)
            )
        st2 = time()

        token_transfers = []
        for event in dict_logs:
            log = self.log_mapper.dict_to_log(event)
            token_transfer = self.extractor.extract_transfer_from_log(log)
            if token_transfer is None:
                continue

            item = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
            if self.enable_enrich is True:
                item = self._enrich_item(item, block_timestamps)
            if self.item_converter is not None:
                item = self._convert_item(item)
            token_transfers.append(item)
        exported = self.item_exporter.export_items(token_transfers)
        st3 = time()

        logging.info(
            f"rpc fetch {start_block, end_block} "
            f"#logs={len(dict_logs)} #xfers={len(token_transfers)} #exported={exported} "
            f"@get_logs={time_elapsed(st0, st1)}s @get_blocks={time_elapsed(st1, st2)}s "
            f"@export_items={time_elapsed(st2, st3)}s "
        )

    def _get_block_timestamps(self, start_block, end_block, blocks) -> Dict[int, int]:
        item_exporter = InMemoryItemExporter(item_types=[EntityType.BLOCK])
        job = ExportBlocksJob(
            start_block,
            end_block,
            self.batch_size,
            self.batch_web3_provider,
            self.max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=False,
            blocks=blocks,
        )
        job.run()
        items = item_exporter.get_items(EntityType.BLOCK)
        return {item["number"]: item["timestamp"] for item in items}

    def _get_block_logs(self, start_block, end_block) -> List[Dict]:
        item_exporter = InMemoryItemExporter(item_types=[EntityType.LOG])
        job = ExportLogsJob(
            start_block,
            end_block,
            self.batch_size,
            self.batch_web3_provider,
            self.max_workers,
            item_exporter,
            topics=[TRANSFER_EVENT_TOPIC, DEPOSIT_EVENT_TOPIC, WITHDRAWAL_EVENT_TOPIC],
            address=self.tokens,
        )
        job.run()
        return item_exporter.get_items(EntityType.LOG)

    def _enrich_item(self, item: Dict, block_timestamps: Dict) -> Dict:
        item["block_timestamp"] = block_timestamps[item["block_number"]]
        return item

    def _convert_item(self, item: Dict) -> Dict:
        if self.item_converter is not None:
            item = self.item_converter(item)
        return item

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
