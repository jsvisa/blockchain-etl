from typing import Dict, List, Union

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.log import EthLog
from ethereumetl.mappers.erc1155_transfer_mapper import EthErc1155TransferMapper
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.service.erc1155_transfer_extractor import EthErc1155TransferExtractor


class ExtractErc1155TransfersJob(BaseJob):
    def __init__(
        self,
        logs_iterable: List[Union[Dict, EthLog]],
        batch_size,
        max_workers,
        item_exporter,
    ):
        self.logs_iterable = logs_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.log_mapper = EthLogMapper()
        self.erc1155_transfer_mapper = EthErc1155TransferMapper()
        self.erc1155_transfer_extractor = EthErc1155TransferExtractor()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.logs_iterable, self._extract_transfers)

    def _extract_transfers(self, logs: List[Union[Dict, EthLog]]):
        for log in logs:
            if isinstance(log, dict):
                log = self.log_mapper.dict_to_log(log)
            self._extract_transfer(log)

    def _extract_transfer(self, log: EthLog):
        xfers = self.erc1155_transfer_extractor.extract_transfer_from_log(log)
        if xfers is None:
            return

        for xfer in xfers:
            self.item_exporter.export_item(
                self.erc1155_transfer_mapper.erc1155_transfer_to_dict(xfer)
            )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
