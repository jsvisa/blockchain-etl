from typing import List, Dict, Union, Optional, Set
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.log import EthLog
from ethereumetl.mappers.erc721_transfer_mapper import EthErc721TransferMapper
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.service.erc721_transfer_extractor import EthErc721TransferExtractor


class ExtractErc721TransfersJob(BaseJob):
    def __init__(
        self,
        logs_iterable: List[Union[Dict, EthLog]],
        batch_size,
        max_workers,
        item_exporter,
        erc20_tokens: Optional[Set] = None,
        chain: Optional[str] = None,
    ):
        self.logs_iterable = logs_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.log_mapper = EthLogMapper()
        self.erc721_transfer_mapper = EthErc721TransferMapper()
        self.erc721_transfer_extractor = EthErc721TransferExtractor(erc20_tokens, chain)

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
        erc721_transfer = self.erc721_transfer_extractor.extract_transfer_from_log(log)
        if erc721_transfer is None:
            return
        self.item_exporter.export_item(
            self.erc721_transfer_mapper.erc721_transfer_to_dict(erc721_transfer)
        )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
