from typing import List, Dict, Union, Optional
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.log import EthLog
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.mappers.erc721_transfer_mapper import EthErc721TransferMapper
from ethereumetl.service.cryptopunk_extractor import EthCryptoPunkTransferExtractor


class ExtractCryptoPunkTransfersJob(BaseJob):
    def __init__(
        self,
        logs_iterable: List[Union[Dict, EthLog]],
        item_exporter,
        chain: Optional[str] = None,
    ):
        self.logs_iterable = logs_iterable
        self.item_exporter = item_exporter

        self.log_mapper = EthLogMapper()
        self.erc721_mapper = EthErc721TransferMapper()
        self.cp_extractor = EthCryptoPunkTransferExtractor(chain)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        cp_logs = []
        for log in self.logs_iterable:
            if isinstance(log, dict):
                log = self.log_mapper.dict_to_log(log)
            xfer = self.cp_extractor.extract(log)
            if xfer is not None:
                cp_logs.append(xfer)

        if len(cp_logs) == 0:
            return

        xfers = self.cp_extractor.merge(cp_logs)
        for xfer in xfers:
            self.item_exporter.export_item(
                self.erc721_mapper.erc721_transfer_to_dict(xfer)
            )

    def _end(self):
        self.item_exporter.close()
