import json

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import generate_get_block_receipts_json_rpc
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from ethereumetl.domain.receipt import EthReceipt
from blockchainetl.utils import rpc_response_batch_to_results, validate_range


# Export block receipts
class ExportBlockReceiptsJob(BaseJob):
    def __init__(
        self,
        start_block,
        end_block,
        batch_size,
        batch_web3_provider,
        max_workers,
        item_exporter,
        export_receipts=True,
        export_logs=True,
        blocks=None,
    ):
        if blocks is not None:
            self.blocks = blocks
        else:
            validate_range(start_block, end_block)
            self.blocks = range(start_block, end_block + 1)

        self.batch_web3_provider = batch_web3_provider

        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_receipts = export_receipts
        self.export_logs = export_logs
        if not self.export_receipts and not self.export_logs:
            raise ValueError(
                "At least one of export_receipts or export_logs must be True"
            )

        self.receipt_mapper = EthReceiptMapper()
        self.log_mapper = EthLogMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.blocks, self._export_batch)

    def _export_batch(self, block_number_batch):
        receipts_rpc = list(generate_get_block_receipts_json_rpc(block_number_batch))
        if self.batch_size == 1:
            receipts_rpc = receipts_rpc[0]
        response = self.batch_web3_provider.make_batch_request(json.dumps(receipts_rpc))
        results = rpc_response_batch_to_results(response, requests=receipts_rpc)
        for result in results:
            for receipt in result:
                self._export_receipt(self.receipt_mapper.json_dict_to_receipt(receipt))

    def _export_receipt(self, receipt: EthReceipt):
        if self.export_receipts:
            self.item_exporter.export_item(self.receipt_mapper.receipt_to_dict(receipt))
        if self.export_logs:
            for log in receipt.logs:
                self.item_exporter.export_item(self.log_mapper.log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
