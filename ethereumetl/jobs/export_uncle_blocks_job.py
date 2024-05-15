import json

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import (
    generate_get_uncle_by_block_hash_and_index_json_rpc,
)
from ethereumetl.mappers.uncle_block_mapper import EthUncleBlockMapper
from blockchainetl.utils import rpc_response_batch_to_results


class ExportUncleBlocksJob(BaseJob):
    def __init__(
        self,
        block_uncle_iterator,
        batch_size,
        batch_web3_provider,
        max_workers,
        item_exporter,
    ):
        self.block_uncle_iterator = block_uncle_iterator

        self.batch_web3_provider = batch_web3_provider

        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.block_mapper = EthUncleBlockMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.block_uncle_iterator, self._export_batch)

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(
            generate_get_uncle_by_block_hash_and_index_json_rpc(block_number_batch)
        )
        if self.batch_size == 1:
            blocks_rpc = blocks_rpc[0]
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(
            response, with_id=True, requests=blocks_rpc
        )
        for result, req_id in results:
            block = self.block_mapper.json_dict_to_block(result)
            block.hermit_blknum = int(req_id.split("-")[0])
            block.hermit_uncle_pos = int(req_id.split("-")[1])
            self._export_block(block)

    def _export_block(self, block):
        self.item_exporter.export_item(self.block_mapper.block_to_dict(block))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
