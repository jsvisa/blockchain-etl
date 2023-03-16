import json
from typing import Optional, List, Union

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.utils import rpc_response_to_result, validate_range
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import generate_get_log_by_number_json_rpc
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.mappers.log_mapper import EthLogMapper


# Export logs
class ExportLogsJob(BaseJob):
    def __init__(
        self,
        start_block,
        end_block,
        batch_size,
        batch_web3_provider: BatchHTTPProvider,
        max_workers,
        item_exporter,
        topics: Optional[List[str]] = None,
        address: Optional[Union[str, List[str]]] = None,
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block
        self.topics = topics
        self.address = address

        self.batch_web3_provider = batch_web3_provider
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter
        self.log_mapper = EthLogMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
        )

    def _export_batch(self, block_number_batch):
        from_block, to_block = block_number_batch[0], block_number_batch[-1]
        logs_rpc = generate_get_log_by_number_json_rpc(
            from_block, to_block, self.topics, self.address
        )
        response = self.batch_web3_provider.make_batch_request(json.dumps(logs_rpc))
        results = rpc_response_to_result(response)
        logs = [self.log_mapper.json_dict_to_log(result) for result in results]

        for log in logs:
            self.item_exporter.export_item(self.log_mapper.log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
