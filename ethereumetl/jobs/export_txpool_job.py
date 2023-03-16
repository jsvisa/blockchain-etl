import json

from blockchainetl.utils import rpc_response_batch_to_results
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import generate_get_txpool_content_json_rpc
from ethereumetl.mappers.txpool_mapper import EthTxpoolMapper
from ethereumetl.domain.txpool import EthTxpool


# Export transaction pools
class ExportTxpoolJob(BaseJob):
    def __init__(self, provider, item_exporter):
        self.provider = provider
        self.item_exporter = item_exporter
        self.tx_pool_mapper = EthTxpoolMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        return self._export_tx_pools()

    def _export_tx_pools(self):
        tx_pools_rpc = generate_get_txpool_content_json_rpc()

        response = self.provider.make_batch_request(json.dumps(tx_pools_rpc))
        result = list(rpc_response_batch_to_results(response))[0]
        tx_pools = self.tx_pool_mapper.json_dict_to_txpools(result)
        for tx_pool in tx_pools:
            self._export_tx_pool(tx_pool)

    def _export_tx_pool(self, tx_pool: EthTxpool):
        self.item_exporter.export_item(self.tx_pool_mapper.txpool_to_dict(tx_pool))

    def _end(self):
        self.item_exporter.close()
