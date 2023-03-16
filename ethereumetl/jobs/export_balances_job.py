import logging
from web3 import Web3
from blockchainetl.utils import rpc_response_to_result
from blockchainetl.jobs.base_job import BaseJob


class ExportBalancesJob(BaseJob):
    def __init__(self, block: int, start: str, web3: Web3, item_exporter):
        self.web3 = web3
        self.block = hex(block)
        self.start = start
        self.item_exporter = item_exporter

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        start = self.start
        count = 0
        while True:
            response = self.web3.provider.make_request(
                "debug_accountRange",
                params=[self.block, start, 0, True, True, True],
            )
            result = rpc_response_to_result(response)

            if "next" not in result:
                logging.info("finished")
                break

            start = result["next"]
            count += 1
            if count % 100 == 0:
                logging.info(f"dump balance count: {count} start: {start}")
            for address, info in result["accounts"].items():
                self.item_exporter.export_item(
                    {
                        "type": "balance",
                        "address": address,
                        "balance": info["balance"],
                        "nonce": info.get("nonce", 0),
                        "root": info["root"],
                        "code_hash": info.get("codeHash", ""),
                        "key": info.get("key", ""),
                    }
                )

    def _end(self):
        self.item_exporter.close()
