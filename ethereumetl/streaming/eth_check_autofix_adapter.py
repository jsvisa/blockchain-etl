import logging
from time import time
from datetime import datetime

from blockchainetl.utils import time_elapsed
from ethereumetl.providers.rpc import BatchHTTPProvider
from .eth_base_adapter import EthBaseAdapter


class EthCheckAutofixAdapter(EthBaseAdapter):
    def __init__(
        self,
        chain,
        checkers,
        batch_web3_provider: BatchHTTPProvider,
        batch_size: int = 10,
        max_workers: int = 10,
        dryrun: bool = False,
    ):
        self.checkers = checkers
        self.dryrun = dryrun
        EthBaseAdapter.__init__(
            self,
            chain,
            batch_web3_provider,
            batch_size=batch_size,
            max_workers=max_workers,
        )

    def export_all(self, start_block, end_block):
        st0 = time()
        blocks = self.export_blocks(start_block, end_block)
        st1 = time()

        st = min(b["timestamp"] for b in blocks)
        et = max(b["timestamp"] for b in blocks)

        st_day = datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")
        et_day = datetime.utcfromtimestamp(et).strftime("%Y-%m-%d")

        succeed = {}
        result = {}
        for name, checker in self.checkers:
            success = checker.check(st_day, et_day, st, et, start_block, end_block)
            fixed = None
            if not success and not self.dryrun:
                fixed = checker.autofix(st_day, et_day, st, et, start_block, end_block)
            succeed[name] = success
            result[name] = fixed
        st2 = time()

        logging.info(
            f"check[{start_block, end_block}] succeed: {succeed} result: {result} "
            f"@get_blocks={time_elapsed(st0, st1)} @autofix={time_elapsed(st1, st2)}"
        )
