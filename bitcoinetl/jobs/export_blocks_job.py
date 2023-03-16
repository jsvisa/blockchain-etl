# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from bitcoinetl.domain.block import BtcBlock
from bitcoinetl.mappers.block_mapper import BtcBlockMapper
from bitcoinetl.mappers.transaction_mapper import BtcTransactionMapper
from bitcoinetl.service.btc_service import BtcService
from blockchainetl.enumeration.chain import Chain
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.utils import validate_range


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
        self,
        start_block,
        end_block,
        batch_size,
        bitcoin_rpc,
        max_workers,
        item_exporter,
        chain: Chain,
        export_blocks=True,
        export_transactions=True,
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError(
                "At least one of export_blocks or export_transactions must be True"
            )

        self.chain = chain
        self.btc_service = BtcService(bitcoin_rpc, chain)
        self.block_mapper = BtcBlockMapper()
        self.transaction_mapper = BtcTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
        )

    def _export_batch(self, block_number_batch):
        blocks = self.btc_service.get_blocks(
            block_number_batch, self.export_transactions
        )

        for block in blocks:
            if self.export_blocks:
                self._export_block(block)
            if self.export_transactions:
                self._export_transaction(block)

    def _export_block(self, block: BtcBlock):
        self.item_exporter.export_item(self.block_mapper.block_to_dict(block))

    def _export_transaction(self, block: BtcBlock):
        for tx in block.transactions:
            if self._is_hardfork_txhash(block.number, tx.hash):
                continue

            self.item_exporter.export_item(
                self.transaction_mapper.transaction_to_dict(tx)
            )

    # special txhash, duplicated txhash
    def _is_hardfork_txhash(self, blknum, txhash) -> bool:
        if self.chain != Chain.BITCOIN:
            return False

        is_blk_91842 = (
            blknum == 91842
            and txhash
            == "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599"
        )
        is_blk_91880 = (
            blknum == 91880
            and txhash
            == "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"
        )
        return is_blk_91842 or is_blk_91880

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
