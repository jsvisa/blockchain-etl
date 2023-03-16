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

from typing import Optional, List

from ethereumetl.domain.transaction import EthTransaction


class EthBlock(object):
    def __init__(self):
        self._st: Optional[int] = None
        self._st_day: Optional[str] = None
        self.number: Optional[int] = None
        self.hash: Optional[str] = None
        self.parent_hash: Optional[str] = None
        self.nonce: Optional[int] = None
        self.sha3_uncles: Optional[str] = None
        self.logs_bloom: Optional[str] = None
        self.transactions_root: Optional[str] = None
        self.state_root: Optional[str] = None
        self.receipts_root: Optional[str] = None
        self.miner: Optional[str] = None
        self.difficulty: Optional[int] = None
        self.total_difficulty: Optional[int] = None
        self.size: Optional[int] = None
        self.extra_data: Optional[str] = None
        self.gas_limit: Optional[int] = None
        self.gas_used: Optional[int] = None
        self.timestamp: Optional[int] = None

        self.transactions: List[EthTransaction] = []
        self.transaction_count: int = 0
        self.base_fee_per_gas: Optional[int] = 0

        # added 2021.12.11
        self.uncle_count: int = 0
        self.uncle0_hash: Optional[str] = None
        self.uncle1_hash: Optional[str] = None


class EthUncleBlock(object):
    def __init__(self):
        self._st: Optional[int] = None
        self._st_day: Optional[str] = None
        self.number: Optional[int] = None
        self.hash: Optional[str] = None
        self.parent_hash: Optional[str] = None
        self.mix_hash: Optional[str] = None
        self.nonce: Optional[int] = None
        self.sha3_uncles: Optional[str] = None
        self.logs_bloom: Optional[str] = None
        self.transactions_root: Optional[str] = None
        self.state_root: Optional[str] = None
        self.receipts_root: Optional[str] = None
        self.miner: Optional[str] = None
        self.difficulty: Optional[int] = None
        self.size: Optional[int] = None
        self.extra_data: Optional[str] = None
        self.gas_limit: Optional[int] = None
        self.gas_used: Optional[int] = None
        self.timestamp: Optional[int] = None
        self.base_fee_per_gas: Optional[int] = 0
        self.hermit_blknum: Optional[int] = 0
        self.hermit_uncle_pos: Optional[int] = 0
        self.uncle_count: int = 0
