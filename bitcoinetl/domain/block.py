# MIT License
#
# Copyright (c) 2018 Omidiora Samuel, samparsky@gmail.com
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
from bitcoinetl.domain.transaction import BtcTransaction


class BtcBlock(object):
    def __init__(self):
        self.hash: Optional[str] = None
        self.size: Optional[str] = None
        self.stripped_size: Optional[str] = None
        self.weight: Optional[str] = None
        self.number: Optional[int] = None
        self.version: Optional[str] = None
        self.merkle_root: Optional[str] = None
        self.timestamp: Optional[int] = None
        self.nonce: Optional[str] = None
        self.bits: Optional[str] = None
        self.difficulty: Optional[float] = None
        self.coinbase_param: Optional[str] = None

        self.transactions: List[BtcTransaction] = []
        self.transaction_count: int = 0

    def has_full_transactions(self) -> bool:
        return len(self.transactions) > 0 and isinstance(
            self.transactions[0], BtcTransaction
        )
