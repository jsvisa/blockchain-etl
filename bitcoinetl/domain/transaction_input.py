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


class BtcTransactionInput(object):
    def __init__(self):
        self.index: int = 0
        self.spent_transaction_hash: Optional[str] = None
        self.spent_output_index: Optional[int] = None
        self.spent_output_count: Optional[int] = None
        self.script_asm: Optional[str] = None
        self.script_hex: Optional[str] = None
        self.coinbase_param: Optional[str] = None
        self.sequence = None

        self.req_sigs: Optional[int] = None
        self.type: Optional[str] = None
        self.addresses: Optional[List[str]] = []
        self.txinwitness: Optional[List[str]] = []
        self.value: Optional[int] = None

    def is_coinbase(self):
        return self.coinbase_param is not None or self.spent_transaction_hash is None
