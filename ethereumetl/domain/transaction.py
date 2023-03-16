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

from typing import Optional


class EthTransaction(object):
    def __init__(self):
        self._st: Optional[int] = None
        self._st_day: Optional[str] = None
        self.hash: Optional[str] = None
        self.nonce: Optional[int] = None
        self.block_hash: Optional[str] = None
        self.block_number: Optional[int] = None
        self.block_timestamp: Optional[int] = None
        self.transaction_index: Optional[int] = None
        self.from_address: Optional[str] = None
        self.to_address: Optional[str] = None
        self.value: Optional[int] = None
        self.gas: Optional[int] = None
        self.gas_price: Optional[int] = None
        self.input: Optional[str] = None
        self.max_fee_per_gas: Optional[int] = None
        self.max_priority_fee_per_gas: Optional[int] = None
        self.transaction_type: Optional[int] = None

        # those fields extracted from Receipt
        self.receipt_log_count: Optional[int] = None
        self.receipt_cumulative_gas_used: Optional[int] = None
        self.receipt_gas_used: Optional[int] = None
        self.receipt_contract_address: Optional[str] = None
        self.receipt_root: Optional[str] = None
        self.receipt_status: Optional[int] = None
        self.receipt_effective_gas_price: Optional[int] = None
