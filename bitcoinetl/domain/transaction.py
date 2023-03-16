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
from bitcoinetl.domain.transaction_input import BtcTransactionInput
from bitcoinetl.domain.transaction_output import BtcTransactionOutput


# https://bitcoin.org/en/developer-reference#raw-transaction-format
class BtcTransaction(object):
    def __init__(self):
        # https://bitcoin.stackexchange.com/questions/77699/whats-the-difference-between-txid-and-hash-getrawtransaction-bitcoind
        self.hash: Optional[str] = None
        self.size: Optional[int] = None
        self.vsize: Optional[int] = None
        self.weight: Optional[int] = None
        self.version: Optional[int] = None
        self.locktime: Optional[int] = None
        self.block_number: Optional[int] = None
        self.block_hash: Optional[str] = None
        self.block_timestamp: Optional[int] = None
        self.is_coinbase: bool = False
        self.index: Optional[int] = None
        self.hex: Optional[str] = None

        self.inputs: List[BtcTransactionInput] = []
        self.outputs: List[BtcTransactionOutput] = []

        # Only for Zcash
        self.join_splits = []
        self.value_balance: Optional[int] = 0

    def add_input(self, input):
        if len(self.inputs) > 0:
            input.index = self.inputs[len(self.inputs) - 1].index + 1
            self.inputs.append(input)
        else:
            input.index = 0
            self.inputs.append(input)

    def add_output(self, output):
        if len(self.outputs) > 0:
            output.index = self.outputs[len(self.outputs) - 1].index + 1
            self.outputs.append(output)
        else:
            output.index = 0
            self.outputs.append(output)

    def calculate_input_value(self):
        return sum([input.value for input in self.inputs if input.value is not None])

    def calculate_output_value(self):
        return sum(
            [output.value for output in self.outputs if output.value is not None]
        )

    def calculate_fee(self):
        if self.is_coinbase:
            return 0
        return self.calculate_input_value() - self.calculate_output_value()
