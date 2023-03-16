# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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


class EthTrace(object):
    def __init__(self):
        self._st: Optional[int] = None
        self._st_day: Optional[str] = None
        self.block_number: Optional[int] = None
        self.transaction_hash: Optional[str] = None
        self.transaction_index: Optional[int] = None
        self.from_address: Optional[str] = None
        self.to_address: Optional[str] = None
        self.value: Optional[int] = None
        self.input: Optional[str] = None
        self.output: Optional[str] = None
        self.trace_type: Optional[str] = None
        self.call_type: Optional[str] = None
        self.reward_type: Optional[str] = None
        self.gas: Optional[int] = None
        self.gas_used: Optional[int] = None
        self.subtraces: int = 0
        self.trace_address: List[int] = []
        self.error: Optional[str] = None
        self.status: Optional[int] = None
        self.trace_id: Optional[str] = None
