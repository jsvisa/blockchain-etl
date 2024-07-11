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

from typing import List, Dict

from bitcoinetl.btc_utils import get_address
from bitcoinetl.domain.transaction_input import BtcTransactionInput


class BtcTransactionInputMapper(object):
    def vin_to_inputs(self, vin: List[Dict]) -> List[BtcTransactionInput]:
        inputs = []
        for index, item in enumerate(vin or []):
            input = self.json_dict_to_input(item)
            input.index = index
            inputs.append(input)

        return inputs

    def json_dict_to_input(self, json_dict: Dict) -> BtcTransactionInput:
        input = BtcTransactionInput()

        input.spent_transaction_hash = json_dict.get("txid")
        input.spent_output_index = json_dict.get("vout")
        input.coinbase_param = json_dict.get("coinbase")
        input.sequence = json_dict.get("sequence")
        input.txinwitness = json_dict.get("txinwitness")
        if "scriptSig" in json_dict:
            script_sig = json_dict.get("scriptSig", dict())
            input.script_asm = script_sig.get("asm")
            input.script_hex = script_sig.get("hex")
            input.req_sigs = script_sig.get("reqSigs")

        return input

    def inputs_to_dicts(self, inputs: List[BtcTransactionInput]) -> List[Dict]:
        result = []
        for input in inputs:
            item = {
                "index": input.index,
                "spent_transaction_hash": input.spent_transaction_hash,
                "spent_output_index": input.spent_output_index,
                "spent_output_count": input.spent_output_count,
                "script_asm": input.script_asm,
                "script_hex": input.script_hex,
                "sequence": input.sequence,
                "req_sigs": input.req_sigs,
                "type": input.type,
                "addresses": input.addresses,
                "txinwitness": input.txinwitness,
                "value": input.value,
            }
            result.append(item)
        return result

    def dicts_to_inputs(self, json_dicts: List[Dict]) -> List[BtcTransactionInput]:
        result = []
        for json_dict in json_dicts:
            input = BtcTransactionInput()
            input.index = json_dict.get("index", 0)
            input.spent_transaction_hash = json_dict.get("spent_transaction_hash")
            input.spent_output_index = json_dict.get("spent_output_index")
            input.spent_output_count = json_dict.get("spent_output_count")
            input.script_asm = json_dict.get("script_asm")
            input.script_hex = json_dict.get("script_hex")
            input.sequence = json_dict.get("sequence")
            input.req_sigs = json_dict.get("req_sigs")
            input.type = json_dict.get("type")
            input.addresses = get_address(json_dict)
            input.txinwitness = json_dict.get("txinwitness")
            input.value = json_dict.get("value")

            result.append(input)
        return result
