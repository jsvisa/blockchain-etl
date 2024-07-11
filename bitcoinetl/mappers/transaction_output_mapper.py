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

from typing import Any, List, Dict

from bitcoinetl.btc_utils import bitcoin_to_satoshi, get_address
from bitcoinetl.domain.transaction_output import BtcTransactionOutput


class BtcTransactionOutputMapper(object):
    def vout_to_outputs(self, vout: List[Dict]):
        outputs = []
        for item in vout or []:
            output = self.json_dict_to_output(item)
            outputs.append(output)
        return outputs

    def json_dict_to_output(self, json_dict: Dict[str, Any]):
        output = BtcTransactionOutput()

        output.index = json_dict.get("n", 0)
        output.addresses = get_address(json_dict)
        output.txinwitness = json_dict.get("txinwitness")
        output.value = bitcoin_to_satoshi(json_dict.get("value"))
        if "scriptPubKey" in json_dict:
            script_pub_key = json_dict.get("scriptPubKey", dict())
            output.script_asm = script_pub_key.get("asm")
            output.script_hex = script_pub_key.get("hex")
            output.req_sigs = script_pub_key.get("reqSigs")
            output.type = script_pub_key.get("type")
            output.addresses = get_address(script_pub_key)

        return output

    def outputs_to_dicts(self, outputs: List[BtcTransactionOutput]) -> List[Dict]:
        result = []
        for output in outputs:
            item = {
                "index": output.index,
                "script_asm": output.script_asm,
                "script_hex": output.script_hex,
                "req_sigs": output.req_sigs,
                "type": output.type,
                "addresses": output.addresses,
                "txinwitness": output.txinwitness,
                "value": output.value,
            }
            result.append(item)
        return result

    def dicts_to_outputs(self, json_dicts: List[Dict]) -> List[BtcTransactionOutput]:
        result = []
        for json_dict in json_dicts:
            output = BtcTransactionOutput()
            output.index = json_dict.get("index", 0)
            output.script_asm = json_dict.get("script_asm")
            output.script_hex = json_dict.get("script_hex")
            output.req_sigs = json_dict.get("req_sigs")
            output.type = json_dict.get("type")
            output.addresses = get_address(json_dict)
            output.txinwitness = json_dict.get("txinwitness")
            output.value = json_dict.get("value")

            result.append(output)
        return result
