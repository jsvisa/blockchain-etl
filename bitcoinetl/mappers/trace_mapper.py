from typing import Optional, Dict

from bitcoinetl.domain.block import BtcTransaction
from bitcoinetl.domain.trace import BtcTrace
from bitcoinetl.domain.transaction_input import BtcTransactionInput
from bitcoinetl.domain.transaction_output import BtcTransactionOutput


# http://chainquery.com/bitcoin-api/getblock
# http://chainquery.com/bitcoin-api/getrawtransaction
class BtcTraceMapper(object):
    def __init__(self):
        pass

    def vin_to_trace(
        self,
        vin: BtcTransactionInput,
        transaction: Optional[BtcTransaction] = None,
    ):
        trace = BtcTrace()
        trace.is_in = True
        trace.vin_seq = vin.sequence
        trace.vin_idx = vin.index
        trace.vin_type = vin.type
        trace.req_sigs = vin.req_sigs
        trace.script_hex = vin.script_hex
        trace.script_asm = vin.script_asm
        trace.value = vin.value
        trace.pxhash = vin.spent_transaction_hash

        # if trace is input, then trace.vout_xxx is the pxhash's attributes
        trace.vout_idx = vin.spent_output_index
        trace.vout_cnt = vin.spent_output_count
        trace.vout_type = None

        if vin.addresses is not None:
            trace.address = ";".join(vin.addresses)
        if vin.txinwitness is not None:
            trace.txinwitness = ";".join(vin.txinwitness)

        if transaction is not None:
            trace.index = transaction.index
            trace.txhash = transaction.hash
            trace.vin_cnt = len(transaction.inputs)
            trace.is_coinbase = transaction.is_coinbase
            trace.block_number = transaction.block_number
            trace.block_hash = transaction.hash
            trace.block_timestamp = transaction.block_timestamp
            trace.tx_in_value = transaction.calculate_input_value()
            trace.tx_out_value = transaction.calculate_output_value()

        return trace

    def vout_to_trace(
        self,
        vout: BtcTransactionOutput,
        transaction: Optional[BtcTransaction] = None,
    ):
        trace = BtcTrace()
        trace.is_in = False
        trace.vin_seq = None
        trace.vin_idx = None
        trace.vin_type = None
        trace.req_sigs = vout.req_sigs
        trace.script_hex = vout.script_hex
        trace.script_asm = vout.script_asm
        trace.value = vout.value
        trace.pxhash = None
        trace.vout_idx = vout.index
        trace.vout_type = vout.type

        if vout.addresses is not None:
            trace.address = ";".join(vout.addresses)
        if vout.txinwitness is not None:
            trace.txinwitness = ";".join(vout.txinwitness)

        if transaction is not None:
            trace.index = transaction.index
            trace.txhash = transaction.hash
            trace.vin_cnt = len(transaction.inputs)
            trace.vout_cnt = len(transaction.outputs)
            trace.is_coinbase = transaction.is_coinbase
            trace.block_number = transaction.block_number
            trace.block_hash = transaction.hash
            trace.block_timestamp = transaction.block_timestamp
            trace.tx_in_value = transaction.calculate_input_value()
            trace.tx_out_value = transaction.calculate_output_value()

        return trace

    def trace_to_dict(self, trace: BtcTrace) -> Dict:
        result = {
            "type": "trace",
            "txhash": trace.txhash,
            "block_number": trace.block_number,
            "block_hash": trace.block_hash,
            "block_timestamp": trace.block_timestamp,
            "index": trace.index,
            "is_coinbase": trace.is_coinbase,
            "is_in": trace.is_in,
            "pxhash": trace.pxhash,
            "tx_in_value": trace.tx_in_value,
            "tx_out_value": trace.tx_out_value,
            "vin_seq": trace.vin_seq,
            "vin_idx": trace.vin_idx,
            "vin_cnt": trace.vin_cnt,
            "vin_type": trace.vin_type,
            "vout_idx": trace.vout_idx,
            "vout_cnt": trace.vout_cnt,
            "vout_type": trace.vout_type,
            "address": trace.address,
            "value": trace.value,
            "script_hex": trace.script_hex,
            "script_asm": trace.script_asm,
            "req_sigs": trace.req_sigs,
            "txinwitness": trace.txinwitness,
        }
        return result

    def dict_to_trace(self, json_dict: Dict) -> BtcTrace:
        trace = BtcTrace()
        trace.is_in = json_dict.get("is_in", False)
        trace.txhash = json_dict.get("txhash")
        trace.pxhash = json_dict.get("pxhash")
        # TODO....

        return trace
