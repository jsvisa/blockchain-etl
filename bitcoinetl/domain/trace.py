from typing import Optional


class BtcTrace(object):
    def __init__(self):
        self.index: Optional[int] = None
        self.txhash: Optional[str] = None
        self.pxhash: Optional[str] = None
        self.block_number: Optional[int] = None
        self.block_hash: Optional[str] = None
        self.block_timestamp: Optional[int] = None
        self.tx_in_value: Optional[int] = None
        self.tx_out_value: Optional[int] = None
        self.is_coinbase: bool = False
        self.is_in: bool = False
        self.vin_seq: Optional[int] = None
        self.vin_idx: Optional[int] = None
        self.vin_cnt: Optional[int] = None
        self.vin_type: Optional[str] = None
        self.vout_idx: Optional[int] = None
        self.vout_cnt: Optional[int] = None
        self.vout_type: Optional[str] = None
        self.address: Optional[str] = None
        self.value: Optional[int] = None
        self.script_hex: Optional[str] = None
        self.script_asm: Optional[str] = None
        self.req_sigs: Optional[int] = None
        self.txinwitness: Optional[str] = None
