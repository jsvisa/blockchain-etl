from typing import Optional


class EthErc1155Transfer(object):
    def __init__(self):
        self._st: Optional[int] = None
        self._st_day: Optional[str] = None
        self.token_address: Optional[str] = None
        self.token_name: Optional[str] = None
        self.operator: Optional[str] = None
        self.from_address: Optional[str] = None
        self.to_address: Optional[str] = None
        self.id: Optional[int] = None
        self.value: Optional[int] = None
        self.id_pos: Optional[int] = None
        self.id_cnt: Optional[int] = None
        self.xfer_type: Optional[str] = None
        self.transaction_hash: Optional[str] = None
        self.transaction_index: Optional[int] = None
        self.log_index: Optional[int] = None
        self.block_number: Optional[int] = None
