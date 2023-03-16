from typing import Optional


class EthTxpool(object):
    def __init__(self):
        self.hash: Optional[str] = None
        self.nonce: Optional[int] = None
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
        self.pool_type: Optional[str] = None
