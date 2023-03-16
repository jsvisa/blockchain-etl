from typing import Optional


class EthBalance(object):
    def __init__(self):
        self.address: Optional[str] = None
        self.balance: Optional[str] = None
        self.nonce: Optional[int] = None
        self.root: Optional[str] = None
        self.code_hash: Optional[str] = None
        self.key: Optional[str] = None
