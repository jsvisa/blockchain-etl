import os
from typing import Dict


class TrackExplorer:
    def __init__(self, explorers: Dict[str, str]):
        self._explorers = explorers

    def tx_url(self, chain: str, tx: str) -> str:
        if chain in self._explorers:
            return os.path.join(self._explorers[chain], tx)
        return ""
