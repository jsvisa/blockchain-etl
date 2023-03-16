from typing import Dict, Optional, List
import nop
from blockchainetl.enumeration.chain import Chain


class NopPlatform:
    def __init__(self, chain: Chain):
        self._meta = {e.platform(): e for e in nop.platforms if e.chain() == chain}

    def get(self, key: str) -> Optional[nop.NopExtractor]:
        return self._meta.get(key)

    def __getitem__(self, key: str) -> nop.NopExtractor:
        return self._meta[key]

    def all(self) -> List[str]:
        return list(self._meta.keys())


def parse_nop_platforms(
    in_platforms: Optional[str], chain: Chain
) -> Dict[str, nop.NopExtractor]:
    nop_platform = NopPlatform(chain)
    if in_platforms is None:
        platforms = nop_platform.all()
    else:
        platforms = in_platforms.lower().split(",")

    unknown = [e for e in platforms if nop_platform.get(e) is None]
    if len(unknown) > 0:
        raise ValueError(
            f"not supported NopPlatform: {unknown}, available: {nop_platform.all()}"
        )

    return {e: nop_platform[e] for e in platforms}
