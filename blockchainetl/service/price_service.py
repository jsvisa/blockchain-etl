from typing import Optional, Union
from requests import Session
from datetime import datetime
from cachetools import cached, TTLCache
from threading import Lock


class PriceService:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.session = Session()

    # cache for 1min
    @cached(cache=TTLCache(maxsize=10000, ttl=60), lock=Lock())
    def get_price(
        self,
        chain: str,
        token_address: Optional[str] = None,
        time: Optional[Union[str, int, datetime]] = None,
    ) -> Optional[float]:
        raise NotImplementedError
