import os
import re
from typing import Optional, Union, List, Dict
from requests import Session
from datetime import datetime, timedelta, date, timezone
from cachetools import cached, TTLCache
from threading import Lock

PRICE_SERVICE_API_KEY_ENV = "BLOCKCHAIN_ETL_PRICE_SERVICE_API_KEY"


class PriceService:
    def __init__(self, endpoint: str, api_key: Optional[str] = None):
        self.endpoint = endpoint
        self.session = Session()
        self.api_key = api_key or os.getenv(PRICE_SERVICE_API_KEY_ENV)

    # cache for 1min
    @cached(cache=TTLCache(maxsize=10000, ttl=60), lock=Lock())
    def get_price(
        self,
        chain: str,
        token_address: Optional[str] = None,
        time: Optional[Union[str, int, datetime]] = None,
    ) -> Optional[float]:
        chain = chain.lower()
        token_address = token_address or "0x0000000000000000000000000000000000000000"
        url = self.endpoint + f"/api/v2/tokens/{token_address}/prices"

        end_time = int(datetime.timestamp(self._parse_time(time)))
        price = self._get(url, chain, end_time)
        assert isinstance(price, dict)
        return price.get("price")

    # cache for 1min
    @cached(cache=TTLCache(maxsize=10000, ttl=60), lock=Lock())
    def get_historical_prices(
        self,
        chain: str,
        token_address: Optional[str] = None,
        start_day: Optional[Union[str, int, datetime]] = None,
        end_day: Optional[Union[str, int, datetime]] = None,
        interval: Optional[str] = "1d",
        order: Optional[str] = "asc",
    ) -> List[Dict]:
        token_address = token_address or "0x0000000000000000000000000000000000000000"
        url = self.endpoint + f"/api/v2/tokens/{token_address}/historical_prices"

        end_time = self._parse_time(end_day)
        if start_day is None:
            start_time = end_time - timedelta(days=7)
        else:
            start_time = self._parse_time(start_day)

        resp = self._get(
            url,
            chain,
            end_time=int(datetime.timestamp(end_time)),
            start_time=int(datetime.timestamp(start_time)),
            order=order,
            interval=interval,
        )
        assert isinstance(resp, list)
        return resp

    def _parse_time(self, day: Optional[Union[str, int, datetime]] = None) -> datetime:
        now = datetime.now()
        if day is None:
            return now
        elif isinstance(day, int):
            return datetime.utcfromtimestamp(day).replace(tzinfo=timezone.utc)
        elif isinstance(day, str):
            regex_layout_dict = {
                "%Y-%m-%d": "^2[0-9]{3}-[0-9]{2}-[0-9]{2}$",
                "%Y-%m-%d %H:%M:%S": "^2[0-9]{3}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$",
            }
            for layout, pattern in regex_layout_dict.items():
                if re.match(pattern=pattern, string=day):
                    return datetime.strptime(day, layout).replace(tzinfo=timezone.utc)
            raise ValueError(f"Invalid day: {day} {type(day)}")

        elif isinstance(day, (date, datetime)):
            return day
        else:
            raise ValueError(f"Invalid day: {day} {type(day)}")

    def _get(
        self, url: str, chain: str, end_time: Union[datetime, int, str], **kwargs
    ) -> Union[List[Dict], Dict]:
        params = {"chain": chain.lower(), "end_time": end_time}
        if self.api_key:
            params.update({"x-api-key": self.api_key})
        params.update(**kwargs)
        resp = self.session.get(url=url, params=params)
        return resp.json()
