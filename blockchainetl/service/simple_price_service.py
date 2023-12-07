import requests
from typing import Optional
from cachetools import cached, TTLCache
from threading import Lock


class SimplePriceService:
    def __init__(self):
        self.session = requests.Session()

    def get_price(
        self, chain: str, token_address: Optional[str] = None, **kwargs
    ) -> Optional[float]:
        chain = chain.lower()

        if (
            token_address is None
            or token_address == "0x0000000000000000000000000000000000000000"
        ):
            return self.get_native(chain)
        return self.get_token(chain, token_address)

    # cache for 5min
    @cached(cache=TTLCache(maxsize=10000, ttl=300), lock=Lock())
    def get_native(self, chain):
        if chain in ("optimistic", "arbitrum"):
            chain = "ethereum"
        elif chain == "bsc":
            chain = "binancecoin"
        elif chain == "bor":
            chain = "matic-network"
        elif chain == "avalanche":
            chain = "avalanche-2"

        url = f"https://api.coingecko.com/api/v3/simple/price?ids={chain}&vs_currencies=usd"
        r = self.session.get(url)
        rs = r.json()
        if chain in rs:
            return rs[chain]["usd"]
        return None

    # cache for 5min
    @cached(cache=TTLCache(maxsize=10000, ttl=300), lock=Lock())
    def get_token(self, chain, token):
        if chain == "optimistic":
            chain = "optimistic-ethereum"
        elif chain == "bsc":
            chain = "binance-smart-chain"
        elif chain == "arbitrum":
            chain = "arbitrum-one"
        elif chain == "avalanche":
            chain = "avalanche-2"
        elif chain == "bor":
            chain = "polygon-pos"

        url = f"https://api.coingecko.com/api/v3/simple/token_price/{chain}"
        r = self.session.get(
            url, params={"contract_address": token, "vs_currencies": "usd"}
        )
        rs = r.json()
        if token in rs:
            return rs[token]["usd"]
        return None
