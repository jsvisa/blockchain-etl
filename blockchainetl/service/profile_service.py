from typing import Dict, List, Union
from decimal import Decimal
from threading import Lock
from sqlalchemy import create_engine
from cachetools import cached, TTLCache


class ProfileService:
    def __init__(self, chain: str, db_url: str):
        self.chain = chain
        self.engine = create_engine(db_url, pool_size=10)

    # cache for 1m
    @cached(cache=TTLCache(maxsize=2000, ttl=60), lock=Lock())
    def get_profile(self, address: str) -> List[Dict[str, Union[str, int]]]:
        address = address.lower()
        sql = f"""
SELECT
    'erc20' AS typo,
    count(*) AS count,
    sum(vin_txs) AS vin_txs,
    sum(out_txs) AS out_txs,
    sum(vin_xfers) AS vin_xfers,
    sum(out_xfers) AS out_xfers,
    sum(vin_value) AS vin_value,
    sum(out_value) AS out_value
FROM
    {self.chain}.token_latest_balances
WHERE
    address = %s
UNION
SELECT
    'ether' AS typo,
    count(*) AS count,
    sum(vin_txs) AS vin_txs,
    sum(out_txs) AS out_txs,
    sum(vin_xfers) AS vin_xfers,
    sum(out_xfers) AS out_xfers,
    sum(vin_value) AS vin_value,
    sum(out_value) AS out_value
FROM
    {self.chain}.latest_balances
WHERE
    address = %s
"""
        result = self.engine.execute(sql, (address, address))
        rows = result.fetchall()
        rows = [e._asdict() for e in rows]
        for row in rows:
            for k, v in row.items():
                if isinstance(v, Decimal):
                    row[k] = int(v)
                elif v is None:
                    row[k] = 0
        return rows
