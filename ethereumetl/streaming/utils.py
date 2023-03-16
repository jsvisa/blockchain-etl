import logging
from typing import Set, List, Union, Optional, Dict, Any
from collections.abc import Callable

import pandas as pd
from cachetools import cached, TTLCache
from web3.types import FilterParams
from sqlalchemy import create_engine
from eth_typing.evm import ChecksumAddress


def convert_token_transfers_to_df(items, ignore_error=False) -> pd.DataFrame:
    # force all data types in objec format,
    # else in some cases,
    # 1. pandas will convert the value fields to int64(in Scientific notation),
    # 2. after those fields submitted into database(via SqlAlchemy),
    # 3. the original data lost some precision
    df = pd.DataFrame(items, dtype=object)
    df.drop(
        columns=["block_hash", "type", "name"],
        inplace=True,
        errors="ignore" if ignore_error else "raise",
    )

    df.rename(
        columns={
            "block_number": "blknum",
            "block_timestamp": "_st",
            "transaction_hash": "txhash",
            "transaction_index": "txpos",
            "log_index": "logpos",
        },
        inplace=True,
    )
    return df


def convert_transactions_to_df(items) -> pd.DataFrame:
    df = pd.DataFrame(items, dtype=object)
    df.drop(columns=["block_hash", "type"], inplace=True)

    df.rename(
        columns={
            "block_number": "blknum",
            "block_timestamp": "_st",
            "hash": "txhash",
            "transaction_index": "txpos",
            "log_index": "logpos",
        },
        inplace=True,
    )
    return df


def build_log_filter(
    start_block: int,
    end_block: int,
    topics: Optional[List[str]] = None,
    address: Optional[
        Union[
            Union[str, ChecksumAddress],
            List[Union[str, ChecksumAddress]],
        ]
    ] = None,
) -> FilterParams:
    # Ref: https://github.com/ethereum/go-ethereum/blob/v1.10.16/interfaces.go#L167-L174
    params: Dict[str, Any] = {"fromBlock": hex(start_block), "toBlock": hex(end_block)}
    if topics is not None:
        assert isinstance(topics, list) and isinstance(
            topics[0], str
        ), f"currenct topics must be a list of Event name, not: {topics}"
        params["topics"] = [topics]
    if address is not None:
        params["address"] = address
    return FilterParams(**params)


def build_erc20_token_reader(
    chain: str, db_url: Optional[str]
) -> Optional[Callable[[], Set[str]]]:
    if db_url is None:
        return None
    engine = create_engine(db_url)

    # cache for 1hour
    @cached(cache=TTLCache(maxsize=10, ttl=3600))
    def cache_read():
        try:
            df = pd.read_sql(
                f"""
SELECT DISTINCT
    address
FROM
    {chain}.tokens
WHERE
    is_erc20 is true
    AND (is_erc721 is false OR is_erc721 is null)
""",
                con=engine,
            )
            return set(df["address"])
        except Exception as e:
            logging.error(f"failed to read ERC20 tokens from {chain}.tokens: {e}")
            return set()

    return cache_read


def fmt_enrich_balance_queue(chain: str, typo: str):
    return f"{chain}:{typo}-enrich-balance-queue"
