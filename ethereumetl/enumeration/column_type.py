from typing import List, Dict, Union

import pandas as pd

from blockchainetl.enumeration.entity_type import EntityType

BLOCK = [
    "_st",
    "_st_day",
    "blknum",
    "blkhash",
    "parent_hash",
    "nonce",
    "sha3_uncles",
    "logs_bloom",
    "txs_root",
    "state_root",
    "receipts_root",
    "miner",
    "difficulty",
    "total_difficulty",
    "blk_size",
    "extra_data",
    "gas_limit",
    "gas_used",
    "tx_count",
    "base_fee_per_gas",
    "uncle_count",
    "uncle0_hash",
    "uncle1_hash",
]

TRANSACTION = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "nonce",
    "from_address",
    "to_address",
    "value",
    "gas",
    "gas_price",
    "input",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "tx_type",
    "receipt_cumulative_gas_used",
    "receipt_gas_used",
    "receipt_contract_address",
    "receipt_root",
    "receipt_status",
    "receipt_effective_gas_price",
    "receipt_log_count",
]

TRACE = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "from_address",
    "to_address",
    "value",
    "input",
    "output",
    "trace_type",
    "call_type",
    "reward_type",
    "gas",
    "gas_used",
    "subtraces",
    "trace_address",
    "error",
    "status",
]

LOG = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "logpos",
    "address",
    "data",
    "topics",
    "n_topics",
    "topics_0",
]

CONTRACT = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "trace_type",
    "trace_address",
    "address",
    "creater",
    "initcode",
    "bytecode",
    "func_sighashes",
    "is_erc20",
    "is_erc721",
]

TOKEN = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "trace_address",
    "address",
    "symbol",
    "name",
    "decimals",
    "total_supply",
    "is_erc20",
    "is_erc721",
]

TOKEN_TRANSFER = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "logpos",
    "token_address",
    "name",
    "from_address",
    "to_address",
    "value",
    "symbol",
    "decimals",
]

ERC721_TRANSFER = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "logpos",
    "token_address",
    "token_name",
    "from_address",
    "to_address",
    "id",
]

ERC1155_TRANSFER = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "logpos",
    "token_address",
    "token_name",
    "operator",
    "from_address",
    "to_address",
    "id",
    "value",
    "id_pos",
    "id_cnt",
    "xfer_type",
]

TOKEN_BALANCE = [
    "_st",
    "_st_day",
    "blknum",
    "address",
    "token_address",
    "balance",
    "total_supply",
    "ranking",
    "page",
    "page_holder",
    "n_page",
    "n_address",
]

TOKEN_HOLDER = [
    "id",
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "address",
    "token_address",
]


class ColumnType:

    column_types = {
        # Convert the float64 into int64, keep the others as default infered types
        # For traces, we have block/uncle reward as traces,
        # those records doesn't have `txpos, gas or gas_used` fields.
        EntityType.TRACE: {e: "Int64" for e in ("txpos", "gas_used")},
        EntityType.TOKEN: {e: "Int64" for e in ("txpos", "decimals")},
        EntityType.TRANSACTION: {
            e: "Int64"
            for e in (
                "txpos",
                "nonce",
                "tx_type",
                "max_fee_per_gas",
                "max_priority_fee_per_gas",
                "receipt_cumulative_gas_used",
                "receipt_gas_used",
                "receipt_status",
                "receipt_effective_gas_price",
                "receipt_log_count",
            )
        },
        # in some cases, the type of balance is uint64, but sqlalchemy can't handle uint64
        # ref https://github.com/pandas-dev/pandas/blob/v1.4.1/pandas/io/sql.py#L1209
        EntityType.TOKEN_BALANCE: {
            e: "Int64"
            for e in (
                "page",
                "n_page",
                "n_address",
            )
        },
    }

    def __init__(self):
        self._meta = {
            EntityType.BLOCK: BLOCK,
            EntityType.TRANSACTION: TRANSACTION,
            EntityType.TRACE: TRACE,
            EntityType.LOG: LOG,
            EntityType.CONTRACT: CONTRACT,
            EntityType.TOKEN: TOKEN,
            EntityType.TOKEN_TRANSFER: TOKEN_TRANSFER,
            EntityType.ERC721_TRANSFER: ERC721_TRANSFER,
            EntityType.ERC1155_TRANSFER: ERC1155_TRANSFER,
            EntityType.TOKEN_BALANCE: TOKEN_BALANCE,
            EntityType.TOKEN_HOLDER: TOKEN_HOLDER,
        }

    def __getitem__(self, key: str) -> List[str]:
        return self._meta[key]

    def astype(
        self, entity_type: str = EntityType.TRACE
    ) -> Dict[str, Union[str, type]]:
        return self.__class__.column_types.get(entity_type, {})

    @staticmethod
    def apply_ethereum_df(df: pd.DataFrame, key: EntityType) -> pd.DataFrame:
        if key == EntityType.BLOCK:
            df.rename(
                columns={
                    "hash": "blkhash",
                    "number": "blknum",
                    "size": "blk_size",
                    "transaction_count": "tx_count",
                    "transactions_root": "txs_root",
                },
                inplace=True,
            )

        elif key == EntityType.TRANSACTION:
            df.rename(
                columns={
                    "hash": "txhash",
                    "transaction_type": "tx_type",
                },
                inplace=True,
            )

        elif key in (EntityType.CONTRACT, EntityType.TOKEN):
            df.rename(
                columns={
                    "function_sighashes": "func_sighashes",
                },
                inplace=True,
            )

        elif key == EntityType.LOG:
            df["n_topics"] = df.topics.apply(lambda xs: len(xs))
            # topics maybe nil
            df["topics_0"] = df.topics.apply(lambda xs: xs[0] if len(xs) > 0 else "")
            df["topics"] = df.topics.apply(lambda xs: ",".join(xs))

        return df
