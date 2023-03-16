from typing import List, Dict, Union
import pandas as pd

from blockchainetl.enumeration.entity_type import EntityType

BLOCK = [
    "_st",
    "_st_day",
    "blknum",
    "blkhash",
    "tx_count",
    "blk_size",
    "stripped_size",
    "weight",
    "version",
    "nonce",
    "bits",
    "difficulty",
    "coinbase_param",
]

TRANSACTION = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "iscoinbase",
    "tx_in_cnt",
    "tx_in_value",
    "tx_out_cnt",
    "tx_out_value",
    "tx_size",
    "tx_vsize",
    "tx_weight",
    "tx_version",
    "tx_locktime",
    "tx_hex",
]

TRACE = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "iscoinbase",
    "isin",
    "pxhash",
    "tx_in_value",
    "tx_out_value",
    "vin_seq",
    "vin_idx",
    "vin_cnt",
    "vin_type",
    "vout_idx",
    "vout_cnt",
    "vout_type",
    "address",
    "value",
    "script_hex",
    "script_asm",
    "req_sigs",
    "txinwitness",
]

TIDB_TRACE = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "iscoinbase",
    "isin",
    "pxhash",
    "tx_in_value",
    "tx_out_value",
    "vin_seq",
    "vin_idx",
    "vin_cnt",
    "vin_type",
    "vout_idx",
    "vout_cnt",
    "vout_type",
    "address",
    "value",
]


class ColumnType:

    column_types = {
        EntityType.TRACE: {
            e: "Int64"
            for e in (
                "txpos",
                "vin_seq",
                "vin_idx",
                "vin_cnt",
                "vout_idx",
                "vout_cnt",
                "req_sigs",
                "value",
                "tx_in_value",
                "tx_out_value",
            )
        }
    }

    def __init__(self):
        self._meta = {
            EntityType.BLOCK: BLOCK,
            EntityType.TRANSACTION: TRANSACTION,
            EntityType.TRACE: TRACE,
        }

    def __getitem__(self, key: str) -> List[str]:
        return self._meta[key]

    def astype(
        self, entity_type: str = EntityType.TRACE
    ) -> Dict[str, Union[str, type]]:
        return self.__class__.column_types.get(entity_type, {})

    @staticmethod
    def apply_bitcoin_df(df: pd.DataFrame, key: EntityType) -> pd.DataFrame:
        if key == EntityType.BLOCK:
            df.rename(
                columns={
                    "hash": "blkhash",
                    "number": "blknum",
                    "size": "blk_size",
                    "transaction_count": "tx_count",
                },
                inplace=True,
            )

        elif key == EntityType.TRANSACTION:
            df.rename(
                columns={
                    "is_coinbase": "iscoinbase",
                    "block_number": "blknum",
                    "hash": "txhash",
                    "input_count": "tx_in_cnt",
                    "input_value": "tx_in_value",
                    "output_count": "tx_out_cnt",
                    "output_value": "tx_out_value",
                    "size": "tx_size",
                    "vsize": "tx_vsize",
                    "weight": "tx_weight",
                    "version": "tx_version",
                    "locktime": "tx_locktime",
                    "hex": "tx_hex",
                },
                inplace=True,
            )

        elif key == EntityType.TRACE:
            df.rename(
                columns={
                    "is_coinbase": "iscoinbase",
                    "is_in": "isin",
                    "block_number": "blknum",
                    "hash": "txhash",
                },
                inplace=True,
            )

        else:
            raise ValueError("bitcoin only support block or transaction entity-type")

        return df
