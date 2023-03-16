import pandas as pd
from datetime import datetime


class ColumnType:
    @staticmethod
    def apply_global_df(df: pd.DataFrame) -> pd.DataFrame:
        # drop useless columns
        df.drop(
            columns=[
                "type",
                "item_id",
                "item_timestamp",
                "block_hash",
                "trace_id",
            ],
            inplace=True,
            errors="ignore",
        )

        # global rename
        df.rename(
            columns={
                "timestamp": "_st",
                "block_timestamp": "_st",
                "block_number": "blknum",
                "transaction_hash": "txhash",
                "transaction_index": "txpos",
                "index": "txpos",  # bitcoin
                "log_index": "logpos",
            },
            inplace=True,
        )

        # set _st_day
        # don't set the null timestamp
        df["_st_day"] = df._st.apply(
            lambda x: datetime.utcfromtimestamp(x).strftime("%Y-%m-%d")
            if x is not None
            else None
        )

        return df
