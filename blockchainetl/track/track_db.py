import logging
import pandas as pd
from typing import Optional, Dict, List
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class TrackDB:
    def __init__(self, db_url: str, track_schema: str, track_table: str = "tracks"):
        self._track_schema = track_schema
        self._track_table = track_table
        logging.info(f"Open track db on {db_url}")
        self._engine = create_engine(db_url)
        self._session = sessionmaker(self._engine)

    def all_items_df(self) -> pd.DataFrame:
        return pd.read_sql(
            f"""
SELECT
    address, original, label, track_id, min(hop) AS hop
FROM
    {self._track_schema}.{self._track_table}
WHERE
    stop is false
GROUP BY
    1,2,3,4
""",
            con=self._engine,
        )

    def upsert(self, df: pd.DataFrame):
        # support upsert from bootstrap, or tracking
        if "source" not in df.columns:
            df["source"] = None

        if "stop" not in df.columns:
            df["stop"] = False

        conn = self._session()
        for _, row in df.iterrows():
            conn.execute(
                self._to_insert_address(),
                row.to_dict(),
            )

        conn.commit()

    def bootstrap(self, dataset: List[Dict]):
        logging.info(f"Bootstrap tracking #{len(dataset)} address")
        df = pd.DataFrame(dataset)
        df["original"] = df["address"]

        # fillin none columns
        for col in (
            "from_address",
            "_st",
            "in_value",
            "out_value",
            "token_address",
            "token_name",
        ):
            if col in df.columns:
                continue
            df[col] = None
        df["hop"] = 0
        df["txhash"] = "0x" + "0" * 60
        df.rename(
            columns={"description": "source", "start_block": "blknum"}, inplace=True
        )
        self.upsert(df)

    @lru_cache(maxsize=102400)
    def get_label_by_address(self, address) -> Optional[str]:
        table = "{}.{}".format(self._track_schema, self._track_table)
        sql = f"SELECT label FROM {table} WHERE address = '{address}'"
        result = self._engine.execute(sql)
        if result is None:
            return None
        row = result.fetchone()
        if row is None:
            return None

        return row["label"]

    def _to_insert_address(self):
        return f"""INSERT INTO {self._track_schema}.{self._track_table}
(
    address, from_address, blknum, _st, txhash, original,
    label, hop, in_value, out_value, track_id, source,
    token_address, token_name, stop
) VALUES (
    :address, :from_address, :blknum, :_st, :txhash, :original,
    :label, :hop, :in_value, :out_value, :track_id, :source,
    :token_address, :token_name, :stop
) ON CONFLICT(address, txhash, original) DO NOTHING
"""
