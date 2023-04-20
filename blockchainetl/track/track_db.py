import logging
import pandas as pd
from typing import Dict, List
from sqlalchemy import create_engine
from blockchainetl.jobs.exporters import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from ethereumetl.streaming.postgres_tables import TRACKS


class TrackDB:
    def __init__(self, db_url: str, track_schema: str):
        self._track_schema = track_schema
        logging.info(f"Open track db on {db_url} with schema: {track_schema}")

        self._engine = create_engine(db_url)
        self._exporter = PostgresItemExporter(
            db_url,
            track_schema,
            item_type_to_insert_stmt_mapping={
                "track": create_insert_statement_for_table(
                    TRACKS,
                    on_conflict_do_update=False,
                ),
            },
            print_sql=False,
            pool_size=10,
            pool_overflow=5,
        )
        self._exporter.open()

    def all_items_df(self) -> pd.DataFrame:
        table = "{}.{}".format(self._track_schema, "tracks")
        return pd.read_sql(
            f"""
SELECT
    address,
    original,
    label,
    track_id,
    min(hop) AS hop
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY address, label, track_id ORDER BY hop, created_at) AS _rank
    FROM
        {table}
    WHERE
        stop IS FALSE
    ) _
WHERE
    _rank = 1
GROUP BY
    1, 2, 3, 4
""",
            con=self._engine,
        )

    def upsert(self, df: pd.DataFrame):
        # support upsert from bootstrap, or tracking
        if "source" not in df.columns:
            df["source"] = None

        if "stop" not in df.columns:
            df["stop"] = False

        # hard coded into track
        df["type"] = "track"
        self._exporter.export_items(df.to_dict("records"))

    def bootstrap(self, dataset: List[Dict]):
        logging.info(f"Bootstrap tracking #{len(dataset)} address")
        df = pd.DataFrame(dataset)
        df["original"] = df["address"]
        df["txhash"] = "0x" + "0" * 60
        df["logpos"] = 0
        df["trace_address"] = ""

        df.rename(
            columns={"description": "source", "start_block": "blknum"}, inplace=True
        )
        self.upsert(df)
