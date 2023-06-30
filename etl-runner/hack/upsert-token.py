#!/usr/bin/env python3

from time import time
import logging
import click
from datetime import datetime

from typing import Dict
from cachetools import cached, TTLCache
from sqlalchemy import create_engine
from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql.dml import Insert

from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.utils import time_elapsed
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import (
    create_insert_statement_for_table,
    excluded_or_exists,
)
from ethereumetl.streaming.tsdb_tables import TOKENS
from hack.get_time_range_of_blocks import get_time_range_of_blocks

logging.basicConfig(
    format="[%(asctime)s] - %(levelname)s - %(message)s", level=logging.INFO
)


def upsert_token(table: Table, stmt: Insert) -> Dict:
    return {
        "name": excluded_or_exists(table, stmt, "name"),
        "symbol": excluded_or_exists(table, stmt, "symbol"),
        "decimals": excluded_or_exists(table, stmt, "decimals"),
        "updated_at": func.current_timestamp(),
    }


def where_token(table: Table, stmt: Insert) -> Dict:
    return (
        table.columns["name"] != stmt.excluded["name"]
        or table.columns["symbol"] != stmt.excluded["symbol"]
        or table.columns["decimals"] != stmt.excluded["decimals"]
    )


class UpsertTokenAdapter:
    def __init__(self, chain, tsdb_url: str, item_exporter):
        self.chain = chain
        self.tsdb_url = tsdb_url
        self.item_exporter = item_exporter
        self.previous_block_timestamp = datetime(2016, 1, 1)

    def open(self):
        self.tsdb = create_engine(self.tsdb_url)
        self.item_exporter.open()

    # cache for 60s
    @cached(cache=TTLCache(maxsize=16, ttl=60))
    def get_current_block_number(self) -> int:
        sql = (
            f"SELECT blknum, extract(epoch from block_timestamp)::int AS timestamp "
            f"FROM {self.chain}.token_xfers "
            "WHERE block_timestamp >= current_date-90 "
            "ORDER BY blknum DESC LIMIT 1"
        )
        logging.info(sql)
        row = self.tsdb.execute(sql).fetchone()
        return (row["blknum"], row["timestamp"])  # type: ignore

    def export_all(self, start_block, end_block):
        min_timestamp, max_timestamp = get_time_range_of_blocks(
            self.chain,
            self.tsdb,
            start_block - 5,
            end_block + 5,
            self.previous_block_timestamp,
        )

        if min_timestamp is None or max_timestamp is None:
            self.previous_block_timestamp = (
                max_timestamp or min_timestamp or self.previous_block_timestamp
            )
            return

        sql = f"""
SELECT DISTINCT
   token_address AS address,
   name,
   symbol,
   decimals
FROM
    {self.chain}.token_xfers
WHERE
    block_timestamp >= '{min_timestamp}'
    AND block_timestamp <= '{max_timestamp}'
    AND blknum >= {start_block-5}
    AND blknum <= {end_block+5}
"""
        st0 = time()
        rows = self.tsdb.execute(sql).fetchall()
        rows = [e._asdict() for e in rows]
        for row in rows:
            row["type"] = EntityType.TOKEN
        st1 = time()

        affected = self.item_exporter.export_items(rows)
        st2 = time()

        logging.info(
            f"UPSERT {self.chain} [{start_block, end_block}] {max_timestamp} "
            f"#items={len(rows)} #affected={affected} "
            f"@all={time_elapsed(st0)}s @extract={time_elapsed(st0, st1)}s "
            f"@export={time_elapsed(st1, st2)}s"
        )
        self.previous_block_timestamp = max_timestamp

    def close(self):
        self.tsdb.dispose()
        self.item_exporter.close()


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-c",
    "--chain",
    required=True,
    show_default=True,
    type=click.Choice([*Chain.ALL_ETHEREUM_FORKS, "tron"]),
    help="The chain network to connect to.",
)
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/update-token-xfer.txt",
    required=True,
    show_default=True,
    help="The file used to store the last synchronized block file",
)
@click.option(
    "--lag",
    default=10,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network.",
)
@click.option(
    "--db-data-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_DSDB_URL",
    show_default=True,
    help="The data TSDB connection url(used to read source items)",
)
@click.option(
    "--db-meta-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_TSDB_URL",
    show_default=True,
    help="The meta TSDB connection url(used to write items)",
)
@click.option(
    "-s",
    "--start-block",
    default=1,
    show_default=True,
    type=int,
    help="Start block, included",
)
@click.option(
    "-e",
    "--end-block",
    default=None,
    show_default=True,
    type=int,
    help="End block, included",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=1000,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
def update(
    chain,
    last_synced_block_file,
    lag,
    db_data_url,
    db_meta_url,
    start_block,
    end_block,
    period_seconds,
    block_batch_size,
    print_sql,
):
    """Upsert {chain}.tokens with {chain}.token_xfers"""

    item_exporter = PostgresItemExporter(
        db_meta_url,
        chain,
        item_type_to_insert_stmt_mapping={
            EntityType.TOKEN: create_insert_statement_for_table(
                TOKENS,
                on_conflict_do_update=True,
                upsert_callback=upsert_token,
                where_callback=where_token,
                schema=chain,
            ),
        },
        print_sql=print_sql,
        workers=4,
        pool_size=10,
        pool_overflow=5,
    )

    streamer_adapter = UpsertTokenAdapter(chain, db_data_url, item_exporter)

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
    )
    streamer.stream()


update()
