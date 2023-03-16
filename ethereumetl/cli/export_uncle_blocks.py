import os
import logging
from datetime import datetime, timedelta

import click
import pandas as pd
from sqlalchemy import create_engine

from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_uncle_blocks_job import ExportUncleBlocksJob
from ethereumetl.providers.auto import get_provider_from_uri


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum connection url",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
)
@click.option(
    "-s",
    "--start-date",
    default=(datetime.utcnow() - timedelta(days=1)).date(),
    show_default=True,
    help="Start datetime(included)",
)
@click.option(
    "-e",
    "--end-date",
    default=datetime.utcnow().date(),
    show_default=True,
    help="End datetime(excluded)",
)
@click.option(
    "-o",
    "--output",
    show_default=True,
    type=click.Path(dir_okay=True),
    required=True,
    help="The output file. If not specified stdout is used.",
)
@click.option(
    "-b",
    "--batch-size",
    default=50,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
)
@click.option(
    "-w",
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="The maximum number of workers.",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the web3 provider",
)
@click.option(
    "--load-into-db",
    is_flag=True,
    default=False,
    show_default=True,
    help="Load the result csv into database or not.",
)
def export_uncle_blocks(
    gp_url,
    gp_schema,
    start_date,
    end_date,
    output,
    batch_size,
    max_workers,
    provider_uri,
    load_into_db,
):
    """Export uncle blocks"""

    engine = create_engine(gp_url)
    os.makedirs(output, exist_ok=True)

    def read_and_load_daily(day, output):
        df = pd.read_sql(
            f"""
    SELECT
        A.blknum,
        A.blkhash,
        A.uncle_count
    FROM
        {gp_schema}.blocks A
    LEFT JOIN {gp_schema}.uncle_blocks B
        ON A._st_day = B._st_day AND A.blknum = B.hermit_blknum
    WHERE
        A._st_day = '{day}'
        AND A.uncle_count > 0
        AND B.blknum is null
    LIMIT 5000
    """,
            con=engine,
        )
        logging.info(f"read uncle-blocks of day: {day} returns {len(df)} result")
        if len(df) == 0:
            return

        block_uncles = []
        for _, row in df.iterrows():
            blknum = row["blknum"]
            for pos in range(0, row["uncle_count"]):
                block_uncles.append((blknum, row["blkhash"], pos))

        item_exporter = InMemoryItemExporter(item_types=[EntityType.UNCLE])
        job = ExportUncleBlocksJob(
            block_uncles,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            max_workers=max_workers,
            item_exporter=item_exporter,
        )

        job.run()

        items = item_exporter.get_items(EntityType.UNCLE)
        df = pd.DataFrame(items)
        df.to_csv(f"{output}/uncle-block-{day}.csv", index=False)

        if load_into_db is True:
            df.drop(columns=["type"], inplace=True)
            df.to_sql(
                "uncle_blocks",
                schema=gp_schema,
                con=engine,
                method="multi",
                if_exists="append",
                index=False,
                chunksize=1000,
            )

    for day in list(pd.date_range(start_date, end_date, freq="1d", inclusive="left")):
        read_and_load_daily(day.strftime("%Y-%m-%d"), output)
