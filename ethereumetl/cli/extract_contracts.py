import os
import logging
import click
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from ethereumetl.jobs.exporters.contracts_item_exporter import contracts_item_exporter
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum conneciton url",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    required=True,
    help="The GreenPlum schema",
)
@click.option(
    "--gp-table",
    type=str,
    default="traces",
    show_default=True,
    help="The GreenPlum token transfers table",
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
    "--output-path",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_CACHE_PATH",
    type=click.Path(exists=True, dir_okay=True, writable=True),
    help="The output path.",
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
    default=5,
    show_default=True,
    type=int,
    help="The maximum number of workers.",
)
@click.option(
    "--load-into-db",
    is_flag=True,
    default=False,
    show_default=True,
    help="Load the result csv into database or not.",
)
def extract_contracts(
    gp_url,
    gp_schema,
    gp_table,
    start_date,
    end_date,
    output_path,
    batch_size,
    max_workers,
    load_into_db,
):
    """Extracts (CREATE2)contracts from daily traces."""

    engine = create_engine(gp_url)

    for day in list(pd.date_range(start_date, end_date, inclusive="left")):
        st_day = day.date().strftime("%Y-%m-%d")
        df = pd.read_sql(
            f"""
WITH contracts AS (
    SELECT
        *
    FROM
        {gp_schema}.contracts
    WHERE
        _st_day = '{st_day}'
),
traces AS (
    SELECT
        *
    FROM
        {gp_schema}.{gp_table}
    WHERE
        _st_day = '{st_day}'
        AND trace_type = 'create2'
        AND status = 1
)
SELECT
    a.*
FROM
    traces a
    LEFT JOIN contracts b ON a.to_address = b.address AND a.txhash = b.txhash AND a.trace_address = B.trace_address
WHERE
    b.address is null -- not exists
""",
            con=engine,
        )
        if df.empty:
            logging.info(f"Skip extract contracts @[{st_day}]")
            continue

        traces_iterable = []
        for _, row in df.iterrows():
            traces_iterable.append(
                {
                    "type": "trace",
                    "_st": row["_st"],
                    "_st_day": row["_st_day"],
                    "block_number": row["blknum"],
                    "transaction_hash": row["txhash"],
                    "transaction_index": row["txpos"],
                    "from_address": row["from_address"],
                    "to_address": row["to_address"],
                    "value": row["value"],
                    "input": row["input"],
                    "output": row["output"],
                    "trace_type": row["trace_type"],
                    "call_type": row["call_type"],
                    "reward_type": row["reward_type"],
                    "gas": row["gas"],
                    "gas_used": row["gas_used"],
                    "subtraces": row["subtraces"],
                    "trace_address": row["trace_address"],
                    "error": row["error"],
                    "status": row["status"],
                }
            )

        output = os.path.join(output_path, f"contract-{st_day}.csv")
        job = ExtractContractsJob(
            traces_iterable=traces_iterable,
            batch_size=batch_size,
            max_workers=max_workers,
            item_exporter=contracts_item_exporter(output, True),
        )

        job.run()

        logging.info(f"Finish extract #{df.shape[0]} contracts @[{st_day}]")

        if load_into_db is False:
            continue

        if os.stat(output).st_size == 0:
            continue

        df = pd.read_csv(output)
        df.rename(
            columns={
                "block_number": "blknum",
                "transaction_hash": "txhash",
                "transaction_index": "txpos",
                "function_sighashes": "func_sighashes",
            },
            inplace=True,
        )

        df.to_sql(
            "contracts",
            schema=gp_schema,
            con=engine,
            method="multi",
            if_exists="append",
            index=False,
            chunksize=5000,
        )
        logging.info(f"Finish INSERT #{df.shape[0]} contracts @[{st_day}]")
