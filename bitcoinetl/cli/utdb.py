import os
import logging
import click
import redis
import pandas as pd
from typing import Dict, Tuple

from blockchainetl.utils import time_elapsed
from blockchainetl.cli.utils import global_click_options
from blockchainetl.jobs.redis_consumer_group import RedisConsumerGroup
from blockchainetl.misc.tidb import TiDBConnector
from blockchainetl.service.redis_stream_service import fmt_redis_key_name
from blockchainetl.service.sql_temp import DEFAULT_FIELD_TERMINATED
from bitcoinetl.service.btc_trace_loader import df_into_tidb
from bitcoinetl.enumeration.column_type import TIDB_TRACE


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "--ti-url",
    "--tidb-url",
    "ti_url",
    type=str,
    required=True,
    envvar="BLOCKCHAIN_ETL_TIDB_URL",
    help="The TiDB conneciton url",
)
@click.option(
    "--ti-in-table",
    "--tidb-in-table",
    type=str,
    default="btc_in_traces",
    help="The TiDB dest table used for store UTXO Inputs",
)
@click.option(
    "--ti-out-table",
    "--tidb-out-table",
    type=str,
    default="btc_out_traces",
    help="The TiDB dest table used for store UTXO Outputs",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/2",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis conneciton url",
)
@click.option(
    "--redis-stream-prefix",
    type=str,
    default="export-stream-",
    show_default=True,
    help="The Redis stream used to load the notified messages.(put behind the chain)",
)
@click.option(
    "--redis-result-prefix",
    type=str,
    default="utdb-result-",
    show_default=True,
    help="The Redis sorted set, used to store the block which has handled(put behind the chain)",
)
@click.option(
    "--consumer-group",
    type=str,
    default="loader-ti",
    show_default=True,
    help="The Redis stream consumer name, put it behind the chain",
)
@click.option(
    "-w",
    "--max-workers",
    default=5,
    show_default=True,
    type=int,
    help="The number of consuming workers",
)
@click.option(
    "--output",
    default=None,
    show_default=True,
    type=click.Path(dir_okay=True),
    envvar="BLOCKCHAIN_ETL_UTDB_OUTPUT_PATH",
    help="A cache path to store the intermediate data",
)
@click.option(
    "--direction",
    default="out",
    show_default=True,
    type=click.Choice(choices=["in", "out", "inout"]),
    help="Load in, our or inout UTXO into TiDB",
)
def btc_utdb(
    chain,
    ti_url,
    ti_in_table,
    ti_out_table,
    redis_url,
    redis_stream_prefix,
    redis_result_prefix,
    consumer_group,
    max_workers,
    output,
    direction,
):
    """Load all UTXO input/output data from CSV files into TiDB."""
    os.makedirs(output, exist_ok=True)

    stream = fmt_redis_key_name(chain, redis_stream_prefix, "trace")
    result = fmt_redis_key_name(chain, redis_result_prefix, "trace")
    cgroup = f"{chain}:{consumer_group}"

    def save_file(tidb, red, st, ti_table, blknum, df, direction):
        # convert boolean into int,
        # pymysql can't convert True/False values
        df = df.astype({"isin": int, "iscoinbase": int})

        # fix the pyright warning
        assert isinstance(df, pd.DataFrame)
        tmpfile = os.path.join(output, f"{blknum}-{direction}.csv")

        (st1, st2, affected_rows) = df_into_tidb(
            tidb,
            df,
            ti_table,
            key=blknum,
            tmpfile=tmpfile,
            retain_tmpfile=True,
        )

        if df.shape[0] != affected_rows:
            raise ValueError(
                f"LOAD {blknum} rows not match, df.shape({df.shape}) != inserted({affected_rows})"
            )

        red.sadd(result, blknum)

        logging.info(
            f"handle [utxo-(into)-tidb file={tmpfile} "
            f"elapsed @all={time_elapsed(st)} @convert={time_elapsed(st, st1)} "
            f"@db-write={time_elapsed(st1, st2)}"
        )

    def handler(conn: Tuple[TiDBConnector, redis.Redis], st: float, keyvals: Dict):
        tidb, red = conn

        blknum = list(keyvals.keys())[0].decode()
        file = list(keyvals.values())[0].decode()

        # skip if already handled
        if red.sismember(result, blknum):
            logging.info(f"block: {blknum} already exists, skip")
            return

        df = pd.read_csv(file, sep=DEFAULT_FIELD_TERMINATED)
        # fix the pyright warning
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"failed to read {file}")

        # fetch those fields only
        df = df[TIDB_TRACE]

        # filter nulldata

        # TODO: if address is nonstandardxxx with value > 0, insert or not?
        df: pd.DataFrame = df[
            (df.value > 0)
            & (df["address"] != "nulldata")
            & (df["address"] != "nonstandard")
        ]

        # all we need is the output
        if direction in ("in", "inout"):
            save_file(
                tidb,
                red,
                st,
                ti_table=ti_in_table,
                blknum=blknum,
                df=df.loc[df["isin"] == True],
                direction="0",
            )

        if direction in ("out", "inout"):
            save_file(
                tidb,
                red,
                st,
                ti_table=ti_out_table,
                blknum=blknum,
                df=df.loc[df["isin"] == False],
                direction="1",
            )

    red_cg = RedisConsumerGroup(
        redis_url,
        stream,
        cgroup,
        consumer_prefix="loader-",
        workers=max_workers,
        worker_mode="process",
        period_seconds=60,
    )

    def initer() -> Tuple[TiDBConnector, redis.Redis]:
        return (TiDBConnector(ti_url), redis.from_url(redis_url))

    def deiniter(conn: Tuple[TiDBConnector, redis.Redis]) -> None:
        conn[0].close()
        conn[1].close()

    red_cg.consume(
        handler,
        initer=initer,
        deiniter=deiniter,
    )
