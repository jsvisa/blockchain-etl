import click
from datetime import datetime

from blockchainetl.cli.utils import global_click_options
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import (
    EntityType,
    parse_entity_types,
)
from bitcoinetl.streaming.enrich import enrich_traces_within_gp, enrich_traces_with_tidb
from blockchainetl.misc.psycopg import set_psycopg2_waitable


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "--by",
    type=click.Choice(["gp", "tidb"]),
    default="tidb",
    help="Enrich by which backend",
)
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum conneciton url",
)
@click.option(
    "-s",
    "--start-date",
    default=None,
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
    "-E",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_ETL),
    show_default=True,
    type=str,
    help="The list of entity types to create.",
)
@click.option(
    "--ti-url",
    "--tidb-url",
    "ti_url",
    type=str,
    envvar="BLOCKCHAIN_ETL_TIDB_URL",
    help="The TiDB conneciton url",
)
@click.option(
    "--ti-table",
    "--tidb-table",
    type=str,
    default="btc_out_traces",
    help="The TiDB dest table",
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
    "--redis-input-stream-prefix",
    "input_stream_prefix",
    type=str,
    default="export-stream-",
    show_default=True,
    help="The Redis input stream, used to consume(This prefix will be put behind the chain)",
)
@click.option(
    "--redis-output-stream-prefix",
    "output_stream_prefix",
    type=str,
    default="enrich-stream-",
    show_default=True,
    help="The Redis output stream, used to produce(This prefix will be put behind the chain)",
)
@click.option(
    "--redis-input-result-prefix",
    "input_result_prefix",
    type=str,
    default="utdb-result-",
    show_default=True,
    help="The Redis sorted set, used to check if the block has been UTDBed(put behind the chain)",
)
@click.option(
    "--redis-output-result-prefix",
    "output_result_prefix",
    type=str,
    default="enrich-result-",
    show_default=True,
    help="The Redis sorted set, used to store the handled block(put behind the chain)",
)
@click.option(
    "--consumer-group",
    type=str,
    default="enricher-ti",
    show_default=True,
    help="The Redis stream consumer group name, put it behind the chain",
)
@click.option(
    "--consumer-prefix",
    type=str,
    default="enricher-",
    show_default=True,
    help="The Redis consumer prefix name",
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
    "-o",
    "--output",
    default=None,
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_ENRICH_OUTPUT_PATH",
    help="A cache path to store the intermediate data",
)
def enrich(
    chain,
    by,
    gp_url,
    start_date,
    end_date,
    entity_types,
    ti_url,
    ti_table,
    redis_url,
    input_stream_prefix,
    output_stream_prefix,
    input_result_prefix,
    output_result_prefix,
    consumer_group,
    consumer_prefix,
    max_workers,
    output,
):
    """Enrich traces by GreenPlum or TiDB"""
    entity_types = parse_entity_types(entity_types)

    if (
        chain == Chain.BITCOIN
        and len(entity_types) == 1
        and entity_types[0] == EntityType.TRACE
    ):

        if by == "gp":
            set_psycopg2_waitable()
            enrich_traces_within_gp(gp_url, start_date, end_date)
        elif by == "tidb":
            enrich_traces_with_tidb(
                ti_url,
                ti_table,
                redis_url,
                input_stream_prefix,
                output_stream_prefix,
                input_result_prefix,
                output_result_prefix,
                consumer_group,
                consumer_prefix,
                max_workers,
                output,
            )
        else:
            raise ValueError("Unknown --by")

    else:
        raise NotImplementedError("currently only support: bitcoin.trace")
