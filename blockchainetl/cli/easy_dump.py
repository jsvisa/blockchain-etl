import os
import logging
import click
import tempfile
import psycopg2

from blockchainetl.cli.utils import (
    global_click_options,
    extract_cmdline_kwargs,
    pick_random_provider_uri,
    str2bool,
)
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.file_item_exporter import FileItemExporter
from blockchainetl.misc.easy_etl import easy_df_saver
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from bitcoinetl.enumeration.column_type import ColumnType as BtcColumnType
from ethereumetl.enumeration.column_type import ColumnType as EthColumnType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.pass_context
@global_click_options
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "-o",
    "--output",
    type=str,
    envvar="BLOCKCHAIN_ETL_DUMP_OUTPUT_PATH",
    help="The output local directory path",
)
@click.option(
    "-s",
    "--start-block",
    default=None,
    show_default=True,
    type=int,
    help="Start block",
)
@click.option(
    "-e",
    "--end-block",
    default=None,
    show_default=True,
    type=int,
    help="End block",
)
@click.option(
    "-E",
    "--entity-type",
    type=click.Choice(EntityType.ALL_FOR_EASY_ETL),
    required=True,
    show_default=True,
    help="The entity type to export.",
)
@click.option(
    "--gp-url",
    type=str,
    help="The GreenPlum conneciton url",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema",
)
@click.option(
    "--gp-table",
    type=str,
    default=None,
    help="The GreenPlum table",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
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
    help="The number of workers",
)
@click.option(
    "--enable-enrich",
    is_flag=True,
    show_default=True,
    help="Enable online enrich",
)
@click.option(
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--load-into-db",
    is_flag=True,
    show_default=True,
    help="Load into db or not",
)
def easy_dump(
    ctx,
    chain,
    provider_uri,
    output,
    start_block,
    end_block,
    entity_type,
    gp_url,
    gp_schema,
    gp_table,
    period_seconds,
    batch_size,
    max_workers,
    enable_enrich,
    pid_file,
    load_into_db,
):
    """Easy dump all data from full-node's json-rpc to CSV file, and then load into GreenPlum/PostgreSQL."""

    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )
    if load_into_db is True:
        assert gp_url is not None, "load-into-db should contains --gp-url"

    kwargs = extract_cmdline_kwargs(ctx)
    logging.info(f"extra kwargs are {kwargs}")

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    if chain in Chain.ALL_BITCOIN_FORKS:
        column_type = BtcColumnType()
    else:
        column_type = EthColumnType()

    gp_conn = psycopg2.connect(gp_url)
    df_saver = easy_df_saver(
        chain,
        entity_type,
        column_type,
        gp_conn=gp_conn,
        gp_schema=gp_schema,
        gp_table=gp_table,
        load_into_db=load_into_db,
    )

    item_exporter = FileItemExporter(chain, output, df_saver=df_saver)
    if chain in Chain.ALL_ETHEREUM_FORKS:
        streamer_adapter = EthStreamerAdapter(
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            item_exporter=item_exporter,
            chain=chain,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=[entity_type],
            is_geth_provider=str2bool(kwargs.get("provider_is_geth")),
            retain_precompiled_calls=str2bool(kwargs.get("retain_precompiled_calls")),
            check_transaction_consistency=str2bool(
                kwargs.get("check_transaction_consistency")
            ),
            ignore_receipt_missing_error=str2bool(
                kwargs.get("ignore_receipt_missing_error")
            ),
        )
    elif chain in Chain.ALL_BITCOIN_FORKS:
        streamer_adapter = BtcStreamerAdapter(
            bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
            item_exporter=item_exporter,
            chain=chain,
            enable_enrich=enable_enrich,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=[entity_type],
        )
    else:
        raise NotImplementedError(
            f"--chain({chain}) is not supported in entity types({entity_type})) "
        )

    last_synced_block_file = os.path.join(tempfile.mkdtemp(), "last_synced_block.txt")
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=0,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=1,
        pid_file=pid_file,
    )
    streamer.stream()

    os.unlink(last_synced_block_file)
    gp_conn.close()
