import logging
import click

from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.cli.utils import global_click_options, pick_random_provider_uri
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.cli.utils import evm_chain_options
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.tsdb_tables import TXPOOLS
from ethereumetl.streaming.eth_txpool_adapter import EthTxpoolAdapter


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@evm_chain_options
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/txpool.txt",
    required=True,
    show_default=True,
    help="The file used to store the last synchronized block file",
)
@click.option(
    "-p",
    "--provider-uri",
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    show_default=True,
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_TSDB_URL",
    show_default=True,
    help="The tsdb connection url(used to store token-id items)",
)
@click.option(
    "--db-threads",
    type=int,
    default=5,
    show_default=True,
    help="The Postgres threads",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
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
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
def export_txpool(
    chain,
    last_synced_block_file,
    provider_uri,
    db_url,
    db_threads,
    period_seconds,
    max_workers,
    print_sql,
):
    """Export Txpool's info to DataBase."""

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    item_exporter = PostgresItemExporter(
        db_url,
        chain,
        item_type_to_insert_stmt_mapping={
            EntityType.TXPOOL: create_insert_statement_for_table(
                TXPOOLS,
                on_conflict_do_update=False,
            ),
        },
        print_sql=print_sql,
        workers=db_threads,
        pool_size=10,
        pool_overflow=5,
    )

    streamer_adapter = EthTxpoolAdapter(
        provider_uri=provider_uri,
        item_exporter=item_exporter,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        chain=chain,
        max_workers=max_workers,
    )

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        start_block=-1,
        period_seconds=period_seconds,
    )
    streamer.stream()
