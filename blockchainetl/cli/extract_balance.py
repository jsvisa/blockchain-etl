import os
import logging
import click

from blockchainetl.enumeration.chain import Chain
from blockchainetl.cli.utils import pick_random_provider_uri, global_click_options
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.jobs.exporters.converters import NanToNoneItemConverter
from blockchainetl.streaming import postgres_utils
from ethereumetl.streaming.eth_balance_adapter import EthBalanceAdapter
from ethereumetl.streaming import postgres_tables as eth_table
from ethereumetl.streaming.postgres_hooks import (
    upsert_latest_balances as eth_upsert_latest_balances,
)
from ethereumetl.providers.auto import get_provider_from_uri

from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_balance_adapter import BtcBalanceAdapter
from bitcoinetl.streaming import postgres_tables as btc_table
from bitcoinetl.streaming.postgres_hooks import (
    upsert_latest_balances as btc_upsert_latest_balances,
)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-l",
    "--last-synced-block-file",
    required=True,
    show_default=True,
    help="The file used to store the last synchronized block file",
)
@click.option(
    "--lag",
    default=100,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network.",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--source-db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The source connection url, used to read chain.traces",
)
@click.option(
    "--target-db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_PG_URL",
    help="The target Postgres connection url, used to write results",
)
@click.option(
    "-s",
    "--start-block",
    default=None,
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
    "-E",
    "--entity-types",
    default=EntityType.LATEST_BALANCE,
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "--period-seconds",
    default=120,
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
    help="How many items are carried in a PostgreSQL exporting transaction",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=100,
    show_default=True,
    type=int,
    help="How many blocks of raw data are extracted at a single time",
)
@click.option(
    "-w",
    "--max-workers",
    default=os.cpu_count(),
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
@click.option(
    "--exporter-is-multiprocess",
    is_flag=True,
    show_default=True,
    help="PostgresItemExporter use multiprocess",
)
@click.option(
    "--read-block-from",
    default="rpc",
    type=click.Choice(["rpc", "source"]),
    show_default=True,
    help="(EXPERIMENTAL) Read block data from which database",
)
@click.option(
    "--read-transaction-from",
    default="rpc",
    type=click.Choice(["rpc", "source"]),
    show_default=True,
    help="(EXPERIMENTAL) Read transaction data from which database",
)
@click.option(
    "--ignore-trace-notmatched-error",
    is_flag=True,
    show_default=True,
    help="Ignore trace notmatched error(used if the chain's data were incomplete)",
)
@click.option(
    "--ignore-transaction-notmatched-error",
    is_flag=True,
    show_default=True,
    help="Ignore transaction notmatched error(used if the chain's data were incomplete)",
)
@click.option(
    "--use-transaction-instead-of-trace",
    is_flag=True,
    show_default=True,
    help="Use transaction instead of trace"
    "(used if the chain's trace data were not compatible with Geth/Erigon)",
)
@click.option(
    "--async-enrich-balance",
    is_flag=True,
    show_default=True,
    help="(EXPERIMENTAL) Async enrich balances, fill in balances from onchain rpc",
)
@click.option(
    "--async-enrich-redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/4",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_ENRICH_BALANCE_REDIS_URL",
    help="The Redis conneciton url used to store the enrich-balance tasks",
)
def extract_balance(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    source_db_url,
    target_db_url,
    start_block,
    end_block,
    entity_types,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    print_sql,
    exporter_is_multiprocess,
    read_block_from,
    read_transaction_from,
    ignore_trace_notmatched_error,
    ignore_transaction_notmatched_error,
    use_transaction_instead_of_trace,
    async_enrich_balance,
    async_enrich_redis_url,
):
    """Extract History/Latest balances from {chain}.traces into PostgreSQL database"""

    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    entity_types = parse_entity_types(entity_types, ignore_unknown=True)
    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)
    assert chain in Chain.ALL_ETHEREUM_FORKS or chain in Chain.ALL_BITCOIN_FORKS

    if chain in Chain.ALL_ETHEREUM_FORKS:
        hist_table = eth_table.HISTORY_BALANCES
        last_table = eth_table.LATEST_BALANCES
        upsert_cb = eth_upsert_latest_balances(within_fee=True, within_coinbase=True)
    else:
        hist_table = btc_table.HISTORY_BALANCES
        last_table = btc_table.LATEST_BALANCES
        upsert_cb = btc_upsert_latest_balances

    history_balances_stmt = postgres_utils.create_insert_statement_for_table(
        table=hist_table,
        on_conflict_do_update=False,
    )
    latest_balances_stmt = postgres_utils.create_insert_statement_for_table(
        table=last_table,
        on_conflict_do_update=True,
        upsert_callback=upsert_cb,
        where_callback=postgres_utils.cond_upsert_on_blknum,
    )

    item_exporter = PostgresItemExporter(
        target_db_url,
        chain,
        item_type_to_insert_stmt_mapping={
            EntityType.HISTORY_BALANCE: history_balances_stmt,
            EntityType.LATEST_BALANCE: latest_balances_stmt,
        },
        converters=(NanToNoneItemConverter(),),
        print_sql=print_sql,
        workers=max_workers,
        pool_size=max_workers,
        pool_overflow=max_workers + 10,
        batch_size=batch_size,
        multiprocess=exporter_is_multiprocess,
    )

    if chain in Chain.ALL_ETHEREUM_FORKS:
        streamer_adapter = EthBalanceAdapter(
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            source_db_url=source_db_url,
            target_db_url=target_db_url,
            target_dbschema=chain,
            item_exporter=item_exporter,
            chain=chain,
            entity_types=entity_types,
            max_workers=max_workers,
            read_block_from=read_block_from,
            read_transaction_from=read_transaction_from,
            ignore_trace_notmatched_error=ignore_trace_notmatched_error,
            ignore_transaction_notmatched_error=ignore_transaction_notmatched_error,
            use_transaction_instead_of_trace=use_transaction_instead_of_trace,
            async_enrich_balance=async_enrich_balance,
            async_enrich_redis_url=async_enrich_redis_url,
        )
    else:
        streamer_adapter = BtcBalanceAdapter(
            source_db_url=source_db_url,
            bitcoin_rpc=BitcoinRpc(provider_uri),
            item_exporter=item_exporter,
            entity_types=entity_types,
            max_workers=max_workers,
            target_db_url=target_db_url,
            target_dbschema=chain,
        )

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
