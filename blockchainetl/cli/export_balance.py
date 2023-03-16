import logging
import click

from blockchainetl.cli.utils import pick_random_provider_uri, evm_chain_options
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.jobs.exporters.converters import NanToNoneItemConverter
from blockchainetl.streaming import postgres_utils
from ethereumetl.streaming.eth_token_balance_adapter import EthTokenBalanceAdapter
from ethereumetl.streaming.postgres_tables import (
    TOKEN_HISTORY_BALANCES,
    TOKEN_LATEST_BALANCES,
)
from ethereumetl.streaming.postgres_hooks import upsert_latest_balances
from ethereumetl.providers.auto import get_provider_from_uri


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@evm_chain_options
@click.option(
    "-l",
    "--last-synced-block-file",
    required=True,
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_SYNCFILE",
    help="The file used to store the last synchronized block file",
)
@click.option(
    "--lag",
    default=20,
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
    "--target-db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_PG_URL",
    help="The target Postgres connection url",
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
    default=EntityType.TOKEN_LATEST_BALANCE,
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
    "--rpc-batch-size",
    default=10,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
)
@click.option(
    "--export-batch-size",
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
    "--rpc-max-workers",
    default=10,
    show_default=True,
    type=int,
    help="The number of RPC workers",
)
@click.option(
    "--export-max-workers",
    default=4,
    show_default=True,
    type=int,
    help="The number of exporting workers",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
@click.option(
    "--token-cache-path",
    type=click.Path(exists=False, readable=True, dir_okay=True, writable=True),
    show_default=True,
    help="The path to store token's attributes",
)
@click.option(
    "--token-address",
    type=str,
    show_default=True,
    multiple=True,
    help="Only run this token address(es)",
)
@click.option(
    "--exporter-is-multiprocess",
    is_flag=True,
    show_default=True,
    help="PostgresItemExporter use multiprocess",
)
@click.option(
    "--read-block-from-target",
    is_flag=True,
    show_default=True,
    help="Read block data from target database",
)
@click.option(
    "--read-log-from",
    type=click.Choice(["rpc", "source"]),
    default="rpc",
    show_default=True,
    help="(EXPERIMENTAL) Read logs from rpc or source database",
)
@click.option(
    "--source-db-url",
    type=str,
    default=None,
    show_default=True,
    help="The source GreenPlum/Postgres connection url",
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
def export_balance(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    target_db_url,
    start_block,
    end_block,
    entity_types,
    period_seconds,
    rpc_batch_size,
    export_batch_size,
    block_batch_size,
    rpc_max_workers,
    export_max_workers,
    print_sql,
    token_cache_path,
    token_address,
    exporter_is_multiprocess,
    read_block_from_target,
    read_log_from,
    source_db_url,
    async_enrich_balance,
    async_enrich_redis_url,
):
    """Export History/Latest balances from RPC into PostgreSQL database"""

    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    entity_types = parse_entity_types(entity_types, ignore_unknown=True)
    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    history_balances_stmt = postgres_utils.create_insert_statement_for_table(
        table=TOKEN_HISTORY_BALANCES,
        on_conflict_do_update=False,
    )
    latest_balances_stmt = postgres_utils.create_insert_statement_for_table(
        table=TOKEN_LATEST_BALANCES,
        on_conflict_do_update=True,
        upsert_callback=upsert_latest_balances(),
        where_callback=postgres_utils.cond_upsert_on_blknum,
    )

    item_exporter = PostgresItemExporter(
        target_db_url,
        chain,
        item_type_to_insert_stmt_mapping={
            EntityType.TOKEN_HISTORY_BALANCE: history_balances_stmt,
            EntityType.TOKEN_LATEST_BALANCE: latest_balances_stmt,
        },
        converters=(NanToNoneItemConverter(),),
        print_sql=print_sql,
        workers=export_max_workers,
        pool_size=export_max_workers,
        pool_overflow=export_max_workers + 10,
        batch_size=export_batch_size,
        multiprocess=exporter_is_multiprocess,
    )

    streamer_adapter = EthTokenBalanceAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        target_db_url=target_db_url,
        target_dbschema=chain,
        item_exporter=item_exporter,
        chain=chain,
        entity_types=entity_types,
        batch_size=rpc_batch_size,
        max_workers=rpc_max_workers,
        token_cache_path=token_cache_path,
        token_addresses=token_address,
        read_block_from_target=read_block_from_target,
        read_log_from=read_log_from,
        async_enrich_balance=async_enrich_balance,
        async_enrich_redis_url=async_enrich_redis_url,
        source_db_url=source_db_url,
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
