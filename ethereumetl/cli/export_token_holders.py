import logging
import time

import click

from blockchainetl.utils import time_elapsed
from blockchainetl.cli.utils import (
    global_click_options,
    pick_random_provider_uri,
)
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from blockchainetl.jobs.exporters.converters import (
    IntToDecimalItemConverter,
    NanToNoneItemConverter,
)
from ethereumetl.streaming.postgres_tables import (
    TOKEN_HOLDERS,
    ERC1155_HOLDERS,
    TOKEN_HOLDERS_V2,
    ERC1155_HOLDERS_V2,
)
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_token_holder_adapter import EthTokenHolderAdapter
from ethereumetl.streaming.postgres_hooks import upsert_token_holders
from blockchainetl.streaming.postgres_utils import build_cond_upsert_on_blknum


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/last_synced_block.txt",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_SYNCFILE",
    type=str,
    help="The file with the last synced block number.",
)
@click.option(
    "--lag",
    default=7,
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
    "--pg-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_PG_URL",
    help="The Postgres connection url",
)
@click.option(
    "--pg-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
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
    default=",".join([EntityType.TOKEN_HOLDER, EntityType.ERC1155_HOLDER]),
    show_default=True,
    type=str,
    help="The list of entity types to export.",
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
    "-B",
    "--block-batch-size",
    default=1,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round, write how many blocks in one CSV file",
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
@click.option(
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--detail",
    is_flag=True,
    show_default=True,
    help="Use the detail UPSERT API or simple one",
)
@click.option(
    "--db-threads",
    default=5,
    show_default=True,
    type=int,
    help="The number of threads to exeucte UPSERT",
)
@click.option(
    "--db-pool-size",
    default=10,
    show_default=True,
    type=int,
    help="The number of connection pool size",
)
@click.option(
    "--db-pool-overflow",
    default=20,
    show_default=True,
    type=int,
    help="The number of connection pool overflow size",
)
def export_token_holders(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    pg_url,
    pg_schema,
    start_block,
    end_block,
    entity_types,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    print_sql,
    pid_file,
    detail,
    db_threads,
    db_pool_size,
    db_pool_overflow,
):
    """Export ERC20/ERC721/ERC1155 token holder into Postgres."""

    st = time.time()
    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    if pg_schema is None:
        pg_schema = chain

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    entity_types = parse_entity_types(entity_types, True)
    logging.info(f"Export: {entity_types}")

    if detail is False:
        token_holder_insert_stmt = create_insert_statement_for_table(
            TOKEN_HOLDERS,
            on_conflict_do_update=False,
        )

        erc1155_holder_insert_stmt = create_insert_statement_for_table(
            ERC1155_HOLDERS,
            on_conflict_do_update=False,
        )

    else:
        token_holder_insert_stmt = create_insert_statement_for_table(
            TOKEN_HOLDERS_V2,
            on_conflict_do_update=True,
            upsert_callback=upsert_token_holders,
            where_callback=build_cond_upsert_on_blknum("updated_blknum"),
        )

        erc1155_holder_insert_stmt = create_insert_statement_for_table(
            ERC1155_HOLDERS_V2,
            on_conflict_do_update=True,
            upsert_callback=upsert_token_holders,
            where_callback=build_cond_upsert_on_blknum("updated_blknum"),
        )

    item_exporter = PostgresItemExporter(
        pg_url,
        pg_schema,
        item_type_to_insert_stmt_mapping={
            EntityType.TOKEN_HOLDER: token_holder_insert_stmt,
            EntityType.ERC1155_HOLDER: erc1155_holder_insert_stmt,
        },
        converters=(IntToDecimalItemConverter(), NanToNoneItemConverter()),
        print_sql=print_sql,
        workers=db_threads,
        pool_size=db_pool_size,
        pool_overflow=db_pool_overflow,
    )

    streamer_adapter = EthTokenHolderAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=item_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        is_detail=detail,
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file,
    )
    streamer.stream()

    logging.info(
        "Finish dump with chain={} provider={} block=[{}, {}] entity-types={} (elapsed: {}s)".format(
            chain,
            provider_uri,
            start_block,
            end_block,
            entity_types,
            time_elapsed(st),
        )
    )
