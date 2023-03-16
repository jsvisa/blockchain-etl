import logging

import click
from web3 import Web3, HTTPProvider

from blockchainetl.cli.utils import (
    global_click_options,
    pick_random_provider_uri,
)
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from blockchainetl.service.price_service import PriceService
from ethereumetl.streaming.postgres_tables import NFT_ORDERBOOKS
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_nft_orderbook_adapter import EthNftOrderbookAdapter
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.enumeration.nop_platform import parse_nop_platforms


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-l",
    "--last-synced-block-file",
    required=True,
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_SYNCFILE",
    help="The directory used to store the last synchronized block file",
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
    "--price-url",
    type=str,
    required=True,
    help="The price connection url, either http or postgres",
)
@click.option(
    "--source-db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    help="The source Sqlalchemy connection url, used to read extra metadatas",
)
@click.option(
    "--target-pg-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_PG_URL",
    help="The target Postgres connection url",
)
@click.option(
    "--gp-schema",
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
    "--nft-platforms",
    default="opensea",
    show_default=True,
    type=str,
    help="The list of nft platforms to export.",
)
@click.option(
    "--debug",
    is_flag=True,
    show_default=True,
    help="Debug mode, send result into console, and not insert into the database",
)
def export_nft_orderbooks(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    price_url,
    source_db_url,
    target_pg_url,
    gp_schema,
    start_block,
    end_block,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    print_sql,
    nft_platforms,
    debug,
):
    """Export NFT(ERC721/ERC1155) Orderbooks from Dex platforms(eg: OpenSea, Seaport)."""

    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    if gp_schema is None:
        gp_schema = chain

    provider_uri = pick_random_provider_uri(provider_uri)
    nft_platforms = parse_nop_platforms(nft_platforms, chain)
    logging.info("Using provider: " + provider_uri)
    logging.info(f"Start with platforms: {list(nft_platforms.keys())}")

    if debug is True:
        item_exporter = ConsoleItemExporter()
    else:
        item_exporter = PostgresItemExporter(
            target_pg_url,
            gp_schema,
            item_type_to_insert_stmt_mapping={
                EntityType.NFT_ORDERBOOK: create_insert_statement_for_table(
                    NFT_ORDERBOOKS,
                    on_conflict_do_update=False,
                )
            },
            print_sql=print_sql,
        )

    web3 = Web3(HTTPProvider(provider_uri))
    token_service = EthTokenService(web3)
    price_service = PriceService(price_url)

    streamer_adapter = EthNftOrderbookAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=item_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        token_service=token_service,
        price_service=price_service,
        nft_platforms=nft_platforms,
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
