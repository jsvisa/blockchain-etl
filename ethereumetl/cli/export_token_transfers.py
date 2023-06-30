import os
import click
import logging
from datetime import datetime
from functools import lru_cache
from typing import Tuple

from blockchainetl.cli.utils import global_click_options
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.exporters.token_transfers_item_exporter import (
    TokenTransferPostresItemExporter,
)
from ethereumetl.streaming.postgres_tables import TOKEN_TRANSFERS, ERC721_TRANSFERS
from ethereumetl.providers.auto import get_provider_from_uri
from blockchainetl.thread_local_proxy import ThreadLocalProxy


@lru_cache(1000)
def to_st_day(value):
    return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-s",
    "--start-block",
    default=0,
    show_default=True,
    type=int,
    help="Start block",
)
@click.option(
    "-e",
    "--end-block",
    required=True,
    type=int,
    help="End block",
)
@click.option(
    "-b",
    "--batch-size",
    default=100,
    show_default=True,
    type=int,
    help="The number of blocks to filter at a time.",
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
    "-p",
    "--provider-uri",
    required=True,
    type=str,
    help="The URI of the web3 provider",
)
@click.option(
    "--pg-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_PG_URL",
    help="The Postgres connection url(used to store token-id items)",
)
@click.option(
    "--pg-threads",
    type=int,
    default=10,
    help="The Postgres threads",
)
@click.option(
    "--pg-schema",
    type=str,
    default=None,
    help="The PostgreSQL schema, default to the name of chain",
)
@click.option(
    "-t",
    "--tokens",
    default=None,
    show_default=True,
    type=str,
    multiple=True,
    help="The list of token addresses to filter by.",
)
@click.option(
    "-T",
    "--tokens-file",
    default=None,
    show_default=True,
    type=click.Path(exists=True, file_okay=True, readable=True),
    help="The list of token addresses to filter by.",
)
@click.option(
    "--is-erc721",
    is_flag=True,
    show_default=True,
    help="The tokens are ERC721",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
def export_token_transfers(
    chain,
    start_block,
    end_block,
    batch_size,
    max_workers,
    provider_uri,
    pg_url,
    pg_threads,
    pg_schema,
    tokens: Tuple,
    tokens_file,
    print_sql,
    is_erc721,
):
    """Export ERC20/ERC721 transfers into PostgreSQL."""

    if tokens_file is not None and os.path.exists(tokens_file):
        if tokens is None:
            tokens = tuple()
        tokens = list(tokens)
        with open(tokens_file, "r") as fp:
            tokens.extend([e.strip() for e in fp.readlines()])

    logging.info(f"Run export with tokens: {tokens} block: {start_block, end_block}")
    if pg_schema is None:
        pg_schema = chain

    pg_exporter = PostgresItemExporter(
        pg_url,
        pg_schema,
        item_type_to_insert_stmt_mapping={
            EntityType.TOKEN_TRANSFER: create_insert_statement_for_table(
                TOKEN_TRANSFERS, on_conflict_do_update=False, schema=pg_schema
            ),
            EntityType.ERC721_TRANSFER: create_insert_statement_for_table(
                ERC721_TRANSFERS, on_conflict_do_update=False, schema=pg_schema
            ),
        },
        print_sql=print_sql,
        workers=pg_threads,
        pool_size=pg_threads,
        pool_overflow=pg_threads * 2,
    )
    item_exporter = TokenTransferPostresItemExporter(
        pg_exporter, addon_erc721=is_erc721
    )

    def item_converter(item):
        rename_columns = {
            "block_number": "blknum",
            "block_timestamp": "_st",
            "transaction_hash": "txhash",
            "transaction_index": "txpos",
            "log_index": "logpos",
        }
        for old, new in rename_columns.items():
            item[new] = item.pop(old)
        item["_st_day"] = to_st_day(item["_st"])
        return item

    job = ExportTokenTransfersJob(
        chain,
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=item_exporter,
        max_workers=max_workers,
        tokens=tokens,
        item_converter=item_converter,
    )
    job.run()
