import os
import logging
import time
from typing import Set, Optional, Union
from collections import defaultdict
from collections.abc import Callable
from threading import Thread

import click
import pandas as pd
from sqlalchemy import create_engine

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
from ethereumetl.streaming.postgres_tables import ERC721_TOKENIDS, ERC1155_TOKENIDS
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_nft_tokenid_adapter import EthNftTokenidAdapter
from ethereumetl.streaming.postgres_hooks import (
    upsert_erc721_token_ids,
    cond_upsert_erc721_token_ids,
    upsert_erc1155_token_ids,
    cond_upsert_erc1155_token_ids,
)
from ethereumetl.cli.utils import validate_and_read_tokens
from ethereumetl.providers.auto import new_web3_provider
from ethereumetl.streaming.utils import build_erc20_token_reader
from ethereumetl.service.eth_token_service import EthTokenService


def build_postgres_exporter(
    pg_url, pg_schema, print_sql, threads=2, pool_size=2, pool_overflow=5
):
    return PostgresItemExporter(
        pg_url,
        pg_schema,
        item_type_to_insert_stmt_mapping={
            EntityType.ERC721_TOKENID: create_insert_statement_for_table(
                ERC721_TOKENIDS,
                on_conflict_do_update=True,
                upsert_callback=upsert_erc721_token_ids,
                where_callback=cond_upsert_erc721_token_ids,
                schema=pg_schema,
            ),
            EntityType.ERC1155_TOKENID: create_insert_statement_for_table(
                ERC1155_TOKENIDS,
                on_conflict_do_update=True,
                upsert_callback=upsert_erc1155_token_ids,
                where_callback=cond_upsert_erc1155_token_ids,
                schema=pg_schema,
            ),
        },
        print_sql=print_sql,
        workers=threads,
        pool_size=pool_size,
        pool_overflow=pool_overflow,
    )


def build_nft_tokenid_streamer(
    chain: str,
    last_synced_block_file: str,
    lag: int,
    item_exporter,
    provider_uri: str,
    start_block: Optional[int],
    end_block: Optional[int],
    entity_types,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    erc721_tokens,
    erc1155_tokens,
    batch_id: Optional[Union[int, str]] = None,
    smooth_erc20_token_reader: Optional[Callable] = None,
    smooth_exclude_tokens: Optional[Set[str]] = None,
    token_service: Optional[EthTokenService] = None,
) -> Streamer:
    streamer_adapter = EthNftTokenidAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        erc721_tokens=erc721_tokens,
        erc1155_tokens=erc1155_tokens,
        item_exporter=item_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        batch_id=batch_id,
        smooth_mode=smooth_erc20_token_reader is not None,
        erc20_token_reader=smooth_erc20_token_reader,
        smooth_exclude_tokens=smooth_exclude_tokens,
        token_service=token_service,
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
    return streamer


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-l",
    "--last-synced-block-dir",
    type=click.Path(exists=True, dir_okay=True, writable=True, readable=True),
    required=True,
    show_default=True,
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
    "--gp-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum connection url(used to read ERC20 token)",
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
    default=2,
    help="The Postgres threads",
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
    "-E",
    "--entity-types",
    default=",".join([EntityType.ERC721_TOKENID, EntityType.ERC1155_TOKENID]),
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "-T",
    "--token-file",
    type=click.Path(exists=True, readable=True, file_okay=True),
    default=None,
    show_default=True,
    help="The token file in YAML",
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
    "--smooth-mode",
    is_flag=True,
    show_default=True,
    help="Run with smooth mode(run with all ERC721/1155 tokens except the known batches)",
)
@click.option(
    "--smooth-token-cache-path",
    type=click.Path(exists=True, readable=True, dir_okay=True, writable=True),
    show_default=True,
    help="The path to store token's attributes",
)
def export_nft_tokenids(
    chain,
    last_synced_block_dir,
    lag,
    provider_uri,
    gp_url,
    pg_url,
    pg_threads,
    gp_schema,
    start_block,
    end_block,
    entity_types,
    token_file,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    print_sql,
    smooth_mode,
    smooth_token_cache_path,
):
    """Export NFT(ERC721/ERC1155) token-id's changelog."""

    st = time.time()
    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    if gp_schema is None:
        gp_schema = chain

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    entity_types = parse_entity_types(entity_types, True)
    logging.info(f"Export: {entity_types}")

    def run_token_streamer(
        last_synced_block_file,
        erc721_tokens,
        erc1155_tokens,
        batch_id=None,
        smooth_mode=False,
        smooth_exclude_tokens=None,
    ):
        token_reader = build_erc20_token_reader(chain, gp_url) if smooth_mode else None
        token_service = (
            EthTokenService(
                new_web3_provider(provider_uri, chain),
                cache_path=smooth_token_cache_path,
            )
            if smooth_mode
            else None
        )
        item_exporter = build_postgres_exporter(
            pg_url,
            gp_schema,
            print_sql,
            threads=pg_threads,
            pool_size=pg_threads + 2,
        )
        streamer = build_nft_tokenid_streamer(
            chain,
            last_synced_block_file,
            lag,
            item_exporter,
            provider_uri,
            start_block,
            end_block,
            entity_types,
            period_seconds,
            batch_size,
            block_batch_size,
            max_workers,
            erc721_tokens,
            erc1155_tokens,
            batch_id,
            smooth_erc20_token_reader=token_reader,
            smooth_exclude_tokens=smooth_exclude_tokens,
            token_service=token_service,
        )
        streamer.stream()

    if token_file is not None and os.path.exists(token_file):
        tokens = validate_and_read_tokens(token_file)
        erc721_tokens = {e["address"]: e for e in tokens.get("erc721_tokens") or []}
        erc1155_tokens = {e["address"]: e for e in tokens.get("erc1155_tokens") or []}
        last_synced_block_file = os.path.join(
            last_synced_block_dir, "nft-tokenid-local.txt"
        )
        run_token_streamer(last_synced_block_file, erc721_tokens, erc1155_tokens)

    else:
        engine = create_engine(pg_url)
        df = pd.read_sql(
            f"""
SELECT
    address,
    COALESCE(symbol, name) AS name,
    token_type,
    batch_id,
    created_blknum
FROM
    {gp_schema}.nft_tokens
WHERE
    batch_id is not null
""",
            con=engine,
        )
        dd = defaultdict(list)
        for d in df.to_dict("records"):
            dd[d["batch_id"]].append(
                {
                    "address": d["address"],
                    "name": d["name"],
                    "token_type": d["token_type"],
                    "created_blknum": d["created_blknum"],
                }
            )
        threads = []
        for batch_id, tokens in dd.items():
            erc721_tokens = {
                e["address"]: e for e in tokens if e["token_type"] == "erc721"
            }
            erc1155_tokens = {
                e["address"]: e for e in tokens if e["token_type"] == "erc1155"
            }
            last_synced_block_file = os.path.join(
                last_synced_block_dir, f"nft-tokenid-{batch_id}.txt"
            )
            threads.append(
                Thread(
                    target=run_token_streamer,
                    args=(
                        last_synced_block_file,
                        erc721_tokens,
                        erc1155_tokens,
                    ),
                    kwargs=dict(batch_id=batch_id),
                )
            )

        if smooth_mode is True:
            threads.append(
                Thread(
                    target=run_token_streamer,
                    args=(
                        os.path.join(last_synced_block_dir, "nft-tokenid-smooth.txt"),
                        None,
                        None,
                    ),
                    kwargs=dict(
                        batch_id="smooth",
                        smooth_mode=True,
                        smooth_exclude_tokens=set(df["address"]),
                    ),
                )
            )

        for p in threads:
            p.start()

        for p in threads:
            if p.is_alive():
                p.join()

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
