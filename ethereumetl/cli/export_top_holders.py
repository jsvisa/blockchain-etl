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
from blockchainetl.jobs.exporters.redis_item_exporter import RedisItemExporter
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_token_holder_adapter import EthTokenHolderAdapter
from blockchainetl.service.redis_top_holder_service import (
    RED_UPSERT_TOKEN_HOLDER_SCRIPT,
)
from ethereumetl.jobs.exporters.top_holder_exporter import top_holder_exporter


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
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/1",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis connection url",
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
    "--max-redis-workers",
    default=20,
    show_default=True,
    type=int,
    help="The number of Redis workers",
)
@click.option(
    "--include-tokens",
    default=None,
    show_default=True,
    type=str,
    help="Run ONLY those tokens, comma separated.",
)
@click.option(
    "--exclude-tokens",
    default=None,
    show_default=True,
    type=str,
    help="Run exclude those tokens, comma separated.",
)
def export_top_holders(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    redis_url,
    start_block,
    end_block,
    entity_types,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    max_redis_workers,
    include_tokens,
    exclude_tokens,
):
    """Export ERC20/ERC721/ERC1155 token holder into Redis."""

    st = time.time()
    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    entity_types = parse_entity_types(entity_types, True)
    logging.info(f"Export: {entity_types}")

    item_exporter = RedisItemExporter(
        chain,
        redis_url,
        entity_types,
        lua_script=RED_UPSERT_TOKEN_HOLDER_SCRIPT,
        threads=max_redis_workers,
        exporter=top_holder_exporter,
    )

    if include_tokens is not None:
        include_tokens = set(include_tokens.lower().split(","))
    if exclude_tokens is not None:
        exclude_tokens = set(exclude_tokens.lower().split(","))

    streamer_adapter = EthTokenHolderAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=item_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        is_detail=True,
        include_tokens=include_tokens,
        exclude_tokens=exclude_tokens,
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
