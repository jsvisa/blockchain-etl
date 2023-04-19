import os
import logging
import click

from web3 import Web3, HTTPProvider
from sqlalchemy import create_engine

from blockchainetl.cli.utils import evm_chain_options, pick_random_provider_uri
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer, read_last_synced_block
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.track_exporter import TrackExporter
from blockchainetl.track.track_db import TrackDB
from blockchainetl.track.track_set import TrackSets
from blockchainetl.track.track_oracle import TrackOracle
from blockchainetl.service.label_service import LabelService
from blockchainetl.service.profile_service import ProfileService
from blockchainetl.service.price_service import PriceService

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.eth_alert_adapter import EthAlertAdapter


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@evm_chain_options
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/last_tracked_block.txt",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_TRACKFILE",
    type=str,
    help="The file with the last synced block number.",
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
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_TRACK),
    show_default=True,
    type=str,
    help="The list of entity types to track.",
)
@click.option(
    "--data-db-url",
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_DSDB_DB_URL",
    help="The source db url(used to read block, tx, trace)",
)
@click.option(
    "-T",
    "--track-db-url",
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_TRACK_DB_URL",
    help="The track db url(postgresql)",
)
@click.option(
    "--track-db-schema",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_TRACK_DB_SCHEMA",
    help="The track db schema",
)
@click.option(
    "--track-db-table",
    default="tracks",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_TRACK_DB_TABLE",
    help="The track db table",
)
@click.option(
    "-F",
    "--track-bootstrap-file",
    type=click.Path(exists=True, readable=True, file_okay=True),
    default="etc/tracks.yaml",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_TRACK_BOOTSTRAP_FILE",
    help="The track file used for bootstrap the process",
)
@click.option(
    "-U",
    "--track-oracle-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_TRACK_ORACLE_URL",
    required=True,
    help="The track oracle conneciton url, used for fetch label and other information",
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
    default=200,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
)
@click.option(
    "-w",
    "--max-workers",
    default=20,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option(
    "--bootstrap",
    "is_bootstrap",
    is_flag=True,
    show_default=True,
    help="Bootstrap with track-file",
)
@click.option(
    "--enable-profile-oracle",
    is_flag=True,
    show_default=True,
    help="Enable address profile's oracle",
)
@click.option(
    "--pending-mode",
    is_flag=True,
    default=False,
    show_default=True,
    help="In pending mode, blocks are not finalized",
)
@click.option(
    "--price-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_PRICE_SERVICE_URL",
    help="The price connection url, used in price service",
)
@click.option(
    "--price-apikey",
    type=str,
    envvar="BLOCKCHAIN_ETL_PRICE_SERVICE_API_KEY",
    help="The price service api key",
)
def track2(
    chain,
    last_synced_block_file,
    provider_uri,
    start_block,
    end_block,
    entity_types,
    data_db_url,
    track_db_url,
    track_db_schema,
    track_db_table,
    track_bootstrap_file,
    track_oracle_url,
    period_seconds,
    batch_size,
    max_workers,
    is_bootstrap,
    enable_profile_oracle,
    pending_mode,
    price_url,
    price_apikey,
):
    """Track the flow of address money"""
    entity_types = parse_entity_types(entity_types)

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)
    logging.info("Boostrap: " + str(is_bootstrap))

    # load rulesets from file
    TrackSets.load(track_bootstrap_file)
    TrackSets.open()
    if track_db_schema is None:
        track_db_schema = chain
    track_db = TrackDB(track_db_url, track_db_schema, track_db_table)
    track_set = TrackSets()[chain]
    labeler = LabelService(track_oracle_url, "addr_labels", chain)
    profiler = None
    if enable_profile_oracle is True:
        profiler = ProfileService(chain, track_oracle_url)
    track_oracle = TrackOracle(chain, labeler, profiler)

    token_service = None
    if chain in Chain.ALL_ETHEREUM_FORKS:
        web3 = Web3(HTTPProvider(provider_uri))
        token_service = EthTokenService(web3)

    price_service = None
    if price_url is not None:
        price_service = PriceService(price_url, price_apikey)

    track_exporter = TrackExporter(
        chain,
        track_db,
        track_set,
        track_oracle,
        entity_types,
        TrackSets.receivers,
        token_service=token_service,
        price_service=price_service,
        is_track2=True,
    )

    engine = create_engine(
        data_db_url,
        pool_size=max_workers,
        max_overflow=max_workers + 10,
        pool_recycle=3600,
    )
    streamer_adapter = EthAlertAdapter(
        engine=engine,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=track_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        is_pending_mode=pending_mode,
    )

    # bootstrap only
    if is_bootstrap:
        start_block = TrackSets.start_block(chain)
        if os.path.exists(last_synced_block_file):
            end_block = read_last_synced_block(last_synced_block_file)
        logging.info(f"Start bootstrap in block: [{start_block}, {end_block}]")
        last_synced_block_file += ".bootstrap"

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=0,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=1,
    )
    streamer.stream()

    TrackSets.close()

    if is_bootstrap:
        logging.info("Finish bootstrap")
