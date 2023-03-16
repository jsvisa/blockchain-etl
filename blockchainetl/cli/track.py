import os
import logging
import click

from web3 import Web3, HTTPProvider

from blockchainetl.cli.utils import (
    global_click_options,
    pick_random_provider_uri,
    str2bool,
)
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer, read_last_synced_block
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.track_exporter import TrackExporter
from blockchainetl.track.track_db import TrackDB
from blockchainetl.track.track_set import TrackSets
from blockchainetl.track.track_oracle import TrackOracle

from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ethereumetl.service.eth_token_service import EthTokenService


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
    "-l",
    "--last-synced-block-file",
    default=".priv/last_tracked_block.txt",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_TRACKFILE",
    type=str,
    help="The file with the last synced block number.",
)
@click.option(
    "--lag",
    default=0,
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
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--bootstrap",
    "is_bootstrap",
    is_flag=True,
    show_default=True,
    help="Bootstrap with track-file",
)
def track(
    ctx,
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    start_block,
    end_block,
    entity_types,
    track_db_url,
    track_db_schema,
    track_db_table,
    track_bootstrap_file,
    track_oracle_url,
    period_seconds,
    batch_size,
    max_workers,
    pid_file,
    is_bootstrap,
):
    """Track the flow of address money"""
    entity_types = parse_entity_types(entity_types)

    args = [x.lstrip("--") for sub in [e.split("=") for e in ctx.args] for x in sub]
    kwargs = dict()
    if len(args) % 2 == 0:
        kwargs = {
            args[i].replace("_", "-"): args[i + 1] for i in range(0, len(args), 2)
        }
        logging.info(f"extra kwargs are {kwargs}")

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
    track_oracle = TrackOracle(chain, track_oracle_url, "addr_labels", chain)

    token_service = None
    if chain in Chain.ALL_ETHEREUM_FORKS:
        web3 = Web3(HTTPProvider(provider_uri))
        token_service = EthTokenService(web3)

    track_exporter = TrackExporter(
        chain,
        track_db,
        track_set,
        track_oracle,
        entity_types,
        TrackSets.receivers,
        token_service=token_service,
    )

    if chain in Chain.ALL_ETHEREUM_FORKS:
        streamer_adapter = EthStreamerAdapter(
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            item_exporter=track_exporter,
            chain=chain,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=entity_types,
            is_geth_provider=str2bool(kwargs.get("provider-is-geth")),
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
            item_exporter=track_exporter,
            chain=chain,
            enable_enrich=True,
            batch_size=batch_size,
            max_workers=max_workers,
        )
    else:
        raise NotImplementedError(
            f"--chain({chain}) is not supported in entity types({entity_types})) "
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
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=1,
        pid_file=pid_file,
    )
    streamer.stream()

    TrackSets.close()

    if is_bootstrap:
        logging.info("Finish bootstrap")
