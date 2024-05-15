import time
import logging

import click

from blockchainetl.cli.utils import (
    extract_cmdline_kwargs,
    pick_random_provider_uri,
    str2bool,
)
from blockchainetl.utils import time_elapsed
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.item_exporter_builder import (
    create_tsdb_exporter,
    evm_exporter_converters,
)
from blockchainetl.jobs.exporters.file_item_exporter import FileItemExporter
from blockchainetl.jobs.exporters.csv_item_exporter import CSVItemExporter
from blockchainetl.jobs.exporters.converters import DropFieldItemConverter

from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.service.redis_stream_service import RedisStreamService

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ethereumetl.streaming.utils import build_erc20_token_reader


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.pass_context
@click.option(
    "-c",
    "--chain",
    required=True,
    show_default=True,
    help="The chain network to connect to.",
)
@click.option(
    "--chain-type",
    required=True,
    show_default=True,
    default="evm",
    type=click.Choice(["evm", "utxo"]),
    help="The chain belows to which network types",
)
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
    "--source-db-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum/PostgreSQL conneciton url, used to read ERC20/ERC721 tokens",
)
@click.option(
    "-o",
    "--output",
    type=str,
    envvar="BLOCKCHAIN_ETL_DUMP_OUTPUT_PATH",
    help="The output local directory path",
)
@click.option(
    "-s",
    "--start-block",
    default=None,
    show_default=True,
    type=int,
    help="Start block, included, -1 stands for `latest block-lag blocks`",
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
    default=",".join(EntityType.ALL_FOR_ETL),
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/4",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis conneciton url",
)
@click.option(
    "--redis-stream-prefix",
    type=str,
    default="export-stream-",
    show_default=True,
    help="The Redis stream used to store notify messages.(Put behind the chain)",
)
@click.option(
    "--redis-result-prefix",
    type=str,
    default="export-result-",
    show_default=True,
    help="The Redis result sorted set used to store thee dumped block.(Put behind the chain)",
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
    "--enable-enrich",
    is_flag=True,
    show_default=True,
    help="Enable online enrich",
)
@click.option(
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--pending-mode",
    is_flag=True,
    show_default=True,
    help="(EXPERIMENTAL) write results into {chain}_pending schema, "
    "always used along with --target-db-url, "
    "currently only support EVM chains",
)
@click.option(
    "--target-db-schema",
    type=str,
    help="(EXPERIMENTAL) The GreenPlum/PostgreSQL database schema, default to {chain} name, "
    "only used if --target-db-url is specified.",
)
@click.option(
    "--target-db-url",
    type=str,
    help="(EXPERIMENTAL) The GreenPlum/PostgreSQL conneciton url, "
    "if specified, then directly write the results into this target, "
    "currently only support pending-mode dump",
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
    help="The path to store token's attributes, used ONLY IN EVM chains",
)
def dump(
    ctx,
    chain,
    chain_type,
    last_synced_block_file,
    lag,
    provider_uri,
    source_db_url,
    output,
    start_block,
    end_block,
    entity_types,
    redis_url,
    redis_stream_prefix,
    redis_result_prefix,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    enable_enrich,
    pid_file,
    pending_mode,
    target_db_schema,
    target_db_url,
    print_sql,
    token_cache_path,
):
    """Dump all data from full-node's json-rpc to CSV file or PostgreSQL."""

    st = time.time()
    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    entity_types = parse_entity_types(entity_types)
    kwargs = extract_cmdline_kwargs(ctx)
    logging.info(f"Start dump with extra kwargs {kwargs}")

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    if target_db_url is not None:
        if target_db_schema is not None and len(target_db_schema) > 0:
            schema = target_db_schema
        else:
            schema = chain
        if pending_mode is True:
            schema += "_pending"
        item_exporter = create_tsdb_exporter(schema, target_db_url, print_sql=print_sql)
    else:
        redis_notify = RedisStreamService(redis_url, entity_types).create_notify(
            chain, redis_stream_prefix, redis_result_prefix
        )
        if chain_type == "evm":
            converters = evm_exporter_converters()
            converters.append(DropFieldItemConverter(["type", "item_timestamp"]))
        else:
            converters = None
        item_exporter = CSVItemExporter(output, entity_types, converters, redis_notify)

    if chain_type == "evm":
        web3_provider = ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        )
        trace_provider = web3_provider
        trace_provider_uri = kwargs.get("trace_provider_uri")
        if trace_provider_uri is not None:
            trace_provider = ThreadLocalProxy(
                lambda: get_provider_from_uri(trace_provider_uri, batch=True)
            )
        streamer_adapter = EthStreamerAdapter(
            batch_web3_provider=web3_provider,
            item_exporter=item_exporter,
            chain=chain,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=entity_types,
            is_geth_provider=str2bool(kwargs.get("provider_is_geth")),
            retain_precompiled_calls=str2bool(kwargs.get("retain_precompiled_calls")),
            erc20_token_reader=build_erc20_token_reader(chain, source_db_url),
            check_transaction_consistency=str2bool(
                kwargs.get("check_transaction_consistency")
            ),
            ignore_receipt_missing_error=str2bool(
                kwargs.get("ignore_receipt_missing_error")
            ),
            enable_enrich=enable_enrich,
            token_cache_path=token_cache_path,
            trace_provider=trace_provider,
        )
    elif chain_type == "utxo":
        streamer_adapter = BtcStreamerAdapter(
            bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
            item_exporter=item_exporter,
            chain=chain,
            enable_enrich=enable_enrich,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=entity_types,
        )
    else:
        raise NotImplementedError(
            f"--chain({chain}) is not supported in entity types({entity_types})) "
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
        "Finish dump with chain={} provider={} block=[{}, {}] entities={} (elapsed: {}s)".format(
            chain,
            provider_uri,
            start_block,
            end_block,
            entity_types,
            time_elapsed(st),
        )
    )
