import time
import logging

import click

from blockchainetl.cli.utils import (
    evm_chain_options,
    extract_cmdline_kwargs,
    pick_random_provider_uri,
    str2bool,
)
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import write_last_synced_block
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.item_exporter_builder import create_tsdb_exporter

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_reorg_adapter import EthReorgAdapter
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
@evm_chain_options
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/last_reorg_block.txt",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_SYNCFILE",
    type=str,
    help="The file with the last reorg checked block number.",
)
@click.option(
    "--lag",
    default=2,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network, lag for more blocks to ensure dump is ready",
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
    "-E",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_ETL),
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
    default=512,
    show_default=True,
    type=int,
    help="How many blocks to be checked",
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
    help="Enable online enrich(token_xfer with name/symbol/decimals)",
)
@click.option(
    "--pending-mode",
    is_flag=True,
    show_default=True,
    help="Write results into {chain}_pending schema, "
    "always used along with --target-db-url, "
    "currently only support EVM chains",
)
@click.option(
    "--target-db-schema",
    type=str,
    help="The PostgreSQL database schema, default to {chain} name, "
    "only used if --target-db-url is specified.",
)
@click.option(
    "--target-db-url",
    type=str,
    help="The PostgreSQL conneciton url, used to write results into",
)
@click.option(
    "--target-db-workers",
    type=int,
    default=2,
    show_default=True,
    help="The PostgreSQL worker count",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
@click.option(
    "--cache-path",
    "--token-cache-path",
    type=click.Path(exists=False, readable=True, dir_okay=True, writable=True),
    show_default=True,
    help="Used as dicskcache,token's attributes for EVM, rawtransaction for Bitcoin",
)
@click.option(
    "--dryrun",
    is_flag=True,
    show_default=True,
    help="Dryrun only",
)
def reorg(
    ctx,
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    source_db_url,
    entity_types,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    enable_enrich,
    pending_mode,
    target_db_schema,
    target_db_url,
    target_db_workers,
    print_sql,
    cache_path,
    dryrun,
):
    """Check and apply ChainReorg data from PostgreSQL(TimescaleDB) with json-rpc block hash diff"""

    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    entity_types = parse_entity_types(entity_types)
    kwargs = extract_cmdline_kwargs(ctx)
    logging.info(f"Start reorg check with extra kwargs {kwargs}")

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    if target_db_schema is not None and len(target_db_schema) > 0:
        schema = target_db_schema
    else:
        schema = chain
    if pending_mode is True:
        schema += "_pending"
    item_exporter = create_tsdb_exporter(
        chain,
        schema,
        target_db_url,
        workers=target_db_workers,
        pool_size=target_db_workers + 5,
        print_sql=print_sql,
    )

    reorg_adapter = EthReorgAdapter(
        target_schema=schema,
        target_db_url=target_db_url,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
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
        token_cache_path=cache_path,
    )

    if dryrun is True:
        blknum, _ = reorg_adapter.get_current_block_number()
        start_block, end_block = blknum - block_batch_size, blknum
        _, diff = reorg_adapter.reconcile_blocks(start_block, end_block)
        logging.info(f"Reorg dryrun check: {diff}")
        return

    while True:
        blknum, _ = reorg_adapter.get_current_block_number()
        start_block, end_block = blknum - block_batch_size - lag, blknum - lag
        reorg_adapter.export_all(start_block, end_block)
        write_last_synced_block(last_synced_block_file, blknum)
        time.sleep(period_seconds)
