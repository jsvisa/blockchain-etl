import logging
import click
import pandas as pd
from time import time
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from blockchainetl.utils import time_elapsed
from blockchainetl.cli.utils import (
    global_click_options,
    extract_cmdline_kwargs,
    str2bool,
)
from blockchainetl.enumeration.chain import Chain
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.streaming.streamer import Streamer
from ethereumetl.providers.auto import get_provider_from_uri, new_web3_provider
from ethereumetl.service.eth_service import EthService
from ethereumetl.streaming.eth_check_autofix_adapter import EthCheckAutofixAdapter
from ethereumetl.jobs.checkers import Checker
from ethereumetl.jobs.checkers.block_checker import EthBlockChecker
from ethereumetl.jobs.checkers.transaction_checker import EthTransactionChecker
from ethereumetl.jobs.checkers.log_checker import EthLogChecker
from ethereumetl.jobs.checkers.tx_receipt_checker import EthTxReceiptChecker
from ethereumetl.jobs.checkers.trace_checker import EthTraceChecker
from ethereumetl.jobs.checkers.token_transfer_checker import EthTokenTransferChecker
from ethereumetl.jobs.checkers.erc721_transfer_checker import EthErc721TransferChecker

CHECKERS = [
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.RECEIPT,
    EntityType.TRACE,
    EntityType.TOKEN_TRANSFER,
    EntityType.ERC721_TRANSFER,
]
CHECKER_CTORS: Dict[str, "Checker"] = {
    EntityType.BLOCK: EthBlockChecker,
    EntityType.TRANSACTION: EthTransactionChecker,
    EntityType.LOG: EthLogChecker,
    EntityType.RECEIPT: EthTxReceiptChecker,
    EntityType.TRACE: EthTraceChecker,
    EntityType.TOKEN_TRANSFER: EthTokenTransferChecker,
    EntityType.ERC721_TRANSFER: EthErc721TransferChecker,
}


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
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--gp-url",
    "--pg-url",
    "--db-rw-url",
    "db_rw_url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The Postgres conneciton url used for Read/Write",
)
@click.option(
    "--db-ro-url",
    "db_ro_url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_RO_URL",
    help="The Postgres conneciton url used for Read ONLY",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
)
@click.option(
    "-s",
    "--start-date",
    default=(datetime.utcnow() - timedelta(days=1)).date(),
    show_default=True,
    help="Start datetime(included)",
)
@click.option(
    "-e",
    "--end-date",
    default=datetime.utcnow().date(),
    show_default=True,
    help="End datetime(excluded)",
)
@click.option(
    "-E",
    "--entity-types",
    default="block",
    show_default=True,
    help="Check with entity types",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=True),
    envvar="BLOCKCHAIN_ETL_DUMP_OUTPUT_PATH",
    help="The output local directory path",
)
@click.option(
    "-b",
    "--batch-size",
    default=50,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round, write how many blocks in one CSV file",
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
    "--dryrun",
    default=False,
    show_default=True,
    is_flag=True,
    help="Only the check scripts are run, but no repair tasks are performed",
)
@click.option(
    "--by",
    type=click.Choice(["timestamp", "date"]),
    show_default=True,
    default="date",
    help="Use which time selector",
)
@click.option(
    "--stream",
    default=False,
    show_default=True,
    is_flag=True,
    help="Run in stream mode",
)
@click.option(
    "-l",
    "--last-checked-block-file",
    default=".priv/last_checked_block.txt",
    show_default=True,
    type=str,
    help="The file with the last synced block number.(used ONLY in stream mode)",
)
@click.option(
    "--lag",
    default=100,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network.(used ONLY in stream mode)",
)
@click.option(
    "--start-block",
    default=None,
    show_default=True,
    type=int,
    help="Start block, included",
)
@click.option(
    "--end-block",
    default=None,
    show_default=True,
    type=int,
    help="End block, included",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
def gp_autofix(
    ctx,
    chain,
    provider_uri,
    db_rw_url,
    db_ro_url,
    gp_schema,
    start_date,
    end_date,
    entity_types,
    output,
    batch_size,
    block_batch_size,
    max_workers,
    dryrun,
    by,
    stream,
    last_checked_block_file,
    lag,
    start_block,
    end_block,
    print_sql,
):
    """Run data consistency check and autofix in PostgreSQL/GreenPlum"""

    if chain not in Chain.ALL_ETHEREUM_FORKS:
        raise click.BadParameter("current only support EVM data autofix")

    checker = set(entity_types.split(","))
    unknown = [c for c in checker if c not in CHECKERS]
    if len(unknown) > 0:
        raise click.BadParameter(f"--checker with unknown parameter: {checker}")

    kwargs = extract_cmdline_kwargs(ctx)
    is_geth_provider = str2bool(kwargs.get("provider_is_geth"))
    gp_schema = gp_schema or chain
    checkers: List[Tuple[str, Checker]] = [
        (
            c,
            CHECKER_CTORS[c](
                chain,
                provider_uri,
                output,
                db_rw_url,
                db_ro_url,
                gp_schema,
                is_geth_provider,
                batch_size,
                max_workers,
                by,
                stream,
                print_sql,
                check_transaction_consistency=str2bool(
                    kwargs.get("check_transaction_consistency")
                ),
                ignore_receipt_missing_error=str2bool(
                    kwargs.get("ignore_receipt_missing_error")
                ),
            ),
        )
        for c in CHECKERS
        if c in checker
    ]

    if stream is True:
        streamer_adapter = EthCheckAutofixAdapter(
            chain,
            checkers=checkers,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            batch_size=batch_size,
            max_workers=max_workers,
            dryrun=dryrun,
        )
        streamer = Streamer(
            blockchain_streamer_adapter=streamer_adapter,
            last_synced_block_file=last_checked_block_file,
            lag=lag,
            start_block=start_block,
            end_block=end_block,
            period_seconds=30,
            block_batch_size=block_batch_size,
        )
        streamer.stream()

    else:
        logging.info(f"Run check in date-range: [{start_date}, {end_date})")

        web3 = new_web3_provider(provider_uri, chain)
        eth_service = EthService(web3)

        for day in list(pd.date_range(start_date, end_date, inclusive="left")):
            st_day = day.date().strftime("%Y-%m-%d")
            st_blk, et_blk = eth_service.get_block_range_for_date(day.date())
            logging.info(f"block range on {st_day} is {st_blk, et_blk}")
            st = int(day.timestamp())
            et = st + 86400 - 1

            for name, checker in checkers:
                st0 = time()
                success = checker.check(st_day, st_day, st, et, st_blk, et_blk)
                st1 = time()
                fixed = None
                if not success and not dryrun:
                    fixed = checker.autofix(
                        st_day, st_day, st_blk=st_blk, et_blk=et_blk
                    )
                logging.info(
                    f"check {name}@{st_day} need-autofix: {not success} "
                    f"fixed: {fixed} (elapsed: {time_elapsed(st0, st1)}s, {time_elapsed(st1)}s)"
                )
