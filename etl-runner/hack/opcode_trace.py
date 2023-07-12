#!/usr/bin/env python3

import os
import time
import json
import logging

import click

from sqlalchemy.sql import func
from sqlalchemy import (
    Table,
    Column,
    DateTime,
    BigInteger,
    String,
    Numeric,
    MetaData,
    Boolean,
    TIMESTAMP,
)
from typing import List, Generator, Dict, Union, Tuple

from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.cli.utils import evm_chain_options, pick_random_provider_uri
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer

# from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
# from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
# from blockchainetl.jobs.exporters.converters import (
#     RenameKeyItemConverter,
#     ListToStringItemConverter,
#     UnixTimestampItemConverter,
# )
from blockchainetl.utils import rpc_response_batch_to_results, time_elapsed

from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_streamer_adapter import EthBaseAdapter
from ethereumetl.json_rpc_requests import generate_json_rpc


metadata = MetaData()

OPCODE_TRACES = Table(
    "op_traces",
    metadata,
    Column("id", BigInteger),
    Column("block_timestamp", TIMESTAMP, primary_key=True),
    Column("blknum", BigInteger),
    Column("txhash", String),
    Column("tx_gas", Numeric),
    Column("failed", Boolean),
    Column("return_value", String),
    Column("step", BigInteger),
    Column("depth", BigInteger),
    Column("gas", Numeric),
    Column("gas_cost", Numeric),
    Column("op", String),
    Column("pc", BigInteger),
    Column("stack", String),
    Column("memory", String),
    Column("rdata", String),
    Column("refund", Numeric),
    Column("error", String),
    Column("item_id", String, primary_key=True),
    Column("created_at", DateTime, server_default=func.current_timestamp()),
    Column("updated_at", DateTime, server_default=func.current_timestamp()),
)


def generate_opcode_trace_by_txhash_json_rpc(
    txhashes: List[str],
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for txhash in txhashes:
        yield generate_json_rpc(
            method="debug_traceTransaction",
            params=[
                txhash,
                {
                    "timeout": "120s",
                    "enableReturnData": True,
                    "enableMemory": True,
                    "disableStack": False,
                    "disableStorage": True,
                },
            ],
            # save block_number in request ID, so later we can identify block number in response
            request_id=txhash,
        )


class OpcodeAdapter(EthBaseAdapter):
    def __init__(
        self,
        chain: str,
        batch_web3_provider: BatchHTTPProvider,
        output: str,
        batch_size=5,
        max_workers=5,
    ):
        self.output = output
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        super().__init__(
            chain, batch_web3_provider, ConsoleItemExporter(), batch_size, max_workers
        )

    def export_all(self, start_block: int, end_block: int):
        _, txs = self.export_blocks_and_transactions(start_block, end_block)
        self.batch_work_executor.execute(
            [(e["hash"], e["block_number"]) for e in txs], self._export_batch
        )

    def _export_batch(self, txhashes: List[Tuple[str, int]]):
        txhashes = {e[0]: e[1] for e in txhashes}
        rpc = list(generate_opcode_trace_by_txhash_json_rpc(txhashes))
        response = self.batch_web3_provider.make_batch_request(json.dumps(rpc))
        results = rpc_response_batch_to_results(response, requests=rpc, with_id=True)

        for result, txhash in results:
            blknum = txhashes[txhash]
            path = os.path.join(self.output, str(blknum))
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, f"{txhash}.json"), "w") as fw:
                json.dump(result, fw, indent=2)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()


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
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-b",
    "--batch-size",
    default=10,
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
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--output",
    default="output",
    show_default=True,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The output path",
)
def dump(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    start_block,
    end_block,
    period_seconds,
    batch_size,
    block_batch_size,
    max_workers,
    pid_file,
    output,
):
    """Dump opcods traces from full-node's json-rpc to local directory."""

    st = time.time()
    if provider_uri is None:
        raise click.BadParameter(
            "-p/--provider-uri or $BLOCKCHAIN_ETL_PROVIDER_URI is required"
        )

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    streamer_adapter = OpcodeAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        chain=chain,
        output=output,
        batch_size=batch_size,
        max_workers=max_workers,
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
        "Finish dump with chain={} provider={} block=[{}, {}] (elapsed: {}s)".format(
            chain,
            provider_uri,
            start_block,
            end_block,
            time_elapsed(st),
        )
    )


dump()
