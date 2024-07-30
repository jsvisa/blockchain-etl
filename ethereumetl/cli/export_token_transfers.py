#!/usr/bin/env python3

from time import time
import requests
from functools import lru_cache
import logging
import click
from typing import List
from web3 import Web3
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.providers.auto import get_provider_from_uri

from blockchainetl.jobs.exporters.converters import (
    UnixTimestampItemConverter,
    RenameKeyItemConverter,
)
from blockchainetl.utils import time_elapsed
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from ethereumetl.streaming.eth_base_adapter import EthBaseAdapter
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.streaming.tsdb_tables import TOKEN_TRANSFERS
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator


class ExtractTokenTransferAdapter(EthBaseAdapter):
    def __init__(
        self,
        chain,
        batch_web3_provider,
        item_exporter,
        batch_size,
        max_workers,
        tokens: List[str],
        enable_enrich=False,
    ):
        self.chain = chain
        self.item_exporter = item_exporter
        self.tokens = tokens
        self.token_service = EthTokenService(Web3(batch_web3_provider))
        self.item_id_calculator = EthItemIdCalculator()
        self.enable_enrich = enable_enrich

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def enrich(self, items):
        for item in items:
            # indent with 10mins
            item["price"] = self.get_token_price(
                item["token_address"], item["block_timestamp"] // 600 * 600
            )

    @lru_cache(maxsize=102400)
    def get_token_price(self, token, timestamp):
        coin = "{chain}:{token}".format(chain=self.chain, token=token)
        url = "https://coins.llama.fi/prices/historical/{ts}/{coin}".format(
            ts=timestamp, coin=coin
        )
        r = requests.get(url)
        try:
            r.raise_for_status()
            res = r.json()["coins"]
            if coin in res:
                return res[coin]["price"]
            return None
        except Exception as e:
            logging.error(f"failed to get {url}: error: {e}")
            return None

    def export_all(self, start_block, end_block):
        st0 = time()
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_TRANSFER])

        job = ExportTokenTransfersJob(
            self.chain,
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            item_exporter=exporter,
            max_workers=self.max_workers,
            tokens=self.tokens,
        )
        job.run()

        logs = exporter.get_items(EntityType.TOKEN_TRANSFER)
        if len(logs) == 0:
            return
        st1 = time()

        for log in logs:
            token = self.token_service.get_token(log["token_address"])
            log.update(
                {
                    "type": EntityType.TOKEN_TRANSFER,
                    "name": token.name,
                    "symbol": token.symbol,
                    "decimals": token.decimals,
                }
            )
        st2 = time()

        for item in logs:
            item["item_id"] = self.item_id_calculator.calculate(item)

        if self.enable_enrich:
            self.enrich(logs)

        self.item_exporter.export_items(logs)
        st3 = time()

        logging.info(
            f"Insert {self.chain} [{start_block, end_block}] "
            f"#logs={len(logs)} "
            f"@all={time_elapsed(st0)}s @extract={time_elapsed(st0, st1)}s "
            f"@enrich={time_elapsed(st1, st2)}s "
            f"@export={time_elapsed(st2, st3)}s"
        )


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-c",
    "--chain",
    required=True,
    show_default=True,
    type=str,
    help="The chain network to connect to.",
)
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/extract-token-transfer.txt",
    required=True,
    show_default=True,
    help="The file used to store the last synchronized block file",
)
@click.option(
    "--lag",
    default=60,
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
    "--db-url",
    type=str,
    default="postgresql://postgres:root@127.0.0.1:5432/postgres",
    envvar="BLOCKCHAIN_ETL_DSDB_URL",
    show_default=True,
    help="The data TSDB connection url(used to read source items)",
)
@click.option(
    "--db-schema",
    type=str,
    show_default=True,
    help="The data schema",
)
@click.option(
    "-s",
    "--start-block",
    default=1,
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
    default=300,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=100,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round",
)
@click.option(
    "-b",
    "--batch-size",
    default=2000,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
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
    "--tokens",
    default=None,
    show_default=True,
    type=str,
    multiple=True,
    help="The list of token address to filter by.",
)
@click.option(
    "--print-sql",
    is_flag=True,
    show_default=True,
    help="Print SQL or not",
)
@click.option(
    "--enable-enrich",
    is_flag=True,
    show_default=True,
    help="Enable enrich with token price(via https://defillama.com/docs/api)",
)
def export_token_transfers(
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    db_url,
    db_schema,
    start_block,
    end_block,
    period_seconds,
    block_batch_size,
    batch_size,
    max_workers,
    tokens,
    print_sql,
    enable_enrich,
):
    """Export ERC20 token transfer into PostgreSQL."""
    logging.info(f"Export token_transfers with token: {tokens}")

    item_exporter = PostgresItemExporter(
        db_url,
        db_schema,
        item_type_to_insert_stmt_mapping={
            EntityType.TOKEN_TRANSFER: create_insert_statement_for_table(
                TOKEN_TRANSFERS, on_conflict_do_update=False
            ),
        },
        converters=(
            UnixTimestampItemConverter(),
            RenameKeyItemConverter(
                key_mapping={
                    "block_number": "blknum",
                    "transaction_hash": "txhash",
                    "transaction_index": "txpos",
                    "log_index": "logpos",
                }
            ),
        ),
        print_sql=print_sql,
        workers=10,
        pool_size=20,
        pool_overflow=5,
    )

    streamer_adapter = ExtractTokenTransferAdapter(
        chain,
        ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),  # type: ignore
        item_exporter,
        batch_size,
        max_workers,
        tokens=tokens,
        enable_enrich=enable_enrich,
    )

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,  # type: ignore
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
    )
    streamer.stream()
