import logging
from time import time
from datetime import datetime
from collections import defaultdict
from collections.abc import Callable
from typing import Set, Optional
from sqlalchemy import create_engine

from web3 import Web3

from blockchainetl.utils import time_elapsed
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.enumeration.entity_type import EntityType, EntityTable
from blockchainetl.enumeration.chain import Chain
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.domain.receipt import EthReceipt
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.enrich import (
    enrich_transactions,
    enrich_logs,
    enrich_token_transfers,
    enrich_erc721_transfers,
    enrich_erc1155_transfers,
    enrich_traces,
    enrich_contracts,
    enrich_tokens,
)
from ethereumetl.streaming.extractor import (
    extract_token_transfers,
    extract_erc721_transfers,
    extract_cryptopunk_transfers,
    extract_erc1155_transfers,
    extract_contracts,
    extract_tokens,
)
from ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from .eth_base_adapter import EthBaseAdapter
from .eth_item_id_calculator import EthItemIdCalculator
from .eth_item_timestamp_calculator import EthItemTimestampCalculator


class EthReorgAdapter(EthBaseAdapter):
    def __init__(
        self,
        target_schema,
        target_db_url,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        entity_types=tuple(EntityType.ALL_FOR_STREAMING),
        is_geth_provider=True,
        retain_precompiled_calls=True,
        erc20_token_reader: Callable[[], Set[str]] = None,
        check_transaction_consistency=False,
        ignore_receipt_missing_error=False,
        enable_enrich=False,
        token_cache_path: Optional[str] = None,
    ):
        if EntityType.ERC721_TRANSFER in entity_types and erc20_token_reader is None:
            raise ValueError(
                "erc20 token reader should not be None for erc721_transfer entity type"
            )

        self.target_schema = target_schema
        self.target_engine = create_engine(target_db_url)
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.is_geth_provider = is_geth_provider
        self.retain_precompiled_calls = retain_precompiled_calls
        self.erc20_token_reader = erc20_token_reader or (lambda: set())
        self.check_transaction_consistency = check_transaction_consistency
        self.ignore_receipt_missing_error = ignore_receipt_missing_error
        self.receipt_mapper = EthReceiptMapper()
        self.token_service = None
        if enable_enrich:
            self.token_service = EthTokenService(
                Web3(batch_web3_provider), cache_path=token_cache_path
            )

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def fetch_old_blocks(self, start_block: int, end_block: int, start_timestamp: int):
        with self.target_engine.connect() as conn:
            result = conn.execute(
                f"SELECT blknum, blkhash FROM {self.target_schema}.blocks WHERE block_timestamp >= %s AND blknum >= %s AND blknum <= %s",
                (datetime.utcfromtimestamp(start_timestamp), start_block, end_block),
            )
            rows = result.fetchall()
        return {e["blknum"]: e["blkhash"] for e in rows}

    def reconcile_blocks(self, new_blocks, old_blocks):
        return [
            e["number"]
            for e in new_blocks
            if e not in old_blocks or e["hash"] != old_blocks[e]
        ]

    def delete_entity(self, entity_type, start_timestamp, blocks):
        et = EntityTable()
        with self.target_engine.begin() as conn:
            result = conn.execute(
                f"DELETE FROM {self.target_schema}.{et[entity_type]} WHERE block_timestamp >= :start_timestamp AND blknum IN (:ids)",
                start_timestamp=start_timestamp,
                ids=blocks,
            )
            return result.rowcount

    def drop_old_blocks(self, start_timestamp, blocks):
        entities = [
            EntityType.BLOCK,
            EntityType.TRANSACTION,
            EntityType.LOG,
            EntityType.TOKEN_TRANSFER,
            EntityType.ERC721_TRANSFER,
            EntityType.ERC1155_TRANSFER,
            EntityType.TRACE,
            EntityType.CONTRACT,
        ]
        rowcount = 0
        for entity in entities:
            if self._should_export(entity):
                rowcount += self.delete_entity(
                    EntityType.BLOCK, start_timestamp, blocks
                )
        return rowcount

    def export_all(self, start_block, end_block):
        st0 = time()

        new_blocks = self.export_blocks(start_block, end_block)
        start_timestamp = min(e["timestamp"] for e in new_blocks) - 3600
        old_blocks = self.fetch_old_blocks(start_block, end_block, start_timestamp)

        diff_blocks = self.reconcile_blocks(new_blocks, old_blocks)
        if len(diff_blocks) == 0:
            logging.info(f"Reorg not detected for blocks:{start_block, end_block}")
            return

        # 0. Export blocks and transactions
        blocks, transactions = self.export_blocks_and_transactions(
            start_block=None, end_block=None, blocks=diff_blocks
        )
        enriched_blocks = blocks if EntityType.BLOCK in self.entity_types else []

        # 1. Export receipts and logs
        receipts, logs = [], []
        if len(transactions) > 0:
            # 1.0 ONLY log is exported, no transaction/receipt is required
            if (
                self._should_export(EntityType.LOG)
                and EntityType.TRANSACTION not in self.entity_types
                and EntityType.RECEIPT not in self.entity_types
            ):
                logs = self.export_logs(
                    start_block=None, end_block=None, blocks=diff_blocks
                )

            # 1.1 receipt/log
            elif self._should_export(EntityType.RECEIPT) or self._should_export(
                EntityType.LOG
            ):
                receipts, logs = self._export_receipts_and_logs(transactions)

        # 2. Enrich transactions with receipt
        enriched_transactions = (
            enrich_transactions(transactions, receipts)
            if EntityType.TRANSACTION in self.entity_types and len(transactions) > 0
            else []
        )
        # 3. Enrich logs with block hash/timestamp
        enriched_logs = (
            enrich_logs(blocks, logs)
            if EntityType.LOG in self.entity_types and len(logs) > 0
            else []
        )

        # 4. Extract token Transfers from logs
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER) and len(logs) > 0:
            token_transfers = extract_token_transfers(
                logs,
                self.batch_size,
                self.max_workers,
                chain=self.chain,
                token_service=self.token_service,
            )

        # 5. Enrich token Transfers with block hash/timestamp
        enriched_token_transfers = (
            enrich_token_transfers(blocks, token_transfers)
            if EntityType.TOKEN_TRANSFER in self.entity_types
            and len(token_transfers) > 0
            else []
        )

        # 6. Extract ERC721 Transfers from logs
        erc721_transfers = []
        if self._should_export(EntityType.ERC721_TRANSFER) and len(logs) > 0:
            erc721_transfers = extract_erc721_transfers(
                logs,
                self.batch_size,
                self.max_workers,
                self.erc20_token_reader(),
                self.chain,
            )
            erc721_transfers += extract_cryptopunk_transfers(logs, self.chain)

        # 7. Enrich ERC721 Transfers with block hash/timestamp
        enriched_erc721_transfers = (
            enrich_erc721_transfers(blocks, erc721_transfers)
            if EntityType.ERC721_TRANSFER in self.entity_types
            and len(erc721_transfers) > 0
            else []
        )

        # 8. Extract ERC1155 Transfers from logs
        erc1155_transfers = []
        if self._should_export(EntityType.ERC1155_TRANSFER) and len(logs) > 0:
            erc1155_transfers = extract_erc1155_transfers(
                logs, self.batch_size, self.max_workers
            )

        # 9. Enrich token Transfers with block hash/timestamp
        enriched_erc1155_transfers = (
            enrich_erc1155_transfers(blocks, erc1155_transfers)
            if EntityType.ERC1155_TRANSFER in self.entity_types
            and len(erc1155_transfers) > 0
            else []
        )

        # 10. Export traces
        # Geth's trace missing txhash
        traces = []
        if self._should_export(EntityType.TRACE):
            traces = self._export_traces(diff_blocks, transactions)

        # 11. Enrich traces with block hash/timestamp and txhash(only Geth)
        enriched_traces = (
            enrich_traces(blocks, traces)
            if EntityType.TRACE in self.entity_types and len(traces) > 0
            else []
        )

        # 12. Extract contracts from traces
        contracts = []
        if self._should_export(EntityType.CONTRACT) and len(traces) > 0:
            contracts = extract_contracts(traces, self.batch_size, self.max_workers)

        # 13. Enrich contract with block hash/timestamp
        enriched_contracts = (
            enrich_contracts(blocks, contracts)
            if EntityType.CONTRACT in self.entity_types and len(contracts) > 0
            else []
        )

        # 14. Extract tokens from traces
        tokens = []
        if self._should_export(EntityType.TOKEN) and len(contracts) > 0:
            tokens = extract_tokens(
                contracts, self.batch_web3_provider, self.max_workers
            )

        # 15. Enrich tokens with block hash/timestamp
        enriched_tokens = (
            enrich_tokens(blocks, tokens)
            if EntityType.TOKEN in self.entity_types and len(tokens) > 0
            else []
        )

        logging.debug("Exporting with " + type(self.item_exporter).__name__)

        all_items = (
            enriched_blocks
            + enriched_transactions
            + enriched_logs
            + enriched_token_transfers
            + enriched_erc721_transfers
            + enriched_erc1155_transfers
            + enriched_traces
            + enriched_contracts
            + enriched_tokens
        )

        st1 = time()
        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        dropped = self.drop_old_blocks(start_timestamp, diff_blocks)
        self.item_exporter.export_items(all_items)
        st2 = time()
        logging.info(
            f"Reorg blocks=({start_block}, {end_block}) diff={diff_blocks} dropped={dropped} added={len(all_items)} "
            f"total-elapsed={time_elapsed(st0, st2)} export-elapsed={time_elapsed(st1, st2)}"
        )

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=[EntityType.RECEIPT, EntityType.LOG])

        transaction_blocks = defaultdict(list)
        if self.check_transaction_consistency:
            for tx in transactions:
                txhash = tx["hash"]
                blknum = tx["block_number"]
                txpos = tx["transaction_index"]
                transaction_blocks[txhash].append((blknum, txpos))

        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["hash"] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
            check_consistency=self.check_transaction_consistency,
            transaction_blocks=transaction_blocks,
            ignore_error=self.ignore_receipt_missing_error,
        )
        job.run()
        receipts = exporter.get_items(EntityType.RECEIPT)
        logs = exporter.get_items(EntityType.LOG)

        receipt_txhashes = set([e["transaction_hash"] for e in receipts])
        if (
            self.check_transaction_consistency is True
            and self.ignore_receipt_missing_error is True
        ):
            for txhash, items in transaction_blocks.items():
                if txhash in receipt_txhashes:
                    continue
                for tx_blknum, txpos in items:
                    logging.warning(
                        f"fill empty receipt of txhash: {txhash} in blknum: {tx_blknum}"
                    )
                    receipt = EthReceipt()
                    receipt.status = -1
                    receipt.transaction_hash = txhash
                    receipt.transaction_index = txpos
                    receipt.block_number = tx_blknum

                    receipts.append(self.receipt_mapper.receipt_to_dict(receipt))

        return receipts, logs

    def _export_traces(self, diff_blocks, transactions):
        block_txhashes = dict()
        # here we are using dict instead of list to store block txhashes
        # in case the upstream returned txlist is not in order
        # the order should be always the same as tx.transaction_index
        for block in diff_blocks:
            block_txhashes[block] = dict()

        if self.is_geth_provider is True:
            for tx in transactions:
                block_txhashes[tx["block_number"]][tx["transaction_index"]] = tx["hash"]

        exporter = InMemoryItemExporter(item_types=[EntityType.TRACE])
        job = ExportTracesJob(
            start_block=None,
            end_block=None,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            include_genesis_traces=self.chain == Chain.ETHEREUM,
            include_daofork_traces=self.chain == Chain.ETHEREUM,
            is_geth_provider=self.is_geth_provider,
            retain_precompiled_calls=self.retain_precompiled_calls,
            txhash_iterable=block_txhashes,
            blocks=diff_blocks,
        )
        job.run()
        traces = exporter.get_items(EntityType.TRACE)
        return traces

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.LOG
            )

        if entity_type == EntityType.RECEIPT:
            return (
                EntityType.TRANSACTION in self.entity_types
                or self._should_export(EntityType.TOKEN_TRANSFER)
                or self._should_export(EntityType.ERC1155_TRANSFER)
            )

        if entity_type == EntityType.LOG:
            return (
                EntityType.LOG in self.entity_types
                or self._should_export(EntityType.TOKEN_TRANSFER)
                or self._should_export(EntityType.ERC721_TRANSFER)
                or self._should_export(EntityType.ERC1155_TRANSFER)
            )

        if entity_type == EntityType.TOKEN_TRANSFER:
            return (
                EntityType.TOKEN_TRANSFER in self.entity_types
                or self._should_export(EntityType.ERC721_TRANSFER)
            )

        if entity_type == EntityType.ERC721_TRANSFER:
            return EntityType.ERC721_TRANSFER in self.entity_types

        if entity_type == EntityType.ERC1155_TRANSFER:
            return EntityType.ERC1155_TRANSFER in self.entity_types

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(
                EntityType.CONTRACT
            )

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(
                EntityType.TOKEN
            )

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError("Unexpected entity type " + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item["item_id"] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item["item_timestamp"] = self.item_timestamp_calculator.calculate(item)
