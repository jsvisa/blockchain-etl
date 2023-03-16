import logging
from time import time
from typing import Optional, Dict, Type

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import nop

from blockchainetl.utils import time_elapsed
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.streaming.enrich import enrich_logs
from blockchainetl.service.price_service import PriceService
from blockchainetl.service.token_service import TokenService

from ethereumetl.streaming.extractor import (
    extract_token_transfers,
    extract_erc1155_transfers,
)
from ethereumetl.streaming.enrich import (
    enrich_token_transfers,
    enrich_erc1155_transfers,
)
from ethereumetl.streaming.utils import (
    convert_token_transfers_to_df,
    convert_transactions_to_df,
)
from .eth_base_adapter import EthBaseAdapter


class EthNftOrderbookAdapter(EthBaseAdapter):
    def __init__(
        self,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        token_service: Optional[TokenService] = None,
        price_service: Optional[PriceService] = None,
        nft_platforms: Dict[str, Type[nop.NopExtractor]] = None,
        source_db_url: Optional[str] = None,
    ):
        self.entity_types = [
            EntityType.TRANSACTION,
            EntityType.LOG,
            EntityType.TOKEN_TRANSFER,
            EntityType.ERC1155_TRANSFER,
        ]
        self.token_service = token_service
        self.price_service = price_service
        nft_platforms = nft_platforms or {"opensea"}
        self.nft_platforms: Dict[str, nop.NopExtractor] = {
            p: ctor() for p, ctor in nft_platforms.items()
        }
        self.source_db_url = source_db_url
        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def _open(self):
        self.source_db_engine = None
        if self.source_db_url is not None:
            self.source_db_engine = create_engine(self.source_db_url)

    def export_all(self, start_block, end_block):
        st0 = time()

        blocks, transactions = self.export_blocks_and_transactions(
            start_block, end_block
        )
        if len(transactions) == 0:
            return

        logs = self.export_logs(start_block, end_block)
        if len(logs) == 0:
            return

        logs = enrich_logs(blocks, logs)

        token_transfers = extract_token_transfers(
            logs, self.batch_size, self.max_workers, self.chain
        )

        token_transfers = enrich_token_transfers(blocks, token_transfers)

        erc1155_transfers = extract_erc1155_transfers(
            logs, self.batch_size, self.max_workers
        )

        erc1155_transfers = enrich_erc1155_transfers(blocks, erc1155_transfers)

        if len(token_transfers) + len(erc1155_transfers) == 0:
            return

        st1 = time()
        orderbooks = {}
        n_orders = {}

        for p, extractor in self.nft_platforms.items():
            orders = list(
                extractor.extract_orderbooks(
                    logs, db_engine=self.source_db_engine, block_range=blocks
                )
            )
            n_orders[p] = len(orders)

            # don't copy/share those DataFrame
            tx_df = convert_transactions_to_df(transactions)
            ob_df = pd.DataFrame(orders)
            tf_df = convert_token_transfers_to_df(token_transfers, ignore_error=True)
            ef_df = convert_token_transfers_to_df(erc1155_transfers, ignore_error=True)

            orderbooks[p] = extractor.calculate(tx_df, ob_df, tf_df, ef_df)

        all_items = self._calculate(orderbooks)
        st2 = time()
        self.item_exporter.export_items(all_items)
        st3 = time()
        n_orderbooks = {k: len(v) for k, v in orderbooks.items()}
        logging.info(
            f"STAT[nft-orderbooks] {start_block, end_block} "
            f"#logs={len(logs)} #orders={n_orders} "
            f"#token_xfers={len(token_transfers)} #erc1155_transfers={len(erc1155_transfers)} "
            f"#nft_orderbooks={len(all_items)} #platform_orderbooks={n_orderbooks} "
            f"elapsed @total={time_elapsed(st0)} @rpc={time_elapsed(st0, st1)} "
            f"@calculate={time_elapsed(st1, st2)} @export={time_elapsed(st2, st3)}"
        )

    def _get_token_price(
        self, value: int, token_address: str, token_decimals: int, timestamp: int
    ):
        if (
            value is None
            or token_address is None
            or token_decimals is None
            or self.price_service is None
        ):
            return None

        price = self.price_service.get_price(self.chain, token_address, timestamp)
        if price is None:
            return None
        return value / pow(10, token_decimals) * price

    def _get_token_decimals(self, token_address: str):
        if self.token_service is None or token_address is None:
            return None

        token = self.token_service.get_token(token_address, self.chain)
        if token.decimals is None:
            logging.warning(f"The decimals of token {token_address} is None")
            return None
        return token.decimals

    def _calculate(self, nft_orderbooks):
        df = pd.concat(list(nft_orderbooks.values()), ignore_index=True)
        if df.empty:
            return []

        df = df.assign(
            type=EntityType.NFT_ORDERBOOK,
            currency_decimals=df.currency.apply(self._get_token_decimals),
            fee_currency_decimals=df.fee_currency.apply(self._get_token_decimals),
        )

        df["value_usd"] = df.apply(
            lambda row: self._get_token_price(
                row["value"],
                row["currency"],
                row["currency_decimals"],
                row["_st"],
            ),
            axis=1,
        )
        df["fee_value_usd"] = df.apply(
            lambda row: self._get_token_price(
                row["fee_value"],
                row["fee_currency"],
                row["fee_currency_decimals"],
                row["_st"],
            ),
            axis=1,
        )

        # replace nan to None,
        # else sqlalchemy/PostgreSQL will convert it into nan
        # and raise exception:
        #   - (psycopg2.errors.NumericValueOutOfRange) integer out of range
        # https://stackoverflow.com/a/54403705/2298986
        df.replace({np.nan: None}, inplace=True)
        return df.to_dict("records")

    def _close(self):
        if self.source_db_engine is not None:
            self.source_db_engine.dispose()
