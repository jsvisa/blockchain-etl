import logging
from typing import Dict, Optional
from time import time
from collections.abc import Callable
from typing import Set

import pandas as pd
from eth_utils.address import to_checksum_address

from blockchainetl.utils import time_elapsed, as_st_day
from blockchainetl.misc.pandas_extra import partition_rank, vsum
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.streaming.extractor import (
    extract_cryptopunk_transfers,
    extract_erc721_transfers,
    extract_erc1155_transfers,
)
from ethereumetl.streaming.enrich import (
    enrich_logs,
    enrich_erc721_transfers,
    enrich_erc1155_transfers,
)
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.utils import convert_token_transfers_to_df
from ethereumetl.service.token_transfer_extractor import TRANSFER_EVENT_TOPIC
from ethereumetl.service.cryptopunk_extractor import (
    CRYPTOPUNK_TOKEN_ADDRESS,
    CRYPTOPUNK_TRANSFER_EVENT_TOPICS,
)
from ethereumetl.service.erc1155_transfer_extractor import (
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
)
from ethereumetl.misc.constant import ZERO_ADDR
from .eth_base_adapter import EthBaseAdapter

TOKENID_GROUPBY_KEY = ["token_address", "token_id"]

FILTER_ADDRESS_LIMIT = 1000


class EthNftTokenidAdapter(EthBaseAdapter):
    def __init__(
        self,
        batch_web3_provider,
        erc721_tokens: Optional[Dict[str, Dict]] = None,
        erc1155_tokens: Optional[Dict[str, Dict]] = None,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        entity_types={EntityType.ERC721_TOKENID},
        batch_id=None,
        smooth_mode=False,
        smooth_exclude_tokens: Set[str] = None,
        erc20_token_reader: Callable[[], Set[str]] = None,
        token_service: Optional[EthTokenService] = None,
    ):
        if erc721_tokens is None and erc1155_tokens is None and smooth_mode is False:
            raise ValueError(
                "erc721_tokens and erc1155_tokens at least one is not None"
            )

        self.token_service = token_service
        self.erc721_tokens = erc721_tokens or dict()
        self.erc1155_tokens = erc1155_tokens or dict()
        self.erc721_token_addresses = set(self.erc721_tokens.keys())
        self.erc1155_token_addresses = set(self.erc1155_tokens.keys())
        self.batch_web3_provider = batch_web3_provider
        self.batch_id = batch_id
        self.smooth_mode = smooth_mode
        self.smooth_exclude_tokens = smooth_exclude_tokens or set()
        self.erc20_token_reader = erc20_token_reader or (lambda: set())

        if (
            erc721_tokens is not None
            and len(self.erc721_token_addresses) > 1
            and chain == Chain.ETHEREUM
            and CRYPTOPUNK_TOKEN_ADDRESS in self.erc721_token_addresses
        ):
            raise ValueError("CryptoPunk is special and needs to be run separately")

        filter_with_address = False
        if (
            smooth_mode is False
            and (len(self.erc721_tokens) == 0 and len(self.erc1155_tokens) > 0)
            or (len(self.erc721_tokens) > 0 and len(self.erc1155_tokens) == 0)
        ):
            token_count = len(erc721_tokens or dict()) + len(erc1155_tokens or dict())
            filter_with_address = token_count <= FILTER_ADDRESS_LIMIT

        self.filter_addresses = None
        if filter_with_address is True:
            tokens = self.erc721_token_addresses or self.erc1155_token_addresses
            self.filter_addresses = [to_checksum_address(e) for e in tokens]

        if smooth_mode is True:
            self.minimum_blknum = 0
        else:
            self.minimum_blknum = min(
                [
                    e.get("created_blknum") or 0
                    for e in list(self.erc721_tokens.values())
                    + list(self.erc1155_tokens.values())
                ]
            )

        self.entity_types = entity_types
        self.log_mapper = EthLogMapper()
        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def _open(self):
        self.topics = self._build_topics()
        logging.info(
            f"[{self.batch_id}] nft-tokenid-adapter: is_cryptopunk: {self._is_cryptopunk()}"
            f" smooth_mode: {self.smooth_mode} #smooth_excluded: {len(self.smooth_exclude_tokens)}"
            f" #ERC721: {len(self.erc721_token_addresses)}"
            f" #ERC1155: {len(self.erc1155_token_addresses)}"
        )

    def export_all(self, start_block, end_block):
        if end_block < self.minimum_blknum:
            return

        st0 = time()

        blocks = self.export_blocks(start_block, end_block)
        json_logs = self.export_logs(
            start_block, end_block, self.topics, self.filter_addresses
        )
        st1 = time()
        logs = enrich_logs(blocks, json_logs)

        erc721_transfers = []

        if self._should_export(EntityType.ERC721_TOKENID):
            if self._is_cryptopunk():
                items = extract_cryptopunk_transfers(logs, self.chain)
            else:
                items = extract_erc721_transfers(
                    logs,
                    self.batch_size,
                    self.max_workers,
                    erc20_tokens=self.erc20_token_reader(),
                    chain=self.chain,
                )
            items = self._keep_by_token_address(items, self.erc721_token_addresses)
            erc721_transfers = enrich_erc721_transfers(blocks, items)

        erc721_tokenids = (
            self.calculate_erc721_tokenids(erc721_transfers)
            if len(erc721_transfers) > 0
            else []
        )

        erc1155_transfers = []
        if self._should_export(EntityType.ERC1155_TOKENID):
            items = extract_erc1155_transfers(logs, self.batch_size, self.max_workers)
            items = self._keep_by_token_address(items, self.erc1155_token_addresses)
            erc1155_transfers = enrich_erc1155_transfers(blocks, items)

        erc1155_tokenids = (
            self.calculate_erc1155_tokenids(erc1155_transfers)
            if len(erc1155_transfers) > 0
            else []
        )

        all_items = erc721_tokenids + erc1155_tokenids
        st2 = time()
        self.item_exporter.export_items(all_items)
        st3 = time()
        logging.info(
            f"STAT[batch_id: {self.batch_id}] {start_block, end_block} #logs={len(logs)} "
            f"#token_holders={len(erc721_tokenids)}/{len(erc721_transfers)} "
            f"#erc1155_hodlers={len(erc1155_tokenids)}/{len(erc1155_transfers)} "
            f"elapsed @total={time_elapsed(st0, st3)} "
            f"@rpc_getLogs={time_elapsed(st0, st1)} @export={time_elapsed(st2, st3)}"
        )

    def _should_export(self, entity_type):
        return entity_type in self.entity_types

    def convert_items_into_df(self, items) -> pd.DataFrame:
        df = convert_token_transfers_to_df(items)
        df.rename(columns={"id": "token_id"}, inplace=True)
        # FIXME: pandas can't handle big number very well
        df = df.astype({"token_id": str})
        return df

    def calculate_erc721_tokenids(self, token_transfers):
        df = self.convert_items_into_df(token_transfers)

        df = self.calculate_tokenids(df)
        df["type"] = EntityType.ERC721_TOKENID
        return df.to_dict("records")

    def calculate_erc1155_tokenids(self, token_transfers):
        df = self.convert_items_into_df(token_transfers)

        # those ERC1155 fields are not used
        df.drop(columns=["operator", "id_pos", "id_cnt", "xfer_type"], inplace=True)

        zero_df = self.calculate_zero_df(df)
        df = self.calculate_tokenids(df)

        df.drop(
            columns=["from_address", "to_address", "value_x", "value_y"],
            inplace=True,
        )

        df = df.merge(zero_df, how="left", on=TOKENID_GROUPBY_KEY)
        df.fillna(0, inplace=True)
        df["type"] = EntityType.ERC1155_TOKENID
        return df.to_dict("records")

    def calculate_zero_df(self, df: pd.DataFrame):
        def to_stat_df(zero_df: pd.DataFrame, stat_column: str):
            df: pd.DataFrame = zero_df.groupby(TOKENID_GROUPBY_KEY).agg(
                {"value": ["count", vsum]}
            )
            df.columns = ["_".join(c) for c in df.columns]
            df.rename(
                columns={
                    "value_count": f"{stat_column}_count",
                    "value_vsum": f"{stat_column}_value",
                },
                inplace=True,
            )
            df.reset_index(inplace=True)
            return df

        zero_df = df[
            (df["from_address"] == ZERO_ADDR) | (df["to_address"] == ZERO_ADDR)
        ].copy()  # type: ignore

        minted_df = to_stat_df(
            zero_df[zero_df["from_address"] == ZERO_ADDR],
            stat_column="minted",
        )
        burned_df = to_stat_df(
            zero_df[zero_df["to_address"] == ZERO_ADDR],
            stat_column="burned",
        )

        stat_df = minted_df.merge(burned_df, how="outer", on=TOKENID_GROUPBY_KEY)
        stat_df.fillna(0, inplace=True)
        return stat_df

    def calculate_tokenids(self, df):
        df = self.df_partition_rank(df)
        df_first: pd.DataFrame = (
            df[df["_rank"] == df["_rank_min"]]
            .rename(  # type: ignore
                columns={
                    "_st": "minted_st",
                    "blknum": "minted_blknum",
                    "txhash": "minted_txhash",
                    "txpos": "minted_txpos",
                    "logpos": "minted_logpos",
                }
            )
            .drop(columns=["_rank", "_rank_min", "_rank_max"])  # type: ignore
        )

        def select_mint_address(row):
            return (
                row["to_address"]
                if row["from_address"] == ZERO_ADDR
                else row["from_address"]
            )

        df_first["from_address"] = df_first.apply(select_mint_address, axis=1)
        df_first.drop(columns=["to_address"], inplace=True)

        df_last: pd.DataFrame = (
            df[df["_rank"] == df["_rank_max"]]
            .rename(  # type: ignore
                columns={
                    "_st": "xfered_st",
                    "blknum": "xfered_blknum",
                    "txhash": "xfered_txhash",
                    "txpos": "xfered_txpos",
                    "logpos": "xfered_logpos",
                }
            )
            .drop(columns=["_rank", "_rank_min", "_rank_max"])  # type: ignore
        )
        df_last.drop(columns=["from_address"], inplace=True)

        df = df_first.merge(df_last, on=TOKENID_GROUPBY_KEY + ["turnover_count"])

        df = df.assign(
            token_name=df.token_address.apply(self._get_token_name),
            minted_st_day=df.minted_st.apply(as_st_day),
            xfered_st_day=df.xfered_st.apply(as_st_day),
        )
        return df

    def df_partition_rank(self, df):
        df.sort_values(["blknum", "logpos"], ascending=True, inplace=True)
        df = partition_rank(df, TOKENID_GROUPBY_KEY)
        df.rename(
            columns={
                "_rank_count": "turnover_count",
            },
            inplace=True,
        )
        return df

    def _get_token_name(self, token_address: str):
        token_name = None
        if token_address in self.erc721_token_addresses:
            token_name = self.erc721_tokens[token_address]["name"]
        elif token_address in self.erc1155_token_addresses:
            token_name = self.erc1155_tokens[token_address]["name"]
        elif self.token_service is not None:
            token = self.token_service.get_token(token_address)
            token_name = token.symbol_or_name()

        # 1. strip unicode
        # 2. keep the first 32bytes
        if token_name is not None:
            token_name = token_name.replace("\x00", "")[:32]
        return token_name

    def _keep_by_token_address(self, items, token_addresses):
        if self.smooth_mode is True:
            return [
                e for e in items if e["token_address"] not in self.smooth_exclude_tokens
            ]
        return [e for e in items if e["token_address"] in token_addresses]

    def _is_cryptopunk(self):
        return (
            len(self.erc721_token_addresses) == 1
            and self.chain == Chain.ETHEREUM
            and CRYPTOPUNK_TOKEN_ADDRESS in self.erc721_token_addresses
        )

    def _build_topics(self):
        topics = []
        if self._should_export(EntityType.ERC721_TOKENID):
            if self._is_cryptopunk():
                for topic in CRYPTOPUNK_TRANSFER_EVENT_TOPICS:
                    topics.append(topic)
            else:
                topics.append(TRANSFER_EVENT_TOPIC)
        if self._should_export(EntityType.ERC1155_TOKENID):
            topics.append(TRANSFER_SINGLE_TOPIC)
            topics.append(TRANSFER_BATCH_TOPIC)
        return topics
