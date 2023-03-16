import logging
import pandas as pd
from time import time
from datetime import datetime
from typing import Optional, Set, List, Dict
from eth_utils.address import to_checksum_address

from blockchainetl.utils import time_elapsed
from blockchainetl.misc.pandas_extra import partition_rank, vsum
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.streaming.extractor import (
    extract_token_transfers,
    extract_erc1155_transfers,
)
from ethereumetl.streaming.enrich import (
    enrich_token_transfers,
    enrich_erc1155_transfers,
)
from ethereumetl.streaming.utils import convert_token_transfers_to_df
from ethereumetl.service.token_transfer_extractor import (
    TRANSFER_EVENT_TOPIC,
    DEPOSIT_EVENT_TOPIC,
    WITHDRAWAL_EVENT_TOPIC,
)
from ethereumetl.service.erc1155_transfer_extractor import (
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
)

from .eth_base_adapter import EthBaseAdapter

IGNORE_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
}


class EthTokenHolderAdapter(EthBaseAdapter):
    def __init__(
        self,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        entity_types={EntityType.TOKEN_HOLDER, EntityType.ERC1155_HOLDER},
        is_detail=False,
        include_tokens: Optional[Set[str]] = None,
        exclude_tokens: Optional[Set[str]] = None,
    ):
        self.entity_types = entity_types
        self.log_mapper = EthLogMapper()
        self.is_detail = is_detail

        self.include_tokens = (
            [to_checksum_address(a) for a in include_tokens] if include_tokens else None
        )
        self.exclude_tokens = exclude_tokens or {}

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def _open(self):
        self.topics = self._build_topics()

    def export_all(self, start_block, end_block):
        st0 = time()

        dict_logs = self.export_logs(
            start_block, end_block, self.topics, self.include_tokens
        )
        dict_logs = [e for e in dict_logs if e["address"] not in self.exclude_tokens]
        if len(dict_logs) == 0:
            return

        st1 = time()
        blocks = self.export_blocks(start_block, end_block)
        st2 = time()

        token_transfers = []
        if self._should_export(EntityType.TOKEN_HOLDER):
            token_transfers = extract_token_transfers(
                dict_logs,
                self.batch_size,
                self.max_workers,
                self.chain,
            )
            token_transfers = enrich_token_transfers(blocks, token_transfers)
        erc1155_transfers = []
        if self._should_export(EntityType.ERC1155_HOLDER):
            erc1155_transfers = extract_erc1155_transfers(
                dict_logs, self.batch_size, self.max_workers
            )
            erc1155_transfers = enrich_erc1155_transfers(blocks, erc1155_transfers)

        st3 = time()
        token_holders = (
            self._export_token_holders(token_transfers)
            if len(token_transfers) > 0
            else []
        )

        erc1155_holders = (
            self._export_erc1155_holders(erc1155_transfers)
            if len(erc1155_transfers) > 0
            else []
        )
        st4 = time()

        all_items = token_holders + erc1155_holders
        # set updated_blknum
        for item in all_items:
            item["updated_blknum"] = end_block

        exported = self.item_exporter.export_items(all_items)
        st5 = time()
        logging.info(
            f"STAT {start_block, end_block} #logs={len(dict_logs)} #exported={exported} "
            f"#token_holders=(H: {len(token_holders)}/ X: {len(token_transfers)}) "
            f"#erc1155_hodlers=(H: {len(erc1155_holders)}/ X: {len(erc1155_transfers)}) "
            f"elapsed @total={time_elapsed(st0, st5)} @rpc_getLogs={time_elapsed(st0, st1)} "
            f"@rpc_getBlocks={time_elapsed(st1, st2)} @extract={time_elapsed(st2, st3)} "
            f"@calculate={time_elapsed(st3, st4)} @export={time_elapsed(st4, st5)} "
        )

    def _should_export(self, entity_type):
        return entity_type in self.entity_types

    def _export_token_holders(self, token_transfers) -> List[Dict]:
        df = convert_token_transfers_to_df(token_transfers)
        if self.is_detail is False:
            df = export_token_holders_v1(df)
        else:
            df = export_token_holders_v2(df)

        for col in df.columns:
            if col.endswith("_st"):
                df[col + "_day"] = df[col].apply(self._to_st_day)  # type: ignore

        df["type"] = EntityType.TOKEN_HOLDER
        return df.to_dict("records")  # type: ignore

    def _export_erc1155_holders(self, erc1155_transfers) -> List[Dict]:
        df = convert_token_transfers_to_df(erc1155_transfers)
        if self.is_detail is False:
            df = export_erc1155_holders_v1(df)
        else:
            df = export_erc1155_holders_v2(df)

        for col in df.columns:
            if col.endswith("_st"):
                df[col + "_day"] = df[col].apply(self._to_st_day)  # type: ignore

        df["type"] = EntityType.ERC1155_HOLDER
        return df.to_dict("records")  # type: ignore

    def _to_st_day(self, value):
        if pd.isna(value):
            return None
        return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")

    def _build_topics(self):
        # we need those events only: Transfer/SingleTransfer/BatchTransfer
        # special adapte to WETH
        topics = []
        if self._should_export(EntityType.TOKEN_HOLDER):
            topics.append(TRANSFER_EVENT_TOPIC)
            topics.append(DEPOSIT_EVENT_TOPIC)
            topics.append(WITHDRAWAL_EVENT_TOPIC)
        if self._should_export(EntityType.ERC1155_HOLDER):
            topics.append(TRANSFER_SINGLE_TOPIC)
            topics.append(TRANSFER_BATCH_TOPIC)
        return topics


def export_token_holders_v1(df: pd.DataFrame):
    df = (
        df[~df.to_address.isin(IGNORE_ADDRESSES)]
        .groupby(["token_address", "to_address"])  # type: ignore
        .agg({"blknum": min, "_st": min})
        .reset_index()  # type: ignore
        .rename(columns={"to_address": "address"})  # type: ignore
    )
    return df


def export_erc1155_holders_v1(df: pd.DataFrame):
    df = (
        df[~df.to_address.isin(IGNORE_ADDRESSES)]
        .groupby(["token_address", "to_address", "id"])  # type: ignore
        .agg({"blknum": min, "_st": min})
        .reset_index()  # type: ignore
        .rename(columns={"to_address": "address", "id": "token_id"})  # type: ignore
    )
    return df


def group_holder_by_token_and_address(df: pd.DataFrame, is_in=True, is_erc1155=False):
    direction = "recv" if is_in else "send"
    address = "to_address" if is_in else "from_address"
    groupby_base = ["token_address", address]
    groupby_extra = ["_st", "blknum", "txhash", "txpos", "logpos"]
    if is_erc1155:
        groupby_base.append("token_id")

    df_grouped: pd.DataFrame = (
        df.groupby(groupby_base + [f"_{direction}_rank_count"])
        .agg({"blknum": "nunique", "txhash": "nunique", "value": vsum})
        .reset_index()  # type: ignore
        .rename(  # type: ignore
            columns={
                address: "address",
                "blknum": f"{direction}_blk_count",
                "txhash": f"{direction}_tx_count",
                "value": f"{direction}_value",
                f"_{direction}_rank_count": f"{direction}_count",
            },
        )
    )
    first_df: pd.DataFrame = df.query(f"_{direction}_rank == _{direction}_rank_min")
    first_df = first_df[groupby_base + groupby_extra].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"first_{direction}_st",
            "blknum": f"first_{direction}_blknum",
            "txhash": f"first_{direction}_txhash",
            "txpos": f"first_{direction}_txpos",
            "logpos": f"first_{direction}_logpos",
        }
    )
    last_df: pd.DataFrame = df.query(f"_{direction}_rank == _{direction}_rank_max")
    last_df = last_df[groupby_base + groupby_extra].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"last_{direction}_st",
            "blknum": f"last_{direction}_blknum",
            "txhash": f"last_{direction}_txhash",
            "txpos": f"last_{direction}_txpos",
            "logpos": f"last_{direction}_logpos",
        }
    )
    merge_on = ["token_address", "address"]
    if is_erc1155:
        merge_on.append("token_id")
    df_grouped = df_grouped.merge(first_df, on=merge_on).merge(last_df, on=merge_on)

    return df_grouped


def merge_holder_with_send_recv_df(
    send_df: pd.DataFrame, recv_df: pd.DataFrame, is_erc1155=False
):
    merge_on = ["token_address", "address"]
    if is_erc1155:
        merge_on.append("token_id")

    df = pd.merge(send_df, recv_df, how="outer", on=merge_on)

    # fill those incrementally columns with zero,
    # those columns' default vaule is 0
    zero_columns = {
        c: 0 for c in df.columns if c.endswith("_count") or c.endswith("_value")
    }
    df.fillna(value=zero_columns, inplace=True)
    return df


def export_token_holders_v2(df: pd.DataFrame):
    df.sort_values(by=["blknum", "logpos"], ascending=True, inplace=True)

    df = partition_rank(
        df,
        group_by=["token_address", "to_address"],
        rank_column="_recv_rank",
    )
    df = partition_rank(
        df,
        group_by=["token_address", "from_address"],
        rank_column="_send_rank",
    )
    send_df = group_holder_by_token_and_address(df, is_in=False)
    recv_df = group_holder_by_token_and_address(df, is_in=True)

    merged = merge_holder_with_send_recv_df(send_df, recv_df, False)
    return merged


def export_erc1155_holders_v2(df: pd.DataFrame):
    df.rename(columns={"id": "token_id"}, inplace=True)
    df.sort_values(by=["blknum", "logpos"], ascending=True, inplace=True)

    df = partition_rank(
        df,
        group_by=["token_address", "token_id", "to_address"],
        rank_column="_recv_rank",
    )
    df = partition_rank(
        df,
        group_by=["token_address", "token_id", "from_address"],
        rank_column="_send_rank",
    )
    send_df = group_holder_by_token_and_address(df, is_in=False, is_erc1155=True)
    recv_df = group_holder_by_token_and_address(df, is_in=True, is_erc1155=True)

    return merge_holder_with_send_recv_df(send_df, recv_df, True)
