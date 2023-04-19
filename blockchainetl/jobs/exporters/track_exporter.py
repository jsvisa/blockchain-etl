import logging
import math
import pandas as pd
import pypeln as pl
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Dict, Optional

from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.track.track_db import TrackDB
from blockchainetl.track.receivers import BaseReceiver
from blockchainetl.track.track_set import TrackSet
from blockchainetl.track.track_oracle import TrackOracle
from blockchainetl.service.token_service import TokenService
from blockchainetl.service.price_service import PriceService
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.domain.token import EthToken
from ethereumetl.misc.constant import DEFAULT_TOKEN_ETH

ETHEREUM_IGNORE_TO_ADDRESS = {
    "0x0000000000000000000000000000000000000000",
    "0x0000000000000000000000000000000000000001",
    "0x000000000000000000000000000000000000dead",
}


class TrackExporter:
    def __init__(
        self,
        chain: Chain,
        track_db: TrackDB,
        trackset: TrackSet,
        track_oracle: TrackOracle,
        entity_types: List[str],
        receivers: Dict[str, BaseReceiver],
        max_workers=5,
        worker_mode="thread",
        token_service: Optional[TokenService] = None,
        price_service: Optional[PriceService] = None,
        is_track2: bool = False,
    ):
        self._chain = chain
        self._receivers = receivers
        self._track_db = track_db
        self._trackset = trackset
        self._oracle = track_oracle
        self._max_workers = max_workers
        if worker_mode == "process":
            self._executor = ProcessPoolExecutor
        else:
            self._executor = ThreadPoolExecutor

        entities = set()
        entity_types = set(entity_types)
        if chain in Chain.ALL_BITCOIN_FORKS or EntityType.TRACE in entity_types:
            entities.add(EntityType.TRACE)
        elif EntityType.TRANSACTION in entity_types:
            entities.add(EntityType.TRANSACTION)

        if EntityType.TOKEN_TRANSFER in entity_types:
            entities.add(EntityType.TOKEN_TRANSFER)
            # compitable with token_xfer(track2) and token_transfer(track1)
            entities.add("token_xfer")

        self._entities = entities
        self._token_service = token_service
        self._price_service = price_service
        self._is_track2 = is_track2
        self._keep_tokens = set()

    def open(self):
        dataset = self._trackset.dump()
        for track in dataset:
            token = track.get("token_address")
            if token:
                self._keep_tokens.add(token)
        self._track_db.bootstrap(dataset)

    def export_items(self, items: List[Dict]):
        # what we need is value > 0 and don't track too much duplicated data
        items = [e for e in items if e["type"] in self._entities and e["value"] > 0]
        if len(items) == 0:
            return

        tracked = self.track(items)
        if tracked is None:
            return

        logging.info(f"hit #{tracked.shape[0]} address -> {tracked.to_dict('records')}")

        self._track_db.upsert(tracked)

        # FIXME: It's not a very efficient way to do it, but I'll do it later
        grouped_tracked = tracked.groupby("track_id")
        for track_id, group in grouped_tracked:
            # filter by the track_id
            track = self._trackset[track_id]
            for rec, receiver in self._receivers.items():
                if rec in track.receivers:
                    receiver.post(self._chain, group)

    def track(self, items: List[Dict]) -> Optional[pd.DataFrame]:
        def remap_entity_type(entity_type: str) -> str:
            return {
                EntityType.TRANSACTION: "tx",
                EntityType.TOKEN_TRANSFER: "token_xfer",
            }.get(entity_type, entity_type)

        for item in items:
            item["type"] = remap_entity_type(item["type"])

        track_df = self._track_db.all_items_df()
        if track_df.empty is True:
            logging.warning("No tracking address found")
            return None

        df = pd.DataFrame(items)
        df.drop(
            columns=["block_hash", "item_id", "item_timestamp"],
            inplace=True,
            errors="ignore",
        )
        if self._chain in Chain.ALL_BITCOIN_FORKS:
            df = self.extract_bitcoin_items(df)
        else:
            df = self.extract_ethereum_items(df)

        # TODO: if status is error, filter or not?
        df: pd.DataFrame = df[df.from_address != df.to_address]

        tracked = df.merge(
            track_df, how="inner", left_on="from_address", right_on="address"
        )
        # no address need to tracking
        if tracked.empty:
            return None

        # the column `address` is from track_df, equals to `from_address`
        # which are the from_address for new tracking items
        tracked.drop(columns=["address"], inplace=True)
        # the tx's receivers are the new tracking items
        tracked.rename(
            columns={
                "to_address": "address",
                "block_number": "blknum",
                "block_timestamp": "_st",
            },
            inplace=True,
        )
        tracked["hop"] += 1

        # bitcoin doesn't have token_address/name
        if "token_address" not in tracked.columns:
            tracked["token_address"] = None
            tracked["token_name"] = None

        # stop if address is known address or pattern
        tracked["stop"] = tracked.apply(self._oracle.shold_stop, axis=1)
        tracked.loc[tracked.stop == True, "label"] = tracked.loc[
            tracked.stop == True, "address"
        ].apply(self._oracle.stop_of)

        return tracked

    def extract_bitcoin_items(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[
            (df.value > 0)
            & (df.vout_type != "nulldata")
            & (~df.address.isin(["nulldata", "nonstandard"]))
        ].copy()  # type: ignore
        df.drop(
            columns=[
                "index",
                "req_sigs",
                "txinwitness",
                "script_hex",
                "script_asm",
                "pxhash",
                "vin_type",
                "vin_seq",
                "vout_type",
            ],
            inplace=True,
        )

        in_tx_df = (
            df[df["is_in"] == True]
            .rename(columns={"address": "from_address", "value": "in_value"})  # type: ignore
            .drop(columns=["is_in", "is_coinbase"])  # type: ignore
            .groupby(by=["block_number", "block_timestamp", "type", "txhash"])  # type: ignore
            .agg({"txhash": "count", "from_address": "nunique"})
            .rename(  # type: ignore
                columns={
                    "txhash": "n_tx_in_utxo",
                    "from_address": "n_tx_in_addr",
                }
            )
            .reset_index()  # type: ignore
        )
        merge_on = ["block_number", "block_timestamp", "type", "txhash"]
        # group by txhash with from/to address, and sum with value
        in_df = (
            df[df["is_in"] == True]
            .rename(columns={"address": "from_address", "value": "in_value"})  # type: ignore
            .drop(columns=["is_in", "is_coinbase"])  # type: ignore
            .groupby(  # type: ignore
                by=["block_number", "block_timestamp", "type", "txhash", "from_address"]
            )
            .agg({"in_value": "sum"})
            .reset_index()  # type: ignore
            .merge(in_tx_df, on=merge_on)  # type: ignore
        )

        out_tx_df = (
            df[df["is_in"] == False]
            .rename(columns={"address": "to_address", "value": "out_value"})  # type: ignore
            .drop(columns=["is_in", "is_coinbase"])  # type: ignore
            .groupby(by=["block_number", "block_timestamp", "type", "txhash"])  # type: ignore
            .agg({"txhash": "count", "to_address": "nunique"})
            .rename(  # type: ignore
                columns={
                    "txhash": "n_tx_out_utxo",
                    "to_address": "n_tx_out_addr",
                }
            )
            .reset_index()  # type: ignore
        )

        # don't need to track coinbase
        out_df = (
            df[(df["is_in"] == False) & (df["is_coinbase"] == False)]
            .rename(columns={"address": "to_address", "value": "out_value"})  # type: ignore
            .drop(columns=["is_in", "is_coinbase"])  # type: ignore
            .groupby(  # type: ignore
                by=["block_number", "block_timestamp", "type", "txhash", "to_address"]
            )
            .agg({"out_value": "sum"})
            .reset_index()  # type: ignore
            .merge(out_tx_df, on=merge_on)  # type: ignore
        )
        df = pd.merge(
            in_df, out_df, on=["block_number", "block_timestamp", "type", "txhash"]
        )

        df["token_name"] = "BTC"
        df["in_value"] = df.in_value / 1e8
        df["out_value"] = df.out_value / 1e8
        return df

    # TODO: copy the df?
    # TODO: filter the failed tx/trace?
    # filter: 1. contract call
    def extract_ethereum_items(self, df: pd.DataFrame) -> pd.DataFrame:
        # no erc20 Transfer, fill in with ETH
        if "token_address" not in df.columns:
            df["token_address"] = DEFAULT_TOKEN_ETH

        # track2 reads from TSDB
        if self._is_track2:
            ETH = Chain.symbol(self._chain)
            if "name" not in df.columns:
                df["name"] = ETH
            if "decimals" not in df.columns:
                df["decimals"] = 18
            df.rename(columns={"name": "token_name"}, inplace=True)

            # set trace's default value
            df.loc[df.type.isin(["tx", "trace"]), "token_name"] = ETH
            df.loc[df.type.isin(["tx", "trace"]), "decimals"] = 18

            # ignore erc721 transfers
            df = df[~df.decimals.isnull()]

        df: pd.DataFrame = df.fillna({"token_address": DEFAULT_TOKEN_ETH})
        df: pd.DataFrame = df[(~df.to_address.isin(ETHEREUM_IGNORE_TO_ADDRESS))]

        tokens = self._keep_tokens.copy()
        df_tokens = set(df.token_address)
        ps = self._price_service
        if ps is not None:
            stage = pl.thread.map(
                lambda t: (t, ps.get_price(self._chain, t)), df_tokens, workers=10
            )
            tokens = tokens.union(set(t[0] for t in stage if t[1] is not None))

        logging.info(f"filter with #{len(tokens)}/{len(df_tokens)} tokens")
        df = df[df.token_address.isin(tokens)]
        if len(df) == 0:
            return df

        df.rename(
            columns={"value": "in_value", "transaction_hash": "txhash"},
            inplace=True,
        )

        if self._is_track2:
            df["in_value"] = df.apply(
                lambda row: row["in_value"] / math.pow(10, row["decimals"]), axis=1
            )
        elif self._token_service is not None:
            ts: EthTokenService = self._token_service

            def apply_decimals(token_address, val):
                token: EthToken = ts.get_token(token_address, self._chain)
                if token.decimals:
                    return val / math.pow(10, token.decimals)
                return val

            df["token_name"] = df.token_address.apply(
                lambda x: ts.get_token(x, self._chain).symbol_or_name()
            )
            df["in_value"] = df.apply(
                lambda row: apply_decimals(row["token_address"], row["in_value"]),
                axis=1,
            )

        df["out_value"] = df["in_value"]
        return df

    def close(self):
        pass
