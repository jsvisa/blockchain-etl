import logging
import redis
import json
import pypeln as pl
import pandas as pd
from time import time
from datetime import datetime
from typing import List, Dict, Optional
import concurrent.futures
from cachetools import cached, TTLCache
from multiprocessing.pool import Pool

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.future import select
from sqlalchemy.sql.expression import desc
from rpq.RpqQueue import RpqQueue

from blockchainetl.misc.sqlalchemy_extra import sqlalchemy_engine_builder
from blockchainetl.utils import time_elapsed, dynamic_batch_iterator
from blockchainetl.misc.pandas_extra import partition_rank, vsum
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from ethereumetl.streaming.postgres_tables import TOKEN_LATEST_BALANCES as T
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.mappers.log_mapper import EthLogMapper
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.extractor import (
    extract_token_transfers,
    extract_erc1155_transfers,
)
from ethereumetl.streaming.enrich import (
    enrich_token_transfers,
    enrich_erc1155_transfers,
)
from ethereumetl.misc.eth_extract_balance import READ_LOG_TEMPLATE
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
from .utils import fmt_enrich_balance_queue


T_COLUMNS = list(set(T.c.keys()) - {"id", "balance", "created_at", "updated_at"})
BALANCE_CUMSUM_DTS = [
    f"{d}_{t}" for d in ("out", "vin") for t in ("blocks", "txs", "xfers", "value")
] + ["value"]


class E:
    engine: Engine = None

    @staticmethod
    def _exec(row: Dict) -> Dict:
        E.engine.pool = E.engine.pool.recreate()
        with E.engine.connect() as conn:
            query = (
                select(T)
                .where(
                    (T.c.address == row["address"])
                    & (T.c.token_address == row["token_address"])
                    & (T.c.blknum <= row["blknum"])
                )
                .order_by(desc(T.c.blknum))
                .limit(1)
            )
            r = conn.execute(query).fetchone()
            if r is not None:
                return r._asdict()
            return None

    @staticmethod
    def connect(db_url, db_schema):
        E.engine = sqlalchemy_engine_builder(db_url, db_schema)(100, 0)

    @staticmethod
    def execute(items: List[Dict]) -> List[Dict]:
        threads = min(max(1, len(items) // 10), 500)

        result = []
        with concurrent.futures.ThreadPoolExecutor(threads) as executor:
            futures = []
            for row in items:
                f = executor.submit(E._exec, row)
                futures.append(f)

            for f in concurrent.futures.as_completed(futures):
                exception = f.exception()
                if exception:
                    logging.error(exception)
                    raise Exception(exception)
                r = f.result()
                if r is not None:
                    result.append(r)
        return result


class EthTokenBalanceAdapter(EthBaseAdapter):
    def __init__(
        self,
        batch_web3_provider: BatchHTTPProvider,
        target_db_url: str,
        target_dbschema: str,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        entity_types={
            EntityType.TOKEN_HISTORY_BALANCE,
            EntityType.TOKEN_LATEST_BALANCE,
        },
        token_cache_path: Optional[str] = None,
        token_addresses: Optional[List[str]] = None,
        read_block_from_target: bool = False,
        read_log_from: str = "rpc",
        async_enrich_balance: bool = False,
        async_enrich_redis_url: Optional[str] = None,
        source_db_url: Optional[str] = None,
    ):
        self.entity_types = set(entity_types)
        self.log_mapper = EthLogMapper()
        self.target_db_url = target_db_url
        self.source_db_url = source_db_url
        self.target_dbschema = target_dbschema
        self.token_cache_path = token_cache_path
        self.token_addresses = None
        if token_addresses is not None and isinstance(
            token_addresses, (list, tuple, set)
        ):
            self.token_addresses = list(set(token_addresses))
        self.read_block_from_target = read_block_from_target

        assert read_log_from in (
            "rpc",
            "source",
        ), "read token transfer only support rpc or source db"
        self.read_log_from = read_log_from
        if read_log_from == "source":
            assert (
                source_db_url is not None
            ), "read token transfer from source requires source_db_url"

        if async_enrich_balance is True:
            if async_enrich_redis_url is None:
                raise ValueError("async enrich balance need redis_url")
            self.async_enrich_queue = RpqQueue(
                redis.from_url(async_enrich_redis_url),
                fmt_enrich_balance_queue(chain, "erc20"),
            )
        self.async_enrich_balance = async_enrich_balance

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def _open(self):
        self.topics = self._build_topics()
        self.token_service = EthTokenService(
            self.web3, cache_path=self.token_cache_path
        )

        self.item_executor = None
        if self._should_export(EntityType.TOKEN_HISTORY_BALANCE):
            self.item_executor = Pool(
                processes=self.max_workers,
                initializer=E.connect,
                initargs=(self.target_db_url, self.target_dbschema),
            )

        self.target_db_engine = None
        if self.read_block_from_target is True:
            self.target_db_engine = create_engine(self.target_db_url)

        self.source_db_engine = None
        if self.read_log_from == "source":
            self.source_db_engine = create_engine(self.source_db_url)

    def export_all(self, start_block, end_block):
        st0 = time()

        dict_logs = self._get_logs(start_block, end_block)
        if len(dict_logs) == 0:
            return

        st1 = time()
        blocks = self._get_blocks(start_block, end_block)
        st2 = time()

        token_transfers = []
        if self._should_export(
            EntityType.TOKEN_LATEST_BALANCE, EntityType.TOKEN_HISTORY_BALANCE
        ):
            token_transfers = extract_token_transfers(
                dict_logs, self.batch_size, self.max_workers, self.chain
            )
            token_transfers = enrich_token_transfers(blocks, token_transfers)
        token_df = convert_token_transfers_to_df(token_transfers, ignore_error=True)

        st3 = time()
        if len(token_df) > 0:
            token_df = self._export_token_balances(token_df, end_block)

        token_items = []
        if len(token_df) > 0 and self._should_export(EntityType.TOKEN_LATEST_BALANCE):
            token_df["type"] = EntityType.TOKEN_LATEST_BALANCE
            token_items.extend(token_df[T_COLUMNS + ["type"]].to_dict("records"))  # type: ignore

        st4 = time()
        st5, st6 = st4, st4
        if len(token_df) > 0 and self._should_export(EntityType.TOKEN_HISTORY_BALANCE):
            token_df = self._get_old_balances(token_df)
            st5 = time()
            token_df = self._cumsum_last_balances(token_df)
            st6 = time()
            token_df["type"] = EntityType.TOKEN_HISTORY_BALANCE
            token_items.extend(token_df[T_COLUMNS + ["type"]].to_dict("records"))  # type: ignore

        erc1155_transfers = []
        if self._should_export(
            EntityType.ERC1155_LATEST_BALANCE, EntityType.ERC1155_HISTORY_BALANCE
        ):
            erc1155_transfers = extract_erc1155_transfers(
                dict_logs, self.batch_size, self.max_workers, self.chain
            )
            erc1155_transfers = enrich_erc1155_transfers(blocks, erc1155_transfers)
        erc1155_df = convert_token_transfers_to_df(erc1155_transfers, ignore_error=True)
        if len(erc1155_df) > 0:
            erc1155_df = self._export_erc1155_balances(erc1155_df)

        erc1155_items = []
        if len(erc1155_df) > 0 and self._should_export(
            EntityType.ERC1155_LATEST_BALANCE
        ):
            erc1155_df["type"] = EntityType.ERC1155_LATEST_BALANCE
            erc1155_items.extend(erc1155_df.to_dict("records"))

        if len(erc1155_df) > 0 and self._should_export(
            EntityType.ERC1155_HISTORY_BALANCE
        ):
            erc1155_df = self._cumsum_last_balances(erc1155_df)
            erc1155_items.extend(erc1155_df.to_dict("records"))

        st7 = time()
        exported = self.item_exporter.export_items(token_items + erc1155_items)
        st8 = time()

        if self.async_enrich_balance is True:
            pl.thread.each(self._enrich_balance, token_items, workers=10, run=True)
        st9 = time()

        logging.info(
            f"STAT {start_block, end_block} #logs={len(dict_logs)} #exported={exported} "
            f"#token_balances=(H: {len(token_items)}/ X: {len(token_transfers)}) "
            f"#erc1155_balances=(H: {len(erc1155_items)}/ X: {len(erc1155_transfers)}) "
            f"elapsed @total={time_elapsed(st0, st9)} @rpc_getLogs={time_elapsed(st0, st1)} "
            f"@rpc_getBlocks={time_elapsed(st1, st2)} @extract={time_elapsed(st2, st3)} "
            f"@calculate={time_elapsed(st3, st4)} @get_old={time_elapsed(st4, st5)} "
            f"@cumsum={time_elapsed(st5, st6)} @export={time_elapsed(st7, st8)} "
            f"@async_enrich={time_elapsed(st8, st9)}"
        )

    def _should_export(self, *entity_types):
        return len(set(entity_types).intersection(self.entity_types)) > 0

    def _export_token_balances(self, df: pd.DataFrame, end_block: int) -> pd.DataFrame:
        df = export_token_balances(df)

        for col in df.columns:
            if col.endswith("_st"):
                df[col + "_day"] = df[col].apply(self._to_st_day)  # type: ignore

        df["blknum"] = end_block
        df["decimals"] = df.token_address.apply(self._get_token_decimals)

        return df

    def _get_token_decimals(self, token_address):
        token = self.token_service.get_token(token_address)
        if token:
            return token.decimals
        return None

    def _export_erc1155_balances(self, df, end_block):
        df = export_erc1155_balances(df)

        for col in df.columns:
            if col.endswith("_st"):
                df[col + "_day"] = df[col].apply(self._to_st_day)  # type: ignore

        df["blknum"] = end_block
        return df.to_dict("records")

    def _to_st_day(self, value):
        if pd.isna(value):
            return None
        return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")

    def _build_topics(self):
        # we need those events only: Transfer/SingleTransfer/BatchTransfer
        # special adapte to WETH
        topics = []
        if self._should_export(
            EntityType.TOKEN_HISTORY_BALANCE, EntityType.TOKEN_LATEST_BALANCE
        ):
            topics.append(TRANSFER_EVENT_TOPIC)
            topics.append(DEPOSIT_EVENT_TOPIC)
            topics.append(WITHDRAWAL_EVENT_TOPIC)
        if self._should_export(
            EntityType.ERC1155_HISTORY_BALANCE, EntityType.ERC1155_LATEST_BALANCE
        ):
            topics.append(TRANSFER_SINGLE_TOPIC)
            topics.append(TRANSFER_BATCH_TOPIC)
        return topics

    def _get_old_balances(self, df: pd.DataFrame) -> pd.DataFrame:
        assert self.item_executor is not None

        fs = []
        for chunk in dynamic_batch_iterator(
            (e[1].to_dict() for e in df.iterrows()), lambda: self.batch_size
        ):
            fs.append(self.item_executor.apply_async(E.execute, args=(chunk,)))
        old_balances = []
        for f in fs:
            old_balances.extend(f.get())

        if len(old_balances) == 0:
            logging.warning(f" #{len(df)} got no old balance")
            old_df = pd.DataFrame(columns=T.columns.keys())
        else:
            old_df = pd.DataFrame(old_balances)

        old_df.rename(
            columns={
                e: "old_" + e
                for e in old_df.columns
                if e not in ("address", "token_address")
            },
            inplace=True,
        )

        df = df.merge(old_df, how="left", on=["address", "token_address"])

        # if old_blknum is greater than new_blknum
        # eg: old block-batch-size(100) > current batch(10)
        # so we can't apply the cumsum relays on an future balance
        df = df[(pd.isna(df["old_blknum"])) | (df.blknum > df["old_blknum"])]
        return df

    def _cumsum_last_balances(self, df: pd.DataFrame) -> pd.DataFrame:
        # no history data found, return this one only
        if "old_blknum" not in df.columns:
            return df

        cond = ~pd.isna(df["old_blknum"])
        for dt in BALANCE_CUMSUM_DTS:
            df.loc[cond, dt] = df.loc[cond].apply(
                lambda row: int(row[dt]) + int(row["old_" + dt]), axis=1
            )

        return df

    def _enrich_balance(self, item):
        priority = int(time())

        # we'll fetch the latest balances for each address, don't store the blknum
        task = {"t": item["token_address"], "a": item["address"]}
        self.async_enrich_queue.push(json.dumps(task), priority=priority)

    def _get_logs(self, start_block, end_block):
        if self.read_log_from == "rpc":
            return self.export_logs(
                start_block, end_block, self.topics, self.token_addresses
            )

        elif self.source_db_engine is not None:
            blocks = self._get_blocks(start_block, end_block)
            min_st: int = min(b["timestamp"] for b in blocks)
            max_st: int = max(b["timestamp"] for b in blocks)
            min_st = datetime.utcfromtimestamp(min_st).strftime("%Y-%m-%d %H:%M:%S")
            max_st = datetime.utcfromtimestamp(max_st).strftime("%Y-%m-%d %H:%M:%S")
            addresses = (
                self.token_addresses if len(self.token_addresses or []) > 0 else None
            )
            topics = self.topics if len(self.topics or []) > 0 else None

            sql = READ_LOG_TEMPLATE.render(
                chain=self.chain,
                st_timestamp=min_st,
                et_timestamp=max_st,
                st_blknum=start_block,
                et_blknum=end_block,
                addresses=addresses,
                topics=topics,
            )
            logging.debug(sql)

            df = pd.read_sql(sql, con=self.source_db_engine)
            if len(df) == 0:
                return []

            df.rename(
                columns={
                    "logpos": "log_index",
                    "txhash": "transaction_hash",
                    "txpos": "transaction_index",
                    "blknum": "block_number",
                },
                inplace=True,
            )
            df["type"] = "log"
            df["block_hash"] = None
            df["topics"] = df.topics.apply(lambda x: x.split(",") if "," in x else [])
            return df.to_dict("records")

        else:
            raise NotImplementedError(
                "get logs only support read from rpc or soruce db"
            )

    @cached(cache=TTLCache(maxsize=4096, ttl=5))
    def _get_blocks(self, start_block, end_block):
        if self.target_db_engine is None:
            blocks = self.export_blocks(start_block, end_block)
        else:
            result = self.target_db_engine.execute(
                f"""
SELECT
    blknum AS number, blkhash AS hash, _st AS timestamp
FROM
    {self.chain}.blocks
WHERE
    blknum >= %s AND blknum <= %s
                """,
                (start_block, end_block),
            )
            rows = result.fetchall()
            # here we don't have the check the block data's consistency,
            # the later enrich will check it.
            blocks = [row._mapping for row in rows]
        return blocks

    def _close(self):
        if self.item_executor is not None:
            self.item_executor.close()


def group_balance_by_token_and_address(df: pd.DataFrame, is_in=True, is_erc1155=False):
    direction = "vin" if is_in else "out"
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
                "blknum": f"{direction}_blocks",
                "txhash": f"{direction}_txs",
                "value": f"{direction}_value",
                f"_{direction}_rank_count": f"{direction}_xfers",
            },
        )
    )

    first_df = df[df[f"_{direction}_rank"] == df[f"_{direction}_rank_min"]][  # type: ignore
        groupby_base + groupby_extra
    ].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"{direction}_1th_st",
            "blknum": f"{direction}_1th_blknum",
        }
    )
    last_df = df[df[f"_{direction}_rank"] == df[f"_{direction}_rank_max"]][  # type: ignore
        groupby_base + groupby_extra
    ].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"{direction}_nth_st",
            "blknum": f"{direction}_nth_blknum",
        }
    )
    merge_on = ["token_address", "address"]
    if is_erc1155:
        merge_on.append("token_id")
    df_grouped = df_grouped.merge(first_df, on=merge_on).merge(last_df, on=merge_on)
    return df_grouped


def merge_balance_with_out_vin_df(
    out_df: pd.DataFrame, vin_df: pd.DataFrame, is_erc1155=False
) -> pd.DataFrame:
    merge_on = ["token_address", "address"]
    if is_erc1155:
        merge_on.append("token_id")

    df = pd.merge(out_df, vin_df, how="outer", on=merge_on)

    # fill those incrementally columns with zero,
    # those columns' default vaule is 0
    zero_columns = {
        c: 0
        for c in df.columns
        if c.endswith("_blocks")
        or c.endswith("_txs")
        or c.endswith("_xfers")
        or c.endswith("_value")
    }
    df.fillna(value=zero_columns, inplace=True)
    df["value"] = df.apply(
        lambda row: int(row["vin_value"]) - int(row["out_value"]), axis=1
    )
    return df


def export_token_balances(df: pd.DataFrame) -> pd.DataFrame:
    df.sort_values(by=["blknum", "logpos"], ascending=True, inplace=True)

    df = partition_rank(
        df,
        group_by=["token_address", "to_address"],
        rank_column="_vin_rank",
    )
    df = partition_rank(
        df,
        group_by=["token_address", "from_address"],
        rank_column="_out_rank",
    )
    out_df = group_balance_by_token_and_address(df, is_in=False)
    vin_df = group_balance_by_token_and_address(df, is_in=True)

    merged = merge_balance_with_out_vin_df(out_df, vin_df, False)
    return merged


def export_erc1155_balances(df: pd.DataFrame):
    df.rename(columns={"id": "token_id"}, inplace=True)
    df.sort_values(by=["blknum", "logpos"], ascending=True, inplace=True)

    df = partition_rank(
        df,
        group_by=["token_address", "token_id", "to_address"],
        rank_column="_vin_rank",
    )
    df = partition_rank(
        df,
        group_by=["token_address", "token_id", "from_address"],
        rank_column="_out_rank",
    )
    out_df = group_balance_by_token_and_address(df, is_in=False, is_erc1155=True)
    vin_df = group_balance_by_token_and_address(df, is_in=True, is_erc1155=True)

    return merge_balance_with_out_vin_df(out_df, vin_df, True)
