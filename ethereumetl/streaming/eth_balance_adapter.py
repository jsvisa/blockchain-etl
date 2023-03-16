import logging
import redis
import json
import pypeln as pl
from time import time
from datetime import datetime

from sqlalchemy import create_engine, Table
from sqlalchemy.engine import Engine
from sqlalchemy.future import select
from sqlalchemy.sql.expression import desc

import pandas as pd
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from multiprocessing.pool import Pool
from rpq.RpqQueue import RpqQueue

from blockchainetl.utils import time_elapsed, dynamic_batch_iterator
from blockchainetl.misc.sqlalchemy_extra import sqlalchemy_engine_builder
from blockchainetl.misc.pandas_extra import partition_rank, vsum
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.streaming.enrich import enrich_transactions
from ethereumetl.streaming.postgres_tables import HISTORY_BALANCES
from ethereumetl.misc.eth_extract_balance import (
    READ_BLOCK_TEMPLATE,
    READ_TX_TEMPLATE,
    READ_TRACE_TEMPLATE,
    READ_TX_AS_TRACE_TEMPLATE,
)
from .eth_base_adapter import EthBaseAdapter
from .eth_block_reward_calculator import EthBlockRewardCalculator
from .utils import fmt_enrich_balance_queue


BALANCE_CUMSUM_DTS = [
    f"{d}_{t}"
    for d in ("out", "vin", "cnb")
    for t in ("blocks", "txs", "xfers", "value")
] + ["value", "fee_value"]


CNB_COLUMNS = [
    "address",
    "cnb_blocks",
    "cnb_txs",
    "cnb_xfers",
    "cnb_value",
    "cnb_1th_st,",
    "cnb_nth_st",
    "cnb_1th_blknum,",
    "cnb_nth_blknum",
    "cnb_1th_st_day,",
    "cnb_nth_st_day",
]


class E:
    engine: Engine = None

    @staticmethod
    def _exec(row: Dict, t: Table) -> Dict:
        E.engine.pool = E.engine.pool.recreate()
        with E.engine.connect() as conn:
            query = (
                select(t)
                .where((t.c.address == row["address"]) & (t.c.blknum <= row["blknum"]))
                .order_by(desc(t.c.blknum))
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
    def execute(items: List[Dict], t: Table) -> List[Dict]:
        threads = min(max(1, len(items) // 10), 500)

        result = []
        with concurrent.futures.ThreadPoolExecutor(threads) as executor:
            futures = []
            for row in items:
                f = executor.submit(E._exec, row, t)
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


class EthBalanceAdapter(EthBaseAdapter):
    def __init__(
        self,
        source_db_url: str,
        target_db_url: str,
        target_dbschema: str,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=2,
        max_workers=5,
        entity_types={EntityType.LATEST_BALANCE, EntityType.HISTORY_BALANCE},
        block_reward_calculator: Optional[EthBlockRewardCalculator] = None,
        read_block_from: str = "rpc",
        read_transaction_from: str = "rpc",
        ignore_trace_notmatched_error: bool = False,
        ignore_transaction_notmatched_error: bool = False,
        use_transaction_instead_of_trace: bool = False,
        async_enrich_balance: bool = False,
        async_enrich_redis_url: Optional[str] = None,
    ):
        if EntityType.HISTORY_BALANCE in entity_types:
            assert (
                target_db_url is not None
            ), "entity.history_balance requires target_db_url"

        self.entity_types = entity_types
        self.source_db_url = source_db_url
        self.target_db_url = target_db_url
        self.target_dbschema = target_dbschema
        self.block_reward_calculator = block_reward_calculator
        self.read_block_from = read_block_from
        self.read_transaction_from = read_transaction_from
        self.ignore_trace_notmatched_error = ignore_trace_notmatched_error
        self.ignore_transaction_notmatched_error = ignore_transaction_notmatched_error
        self.use_transaction_instead_of_trace = use_transaction_instead_of_trace

        if async_enrich_balance is True:
            if async_enrich_redis_url is None:
                raise ValueError("async enrich balance need redis_url")
            self.async_enrich_queue = RpqQueue(
                redis.from_url(async_enrich_redis_url),
                fmt_enrich_balance_queue(chain, "ether"),
            )
        self.async_enrich_balance = async_enrich_balance

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    def _open(self):
        self.source_db_engine = create_engine(self.source_db_url)
        self.target_db_engine = create_engine(self.target_db_url)
        self.item_executor = None
        if self._should_export(EntityType.HISTORY_BALANCE):
            self.item_executor = Pool(
                processes=self.max_workers,
                initializer=E.connect,
                initargs=(self.target_db_url, self.target_dbschema),
            )

    def export_all(self, start_block: int, end_block: int):
        st0 = time()
        min_st, max_st, block_txs, txs = self._read_blocks(start_block, end_block)
        st1 = time()

        if block_txs == 0 and self.block_reward_calculator is None:
            return

        tx_df = pd.DataFrame()
        if block_txs > 0:
            tx_df = self._read_txs(start_block, end_block, min_st, max_st, txs)
            tx_txs = len(tx_df)
            if block_txs != tx_txs:
                msg = (
                    f"tx count for block: {[start_block, end_block]} not match, "
                    f"block_txs: {block_txs} tx_txs: {tx_txs}"
                )
                if self.ignore_transaction_notmatched_error:
                    logging.error(msg)
                else:
                    raise ValueError(msg)
        st2 = time()

        trace_df = pd.DataFrame()
        if block_txs > 0:
            trace_df = self._read_traces(start_block, end_block, min_st, max_st)
            trace_txs = len(set(trace_df["txhash"]))
            if trace_txs != block_txs:
                msg = (
                    f"trace count for block: {[start_block, end_block]} not match, "
                    f"block_txs: {block_txs} trace_txs: {trace_txs}"
                )
                if self.ignore_trace_notmatched_error:
                    logging.error(msg)
                else:
                    raise ValueError(msg)

        st3 = time()

        # df contains a list of address {vin,out,fee}_{value,txs,xfers} ...
        df: pd.DataFrame = self._export_balances(
            start_block, end_block, tx_df, trace_df
        )
        st4 = time()

        all_items = []

        if block_txs > 0 and self._should_export(EntityType.LATEST_BALANCE):
            items = df.assign(type=EntityType.LATEST_BALANCE).to_dict("records")
            all_items.extend(items)

        st5 = time()
        if block_txs > 0 and self._should_export(EntityType.HISTORY_BALANCE):
            df = self._get_old_balances(df, HISTORY_BALANCES)
        st6 = time()
        if block_txs > 0 and self._should_export(EntityType.HISTORY_BALANCE):
            df = self._cumsum_balances(df)

        st7 = time()
        if block_txs > 0 and self._should_export(EntityType.HISTORY_BALANCE):
            items = df.assign(type=EntityType.HISTORY_BALANCE).to_dict("records")
            all_items.extend(items)

        st8 = time()
        self.item_exporter.export_items(all_items)
        st9 = time()

        if self.async_enrich_balance is True:
            pl.thread.each(self._enrich_balance, all_items, workers=10, run=True)
        st10 = time()

        logging.info(
            f"PERF export blocks=({start_block}, {end_block}) #size={len(all_items)} "
            f"@total={time_elapsed(st0, st10)} @rpc={time_elapsed(st0, st1)} "
            f"@read_tx={time_elapsed(st1, st2)} "
            f"@read_trace={time_elapsed(st2, st3)} "
            f"@calculate={time_elapsed(st3, st4)} "
            f"@to_dict={time_elapsed(st4, st5)} + {time_elapsed(st7, st8)} "
            f"@get_old={time_elapsed(st5, st6)} @cumsum={time_elapsed(st6, st7)} "
            f"@export={time_elapsed(st8, st9)} "
            f"@async_enrich={time_elapsed(st9, st10)}"
        )

    def _read_blocks(
        self, start_block: int, end_block: int
    ) -> Tuple[int, int, int, Optional[List]]:
        if self.read_block_from == "rpc":
            if self.read_transaction_from == "rpc":
                blocks, txs = self.export_blocks_and_transactions(
                    start_block, end_block
                )
            else:
                blocks = self.export_blocks(start_block, end_block)
                txs = None

            min_st: int = min(b["timestamp"] for b in blocks)
            max_st: int = max(b["timestamp"] for b in blocks)
            block_txs = sum(e["transaction_count"] for e in blocks)
            return (
                datetime.utcfromtimestamp(min_st).strftime("%Y-%m-%d %H:%M:%S"),
                datetime.utcfromtimestamp(max_st).strftime("%Y-%m-%d %H:%M:%S"),
                block_txs,
                txs,
            )

        else:
            sql = READ_BLOCK_TEMPLATE.render(
                chain=self.chain,
                st_blknum=start_block,
                et_blknum=end_block,
            )
            result = self.source_db_engine.execute(sql)
            row = result.fetchone()
            return row["min_st"], row["max_st"], row["block_txs"], None

    def _read_txs(
        self,
        start_block: int,
        end_block: int,
        min_timestamp: str,
        max_timestamp: str,
        txs: Optional[List[Dict]],
    ) -> pd.DataFrame:
        if self.read_transaction_from == "rpc":
            receipts = self.export_receipts(txs)
            tx_receipts = enrich_transactions(txs, receipts)
            for tx in tx_receipts:
                gas_price = tx.get("receipt_effective_gas_price")
                if gas_price is None:
                    gas_price = tx.get("gas_price")
                tx["fee_value"] = gas_price * tx["receipt_gas_used"]
            df: pd.Dataframe = pd.DataFrame(tx_receipts, dtype=object)
            df: pd.Dataframe = df.rename(
                columns={"hash": "txhash", "receipt_gas_used": "gas_used"}
            )

        else:
            sql = READ_TX_TEMPLATE.render(
                chain=self.chain,
                st_timestamp=min_timestamp,
                et_timestamp=max_timestamp,
                st_blknum=start_block,
                et_blknum=end_block,
            )
            df = pd.read_sql(sql, con=self.source_db_engine)
        return df

    def _read_traces(
        self, start_block: int, end_block: int, min_timestamp: str, max_timestamp: str
    ) -> pd.DataFrame:

        if self.use_transaction_instead_of_trace is True:
            sql = READ_TX_AS_TRACE_TEMPLATE.render(
                chain=self.chain,
                st_timestamp=min_timestamp,
                et_timestamp=max_timestamp,
                st_blknum=start_block,
                et_blknum=end_block,
            )
        else:
            sql = READ_TRACE_TEMPLATE.render(
                chain=self.chain,
                st_timestamp=min_timestamp,
                et_timestamp=max_timestamp,
                st_blknum=start_block,
                et_blknum=end_block,
            )

        return pd.read_sql(sql, con=self.source_db_engine)

    def _should_export(self, entity_type):
        return entity_type in self.entity_types

    def _export_balances(
        self,
        start_block: int,
        end_block: int,
        tx_df: pd.DataFrame,
        trace_df: pd.DataFrame,
    ) -> pd.DataFrame:
        fee_df: pd.DataFrame = (
            tx_df.groupby("from_address")
            .agg({"fee_value": vsum})
            .reset_index()  # type: ignore
            .rename(columns={"from_address": "address"})  # type: ignore
            .astype(object)  # type: ignore
        )
        if self.block_reward_calculator is not None:
            cnb_df = self.block_reward_calculator.calculate(start_block, end_block)
        else:
            cnb_df = pd.DataFrame(columns=CNB_COLUMNS)

        df = export_balances(trace_df, fee_df, cnb_df)

        for col in df.columns:
            if col.endswith("_st"):
                df[col + "_day"] = df[col].apply(self._to_st_day)  # type: ignore

        df["blknum"] = end_block

        return df

    def _get_old_balances(self, df: pd.DataFrame, t: Table) -> pd.DataFrame:
        assert self.item_executor is not None

        fs = []
        for chunk in dynamic_batch_iterator(
            (e[1].to_dict() for e in df.iterrows()), lambda: self.batch_size
        ):
            fs.append(self.item_executor.apply_async(E.execute, args=(chunk, t)))
        old_balances = []
        for f in fs:
            old_balances.extend(f.get())

        if len(old_balances) == 0:
            logging.warning(f"balance of #{len(df)} got no old balance")
            old_df = pd.DataFrame(columns=t.columns.keys(), dtype=object)
        else:
            old_df = pd.DataFrame(old_balances, dtype=object)

        old_df.rename(
            columns={e: "old_" + e for e in old_df.columns if e != "address"},
            inplace=True,
        )

        df = df.merge(old_df, how="left", on=["address"])

        # if old_blknum is greater than new_blknum
        # eg: old block-batch-size(100) > current batch(10)
        # so we can't apply the cumsum relays on an future balance
        df = df[(pd.isna(df["old_blknum"])) | (df.blknum > df.old_blknum)]
        return df

    def _cumsum_balances(self, df: pd.DataFrame) -> pd.DataFrame:
        # no history data found, return this one only
        if "old_blknum" not in df.columns:
            return df

        cond = ~pd.isna(df["old_blknum"])
        for dt in BALANCE_CUMSUM_DTS:
            df.loc[cond, dt] = df.loc[cond].apply(
                lambda row: int(row[dt]) + int(row["old_" + dt]), axis=1
            )

        return df

    def _to_st_day(self, value):
        if pd.isna(value):
            return None
        return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")

    def _enrich_balance(self, item):
        priority = int(time())

        # we'll fetch the latest balances for each address, don't store the blknum
        task = {"a": item["address"]}
        self.async_enrich_queue.push(json.dumps(task), priority=priority)

    def _close(self):
        self.source_db_engine.dispose()
        self.target_db_engine.dispose()
        if self.item_executor is not None:
            self.item_executor.dispose()


def group_balance_by_address(df: pd.DataFrame, is_in=True):
    direction = "vin" if is_in else "out"
    address = "to_address" if is_in else "from_address"
    groupby_base = [address]
    groupby_extra = ["_st", "blknum"]

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

    fst_df = df[df[f"_{direction}_rank"] == df[f"_{direction}_rank_min"]][  # type: ignore
        groupby_base + groupby_extra
    ].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"{direction}_1th_st",
            "blknum": f"{direction}_1th_blknum",
        }
    )
    nst_df = df[df[f"_{direction}_rank"] == df[f"_{direction}_rank_max"]][  # type: ignore
        groupby_base + groupby_extra
    ].rename(  # type: ignore
        columns={
            address: "address",
            "_st": f"{direction}_nth_st",
            "blknum": f"{direction}_nth_blknum",
        }
    )
    merge_on = ["address"]
    df_grouped = df_grouped.merge(fst_df, on=merge_on).merge(nst_df, on=merge_on)
    return df_grouped


def merge_balance(
    out_df: pd.DataFrame,
    vin_df: pd.DataFrame,
    fee_df: pd.DataFrame,
    cnb_df: pd.DataFrame,
) -> pd.DataFrame:
    on = ["address"]
    df = (
        out_df.merge(vin_df, how="outer", on=on)
        .merge(fee_df, how="outer", on=on)
        .merge(cnb_df, how="outer", on=on)
    )

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

    def _cal(row):
        return str(
            0
            + int(row["vin_value"])
            + int(row["cnb_value"])
            - int(row["out_value"])
            - int(row["fee_value"])
        )

    df["value"] = df.apply(_cal, axis=1)
    return df


def export_balances(
    df: pd.DataFrame, fee_df: pd.DataFrame, cnb_df: pd.DataFrame
) -> pd.DataFrame:
    df.sort_values(by=["blknum", "txpos"], ascending=True, inplace=True)

    df = partition_rank(df, group_by=["to_address"], rank_column="_vin_rank")
    df = partition_rank(df, group_by=["from_address"], rank_column="_out_rank")
    out_df = group_balance_by_address(df, is_in=False)
    vin_df = group_balance_by_address(df, is_in=True)

    merged = merge_balance(out_df, vin_df, fee_df, cnb_df)
    return merged
