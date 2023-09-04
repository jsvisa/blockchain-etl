import logging
from time import time
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.future import select
from sqlalchemy.sql.expression import desc


import pandas as pd
from typing import Optional, Callable, Dict, List, NamedTuple
import concurrent.futures

try:
    from loky import get_reusable_executor as get_executor  # type: ignore
except Exception:
    from concurrent.futures import ThreadPoolExecutor

    get_executor = lambda **kwargs: ThreadPoolExecutor(
        max_workers=kwargs.get("max_workers")
    )


from jinja2 import Template
from blockchainetl.utils import time_elapsed, dynamic_batch_iterator
from blockchainetl.misc.sqlalchemy_extra import (
    sqlalchemy_engine_builder,
    execute_in_threadpool,
)
from blockchainetl.enumeration.chain import Chain
from bitcoinetl.streaming.postgres_tables import HISTORY_BALANCES as T
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.service.btc_service import BtcService


READ_TRACE_TEMPLATE = Template(
    """
WITH traces AS (
    SELECT
        *
    FROM
        "{{chain}}".traces
    WHERE
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
        AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
),
cnb_stats AS (
    SELECT
        address AS cnb_address,
        count(distinct blknum) AS cnb_blocks,
        count(distinct txhash) AS cnb_txs,
        count(*) AS cnb_xfers,
        sum(value) AS cnb_value,
        min(_st) AS cnb_1th_st,
        max(_st) AS cnb_nth_st,
        min(blknum) AS cnb_1th_blknum,
        max(blknum) AS cnb_nth_blknum,
        min(_st_day) AS cnb_1th_st_day,
        max(_st_day) AS cnb_nth_st_day
    FROM
        traces
    WHERE
        iscoinbase is true
    GROUP BY
        1
),
vin_stats AS (
    SELECT
        address AS vin_address,
        count(distinct blknum) AS vin_blocks,
        count(distinct txhash) AS vin_txs,
        count(*) AS vin_xfers,
        sum(value) AS vin_value,
        min(_st) AS vin_1th_st,
        max(_st) AS vin_nth_st,
        min(blknum) AS vin_1th_blknum,
        max(blknum) AS vin_nth_blknum,
        min(_st_day) AS vin_1th_st_day,
        max(_st_day) AS vin_nth_st_day
    FROM
        traces
    WHERE
        iscoinbase is false
        AND isin is false
    GROUP BY
        1
),
out_stats AS (
    SELECT
        address AS out_address,
        count(distinct blknum) AS out_blocks,
        count(distinct txhash) AS out_txs,
        count(*) AS out_xfers,
        sum(value) AS out_value,
        min(_st) AS out_1th_st,
        max(_st) AS out_nth_st,
        min(blknum) AS out_1th_blknum,
        max(blknum) AS out_nth_blknum,
        min(_st_day) AS out_1th_st_day,
        max(_st_day) AS out_nth_st_day
    FROM
        traces
    WHERE
        iscoinbase is false
        AND isin is true
    GROUP BY
        1
),

inout_flows AS (
    SELECT
        COALESCE(A.cnb_address, B.vin_address, C.out_address) AS address,
        A.*,
        B.*,
        C.*
    FROM
        cnb_stats A
    FULL JOIN vin_stats B ON A.cnb_address = B.vin_address
    FULL JOIN out_stats C ON A.cnb_address = C.out_address
)

-- FULL JOIN may return multi columns for the same address(_st_day maybe null)
SELECT
    *,
    vin_value + cnb_value - out_value AS value
FROM (
    SELECT
        address,
        {{et_blknum}} AS blknum,

        sum(COALESCE(out_blocks, 0))::BIGINT AS out_blocks,
        sum(COALESCE(vin_blocks, 0))::BIGINT AS vin_blocks,
        sum(COALESCE(cnb_blocks, 0))::BIGINT AS cnb_blocks,

        sum(COALESCE(out_txs, 0))::BIGINT AS out_txs,
        sum(COALESCE(vin_txs, 0))::BIGINT AS vin_txs,
        sum(COALESCE(cnb_txs, 0))::BIGINT AS cnb_txs,

        sum(COALESCE(out_xfers, 0))::BIGINT AS out_xfers,
        sum(COALESCE(vin_xfers, 0))::BIGINT AS vin_xfers,
        sum(COALESCE(cnb_xfers, 0))::BIGINT AS cnb_xfers,
        sum(COALESCE(out_value, 0))::BIGINT AS out_value,
        sum(COALESCE(vin_value, 0))::BIGINT AS vin_value,
        sum(COALESCE(cnb_value, 0))::BIGINT AS cnb_value,

        min(out_1th_st) AS out_1th_st,
        min(vin_1th_st) AS vin_1th_st,
        min(cnb_1th_st) AS cnb_1th_st,
        max(out_nth_st) AS out_nth_st,
        max(vin_nth_st) AS vin_nth_st,
        max(cnb_nth_st) AS cnb_nth_st,

        min(out_1th_blknum) AS out_1th_blknum,
        min(vin_1th_blknum) AS vin_1th_blknum,
        min(cnb_1th_blknum) AS cnb_1th_blknum,
        max(out_nth_blknum) AS out_nth_blknum,
        max(vin_nth_blknum) AS vin_nth_blknum,
        max(cnb_nth_blknum) AS cnb_nth_blknum,

        min(out_1th_st_day) AS out_1th_st_day,
        min(vin_1th_st_day) AS vin_1th_st_day,
        min(cnb_1th_st_day) AS cnb_1th_st_day,
        max(out_nth_st_day) AS out_nth_st_day,
        max(vin_nth_st_day) AS vin_nth_st_day,
        max(cnb_nth_st_day) AS cnb_nth_st_day
    FROM
        inout_flows
    GROUP BY
        1
) _
    """
)


class BtcBalanceAdapter:
    def __init__(
        self,
        source_db_url: str,
        bitcoin_rpc: BitcoinRpc,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.BITCOIN,
        batch_size=2,
        max_workers=5,
        entity_types={EntityType.LATEST_BALANCE, EntityType.HISTORY_BALANCE},
        target_db_url: Optional[str] = None,
        target_dbschema: Optional[str] = None,
    ):
        if EntityType.HISTORY_BALANCE in entity_types:
            assert (
                target_db_url is not None
            ), "entity.history_balance requires target_db_url"

        self.db_engine = create_engine(source_db_url)
        self.bitcoin_service = BtcService(bitcoin_rpc)
        self.chain = chain
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.target_db_url = target_db_url or ""
        self.target_dbschema = target_dbschema or ""

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self) -> int:
        current_block = self.bitcoin_service.bitcoin_rpc.getblockcount()
        if current_block is None:
            raise ValueError("current block is none")
        return current_block

    def export_all(self, start_block: int, end_block: int):
        # Bitcoin's block height and time do not correspond exactly
        # {
        #     64496: (1278374513, "2010-07-06 00:01:53"),
        #     64497: (1278374948, "2010-07-06 00:09:08"),
        #     64498: (1278375471, "2010-07-06 00:17:51"),
        #     64499: (1278373874, "2010-07-05 23:51:14"),
        #     64500: (1278376004, "2010-07-06 00:26:44"),
        #     64501: (1278376052, "2010-07-06 00:27:32"),
        # }
        # So we can't simply get date via start and end block

        st0 = time()
        blocks = self.bitcoin_service.get_blocks(range(start_block, end_block + 1))

        st1 = time()
        trace_df = self._read_traces(
            start_block,
            end_block,
            min(b.timestamp for b in blocks),
            max(b.timestamp for b in blocks),
        )

        all_items = []
        st2 = time()
        if len(trace_df) > 0 and self._should_export(EntityType.LATEST_BALANCE):
            trace_df["type"] = EntityType.LATEST_BALANCE
            all_items.extend(trace_df.to_dict("records"))

        if len(trace_df) > 0 and self._should_export(EntityType.HISTORY_BALANCE):
            df = self._cumsum_last_balances(trace_df)
            df["type"] = EntityType.HISTORY_BALANCE
            all_items.extend(df.to_dict("records"))

        st3 = time()
        self.item_exporter.export_items(all_items)
        st4 = time()

        logging.info(
            f"PERF export blocks=({start_block}, {end_block}) #size={len(all_items)} "
            f"@total={time_elapsed(st0, st4)} @rpc={time_elapsed(st0, st1)} @read_trace={time_elapsed(st1, st2)} "
            f"@read_history={time_elapsed(st2, st3)} @export={time_elapsed(st3, st4)} "
            f"@export-avg-kreq={round((st4-st3)*1000/(len(all_items)+1), 4)}s"
        )

    def _read_traces(
        self, start_block: int, end_block: int, min_timestamp: int, max_timestamp: int
    ) -> pd.DataFrame:
        def _fmt(st):
            return datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")

        sql = READ_TRACE_TEMPLATE.render(
            chain=self.chain,
            st_day=_fmt(min_timestamp),
            et_day=_fmt(max_timestamp),
            st_blknum=start_block,
            et_blknum=end_block,
        )

        return pd.read_sql(sql, con=self.db_engine)

    def _cumsum_last_balances(self, df: pd.DataFrame) -> pd.DataFrame:
        executor = get_executor(max_workers=self.max_workers, timeout=60)
        futures = []

        enginer = sqlalchemy_engine_builder(self.target_db_url, self.target_dbschema)
        for chunk in dynamic_batch_iterator(
            df.itertuples(index=False, name="Balance"), lambda: 1000
        ):
            f = executor.submit(execute, enginer, chunk)
            futures.append(f)

        old_balances = []
        for f in concurrent.futures.as_completed(futures):
            exception = f.exception()
            if exception:
                logging.error(exception)
                raise Exception(exception)
            old_balances.extend(f.result())

        if len(old_balances) == 0:
            return df

        old_df = pd.DataFrame(old_balances)
        old_df.rename(
            columns={e: "old_" + e for e in old_df.columns if e != "address"},
            inplace=True,
        )

        df = df.merge(old_df, how="left", on=["address"])
        df = df.query("blknum != old_blknum")
        df["out_blocks"] = df["out_blocks"] + df["old_out_blocks"]
        dts = [
            (d, t)
            for d in ("out", "vin", "cnb")
            for t in ("blocks", "txs", "xfers", "value")
        ]
        for d, t in dts:
            df["f{d}_{t}"] = df[f"{d}_{t}"] + df[f"old_{d}_{t}"]

        columns = set(T.c.keys())
        columns -= {"id", "created_at", "updated_at"}
        df["value"] = df["value"] + df["old_value"]

        return df[list(columns)]

    def _should_export(self, entity_type):
        return entity_type in self.entity_types

    def close(self):
        self.item_exporter.close()


class Balance(NamedTuple):
    address: str
    blknum: int


def execute(enginer: Callable[[int, int], Engine], items: List[Dict]) -> List[Dict]:
    def _exec(engine: Engine, chunk: List[Balance]) -> Dict:
        row = chunk[0]
        with engine.connect() as conn:
            query = (
                select(T)
                .where((T.c.address == row.address) & (T.c.blknum <= row.blknum))
                .order_by(desc(T.c.blknum))
                .limit(1)
            )
            r = conn.execute(query).fetchone()
            if r is not None:
                return r._asdict()
            return None

    result = execute_in_threadpool(enginer, _exec, items, batch_size=1)
    result = [e for e in result if e is not None]
    return result
