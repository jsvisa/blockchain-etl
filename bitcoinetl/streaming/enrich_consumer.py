import os
import redis
import logging
from time import time, sleep
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine

from blockchainetl.enumeration.entity_type import EntityType
from bitcoinetl.enumeration.column_type import ColumnType as BtcColumnType
from blockchainetl.misc.pd_write_file import DEFAULT_FIELD_TERMINATED, save_df_into_file
from blockchainetl.service.redis_stream_service import RED_UNIQUE_STREAM_SCRIPT


SQL_TEMP = """
SELECT
    CONCAT(vout_cnt, '^', vout_type, '^', address, '^', value) AS _a
FROM
    {tbl}
WHERE
    txhash = %(txhash)s
    AND vout_idx = %(vout_idx)s
"""


class BitcoinTraceEnrichConsumer(object):
    def __init__(
        self,
        ti_url: str,
        ti_table: str,
        redis_url: str,
        dst_stream: str,
        src_result: str,
        dst_result: str,
        output_path: str,
    ):
        self._ti_engine = create_engine(ti_url)
        self._red = redis.from_url(redis_url)
        self._sha = self._red.script_load(RED_UNIQUE_STREAM_SCRIPT)
        self._sql_temp = SQL_TEMP.format(tbl=ti_table)
        self._ct = BtcColumnType()

        self._dst_stream = dst_stream
        self._src_result = src_result
        self._dst_result = dst_result
        self._output_path = output_path

    def fetch(self, row) -> str:
        result = self._ti_engine.execute(
            self._sql_temp,
            txhash=row["pxhash"],
            vout_idx=row["vout_idx"],
        ).fetchone()

        if result is None:
            raise ValueError(
                f"failed to read from tidb with blknum={row['blknum']} "
                f"txhash={row['pxhash']} vout_idx={row['vout_idx']}"
            )

        return result[0]

    def handler(self, inited, st: float, keyvals: Dict):
        inited = inited
        blknum = list(keyvals.keys())[0].decode()
        file = list(keyvals.values())[0].decode()

        if self._red.sismember(self._dst_result, blknum):
            logging.info(f"block: {blknum} has already handled, skip")
            return

        ready = False
        # sleep up to 36minutes
        for retry in range(1, 30):
            if self._red.sismember(self._src_result, blknum):
                ready = True
                break
            else:
                logging.warn(f"block: {blknum} is not ready, retry: {retry}")
                sleep(5 * retry)

        if ready is False:
            raise Exception(f"block: {blknum} is not ready, after 5 retry")

        df = pd.read_csv(file, sep=DEFAULT_FIELD_TERMINATED)
        # fix the pyright warning
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"failed to read {file}")

        original_df_shape = df.shape

        pxhash_len = df.loc[df["isin"] == True].shape[0]

        st1 = time()
        if pxhash_len > 0:
            df.loc[df["isin"] == True, "_out"] = df.loc[df["isin"] == True].apply(
                self.fetch, axis=1
            )

            st2 = time()
            df.loc[df["isin"] == True, "vout_cnt"] = df.loc[
                df["isin"] == True, "_out"
            ].apply(lambda xs: int(xs.split("^")[0]))
            df.loc[df["isin"] == True, "vout_type"] = df.loc[
                df["isin"] == True, "_out"
            ].apply(lambda xs: xs.split("^")[1])
            df.loc[df["isin"] == True, "address"] = df.loc[
                df["isin"] == True, "_out"
            ].apply(lambda xs: xs.split("^")[2])
            df.loc[df["isin"] == True, "value"] = df.loc[
                df["isin"] == True, "_out"
            ].apply(lambda xs: int(xs.split("^")[3]))

            df.drop(columns=["_out", "tx_in_value"], inplace=True)

            logging.info(f"enrich {blknum} with file {file}")

            tx_in_df = (
                df.loc[df["isin"] == True]
                .groupby(["txhash"], as_index=False)
                .agg({"value": "sum"})
                .rename(columns={"value": "tx_in_value"})
            )
            df = df.merge(tx_in_df, on="txhash", how="left")
        else:
            st2 = st1

        st3 = time()

        # fix the pyright warning
        assert isinstance(df, pd.DataFrame)
        base_dir = os.path.join(
            self._output_path, df.iloc[0]["_st_day"], EntityType.TRACE
        )
        os.makedirs(base_dir, exist_ok=True)
        outfile = os.path.join(base_dir, f"{blknum}.csv")

        if df.shape != original_df_shape:
            raise ValueError(
                f"enrich {file} rows not match, before({original_df_shape}) != after({df.shape})"
            )

        save_df_into_file(
            df,
            outfile,
            columns=self._ct[EntityType.TRACE],
            types=self._ct.astype(EntityType.TRACE),
            entity_type=EntityType.TRACE,
        )

        st4 = time()
        self._red.evalsha(
            self._sha,
            2,
            self._dst_stream,
            self._dst_result,
            blknum,
            outfile,
        )
        logging.info(
            f"handle [enrich-with-tidb #pxhashes={pxhash_len} srcfile={file} dstfile={outfile} "
            f"elapsed @all={st4-st} @file-read={st1-st} @tidb-read={st2-st1} "
            f"@df-compute={st3-st2} @file-write={st4-st3}"
        )

    def close(self):
        self._ti_engine.dispose()
