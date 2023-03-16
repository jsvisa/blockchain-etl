import os
import logging
import pandas as pd
import concurrent.futures
from itertools import groupby
from datetime import datetime
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional

from blockchainetl.enumeration.chain import Chain
from blockchainetl.misc.pd_write_file import save_df_into_file
from blockchainetl.enumeration.column_type import ColumnType
from bitcoinetl.enumeration.column_type import ColumnType as BtcColumnType
from ethereumetl.enumeration.column_type import ColumnType as EthColumnType


class PandasItemExporter:
    def __init__(
        self,
        chain: Chain,
        output_dir: Optional[str],
        notify_callback=None,
        output_file: Optional[str] = None,
        save: bool = False,
        engine: Optional[Engine] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        column_type=None,
    ):
        if save is True:
            assert None not in (
                engine,
                schema,
                table,
            ), "save without engine/schema/table"

        self._chain = chain
        self._base_dir = output_dir
        self._notify_callback = notify_callback
        self._output_file = output_file
        self._save = save
        self._engine = engine
        self._schema = schema
        self._table = table
        self._ct = column_type

        if self._base_dir is not None:
            os.makedirs(self._base_dir, exist_ok=True)

    def open(self):
        pass

    def export_items(self, items: List[Dict]):
        # in some cases, the items is empty,
        # such as entity-type is receipt without transaction,
        # then the output is empty
        if len(items) == 0:
            return

        def keyfunc(item):
            return item["type"]

        item0 = items[0]
        blknum = item0.get("blknum", item0.get("number", item0.get("block_number")))
        st = item0.get("_st", item0.get("timestamp", item0.get("block_timestamp")))
        if st is None:
            raise ValueError(
                f"the field `timestamp`/`block_timestamp` not given for items: {items}"
            )

        base_dir = None
        if self._base_dir is not None:
            # use the time of the first row of this batch
            st_day = datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")
            base_dir = os.path.join(self._base_dir, st_day)
            os.makedirs(base_dir, exist_ok=True)

        items = sorted(items, key=keyfunc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    self.export_kind_items,
                    base_dir,
                    blknum,
                    entity,
                    list(elements),  # TODO: why we need to extract the elements?
                ): entity
                for entity, elements in groupby(items, key=keyfunc)
            }

            for future in concurrent.futures.as_completed(futures):
                entity = futures[future]
                output = future.result()
                if self._notify_callback is not None and output is not None:
                    self._notify_callback(blknum, entity, output)

    def export_kind_items(
        self,
        base_dir: Optional[str],
        block_num: str,
        entity_type: str,
        items: List[Dict],
    ) -> Optional[str]:
        if len(items) == 0:
            logging.info("items is empty")
            return None

        df = pd.DataFrame(items)
        if self._chain in Chain.ALL_BITCOIN_FORKS:
            df = BtcColumnType.apply_bitcoin_df(df, entity_type)
        elif self._chain in Chain.ALL_ETHEREUM_FORKS:
            df = EthColumnType.apply_ethereum_df(df, entity_type)

        df = ColumnType.apply_global_df(df)

        if base_dir is not None:
            base_dir = os.path.join(base_dir, entity_type)
            os.makedirs(base_dir, exist_ok=True)
            output = os.path.join(base_dir, f"{block_num}.csv")
        else:
            output = self._output_file

        if self._ct:
            df = df.astype(self._ct.astype(entity_type))

        if output is not None:
            save_df_into_file(
                df,
                output,
                columns=self._ct[entity_type] if self._ct else df.columns,
                types=None,
                entity_type=entity_type,
            )

        if self._save:
            df.to_sql(
                self._table,
                schema=self._schema,
                con=self._engine,
                method="multi",
                if_exists="append",
                index=False,
                chunksize=1000,
            )
            logging.info(f"save #{df.shape[0]} to {self._table}")

        return output

    def close(self):
        pass
