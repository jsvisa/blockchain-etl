import os
import logging
import pandas as pd
from itertools import groupby
from datetime import datetime
from time import time
import concurrent.futures

from typing import List, Dict, Optional
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.column_type import ColumnType
from blockchainetl.misc.pd_write_file import save_df_into_file
from blockchainetl.utils import time_elapsed
from bitcoinetl.enumeration.column_type import ColumnType as BtcColumnType
from ethereumetl.enumeration.column_type import ColumnType as EthColumnType


class FileItemExporter:
    def __init__(
        self,
        chain: Chain,
        output_dir: Optional[str],
        notify_callback=None,
        output_file: Optional[str] = None,
        df_saver=None,
    ):
        assert not (
            output_dir is None and output_file is None
        ), "output_dir or output_file both canot be None"
        self._chain = chain
        self._base_dir = output_dir
        self._notify_callback = notify_callback
        self._output_file = output_file
        self._df_saver = df_saver

        if output_dir is not None and not output_dir.startswith("s3://"):
            os.makedirs(output_dir, exist_ok=True)

        if chain in Chain.ALL_BITCOIN_FORKS:
            self._ct = BtcColumnType()
        elif chain in Chain.ALL_ETHEREUM_FORKS:
            self._ct = EthColumnType()

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

        blknum = items[0].get("number", items[0].get("block_number"))
        st = items[0].get("timestamp", items[0].get("block_timestamp"))
        if st is None:
            raise ValueError(
                f"the field `timestamp`/`block_timestamp` not given for items: {items}"
            )

        base_dir = None
        if self._base_dir is not None:
            # use the time of the first row of this batch
            st_day = datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")
            base_dir = os.path.join(self._base_dir, st_day)
            if not base_dir.startswith("s3://"):
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
                if self._notify_callback is not None:
                    self._notify_callback(blknum, entity, output)

    def to_df(self, key: EntityType, items: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(items, dtype=object)
        if self._chain in Chain.ALL_BITCOIN_FORKS:
            df = BtcColumnType.apply_bitcoin_df(df, key)
        elif self._chain in Chain.ALL_ETHEREUM_FORKS:
            df = EthColumnType.apply_ethereum_df(df, key)
        else:
            raise ValueError(f"chain({self._chain}) not supported")

        df = ColumnType.apply_global_df(df)

        return df

    def export_kind_items(
        self,
        base_dir: Optional[str],
        block_num: int,
        entity_type: str,
        items: List[Dict],
    ) -> str:
        st0 = time()
        df = self.to_df(entity_type, items)
        st1 = time()

        if base_dir is not None:
            base_dir = os.path.join(base_dir, entity_type)
            if not base_dir.startswith("s3://"):
                os.makedirs(base_dir, exist_ok=True)
            output = os.path.join(base_dir, f"{block_num}.csv")
        else:
            output = self._output_file

        save_df_into_file(
            df,
            output,
            columns=self._ct[entity_type],
            types=self._ct.astype(entity_type),
            entity_type=entity_type,
        )

        st2 = time()
        if len(df) > 1024:
            logging.info(
                f"PERF save file={output} lines=#{len(df)} "
                f"@to_df={time_elapsed(st0,st1)}s @to_file={time_elapsed(st1,st2)}s"
            )

        if self._df_saver is not None:
            self._df_saver(df, block_num, entity_type)

        return output

    def close(self):
        pass
