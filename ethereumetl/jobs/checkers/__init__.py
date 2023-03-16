import logging
import psycopg2
from time import time
from typing import Any, Iterable, Optional
from datetime import datetime, timezone

from jinja2 import Template
from sqlalchemy import create_engine

from blockchainetl.utils import chunkify, time_elapsed
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.misc.easy_etl import easy_df_saver
from ethereumetl.misc.easy_etl import easy_etl
from ethereumetl.enumeration.column_type import ColumnType as EthColumnType


class Checker(object):
    diff_rows = None

    def __init__(
        self,
        chain: Chain,
        provider_uri: str,
        output: str,
        db_rw_url: str,
        db_ro_url: Optional[str],
        gp_schema: str,
        is_geth_provider: bool = True,
        batch_size: int = 10,
        max_workers: int = 2,
        by: str = "date",
        stream: bool = False,
        print_sql: bool = False,
        check_transaction_consistency=False,
        ignore_receipt_missing_error=False,
    ):
        self.chain = chain
        self.provider_uri = provider_uri
        self.output = output
        self.gp_conn = psycopg2.connect(db_rw_url)
        self.gp_schema = gp_schema
        self.rw_engine = create_engine(db_rw_url)
        self.ro_engine = create_engine(db_ro_url or db_rw_url)
        self.is_geth_provider = is_geth_provider
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.check_by_date = by == "date"
        self.stream_check = stream
        self.print_sql = print_sql
        self.check_transaction_consistency = check_transaction_consistency
        self.ignore_receipt_missing_error = ignore_receipt_missing_error

    def check(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> bool:
        passed = self._check(st_day, et_day, st, et, st_blk, et_blk)
        if not passed and self.diff_rows is not None:
            x = self._x_column()
            y = self._y_column()
            malformed = [row[y] for row in self.diff_rows or [] if row[x] is None]
            if len(malformed) > 0:
                raise Exception(
                    f"[{st_day}] those blocks doesn't have {x}, but got {y}: {malformed}"
                )

        return passed

    def autofix(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> Any:
        return self._autofix(st_day, et_day, st, et, st_blk, et_blk)

    def execute(
        self,
        template: Template,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
        is_readonly: Optional[bool] = True,
        **kwargs,
    ):
        sd = datetime.strptime(st_day, "%Y-%m-%d")
        if self.check_by_date is True:
            st = int(sd.replace(tzinfo=timezone.utc).timestamp())
            et = st + 86400 - 1

        sql = Template(template).render(
            gp_schema=self.gp_schema,
            check_by_date=self.check_by_date,
            stream_check=self.stream_check,
            st_day=st_day,
            et_day=et_day,
            st=st,
            et=et,
            st_blk=st_blk,
            et_blk=et_blk,
            **kwargs,
        )
        if self.print_sql:
            logging.info(sql)

        engine = self.ro_engine if is_readonly else self.rw_engine
        return engine.execute(sql)

    def easyetl(
        self, st_day: str, entity_type: EntityType, blocks: Iterable[int]
    ) -> int:
        blocks = set(list(blocks))
        if len(blocks) == 0:
            return 0

        block_ranges = chunkify(blocks, self.batch_size)

        logging.info(
            f"Need to insert #{len(blocks)} missing blocks @{st_day} for entity type: {entity_type}"
        )

        df_saver = easy_df_saver(
            self.chain,
            entity_type,
            EthColumnType(),
            gp_conn=self.gp_conn,
            gp_schema=self.chain,
            gp_table=entity_type,
            load_into_db=True,
        )

        def etl(start_block, end_block):
            easy_etl(
                self.chain,
                self.provider_uri,
                (start_block, end_block),
                entity_type,
                self.output,
                batch_size=self.batch_size,
                max_workers=self.max_workers,
                df_saver=df_saver,
                is_geth_provider=self.is_geth_provider,
                check_transaction_consistency=self.check_transaction_consistency,
                ignore_receipt_missing_error=self.ignore_receipt_missing_error,
            )

        finished = 0
        for block_range in block_ranges:
            start_block, end_block = block_range[0], block_range[-1]
            st = time()
            etl(start_block, end_block)
            finished += len(block_range)
            logging.info(
                f"finish easyetl @{st_day} {start_block, end_block} "
                f"stat: #{finished}/#{len(blocks)} "
                f"for entity type: {entity_type} elapsed: {time_elapsed(st)}"
            )

        return len(blocks)

    def _check(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> bool:
        raise NotImplementedError

    def _autofix(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> bool:
        raise NotImplementedError

    def _x_column(self) -> str:
        raise NotImplementedError

    def _y_column(self) -> str:
        raise NotImplementedError
