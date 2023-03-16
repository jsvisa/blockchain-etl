import logging
import concurrent.futures
from functools import lru_cache
from multiprocessing.pool import Pool
from typing import List, Dict

from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql.dml import Insert

from blockchainetl.utils import dynamic_batch_iterator
from blockchainetl.misc.sqlalchemy_extra import sqlalchemy_engine_builder
from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


class PostgresItemExporter:
    def __init__(
        self,
        connection_url,
        dbschema,
        item_type_to_insert_stmt_mapping: Dict[str, Insert],
        converters=(),
        print_sql=True,
        workers=2,
        pool_size=5,
        pool_overflow=10,
        batch_size=100,
        multiprocess=False,
    ):
        self.connection_url = connection_url
        self.dbschema = dbschema
        self.item_type_to_insert_stmt_mapping = item_type_to_insert_stmt_mapping
        self.converter = CompositeItemConverter(converters)
        self.print_sql = print_sql
        self.workers = workers
        self.pool_size = pool_size
        self.pool_overflow = pool_overflow
        self.batch_size = batch_size

        self.engine = self.create_engine()
        self.process_mode = multiprocess
        if multiprocess is True:
            self.executor = Pool(processes=workers)

    def open(self):
        pass

    def export_items(self, items: List[Dict]) -> int:
        if len(items) == 0:
            return 0

        items_grouped_by_type = group_by_item_type(items)
        if self.process_mode is True:
            return self._export_items_in_processpool(items_grouped_by_type)
        else:
            return self._export_items_in_threadpool(items_grouped_by_type)

    def _export_items_in_threadpool(self, items_grouped_by_type):
        rowcount = 0
        futures = []
        with concurrent.futures.ThreadPoolExecutor(self.workers) as executor:
            for item_type, insert_stmt in self.item_type_to_insert_stmt_mapping.items():
                item_group = items_grouped_by_type.get(item_type)
                if item_group is None:
                    continue

                converted_items = self.convert_items(item_group)
                for chunk in dynamic_batch_iterator(
                    converted_items, lambda: self.batch_size
                ):
                    f = executor.submit(
                        execute_in_thread, self.engine, insert_stmt, chunk
                    )
                    futures.append(f)
            for f in concurrent.futures.as_completed(futures):
                exception = f.exception()
                if exception:
                    logging.error(exception)
                    raise Exception(exception)
                rowcount += f.result()
        return rowcount

    def _export_items_in_processpool(self, items_grouped_by_type):
        rowcount = 0
        futures = []
        for item_type, insert_stmt in self.item_type_to_insert_stmt_mapping.items():
            item_group = items_grouped_by_type.get(item_type)
            if item_group is None:
                continue

            converted_items = self.convert_items(item_group)
            for chunk in dynamic_batch_iterator(
                converted_items, lambda: self.batch_size
            ):
                f = self.executor.apply_async(
                    execute_in_process,
                    args=(
                        self.connection_url,
                        self.dbschema,
                        self.print_sql,
                        insert_stmt,
                        chunk,
                    ),
                )
                futures.append(f)
        for f in futures:
            result = f.get()
            rowcount += result
        return rowcount

    def export_item(self, item: Dict) -> int:
        item = self.converter.convert_item(item)
        insert_stmt = self.item_type_to_insert_stmt_mapping[item["type"]]
        return execute_in_thread(self.engine, insert_stmt, [item])

    def convert_items(self, items: List[Dict]):
        for item in items:
            yield self.converter.convert_item(item)

    def create_engine(self) -> Engine:
        builder = sqlalchemy_engine_builder(
            self.connection_url, self.dbschema, self.print_sql
        )
        return builder(self.pool_size, self.pool_overflow)

    def close(self):
        self.engine.dispose()


def execute_in_thread(engine: Engine, stmt: Insert, items: List[Dict]) -> int:
    with engine.connect() as conn:
        result = conn.execute(stmt, items)
        return result.rowcount


@lru_cache(maxsize=None)
def connect(url, dbschema, print_sql, pool_size) -> Engine:
    return sqlalchemy_engine_builder(url, dbschema, print_sql)(pool_size, 0)


def execute_in_process(
    target_db_url,
    target_dbschema,
    print_sql,
    stmt: Insert,
    items: List[Dict],
) -> List[Dict]:
    threads = min(max(1, len(items) // 10), 500)
    # engine is lru cached, so don't need to close
    engine = connect(target_db_url, target_dbschema, print_sql, 100)

    def _exec(engine: Engine, chunk: List[Dict]) -> int:
        with engine.connect() as conn:
            return conn.execute(stmt, chunk).rowcount

    result = 0
    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        futures = []
        for row in items:
            f = executor.submit(_exec, engine, row)
            futures.append(f)

        for f in concurrent.futures.as_completed(futures):
            exception = f.exception()
            if exception:
                logging.error(exception)
                raise Exception(exception)
            result += f.result()
    return result
