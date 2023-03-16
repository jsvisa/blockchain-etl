import logging
from typing import Callable, List, Dict, Any
import concurrent.futures

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from blockchainetl.utils import dynamic_batch_iterator


def sqlalchemy_engine_builder(
    connection_url: str, dbschema: str, print_sql: bool = False
):
    connect_args = {"options": "-csearch_path={}".format(dbschema)}

    def builder(pool_size: int, pool_overflow: int):
        engine = sa.create_engine(
            connection_url,
            echo=print_sql,
            pool_size=pool_size,
            max_overflow=pool_overflow,
            pool_recycle=3600,
            connect_args=connect_args,
        )
        return engine

    return builder


def execute_in_threadpool(
    enginer: Callable[[int, int], Engine],
    func: Callable[[Engine, List[Dict]], Any],
    items: List[Dict],
    pool_maxsize=50,
    pool_overflow=5,
    batch_size=10,
) -> List[Any]:
    threads = min(max(1, len(items) // 10), 500)
    pool_size = max(threads // 2, pool_maxsize)
    pool_overflow = pool_overflow
    engine = enginer(pool_size, pool_overflow)

    result = []
    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        futures = []
        for chunk in dynamic_batch_iterator(items, lambda: batch_size):
            f = executor.submit(func, engine, chunk)
            futures.append(f)
        for f in concurrent.futures.as_completed(futures):
            exception = f.exception()
            if exception:
                logging.error(exception)
                raise Exception(exception)
            result.append(f.result())
    engine.dispose()
    return result
