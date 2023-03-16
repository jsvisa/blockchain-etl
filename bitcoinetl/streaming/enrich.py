import os
import logging
import pandas as pd
import psycopg2 as psycopg

from blockchainetl.service.redis_stream_service import fmt_redis_key_name
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import (
    EntityType,
    chain_entity_table,
)
from blockchainetl.jobs.redis_consumer_group import RedisConsumerGroup
from .enrich_consumer import BitcoinTraceEnrichConsumer


def enrich_bitcoin_traces(tbl: str, day: str) -> str:
    return f"""
UPDATE {tbl} a
SET
    address = b.address,
    value = b.value,
    vout_cnt = b.vout_cnt,
    vout_type = b.vout_type
FROM {tbl} b
WHERE
    a._st_day = '{day}'
AND b._st_day <= '{day}'
AND a.pxhash = b.txhash
AND a.vout_idx = b.vout_idx
AND a.isin = true
AND b.isin = false
AND a.address is null
AND b.address is not null
"""


def enrich_traces_within_gp(gp_url: str, start_date: str, end_date: str) -> None:
    assert gp_url is not None
    gp = psycopg.connect(gp_url)
    tbl = chain_entity_table(Chain.BITCOIN, EntityType.TRACE)
    with gp.cursor() as cursor:
        for day in list(pd.date_range(start_date, end_date, inclusive="left")):
            st = day.date().strftime("%Y-%m-%d")

            statement = enrich_bitcoin_traces(tbl, st)
            cursor.execute(statement)
            gp.commit()
            logging.info(f"UPDATE {tbl} @{st} rows={cursor.rowcount}")
    gp.close()


def enrich_traces_with_tidb(
    ti_url: str,
    ti_table: str,
    redis_url: str,
    input_stream_prefix: str,
    output_stream_prefix: str,
    input_result_prefix: str,
    output_result_prefix: str,
    consumer_group: str,
    consumer_prefix: str,
    max_workers: int,
    output_path: str,
) -> None:
    assert ti_url is not None

    chain = Chain.BITCOIN
    os.makedirs(output_path, exist_ok=True)

    if ti_url.startswith("mysql://"):
        ti_url = ti_url.replace("mysql://", "mysql+pymysql://")

    entity_type = EntityType.TRACE
    src_stream = fmt_redis_key_name(chain, input_stream_prefix, entity_type)
    dst_stream = fmt_redis_key_name(chain, output_stream_prefix, entity_type)
    src_result = fmt_redis_key_name(chain, input_result_prefix, entity_type)
    dst_result = fmt_redis_key_name(chain, output_result_prefix, entity_type)
    cgroup = f"{chain}:{consumer_group}"

    red_cg = RedisConsumerGroup(
        redis_url,
        src_stream,
        cgroup,
        consumer_prefix,
        workers=max_workers,
        worker_mode="process",
        period_seconds=60,
    )

    consumer = BitcoinTraceEnrichConsumer(
        ti_url,
        ti_table,
        redis_url,
        dst_stream,
        src_result,
        dst_result,
        output_path,
    )

    red_cg.consume(consumer.handler)
