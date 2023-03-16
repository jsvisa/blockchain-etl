import os
import socket
import logging
from typing import Dict
from multiprocessing import Process

import click
import redis
import pandas as pd
from multicall.service.ether_service import EtherService
from web3 import Web3, HTTPProvider

from blockchainetl.cli.utils import global_click_options
from blockchainetl.utils import time_elapsed
from blockchainetl.misc.requests import make_retryable_session
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.redis_consumer_group import RedisConsumerGroup
from blockchainetl.service.redis_stream_service import (
    fmt_redis_key_name,
    RedisStreamService,
)
from ethereumetl.jobs.export_token_balances_job import ExportTokenBalancesJob


def cpu_count():
    c = os.cpu_count()
    if c:
        return c - 2
    return 4


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the web3 provider",
)
@click.option(
    "-o",
    "--output",
    required=True,
    show_default=True,
    type=str,
    help="The output dir.",
)
@click.option(
    "-b",
    "--batch-size",
    default=10,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
)
@click.option(
    "-w",
    "--max-workers",
    show_default=True,
    type=int,
    default=cpu_count(),
    help="The worker's amount to produce the data",
)
@click.option(
    "--period-seconds",
    default=60,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/4",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis conneciton url",
)
@click.option(
    "--consumer-group",
    type=str,
    default="export-token-balance",
    show_default=True,
    help="The Redis stream consumer name, put it behind the chain",
)
@click.option(
    "--consumer-prefix",
    type=str,
    default=socket.gethostname(),
    show_default=True,
    help="The Redis consumer prefix used to identity consumer",
)
@click.option(
    "--redis-input-stream-prefix",
    type=str,
    default="extract-stream-",
    show_default=True,
    help="The Redis stream used to store notify messages.(Put behind the chain)",
)
@click.option(
    "--redis-input-result-prefix",
    type=str,
    default="export-result-",
    show_default=True,
    help="The Redis result sorted set used to store thee dumped block.(Put behind the chain)",
)
@click.option(
    "--redis-output-stream-prefix",
    type=str,
    default="export-stream-",
    show_default=True,
    help="The Redis output stream, used to produce(Put behind the chain)",
)
@click.option(
    "--redis-output-result-prefix",
    type=str,
    default="export-result-",
    show_default=True,
    help="The Redis result sorted set used to store thee dumped block.(Put behind the chain)",
)
def export_token_balances(
    chain,
    provider_uri,
    output,
    batch_size,
    max_workers,
    period_seconds,
    redis_url,
    consumer_group,
    consumer_prefix,
    redis_input_stream_prefix,
    redis_input_result_prefix,
    redis_output_stream_prefix,
    redis_output_result_prefix,
):
    """Export ERC20/ERC721 token's balances(from redis stream)."""

    # consumes from token_holder, and write into token_balance
    stream = fmt_redis_key_name(
        chain, redis_input_stream_prefix, EntityType.TOKEN_HOLDER
    )
    result = fmt_redis_key_name(
        chain, redis_input_result_prefix, EntityType.TOKEN_HOLDER
    )
    cgroup = f"{chain}:{consumer_group}"
    ignore_errors = (pd.errors.EmptyDataError,)

    def consume(cid):
        red = redis.from_url(redis_url)

        redis_notify = RedisStreamService(
            redis_url, [EntityType.TOKEN_BALANCE]
        ).create_notify(
            chain,
            redis_output_stream_prefix,
            redis_output_result_prefix,
        )
        session = make_retryable_session()
        web3_provider = Web3(HTTPProvider(provider_uri))
        ether_service = EtherService(provider_uri, session=session)

        def handler(_, st: float, keyvals: Dict):
            job_key = list(keyvals.keys())[0].decode()
            job_item = list(keyvals.values())[0].decode()

            if red.sismember(result, job_key):
                logging.info(f"job: {job_key} has handled, skip")
                return

            try:
                job = ExportTokenBalancesJob(
                    web3_provider,
                    ether_service,
                    job_key,
                    job_item,
                    output,
                    batch_size,
                    notify_callback=redis_notify,
                )
                job.run()
            except Exception as e:
                msg = (
                    f"an execution({e}) occurred while processing "
                    f"job-key: {job_key} job-item: {job_item}"
                )
                if type(e) in ignore_errors:
                    msg += " (and ignored)"
                    logging.warning(msg)
                else:
                    raise Exception(msg) from e

            logging.info(
                f"handle job: export-token-balance {str(job_key)} -> {job_item} "
                f"elapsed={time_elapsed(st)}"
            )
            red.sadd(result, job_key)

        red_cg = RedisConsumerGroup(
            redis_url,
            stream,
            cgroup,
            consumer_prefix=f"{consumer_prefix}-{cid}",
            workers=1,
            worker_mode="thread",
            period_seconds=period_seconds,
        )

        red_cg.consume(handler, is_leader=cid == 0)

    threads = [
        Process(
            target=consume,
            args=(cid,),
        )
        for cid in range(0, max_workers)
    ]

    for p in threads:
        p.start()

    for p in threads:
        if p.is_alive():
            p.join()
