import logging
import click
import redis
import time
from threading import Thread


from blockchainetl.cli.utils import global_click_options
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from datetime import datetime, timedelta
from ethereumetl.providers.auto import new_web3_provider
from ethereumetl.service.eth_service import EthService
from blockchainetl.service.redis_stream_service import fmt_redis_key_name

BATCH = 10000
SOFT_SCARD_LIMIT = 10 * BATCH


def srem_entity(chain, redis_url, min_blknum, max_blknum, prefixes, entity_type):
    red = redis.from_url(redis_url)

    for prefix in prefixes:
        result = fmt_redis_key_name(chain, prefix, entity_type)
        scard = red.scard(result)
        logging.info(f"before srem, scard({result})={scard}")
        if scard < SOFT_SCARD_LIMIT:
            logging.info(f"no need to evict {result}")
            break

        for blknum in range(min_blknum, max_blknum, BATCH):
            s = blknum
            e = min(max_blknum, blknum + BATCH)
            sremed = red.srem(result, *list(range(s, e)))
            logging.info(f"srem {s, e} D({result}): #{sremed}")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "-E",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_STREAMING),
    show_default=True,
    type=str,
    help="The list of entity types to load.",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/2",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis conneciton url",
)
@click.option(
    "--redis-result-prefixes",
    type=str,
    default="export-result-,load-result-,load-result-pg-",
    show_default=True,
    help="The Redis sorted set used to store the dumped/loaded block."
    "(This prefix will be put behind the chain, split with comma)",
)
@click.option(
    "--start-block",
    type=int,
    show_default=True,
    help="Start block number(included)",
)
@click.option(
    "--end-block",
    type=int,
    show_default=True,
    help="End block number(excluded)",
)
@click.option(
    "--start-date",
    default=(datetime.utcnow() - timedelta(days=600)).strftime("%Y-%m-%d"),
    type=lambda d: datetime.strptime(d, "%Y-%m-%d"),
    show_default=True,
    help="Start datetime(included)",
)
@click.option(
    "--end-date",
    default=(datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%d"),
    type=lambda d: datetime.strptime(d, "%Y-%m-%d"),
    show_default=True,
    help="End datetime(excluded)",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    type=str,
    help="The URI of the web3 provider e.g. ",
)
def srem(
    chain,
    entity_types,
    redis_url,
    redis_result_prefixes,
    start_block,
    end_block,
    start_date,
    end_date,
    provider_uri,
):
    """Evict old data stored in Redis SSET"""
    entity_types = parse_entity_types(entity_types)

    if None not in (start_block, end_block):
        min_block, max_block = start_block, end_block
    else:
        web3 = new_web3_provider(provider_uri, chain)
        eth_service = EthService(web3)
        try:
            min_block, _ = eth_service.get_block_range_for_date(start_date)
        except Exception:
            min_block = 1
        max_block, _ = eth_service.get_block_range_for_date(end_date)

    prefixes = redis_result_prefixes.split(",")

    logging.info(f"Start to SREM chain: {chain} with block: {min_block, max_block}")
    time.sleep(5)
    threads = [
        Thread(
            target=srem_entity,
            args=(
                chain,
                redis_url,
                min_block,
                max_block,
                prefixes,
                entity_type,
            ),
        )
        for entity_type in entity_types
    ]

    for p in threads:
        p.start()

    for p in threads:
        if p.is_alive():
            p.join()
