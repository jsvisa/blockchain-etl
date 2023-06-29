import os
import logging
import click
import redis
from threading import Thread
from typing import Dict, Optional, Tuple, Set, List

import psycopg2 as psycopg
from psycopg2.extensions import connection


# import psycopg2.extensions
# import psycopg2.extras
# from psycopg2.extensions import connection
# from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor
# from psycopg2.pool import ThreadedConnectionPool


from blockchainetl.utils import time_elapsed
from blockchainetl.cli.utils import (
    global_click_options,
    extract_cmdline_kwargs,
    str2bool,
)
from blockchainetl.enumeration.entity_type import (
    EntityType,
    EntityTable,
    chain_entity_table,
    parse_entity_types,
)
from blockchainetl.enumeration.chain import Chain
from blockchainetl.jobs.redis_consumer_group import RedisConsumerGroup
from blockchainetl.service.redis_stream_service import fmt_redis_key_name
from blockchainetl.streaming.postgres_utils import (
    save_file_into_table,
    ensure_external_load_path,
    external_copy_file_into_redo,
    external_load_files_into_table,
)
from bitcoinetl.enumeration.column_type import ColumnType as BtcColumnType
from ethereumetl.enumeration.column_type import ColumnType as EthColumnType
from ethereumetl.misc.easy_etl import easy_etl as eth_easy_etl

# cache in ~4hours
RESULT_TTL_SECONDS = 14400


class Loader:
    def __init__(
        self,
        chain: str,
        redis_url: str,
        postgres_url: str,
        entity_types: List[str],
        enriched_types: Set[str],
        provider_uri: str,
        provider_is_geth: bool,
        autofix: bool,
        ignore_file_missing_error: bool,
        ignore_postgres_copy_error: bool,
        dev_mode: bool,
        consumer_group: str,
        redis_stream_prefix: str,
        redis_result_prefix: str,
        redis_enriched_stream_prefix: str,
        max_workers: int,
        period_seconds: int,
        minimum_block: Optional[str] = None,
    ):
        self.chain = chain
        self.redis_url = redis_url
        self.postgres_url = postgres_url
        self.entity_types = entity_types
        self.enriched_types = enriched_types
        self.provider_uri = provider_uri
        self.provider_is_geth = provider_is_geth
        self.autofix = autofix
        self.ignore_file_missing_error = ignore_file_missing_error
        self.ignore_postgres_copy_error = ignore_postgres_copy_error
        self.dev_mode = dev_mode
        self.consumer_group = consumer_group
        self.redis_stream_prefix = redis_stream_prefix
        self.redis_result_prefix = redis_result_prefix
        self.redis_enriched_stream_prefix = redis_enriched_stream_prefix
        self.max_workers = max_workers
        self.period_seconds = period_seconds
        self.minimum_blknums = {}
        if minimum_block is not None:
            # eg: block:100,trace:1000
            self.minimum_blknums = {
                e.split(":")[0]: int(e.split(":")[1]) for e in minimum_block.split(",")
            }

        ct = None
        if chain in Chain.ALL_BITCOIN_FORKS:
            ct = BtcColumnType()
        elif chain in Chain.ALL_ETHEREUM_FORKS:
            ct = EthColumnType()
        self.ct = ct

    def spawn_loading_threads(
        self,
        external_types: Set,
        external_load: bool,
        external_load_no_backup: bool,
        external_load_count: int,
        external_load_path: str,
    ):
        et = EntityTable()

        threads = []
        for entity_type in self.entity_types:
            # loading with redis consumer-group
            rp = None

            if external_load is True and entity_type in external_types:
                # file flow: csv -> redo{in batch} -> todo -> INSERT INTO DB --Success-> done/remove
                #                                                            --Failed--> fail
                table = et[entity_type]
                rp, tp, fp, dp = ensure_external_load_path(external_load_path, table)
                src_tbl = self.chain + "." + "ext_" + table
                dst_tbl = self.chain + "." + table

                threads.append(
                    Thread(
                        target=external_load_files_into_table,
                        args=(
                            self.postgres_url,
                            src_tbl,
                            dst_tbl,
                            entity_type,
                            self.ct,
                        ),
                        kwargs=dict(
                            redo_path=rp,
                            todo_path=tp,
                            fail_path=fp,
                            done_path=dp,
                            limit=external_load_count,
                            ignore_error=self.ignore_postgres_copy_error,
                            backup=not external_load_no_backup,
                        ),
                    )
                )
                threads.append(
                    Thread(
                        target=self.external_copy_into_redo_path,
                        args=(entity_type, rp, external_load_count * 2),
                    )
                )
            else:
                threads.append(
                    Thread(target=self.psycopg_copy_into_postgres, args=(entity_type,))
                )
        return threads

    def external_copy_into_redo_path(
        self, entity_type: str, redo_path: str, copy_limit: int
    ):
        cgroup = f"{self.chain}:{self.consumer_group}"
        stream, result, _ = self._stream_result_table_of_entity(entity_type)
        red = redis.from_url(self.redis_url)

        def handler(_, __, keyvals: Dict):
            blk, file = self._decode_task(keyvals)
            result_key = f"{result}:{blk}"

            # we store the blk in result as sorted-set previously
            # adjusted to use ttl key after 2022.09.22
            if red.get(result_key) is not None or red.sismember(result, blk):
                logging.info(f"block: {blk} has handled, skip")
                return

            if self.ignore_file_missing_error is True and not self._file_exists(file):
                logging.warning(f"file: {file} missed, skip")
                return

            self.check_and_autofix_block(entity_type, int(blk), file)

            external_copy_file_into_redo(file, redo_path, copy_limit)
            red.setex(result_key, RESULT_TTL_SECONDS, 1)

        red_cg = RedisConsumerGroup(
            self.redis_url,
            stream,
            cgroup,
            workers=self.max_workers,
            worker_mode="thread",
            period_seconds=self.period_seconds,
        )

        red_cg.consume(handler)

    def psycopg_copy_into_postgres(self, entity_type: str):
        cgroup = f"{self.chain}:{self.consumer_group}"
        stream, result, table = self._stream_result_table_of_entity(entity_type)
        red = redis.from_url(self.redis_url)

        minimum_blknum = self.minimum_blknums.get(entity_type, 0)

        def handler(conn: connection, st: float, keyvals: Dict):
            blk, file = self._decode_task(keyvals)

            if int(blk) < minimum_blknum:
                logging.debug(f"[{entity_type}] blknum: {blk} < {minimum_blknum}")
                return

            result_key = f"{result}:{blk}"

            if red.get(result_key) is not None or red.sismember(result, blk):
                logging.info(f"block: {blk} has handled, skip")
                return

            if self.ignore_file_missing_error is True and not self._file_exists(file):
                logging.warning(f"file: {file} missed, skip")
                return

            self.check_and_autofix_block(entity_type, int(blk), file)

            rowcount = save_file_into_table(
                conn,
                table,
                entity_type,
                file,
                self.ct,
                ignore_error=self.ignore_postgres_copy_error,
            )
            logging.info(
                f"handle save table={table} #row={rowcount} file={file} elapsed={time_elapsed(st)}"
            )
            red.setex(result_key, RESULT_TTL_SECONDS, 1)

        def initer() -> connection:
            return psycopg.connect(self.postgres_url)

        def deiniter(conn: Optional[connection]) -> None:
            if conn is not None and bool(conn.closed) is False:
                conn.close()

        red_cg = RedisConsumerGroup(
            self.redis_url,
            stream,
            cgroup,
            workers=self.max_workers,
            worker_mode="thread",
            period_seconds=self.period_seconds,
        )

        red_cg.consume(handler, initer, deiniter)

    def check_and_autofix_block(self, entity_type: str, blknum: int, file: str):
        if self._file_exists(file):
            return

        if self._need_autofix() is False:
            return

        logging.warning(f"file: {file} is missing or corrupted, try to autofix it")
        eth_easy_etl(
            self.chain,
            self.provider_uri,
            blknum,
            entity_type,
            output_file=file,
            is_geth_provider=self.is_geth_provider,
        )

    def _stream_result_table_of_entity(self, entity_type) -> Tuple[str, str, str]:
        stream_prefix = (
            self.redis_enriched_stream_prefix
            if entity_type in self.enriched_types
            else self.redis_stream_prefix
        )
        stream = fmt_redis_key_name(self.chain, stream_prefix, entity_type)
        result = fmt_redis_key_name(self.chain, self.redis_result_prefix, entity_type)
        table = chain_entity_table(self.chain, entity_type, dev_mode=self.dev_mode)
        return stream, result, table

    def _decode_task(self, keyvals):
        blk = list(keyvals.keys())[0].decode()
        file = list(keyvals.values())[0].decode()

        return blk, file

    def _need_autofix(self) -> bool:
        return self.autofix is True and self.chain in Chain.ALL_ETHEREUM_FORKS

    def _file_exists(self, file: str) -> bool:
        return os.path.exists(file)


@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.pass_context
@global_click_options
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    required=True,
    help="The GreenPlum conneciton url",
)
@click.option(
    "-E",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_ETL),
    show_default=True,
    type=str,
    help="The list of entity types to load.",
)
@click.option(
    "--enriched-entity-types",
    default="",
    show_default=True,
    type=str,
    help="The list of entity types to export from the enriched results.",
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
    "--redis-stream-prefix",
    type=str,
    default="export-stream-",
    show_default=True,
    help="The Redis stream used to load messages.(Put behind the chain)",
)
@click.option(
    "--redis-result-prefix",
    type=str,
    default="load-result-",
    show_default=True,
    help="The Redis key used to store the loaded block.(Put behind the chain)",
)
@click.option(
    "--redis-enriched-stream-prefix",
    type=str,
    default="enrich-stream-",
    show_default=True,
    help="The Redis stream used to load messages.(Put behind the chain)",
)
@click.option(
    "--consumer-group",
    type=str,
    default="loader-cg",
    show_default=True,
    help="The Redis stream consumer name, put it behind the chain",
)
@click.option(
    "-w",
    "--max-workers",
    default=5,
    show_default=True,
    type=int,
    help="The number of consuming workers for one entity",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "--dev-mode",
    is_flag=True,
    show_default=True,
    help="Run load in development mode, data will be INSERTED into {CHAIN}_dev schema",
)
@click.option(
    "--autofix",
    is_flag=True,
    show_default=True,
    help="Autofix mode, re-dump the missed block",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--ignore-file-missing-error",
    is_flag=True,
    show_default=True,
    help="Ignore error of source file missing",
)
@click.option(
    "--ignore-postgres-copy-error",
    is_flag=True,
    show_default=True,
    help="Ignore any errors in postgres copy process",
)
@click.option(
    "--external-load",
    is_flag=True,
    show_default=True,
    help="Load data using External Table Load",
)
@click.option(
    "--external-load-no-backup",
    is_flag=True,
    show_default=True,
    help="Load data using External Table Load without backup",
)
@click.option(
    "--external-load-types",
    show_default=True,
    type=str,
    help="The list of entity types to load with External Table Load.",
)
@click.option(
    "--external-load-count",
    show_default=True,
    type=int,
    default=100,
    help="How many files to be load in one batch",
)
@click.option(
    "--external-load-path",
    show_default=True,
    type=click.Path(exists=True, dir_okay=True),
    help="The directory to store todo and failed csv files",
)
@click.option(
    "--minimum-block",
    show_default=True,
    type=str,
    help="(EXPERIMENTAL) Load with required minimum block number, ONLY available in psycopg load. "
    "Comma separated, eg: trace:100,block:10",
)
def load(
    ctx,
    chain,
    gp_url,
    entity_types,
    enriched_entity_types,
    redis_url,
    redis_stream_prefix,
    redis_result_prefix,
    redis_enriched_stream_prefix,
    consumer_group,
    max_workers,
    period_seconds,
    dev_mode,
    autofix,
    provider_uri,
    ignore_file_missing_error,
    ignore_postgres_copy_error,
    external_load,
    external_load_no_backup,
    external_load_types,
    external_load_count,
    external_load_path,
    minimum_block,
):
    """Load all data from CSV files into GreenPlum/PostgreSQL."""
    entity_types = parse_entity_types(entity_types)
    enriched_types = set(parse_entity_types(enriched_entity_types))
    kwargs = extract_cmdline_kwargs(ctx)
    provider_is_geth = str2bool(kwargs.get("provider_is_geth"))
    external_types = set(
        parse_entity_types(external_load_types)
        if external_load_types is not None
        else []
    )

    loader = Loader(
        chain,
        redis_url,
        gp_url,
        entity_types,
        enriched_types,
        provider_uri,
        provider_is_geth,
        autofix,
        ignore_file_missing_error,
        ignore_postgres_copy_error,
        dev_mode,
        consumer_group,
        redis_stream_prefix,
        redis_result_prefix,
        redis_enriched_stream_prefix,
        max_workers,
        period_seconds,
        minimum_block,
    )

    threads = loader.spawn_loading_threads(
        external_types,
        external_load,
        external_load_no_backup,
        external_load_count,
        external_load_path,
    )

    for p in threads:
        p.start()

    for p in threads:
        if p.is_alive():
            p.join()
