import logging
from time import time
from typing import Tuple
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.engine import Engine
from cachetools import cached, TTLCache

from blockchainetl.utils import time_elapsed
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.misc.eth_alert import (
    READ_BLOCK_TEMPLATE,
    READ_TX_TEMPLATE,
    READ_TRACE_TEMPLATE,
    READ_LOG_TEMPLATE,
    READ_TOKEN_XFER_TEMPLATE,
)

from .eth_base_adapter import EthBaseAdapter


class EthAlertAdapter(EthBaseAdapter):
    def __init__(
        self,
        engine: Engine,
        batch_web3_provider: BatchHTTPProvider,
        item_exporter=ConsoleItemExporter(),
        chain=Chain.ETHEREUM,
        batch_size=100,
        max_workers=5,
        entity_types=tuple(EntityType.ALL_FOR_STREAMING),
        is_pending_mode=True,
        print_sql=False,
    ):
        self.engine = engine
        self.entity_types = entity_types
        self.is_pending_mode = is_pending_mode
        self.print_sql = print_sql

        EthBaseAdapter.__init__(
            self, chain, batch_web3_provider, item_exporter, batch_size, max_workers
        )

    @cached(cache=TTLCache(maxsize=16, ttl=10))
    def get_current_block_number(self) -> Tuple[int, int]:
        start_date = datetime.utcnow() - timedelta(days=10)
        row = self.engine.execute(
            f"SELECT blknum, extract(epoch from block_timestamp)::int AS timestamp "
            f"FROM {self._schema()}.blocks "
            f"WHERE block_timestamp >= '{start_date}' "
            "ORDER BY blknum DESC LIMIT 1"
        ).fetchone()
        return (row["blknum"], row["timestamp"])  # type: ignore

    def export_all(self, start_block, end_block):
        st0 = time()

        logging.debug("Exporting with " + type(self.item_exporter).__name__)
        all_items = []
        blocks = self._export_blocks(start_block, end_block)
        if self._should_export(EntityType.BLOCK):
            all_items.extend(blocks)

        st_timestamp = min(e["timestamp"] for e in blocks)
        et_timestamp = max(e["timestamp"] for e in blocks)

        if self._should_export(EntityType.TRANSACTION):
            transactions = self._export_txs(
                start_block, end_block, st_timestamp, et_timestamp
            )
            all_items.extend(transactions)

        if self._should_export(EntityType.LOG):
            logs = self._export_logs(start_block, end_block, st_timestamp, et_timestamp)
            all_items.extend(logs)

        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._export_token_xfers(
                start_block, end_block, st_timestamp, et_timestamp
            )
            all_items.extend(token_transfers)

        if self._should_export(EntityType.TRACE):
            traces = self._export_traces(
                start_block, end_block, st_timestamp, et_timestamp
            )
            all_items.extend(traces)

        st1 = time()

        if len(all_items) == 0:
            logging.warning(
                f"Handle blocks [{start_block}, {end_block}] "
                f"with entity-types: {self.entity_types} return emtpy"
            )
            return

        self.item_exporter.export_items(all_items)
        st2 = time()
        logging.info(
            f"PERF export blocks=({start_block}, {end_block}) size=#{len(all_items)} "
            f"@all={time_elapsed(st0, st2)} @db_read={time_elapsed(st0, st1)} "
            f"@export={time_elapsed(st1, st2)}"
        )

    def _should_export(self, entity_type):
        return entity_type in self.entity_types

    def _export_blocks(self, start_block, end_block):
        st_timestamp = datetime.utcnow() - timedelta(days=60)
        et_timestamp = datetime.utcnow() + timedelta(days=1)
        blocks = self._read_sql(
            READ_BLOCK_TEMPLATE, start_block, end_block, st_timestamp, et_timestamp
        )
        missing = set(e for e in range(start_block, end_block + 1))
        missing -= set(e["number"] for e in blocks)
        if len(missing) > 0:
            raise ValueError(
                f"missing some blocks want: [{start_block}, {end_block}] missing: {missing}"
            )
        return blocks

    def _export_txs(self, start_block, end_block, st_timestamp, et_timestamp):
        return self._read_sql(
            READ_TX_TEMPLATE, start_block, end_block, st_timestamp, et_timestamp
        )

    def _export_logs(self, start_block, end_block, st_timestamp, et_timestamp):
        logs = self._read_sql(
            READ_LOG_TEMPLATE, start_block, end_block, st_timestamp, et_timestamp
        )
        for log in logs:
            log["topics"] = log["topics"].split(",")
        return logs

    def _export_traces(self, start_block, end_block, st_timestamp, et_timestamp):
        return self._read_sql(
            READ_TRACE_TEMPLATE, start_block, end_block, st_timestamp, et_timestamp
        )

    def _export_token_xfers(self, start_block, end_block, st_timestamp, et_timestamp):
        return self._read_sql(
            READ_TOKEN_XFER_TEMPLATE, start_block, end_block, st_timestamp, et_timestamp
        )

    def _read_sql(self, template, start_block, end_block, st_timestamp, et_timestamp):
        sql = template.render(
            schema=self._schema(),
            st_blknum=start_block,
            et_blknum=end_block,
            st_timestamp=st_timestamp,
            et_timestamp=et_timestamp,
        )
        if self.print_sql:
            logging.info(sql)

        rows = self.engine.execute(sql).fetchall()
        rows = [e._asdict() for e in rows]
        # TODO: global cast
        for row in rows:
            for k, v in row.items():
                if isinstance(v, Decimal):
                    row[k] = int(v)
        return rows

    def _schema(self):
        schema = self.chain
        if self.is_pending_mode is True:
            schema += "_pending"
        return schema
