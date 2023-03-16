import logging
import pandas as pd
from typing import Optional
from . import Checker

from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.providers.auto import get_provider_from_uri

SQL_CHECK_SQL = r"""
WITH txs AS (
    SELECT
        txhash
    FROM
        "{{gp_schema}}".txs
    WHERE
{% if check_by_date %}
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
        _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
        AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
        AND receipt_log_count is not null
)
SELECT
    distinct txhash
FROM
    "{{gp_schema}}".logs
WHERE
{% if check_by_date %}
    _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
    _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
    AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
    AND txhash NOT IN (
        SELECT DISTINCT
            txhash
        FROM
            "{{gp_schema}}".tx_receipts
        WHERE
{% if check_by_date %}
            _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
            _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
            AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
    )
    AND txhash NOT IN (SELECT txhash FROM txs)
LIMIT 10000
"""

SQL_UPDATE_SQL = r"""
UPDATE
    "{{gp_schema}}".txs a
SET
    receipt_log_count = b.receipt_log_count
FROM
    "{{gp_schema}}".tx_receipts b
WHERE
{% if check_by_date %}
    a._st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
    AND b._st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
    a._st >= {{st}} AND a._st <= {{et}}
    AND b._st >= {{st}} AND b._st <= {{et}}
{% endif %}
{% if stream_check %}
    AND a.blknum >= {{st_blk}} AND a.blknum <= {{et_blk}}
    AND b.blknum >= {{st_blk}} AND b.blknum <= {{et_blk}}
{% endif %}
    AND a.txhash = b.txhash
    AND a.receipt_log_count IS NULl
"""

SQL_DELETE_SQL = r"""
DELETE FROM
    "{{gp_schema}}".tx_receipts
WHERE
{% if check_by_date %}
    _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
    _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
    AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
"""


class EthTxReceiptChecker(Checker):
    transactions = []
    batch_web3_provider = None

    def _check(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> bool:
        rows = self.execute(
            SQL_CHECK_SQL, st_day, et_day, st, et, st_blk, et_blk
        ).fetchall()
        if len(rows) > 0:
            self.transactions = rows
        return len(rows) == 0

    def _autofix(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ):
        self.batch_web3_provider = ThreadLocalProxy(
            lambda: get_provider_from_uri(self.provider_uri, batch=True)
        )
        inserted = 0
        updated = 0
        while True:
            inserted += self._insert_missing(st_day, et_day, st, et, st_blk, et_blk)
            updated += self._update_tx_receipt(st_day, et_day, st, et, st_blk, et_blk)
            logging.info(f"I: {inserted} U: {updated}")
            if self._check(st_day, et_day, st, et, st_blk, et_blk):
                break

        deleted = self._delete_tx_receipt(st_day, et_day, st, et, st_blk, et_blk)

        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    def _update_tx_receipt(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> int:
        return self.execute(
            SQL_UPDATE_SQL, st_day, et_day, st, et, st_blk, et_blk
        ).rowcount

    def _delete_tx_receipt(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> int:
        return self.execute(
            SQL_DELETE_SQL,
            st_day,
            et_day,
            st,
            et,
            st_blk,
            et_blk,
            is_readonly=False,
        ).rowcount

    def _insert_missing(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> int:
        exporter = InMemoryItemExporter(item_types=[EntityType.RECEIPT])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["txhash"] for transaction in self.transactions
            ),
            batch_size=500,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=10,
            item_exporter=exporter,
            export_receipts=True,
            export_logs=False,
        )
        job.run()
        receipts = exporter.get_items(EntityType.RECEIPT)
        df = pd.DataFrame(receipts)
        if df.empty:
            return 0

        df = df[["transaction_hash", "log_count"]].rename(
            columns={
                "transaction_hash": "txhash",
                "log_count": "receipt_log_count",
            }
        )

        df["_st_day"] = st_day

        df.to_sql(
            "tx_receipts",
            con=self.rw_engine,
            schema=self.gp_schema,
            method="multi",
            if_exists="append",
            index=False,
            chunksize=5000,
        )
        return df.shape[0]
