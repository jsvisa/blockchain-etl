from typing import Optional
from blockchainetl.enumeration.entity_type import EntityType
from . import Checker


SQL_CHECK_SQL = r"""
WITH txs AS (
    SELECT
        _st_day,
        blknum,
        count(*) AS tx_count,
        sum(COALESCE(receipt_log_count, 0)) AS log_count
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
        AND txhash is not null
    GROUP BY
        _st_day,
        blknum
),
logs AS (
    SELECT
        _st_day,
        blknum,
        count(DISTINCT txhash) AS log_tx_count,
        count(*) AS log_count,
        count(DISTINCT(blknum, logpos)) AS log_distinct_count
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
    GROUP BY
        _st_day,
        blknum
)

SELECT
    A._st_day,
    A.blknum AS tx_blknum,
    A.tx_count AS tx_tx_count,
    A.log_count AS tx_log_count,
    B.blknum AS log_blknum,
    B.log_tx_count AS log_tx_count,
    B.log_count AS log_log_count,
    B.log_distinct_count AS log_distinct_count
FROM
    txs A
    FULL JOIN logs B
        ON A._st_day = B._st_day
        AND A.blknum = B.blknum
WHERE
    A.blknum is NULL
    OR (B.blknum is NULL AND A.log_count <> 0)
    OR A.log_count <> B.log_count
    OR B.log_count <> B.log_distinct_count
ORDER BY
    A._st_day ASC,
    A.blknum ASC
"""

SQL_DELETE_DUPLICATED_BLOCK = r"""
DELETE FROM "{{gp_schema}}".logs a USING (
    SELECT
        blknum,
        logpos,
        max(ctid) as ctid
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
    GROUP BY
        blknum,
        logpos
    HAVING
        count(*) > 1
) b
WHERE
{% if check_by_date %}
    a._st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
    a._st >= {{st}} AND a._st <= {{et}}
{% endif %}
{% if stream_check %}
    AND a.blknum >= {{st_blk}} AND a.blknum <= {{et_blk}}
{% endif %}
    AND a.blknum = b.blknum
    AND a.logpos = b.logpos
    AND a.ctid <> b.ctid
"""


class EthLogChecker(Checker):
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
            self.diff_rows = rows
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
        deleted = self._delete_duplicated(st_day, et_day, st, et, st_blk, et_blk)
        # delete it first, refetch all rows
        self._check(st_day, et_day, st, et, st_blk, et_blk)
        blocks = (
            row["tx_blknum"]
            for row in self.diff_rows or []
            if row["tx_log_count"] != row["log_log_count"]
        )

        inserted = self.easyetl(st_day, EntityType.LOG, blocks)
        deleted += self._delete_duplicated(st_day, et_day, st, et, st_blk, et_blk)
        return {"deleted": deleted, "inserted": inserted}

    def _delete_duplicated(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> int:
        return self.execute(
            SQL_DELETE_DUPLICATED_BLOCK,
            st_day,
            et_day,
            st,
            et,
            st_blk,
            et_blk,
            is_readonly=False,
        ).rowcount

    def _x_column(self) -> str:
        return "tx_blknum"

    def _y_column(self) -> str:
        return "log_blknum"
