from typing import Optional
from . import Checker


SQL_CHECK_SQL = r"""
WITH erc721_xfers AS (
    SELECT
        _st_day,
        blknum,
        count(*) AS xfer_count,
        count(DISTINCT txhash) AS xfer_tx_count,
        count(DISTINCT(blknum, logpos)) AS xfer_distinct_count
    FROM
        "{{gp_schema}}".erc721_xfers
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
    A.blknum AS xfer_blknum,
    A.xfer_count AS xfer_log_count,
    A.xfer_tx_count AS xfer_tx_count,
    A.xfer_distinct_count AS xfer_distinct_count
FROM
    erc721_xfers A
WHERE
    A.xfer_count <> A.xfer_distinct_count
ORDER BY
    A._st_day ASC,
    A.blknum ASC
"""

SQL_DELETE_DUPLICATED_BLOCK = r"""
DELETE FROM "{{gp_schema}}".erc721_xfers a USING (
    SELECT
        blknum,
        logpos,
        max(ctid) as ctid
    FROM
        "{{gp_schema}}".erc721_xfers
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
    a._st_day >= '{{st_day}}' AND a._st_day <= '{{et_day}}'
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


class EthErc721TransferChecker(Checker):
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
        self.diff_rows = self.execute(
            SQL_CHECK_SQL, st_day, et_day, st, et, st_blk, et_blk
        ).fetchall()
        inserted = self._insert_missing(st_day, et_day, st, et, st_blk, et_blk)
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

    def _insert_missing(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> int:
        # TODO: regenerate from {{chain}}.logs
        return 0
