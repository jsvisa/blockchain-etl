from typing import Optional

from . import Checker

from blockchainetl.enumeration.entity_type import EntityType

SQL_CHECK_SQL = r"""
SELECT
    min(blknum) AS min_blk,
    max(blknum) AS max_blk,
    count(blknum) AS blk_count,
    count(distinct blknum) AS blk_distinct_count,
    max(blknum) - min(blknum) + 1 AS blk_range_count
FROM
    "{{gp_schema}}".blocks
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

SQL_SELECT_MISSING_BLOCK = r"""
WITH blocks AS (
    SELECT
        blknum
    FROM
        "{{gp_schema}}".blocks
    WHERE
{% if check_by_date %}
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else %}
        _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
        AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
),
gl_gr AS (
    SELECT
        blknum
    FROM
        generate_series({{st_blk}}, {{et_blk}}) blknum
)
SELECT
    a.blknum
FROM
    gl_gr a
    LEFT JOIN blocks b ON a.blknum = b.blknum
WHERE
    b.blknum IS NULL
ORDER BY
    1
"""

SQL_DELETE_DUPLICATED_BLOCK = r"""
DELETE FROM "{{gp_schema}}".blocks a USING (
    SELECT
        blknum, min(ctid) as ctid
    FROM
        "{{gp_schema}}".blocks
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
        blknum
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
    AND a.ctid <> b.ctid
"""


class EthBlockChecker(Checker):
    def _check(
        self,
        st_day: str,
        et_day: str,
        st: Optional[int] = None,
        et: Optional[int] = None,
        st_blk: Optional[int] = None,
        et_blk: Optional[int] = None,
    ) -> bool:
        result = self.execute(
            SQL_CHECK_SQL, st_day, et_day, st, et, st_blk, et_blk
        ).fetchone()
        return (
            result is not None
            and result["min_blk"] == st_blk
            and result["max_blk"] == et_blk
            and result["blk_count"] == result["blk_distinct_count"]
            and result["blk_count"] == result["blk_range_count"]
        )

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
        missing = self.execute(
            SQL_SELECT_MISSING_BLOCK, st_day, et_day, st, et, st_blk, et_blk
        ).fetchall()
        blocks = (row["blknum"] for row in missing or [])
        inserted = self.easyetl(
            st_day,
            EntityType.BLOCK,
            blocks,
        )
        deleted = self._delete_duplicated(st_day, et_day, st, et, st_blk, et_blk)
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
