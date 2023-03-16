from typing import Optional
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.enumeration.chain import Chain
from . import Checker


SQL_CHECK_SQL = r"""
WITH logs AS (
    SELECT
        _st_day,
        blknum,
        count(*) AS log_count,
        count(DISTINCT txhash) AS log_tx_count
    FROM
        "{{gp_schema}}".logs
    WHERE
{% if check_by_date -%}
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else -%}
        _st >= {{st}} AND _st <= {{et}}
{% endif %}
{% if stream_check %}
        AND blknum >= {{st_blk}} AND blknum <= {{et_blk}}
{% endif %}
        AND (
            (
                topics_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND n_topics + (length(data)-2)/64 = 4
            )
{% if is_ethereum %}
            OR
            (
                address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
                AND topics_0 IN (
                    -- 'Deposit(address indexed dst, uint256 wad)'
                    '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c',
                    -- 'Withdrawal(address indexed src, uint256 wad)'
                    '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                )
            )
{% endif %}
        )
    GROUP BY
        _st_day,
        blknum
),
token_xfers AS (
    SELECT
        _st_day,
        blknum,
        count(*) AS xfer_count,
        count(DISTINCT txhash) AS xfer_tx_count,
        count(DISTINCT(blknum, logpos)) AS xfer_distinct_count
    FROM
        "{{gp_schema}}".token_xfers
    WHERE
{% if check_by_date -%}
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else -%}
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
    A.blknum AS log_blknum,
    A.log_count AS log_log_count,
    A.log_tx_count AS log_tx_count,
    B.blknum AS xfer_blknum,
    B.xfer_count AS xfer_log_count,
    B.xfer_tx_count AS xfer_tx_count,
    B.xfer_distinct_count AS xfer_distinct_count
FROM
    logs A
    FULL JOIN token_xfers B ON
        A._st_day = B._st_day
        AND A.blknum = B.blknum
WHERE
    A.blknum is NULL
    OR B.blknum is NULL
    OR A.log_count <> B.xfer_count
    OR A.log_tx_count <> B.xfer_tx_count
    OR B.xfer_count <> B.xfer_distinct_count
ORDER BY
    A._st_day ASC,
    A.blknum ASC
"""

SQL_DELETE_DUPLICATED_BLOCK = r"""
DELETE FROM "{{gp_schema}}".token_xfers a USING (
    SELECT
        blknum,
        logpos,
        max(ctid) AS ctid
    FROM
        "{{gp_schema}}".token_xfers
    WHERE
{% if check_by_date -%}
        _st_day >= '{{st_day}}' AND _st_day <= '{{et_day}}'
{% else -%}
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
{% if check_by_date -%}
    a._st_day >= '{{st_day}}' AND a._st_day <= '{{et_day}}'
{% else -%}
    a._st >= {{st}} AND a._st <= {{et}}
{% endif %}
{% if stream_check %}
    AND a.blknum >= {{st_blk}} AND a.blknum <= {{et_blk}}
{% endif %}
    AND a.blknum = b.blknum
    AND a.logpos = b.logpos
    AND a.ctid <> b.ctid
"""


class EthTokenTransferChecker(Checker):
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
            SQL_CHECK_SQL,
            st_day,
            et_day,
            st,
            et,
            st_blk,
            et_blk,
            is_ethereum=self.chain == Chain.ETHEREUM,
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
        malformed = [
            row["xfer_blknum"]
            for row in self.diff_rows or []
            if row["log_blknum"] is None
        ]
        if len(malformed) > 0:
            raise Exception(
                f"[{st_day}] those blocks doesn't have logs, but got token_xfers: {malformed}"
            )

        blocks = (
            row["log_blknum"]
            for row in self.diff_rows or []
            if row["log_log_count"] != row["xfer_log_count"]
        )
        inserted = self.easyetl(st_day, EntityType.TOKEN_TRANSFER, blocks)
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
        return "log_blknum"

    def _y_column(self) -> str:
        return "xfer_blknum"
