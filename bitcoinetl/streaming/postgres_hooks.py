from typing import Dict
from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql.dml import Insert

from blockchainetl.streaming.postgres_utils import (
    excluded_or_exists,
    exists_or_excluded,
)


def upsert_latest_balances(table: Table, stmt: Insert) -> Dict:
    excluded = stmt.excluded
    # ++
    upserted = {
        "blknum": excluded["blknum"],
        "out_blocks": table.c.out_blocks + excluded["out_blocks"],
        "vin_blocks": table.c.vin_blocks + excluded["vin_blocks"],
        "cnb_blocks": table.c.cnb_blocks + excluded["cnb_blocks"],
        "out_txs": table.c.out_txs + excluded["out_txs"],
        "vin_txs": table.c.vin_txs + excluded["vin_txs"],
        "cnb_txs": table.c.cnb_txs + excluded["cnb_txs"],
        "out_xfers": table.c.out_xfers + excluded["out_xfers"],
        "vin_xfers": table.c.vin_xfers + excluded["vin_xfers"],
        "cnb_xfers": table.c.cnb_xfers + excluded["cnb_xfers"],
        "out_value": table.c.out_value + excluded["out_value"],
        "vin_value": table.c.vin_value + excluded["vin_value"],
        "cnb_value": table.c.cnb_value + excluded["cnb_value"],
        "value": table.c.value + excluded["value"],
        "updated_at": func.current_timestamp(),
    }

    # use current value or exists
    upserted.update(
        {
            c: excluded_or_exists(table, excluded, c)
            for c in [
                "out_nth_st",
                "vin_nth_st",
                "cnb_nth_st",
                "out_nth_blknum",
                "vin_nth_blknum",
                "cnb_nth_blknum",
                "out_nth_st_day",
                "vin_nth_st_day",
                "cnb_nth_st_day",
            ]
        }
    )

    # use exists value or current
    upserted.update(
        {
            c: exists_or_excluded(table, excluded, c)
            for c in [
                "out_1th_st",
                "vin_1th_st",
                "cnb_1th_st",
                "out_1th_blknum",
                "vin_1th_blknum",
                "cnb_1th_blknum",
                "out_1th_st_day",
                "vin_1th_st_day",
                "cnb_1th_st_day",
            ]
        }
    )

    return upserted
