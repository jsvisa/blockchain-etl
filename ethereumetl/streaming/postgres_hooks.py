from typing import Dict
from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql.dml import Insert
from blockchainetl.streaming.postgres_utils import (
    excluded_or_exists,
    exists_or_excluded,
    cond_upsert_on_blknum,
)


def upsert_erc721_token_ids(table: Table, stmt: Insert) -> Dict:
    return {
        "turnover_count": table.c.turnover_count + stmt.excluded["turnover_count"],
        "xfered_st": stmt.excluded["xfered_st"],
        "xfered_st_day": stmt.excluded["xfered_st_day"],
        "xfered_blknum": stmt.excluded["xfered_blknum"],
        "xfered_txhash": stmt.excluded["xfered_txhash"],
        "xfered_txpos": stmt.excluded["xfered_txpos"],
        "xfered_logpos": stmt.excluded["xfered_logpos"],
        "to_address": stmt.excluded["to_address"],
        "updated_at": func.current_timestamp(),
    }


def upsert_erc1155_token_ids(table: Table, stmt: Insert) -> Dict:
    return {
        "turnover_count": table.c.turnover_count + stmt.excluded["turnover_count"],
        "minted_count": table.c.minted_count + stmt.excluded["minted_count"],
        "minted_value": table.c.minted_value + stmt.excluded["minted_value"],
        "burned_count": table.c.burned_count + stmt.excluded["burned_count"],
        "burned_value": table.c.burned_value + stmt.excluded["burned_value"],
        "xfered_st": stmt.excluded["xfered_st"],
        "xfered_st_day": stmt.excluded["xfered_st_day"],
        "xfered_blknum": stmt.excluded["xfered_blknum"],
        "xfered_txhash": stmt.excluded["xfered_txhash"],
        "xfered_txpos": stmt.excluded["xfered_txpos"],
        "xfered_logpos": stmt.excluded["xfered_logpos"],
        "updated_at": func.current_timestamp(),
    }


def upsert_token_holders(table: Table, stmt: Insert) -> Dict:
    # ++
    upserted = {
        "send_value": table.c.send_value + stmt.excluded["send_value"],
        "recv_value": table.c.recv_value + stmt.excluded["recv_value"],
        "send_count": table.c.send_count + stmt.excluded["send_count"],
        "recv_count": table.c.recv_count + stmt.excluded["recv_count"],
        "updated_blknum": stmt.excluded["updated_blknum"],
        "updated_at": func.current_timestamp(),
    }

    # use current value or exists
    upserted.update(
        {
            c: excluded_or_exists(table, stmt, c)
            for c in [
                "last_send_st",
                "last_send_st_day",
                "last_send_blknum",
                "last_send_txhash",
                "last_send_txpos",
                "last_send_logpos",
                "last_recv_st",
                "last_recv_st_day",
                "last_recv_blknum",
                "last_recv_txhash",
                "last_recv_txpos",
                "last_recv_logpos",
            ]
        }
    )

    # use exists value or current
    upserted.update(
        {
            c: exists_or_excluded(table, stmt, c)
            for c in [
                "first_send_st",
                "first_send_st_day",
                "first_send_blknum",
                "first_send_txhash",
                "first_send_txpos",
                "first_send_logpos",
                "first_recv_st",
                "first_recv_st_day",
                "first_recv_blknum",
                "first_recv_txhash",
                "first_recv_txpos",
                "first_recv_logpos",
            ]
        }
    )

    return upserted


def upsert_latest_balances(within_fee=False, within_coinbase=False):
    add_columns = [
        "out_blocks",
        "vin_blocks",
        "out_txs",
        "vin_txs",
        "out_xfers",
        "vin_xfers",
        "out_value",
        "vin_value",
        "value",
    ]
    new_columns = [
        "out_nth_st",
        "vin_nth_st",
        "out_nth_blknum",
        "vin_nth_blknum",
        "out_nth_st_day",
        "vin_nth_st_day",
    ]
    old_columns = [
        "out_1th_st",
        "vin_1th_st",
        "out_1th_blknum",
        "vin_1th_blknum",
        "out_1th_st_day",
        "vin_1th_st_day",
    ]
    if within_fee is True:
        add_columns.append("fee_value")

    if within_coinbase is True:
        new_columns += ["cnb_nth_st", "cnb_nth_blknum", "cnb_nth_st_day"]
        old_columns += ["cnb_1th_st", "cnb_1th_blknum", "cnb_1th_st_day"]

    def _upsert(table: Table, stmt: Insert) -> Dict:
        # ++
        upserted = {
            "blknum": stmt.excluded["blknum"],
            "updated_at": func.current_timestamp(),
        }
        upserted.update({c: table.columns[c] + stmt.excluded[c] for c in add_columns})

        # use current value or exists
        upserted.update({c: excluded_or_exists(table, stmt, c) for c in new_columns})

        # use exists value or current
        upserted.update({c: exists_or_excluded(table, stmt, c) for c in old_columns})

        return upserted

    return _upsert


def cond_upsert_erc721_token_ids(table: Table, stmt: Insert) -> bool:
    return cond_upsert_on_blknum(table, stmt, "xfered_blknum")


def cond_upsert_erc1155_token_ids(table: Table, stmt: Insert) -> bool:
    return cond_upsert_on_blknum(table, stmt, "xfered_blknum")
