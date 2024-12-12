import io
import logging
import pandas as pd
import psycopg2
from typing import Optional
from psycopg2.extensions import connection

from blockchainetl.misc.pd_write_file import rewrite_file_with_types
from blockchainetl.streaming.postgres_utils import (
    cursor_copy_from_stream,
    copy_from_csv_file,
    ColumnType,
)


def copy_from_pandas_df(
    conn: connection,
    tbl: str,
    df: pd.DataFrame,
    rollback: bool = False,
    delimiter: str = "^",
    on_conflict_do_nothing: bool = True,
) -> int:
    fr = io.StringIO()
    df.to_csv(fr, index=False, sep=delimiter)

    with conn.cursor() as cursor:
        return cursor_copy_from_stream(
            conn, cursor, tbl, fr, rollback, delimiter, on_conflict_do_nothing
        )


def save_file_into_table(
    conn: connection,
    tbl: str,
    entity_type: str,
    file: str,
    ct: Optional[ColumnType],
    ignore_error: bool = False,
    on_conflict_do_nothing: bool = True,
) -> int:
    rowcount = 0
    try:
        rowcount = copy_from_csv_file(
            conn,
            tbl,
            file,
            rollback=False,
            on_conflict_do_nothing=on_conflict_do_nothing,
        )

    except psycopg2.errors.InvalidTextRepresentation as e:
        logging.warning(f"failed to load file: {file} error: {e}, try to rewrite it")
        if ct is None:
            raise ValueError("not supported column type") from e

        rewrite_file_with_types(file, ct.astype(entity_type))
        rowcount = copy_from_csv_file(
            conn,
            tbl,
            file,
            rollback=True,
            on_conflict_do_nothing=on_conflict_do_nothing,
        )

    except Exception as e:
        with conn.cursor() as cursor:
            cursor.execute("ROLLBACK")
        conn.rollback()

        msg = f"failed to load file: {file} into table: {tbl}, error: {e}"
        if ignore_error:
            logging.error(msg)
        else:
            logging.fatal(msg)

    return rowcount
