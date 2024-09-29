import io
import os
import shutil
import logging
import random
from copy import copy
from time import time, sleep
from typing import Optional, Protocol, Dict, Union, List, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
import psycopg2.errors
from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.dml import Insert
from psycopg2.extensions import connection, cursor

from blockchainetl.utils import time_elapsed
from blockchainetl.file_utils import smart_open, smart_copy_file
from blockchainetl.misc.pd_write_file import rewrite_file_with_types


POSTGRES_COPY_BUFFER_SIZE = 409600


def create_insert_statement_for_table(
    table: Table,
    on_conflict_do_update: bool = True,
    upsert_callback=None,
    where_callback=None,
    schema: Optional[str] = None,
) -> Insert:
    # set schema if given
    if schema is not None:
        table = copy(table)
        table.schema = schema
    insert_stmt: Insert = insert(table)

    primary_key_fields = [column.name for column in table.columns if column.primary_key]
    if primary_key_fields:
        if on_conflict_do_update:
            if upsert_callback is None:
                upserted_columns = {
                    column.name: insert_stmt.excluded[column.name]
                    for column in table.columns
                    if not column.primary_key
                }
            else:
                upserted_columns = upsert_callback(table, insert_stmt)

            if where_callback is None:
                where = None
            else:
                where = where_callback(table, insert_stmt)

            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=primary_key_fields,
                set_=upserted_columns,
                where=where,
            )  # type: ignore
        else:
            insert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=primary_key_fields
            )  # type: ignore

    return insert_stmt


def excluded_or_exists(table: Table, stmt: Insert, column):
    return func.coalesce(stmt.excluded[column], table.columns[column])


def exists_or_excluded(table: Table, stmt: Insert, column):
    return func.coalesce(table.columns[column], stmt.excluded[column])


# we are updated by block, only the blknum needs to check, don't need to check by logpos
def cond_upsert_on_blknum(table: Table, stmt: Insert, block_column="blknum") -> bool:
    return table.columns[block_column] < stmt.excluded[block_column]


def build_cond_upsert_on_blknum(block_column="blknum"):
    def cond(table: Table, stmt: Insert) -> bool:
        return cond_upsert_on_blknum(table, stmt, block_column)

    return cond


class ColumnType(Protocol):
    def astype(self, entity_type: str) -> Dict[str, Union[str, type]]: ...

    def __getitem__(self, key: str) -> List[str]: ...


def psycopg_connect(pg_url: str, retry=10):
    def connect():
        try:
            return psycopg2.connect(pg_url)
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            return None

    c = 0
    while True:
        conn = connect()
        if conn is not None:
            return conn
        if c > retry:
            return None

        logging.error(f"failed to connect postgresql, retry: {c}")
        sleep(3 + c * 2)
        c += 1


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
        logging.warn(f"failed to load file: {file} error: {e}, try to rewrite it")
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


def copy_redo_path_into_todo_path(redo_path, todo_path, limit):
    redo_files = sorted(os.listdir(redo_path))
    if len(redo_files) > limit:
        redo_files = redo_files[:limit]

    for file in redo_files:
        src = os.path.join(redo_path, file)
        dst = os.path.join(todo_path, file)
        if os.path.isfile(src) and file.endswith(".csv"):
            shutil.move(src, dst)

    todo_files = sorted(os.listdir(todo_path))
    todo_lines = 0
    for file in todo_files:
        todo_file = os.path.join(todo_path, file)
        if not os.path.isfile(todo_file) or not file.endswith(".csv"):
            continue
        with open(todo_file, "r") as fp:
            # each csv file has a HEADER
            todo_lines += sum(1 for line in fp if line.rstrip()) - 1
    return todo_files, todo_lines


def external_copy_file_into_redo(file: str, redo_path: str, copy_limit: int):
    smart_copy_file(file, redo_path)
    redo_files = os.listdir(redo_path)
    if len(redo_files) > copy_limit:
        logging.info(f"backoff in {redo_path} ({len(redo_files)} > {copy_limit})")
        sleep(1)


def external_load_files_into_table(
    gp_url: str,
    src_tbl: str,
    dst_tbl: str,
    entity_type: str,
    ct: ColumnType,
    redo_path: str,
    todo_path: str,
    fail_path: str,
    done_path: str,
    limit: int,
    ignore_error: bool = False,
    backup: bool = True,
):
    # GreenPlum's concurrent LOADING performance is very poor, don't LOADING in the same time
    sleep(random.randint(1, 10))
    conn = psycopg_connect(gp_url, 10)
    assert conn is not None

    last_st = time()
    while True:
        todo_files, todo_lines = copy_redo_path_into_todo_path(
            redo_path, todo_path, limit
        )

        # 100 files or too much time waiting
        n_files = len(todo_files)
        now = time()
        lag = now - last_st
        # don't INSERT small files in high frequency
        # at most 1 qps per minute,
        # increase this value if too much chains were loading in the same time.
        need_copy = (lag > 60 and n_files > 100) or (lag > 300 and n_files > 0)
        if not need_copy:
            sleep(5)
            continue

        sleep(random.randint(1, 5))
        logging.info(f"start to insert from {src_tbl} into {dst_tbl}")
        columns = ",".join(ct[entity_type])
        try:
            st = time()
            sql = "INSERT INTO {dst_table}({columns}) SELECT {columns} FROM {src_table}".format(
                src_table=src_tbl,
                dst_table=dst_tbl,
                columns=columns,
            )
            rowcount = 0
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()
                rowcount = cursor.rowcount

            if rowcount == 0:
                logging.error(f"{sql} doesn't insert any rows")
                for file in todo_files:
                    shutil.move(os.path.join(todo_path, file), fail_path)
                continue
            elif rowcount < todo_lines:
                msg = f"{sql} inserted less rows(inserted: {rowcount}, source: {todo_lines})"
                logging.fatal(msg)
            elif rowcount > todo_lines:
                msg = f"{sql} inserted more rows(inserted: {rowcount}, source: {todo_lines})"
                logging.warning(msg)

            logging.info(
                f"successfully inserted from {src_tbl} to {dst_tbl} with #{len(todo_files)} "
                f"[{todo_files[0]} ... {todo_files[-1]}] (elapsed: {time_elapsed(st)}s) "
                f"#rows: {rowcount}"
            )
            for file in todo_files:
                todo_file = os.path.join(todo_path, file)
                done_file = os.path.join(done_path, file)

                # If the file is already in the done path, the file has been copied.
                # gp-autofix will fix this later(delete the duplicated records)
                if backup and not os.path.exists(done_file):
                    shutil.move(todo_file, done_file)
                else:
                    os.unlink(todo_file)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.info(f"failed: {e} need reconnect...")
            conn = psycopg_connect(gp_url, 30)
            assert conn is not None, "failed to reconnect to postgresql"
            continue

        except psycopg2.Error as e:
            conn.rollback()
            if ignore_error is False:
                logging.fatal(e)

            logging.error(e)
            for file in todo_files:
                todo_file = os.path.join(todo_path, file)
                fail_file = os.path.join(fail_path, file)
                if os.path.exists(fail_file):
                    continue
                shutil.move(todo_file, fail_file)

        last_st = now


def ensure_external_load_path(path: str, table: str) -> Tuple[str, str, str, str]:
    redo_path = os.path.join(path, table, "redo")
    todo_path = os.path.join(path, table, "todo")
    fail_path = os.path.join(path, table, "fail")
    done_path = os.path.join(path, table, "done")
    for path in (todo_path, fail_path, done_path, redo_path):
        os.makedirs(path, exist_ok=True)
    return redo_path, todo_path, fail_path, done_path


def externaldone_path_load_file_into_table(
    conn: connection,
    src_tbl: str,
    dst_tbl: str,
    entity_type: str,
    file: str,
    ct: ColumnType,
    todo_path: str,
    fail_path: str,
    done_path: str,
    limit: int,
    ignore_error: bool = False,
    backup: bool = True,
):
    smart_copy_file(file, todo_path)
    todo_files = sorted(os.listdir(todo_path))
    if len(todo_files) < limit:
        return

    columns = ",".join(ct[entity_type])
    try:
        st = time()
        rowcount = copy_from_external_table(conn, src_tbl, dst_tbl, columns)
        if rowcount == 0:
            logging.error(f"COPY FROM {src_tbl} TO {dst_tbl} doesn't insert any rows")
            for file in todo_files:
                shutil.move(os.path.join(todo_path, file), fail_path)
            return

        logging.info(
            f"successfully inserted from {src_tbl} to {dst_tbl} with #{len(todo_files)} "
            f"[{todo_files[0]} ... {todo_files[-1]}] (elapsed: {time_elapsed(st)}s) "
            f"#rows: {rowcount}"
        )
        for file in todo_files:
            todo_file = os.path.join(todo_path, file)
            if backup:
                shutil.move(todo_file, os.path.join(done_path, file))
            else:
                os.unlink(todo_file)

    except psycopg2.Error as e:
        conn.rollback()
        if ignore_error is False:
            logging.fatal(e)

        logging.error(e)
        for file in todo_files:
            shutil.move(os.path.join(todo_path, file), fail_path)


def copy_from_external_table(
    conn: connection, src_tbl: str, dst_tbl: str, columns: Optional[str] = None
) -> int:
    if columns is None:
        sql = "INSERT INTO {dst_table} SELECT * FROM {src_table}".format(
            src_table=src_tbl,
            dst_table=dst_tbl,
        )
    else:
        sql = "INSERT INTO {dst_table}({columns}) SELECT {columns} FROM {src_table}".format(
            src_table=src_tbl,
            dst_table=dst_tbl,
            columns=columns,
        )

    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        return cursor.rowcount


def copy_from_csv_file(
    conn: connection,
    tbl: str,
    file: str,
    rollback: bool = False,
    delimiter: str = "^",
    on_conflict_do_nothing: bool = True,
) -> int:
    with conn.cursor() as cursor, smart_open(file, "r") as fr:
        return cursor_copy_from_stream(
            conn, cursor, tbl, fr, rollback, delimiter, on_conflict_do_nothing
        )


def copy_into_csv_file(
    conn: connection,
    query: str,
    file: str,
    delimiter: str = "^",
) -> int:
    with conn.cursor() as cursor, smart_open(file, "w") as fw:
        return cursor_copy_into_stream(cursor, query, fw, delimiter)


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


def cursor_copy_from_stream(
    conn: connection,
    cursor: cursor,
    tbl: str,
    stream: io.TextIOBase,
    rollback: bool = False,
    delimiter: str = "^",
    on_conflict_do_nothing: bool = True,
) -> int:
    if rollback:
        # ref https://stackoverflow.com/a/48503125/2298986
        cursor.execute("ROLLBACK")

    try:
        # Ref https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
        # read the column header from the beginning of the csv/stream file
        stream.seek(0)
        columns = ",".join(stream.readline().split(delimiter))
        cursor.copy_expert(
            f"COPY {tbl} ({columns}) FROM STDIN WITH DELIMITER '{delimiter}' CSV",
            stream,
            POSTGRES_COPY_BUFFER_SIZE,
        )

        # don't forget to commit the changes
        conn.commit()
        return cursor.rowcount

    except psycopg2.errors.UniqueViolation as e:
        if on_conflict_do_nothing is False:
            raise e

        # ref https://stackoverflow.com/a/30985541
        with conn.cursor() as cursor:
            cursor.execute("ROLLBACK")
            # reset the file position to the beginning of the file
            stream.seek(0)
            columns = ",".join(stream.readline().strip().split(delimiter))

            # rewrite the empty string with None
            # else psycopg2 will raise InvalidTextRepresentation for the numeric columns
            data = []
            for e in stream.readlines():
                cols = []
                for c in e.strip().split(delimiter):
                    col = c if len(c) > 0 else None
                    # if the column is quoted and contains escaped quotes, unescape them,
                    # this is always used when the column is a JSON or JSONB
                    if (
                        col is not None
                        and col.startswith('"')
                        and col.endswith('"')
                        and '""' in col
                    ):
                        col = col.replace('""', '"')[1:-1]
                    cols.append(col)
                data.append(tuple(cols))
            query = "INSERT INTO {}({}) VALUES %s ON CONFLICT DO NOTHING".format(
                tbl, columns
            )

            psycopg2.extras.execute_values(
                cursor, query, data, template=None, page_size=100
            )
            return -1


def cursor_copy_into_stream(
    cursor: cursor,
    query: str,
    stream: io.TextIOBase,
    delimiter: str = "^",
) -> int:
    cursor.copy_expert(
        f"COPY ({query}) TO STDOUT WITH DELIMITER '{delimiter}' CSV HEADER",
        stream,
        POSTGRES_COPY_BUFFER_SIZE,
    )
    return cursor.rowcount
