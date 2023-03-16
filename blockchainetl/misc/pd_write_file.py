import os
import csv
import pandas as pd
from io import StringIO
from typing import List, Dict, Union, Optional
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
import s3fs

s3 = s3fs.S3FileSystem()


DEFAULT_FIELD_TERMINATED = "^"


def save_df_into_file(
    df: pd.DataFrame,
    output: str,
    columns: List[str],
    types: Optional[Dict[str, Union[str, type]]],
    entity_type: str,
):
    if types is not None and len(types) > 0:
        # in some case, the raw data is incomplete, and astype maybe fail of below exception:
        # File "pandas/core/arrays/integer.py", line 120, in safe_cast
        # return values.astype(dtype, casting="safe", copy=copy)
        # TypeError: Cannot cast array data from dtype('O') to dtype('int64') according to the rule 'safe' # noqa
        # ValueError: invalid literal for int() with base 10: ''
        df = df.astype(types)

    try:
        df.to_csv(
            output,
            columns=columns,
            sep=DEFAULT_FIELD_TERMINATED,
            index=False,
        )
    except KeyError as e:
        raise ValueError(
            f"to be saved column diffs for type({entity_type}) "
            f"is: {set(columns)-set(df.columns)}"
        ) from e


def rewrite_file_with_types(file: str, types: Dict[str, Union[str, type]]):
    if types is None or len(types) == 0:
        return

    df = pd.read_csv(file, sep=DEFAULT_FIELD_TERMINATED)

    # convert typing!
    df = df.astype(types)

    bak = file + ".bak"
    df.to_csv(bak, sep=DEFAULT_FIELD_TERMINATED, index=False)

    if file.startswith("s3://"):
        s3.rename(bak, file)
    else:
        os.rename(bak, file)


# Alternative to_sql() *method* for DBs that support COPY FROM
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def make_postgres_upsert_method(engine, schema, primary_key_fields):
    """
    The bind && reflect takes a long time to finish, DON'T use in production
    """
    meta = sa.MetaData(schema=schema)
    meta.bind = engine
    meta.reflect(views=True)

    def upsert(table, conn, keys, data_iter):
        for data in data_iter:
            data = {k: data[i] for i, k in enumerate(keys)}
            insert_stmt = insert(meta.tables[table.name]).values(**data)
            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=primary_key_fields,
            )
            conn.execute(upsert_stmt)

    return upsert
