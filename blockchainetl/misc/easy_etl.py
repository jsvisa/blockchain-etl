import logging
import pandas as pd
from typing import Optional, Callable
from psycopg2.extensions import connection

from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, chain_entity_table
from blockchainetl.streaming.postgres_utils import copy_from_pandas_df


def easy_df_saver(
    chain: Chain,
    entity_type: EntityType,
    column_type,
    gp_conn: Optional[connection] = None,
    gp_schema=None,
    gp_table=None,
    load_into_db=True,
) -> Optional[Callable[[pd.DataFrame, int, str], int]]:
    if load_into_db is False:
        return None

    table = chain_entity_table(gp_schema or chain, gp_table or entity_type)
    dtypes = column_type.astype(entity_type)

    # because pd.to_sql will check the column types(uint64 is not supported),
    # so we can't directly use the pd.to_sql
    # use postgres' COPY instead
    def df_saver(df: pd.DataFrame, block_num: int, entity_type: str) -> int:
        logging.info(f"save df {entity_type} @{block_num} #{len(df)} into {table}")
        return copy_from_pandas_df(gp_conn, table, df.astype(dtypes))

    return df_saver
