import os
import pandas as pd
from time import time
from typing import Tuple
from blockchainetl.misc.tidb import TiDBConnector
from blockchainetl.service.sql_temp import to_loadcsv_sql, DEFAULT_FIELD_TERMINATED


def df_into_tidb(
    tidb: TiDBConnector,
    df: pd.DataFrame,
    target_table: str,
    key: str,
    tmpfile: str,
    retain_tmpfile: bool = False,
) -> Tuple[float, float, int]:
    df.to_csv(tmpfile, index=False, header=True, sep=DEFAULT_FIELD_TERMINATED)

    st1 = time()
    write_sql = to_loadcsv_sql(
        tmpfile,
        columns=",".join(list(df.columns)),
        table=target_table,
        ignore_lines=1,
    )
    affected_rows = tidb.execute(write_sql, "LOAD", key)
    st2 = time()
    if retain_tmpfile is False:
        os.unlink(tmpfile)

    return (st1, st2, affected_rows)
