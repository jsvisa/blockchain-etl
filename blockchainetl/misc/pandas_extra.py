from typing import List
from functools import reduce

import pandas as pd


def partition_rank(
    df: pd.DataFrame, group_by: List, rank_column="_rank"
) -> pd.DataFrame:
    df[rank_column] = df.groupby(group_by).cumcount()
    df_rank: pd.DataFrame = df.groupby(group_by).agg({rank_column: [min, max, "count"]})
    df_rank.columns = ["_".join(c) for c in df_rank.columns]
    df_rank.reset_index(inplace=True)

    return df.merge(df_rank, on=group_by)


def vsum(series: pd.Series) -> int:
    if len(series) == 0:
        return 0
    return reduce(lambda x, y: int(x) + int(y), series)
