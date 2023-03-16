from typing import Tuple, Optional
from datetime import datetime, timedelta
from sqlalchemy.engine import Engine


def get_time_range_of_blocks(
    chain: str,
    engine: Engine,
    start_block: int,
    end_block: int,
    start_timestamp: datetime,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    # backfill and forefill 2 days
    st = start_timestamp - timedelta(days=2)

    row = engine.execute(
        f"""SELECT
    min(block_timestamp) AS min_timestamp,
    max(block_timestamp) AS max_timestamp
FROM
    {chain}.blocks
WHERE
    block_timestamp >= '{st}'
    AND blknum >= {start_block}
    AND blknum <= {end_block}
    """
    ).fetchone()

    if row is None:
        return None, None
    return row["min_timestamp"], row["max_timestamp"]
