from typing import Optional, Set
from threading import Lock
from sqlalchemy import create_engine
from cachetools import cached, TTLCache


class LabelService:
    def __init__(self, db_url: str, db_table: str, db_schema: Optional[str]):
        self._engine = create_engine(db_url)
        if db_schema is None:
            self._db_table = db_table
        else:
            self._db_table = db_schema + "." + db_table

    # cache for 1hour
    @cached(cache=TTLCache(maxsize=10000, ttl=3600), lock=Lock())
    def label_of(self, address: str) -> Optional[Set[str]]:
        address = address.lower()
        result = self._engine.execute(
            f"SELECT address, label FROM {self._db_table} WHERE address = %s LIMIT 10",
            address,
        )
        if result is None:
            return None
        rows = result.fetchall()
        if len(rows) == 0:
            return None
        return set(row["label"] for row in rows)

    def category_of(self, address: str) -> Set[str]:
        labels = self.label_of(address)
        if labels is None:
            return set()
        return set(e.split(",")[0] for e in labels)
