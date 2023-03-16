import json
import pandas as pd
from typing import List, Dict
from sqlalchemy import create_engine
from datetime import datetime
import logging

from . import BaseReceiver
from ..rule import Rule


class GpwriteReceiver(BaseReceiver):

    WECHAT_TITLE_COLORS = {
        "green": "info",
        "gray": "comment",
        "red": "warning",
    }

    def __init__(self, url: str, table: str):
        self._url = url
        self._table = table
        super().__init__()

    def open(self):
        self._engine = create_engine(self._url)

    def post(
        self,
        rule: Rule,
        result: List[Dict],
    ):
        schema = rule.chain

        message = []
        for item in result:
            # item is in nested dict
            # FIXME: use dict2obj instead?
            inner = list(item.values())[0]
            st = inner.get("block_timestamp", inner.get("timestamp"))
            if st is None:
                logging.warning(f"missing timestamp {inner}")
                continue

            if isinstance(st, int):
                st = datetime.utcfromtimestamp(st)

            blknum = inner.get("block_number", inner.get("number"))
            txhash = inner.get("transaction_hash", inner.get("hash"))
            labels = rule.labels.format(item)
            labels = {
                k: v
                for k, v in labels.items()
                if k not in set(["datetime", "blknum", "txhash"])
            }
            output = rule.output.format(item)

            message.append(
                {
                    "_st": int(st.timestamp()),
                    "_st_day": st.strftime("%Y-%m-%d"),
                    "blknum": blknum,
                    "txhash": txhash,
                    "scope": rule.scope,
                    "rule_id": rule.id,
                    "labels": json.dumps(labels),
                    "output": output,
                }
            )

        df = pd.DataFrame(message)
        df["rule_id"] = rule.id

        df.to_sql(
            self._table,
            con=self._engine,
            schema=schema,
            index=False,
            if_exists="append",
            method="multi",
            chunksize=100,
        )
