from millify import millify
import pandas as pd
from typing import Optional, Union
import requests
from datetime import datetime

from . import BaseReceiver
from ..track_explorer import TrackExplorer

import logging


class SlackReceiver(BaseReceiver):
    def __init__(
        self,
        url: str,
        explorer: TrackExplorer,
        username: str,
        icon_emoji: str = ":skull:",
        channel: Optional[str] = None,
        icon_url: Optional[str] = None,
    ):
        self._url = url
        self._explorer = explorer
        self._username = username
        self._icon_emoji = icon_emoji
        self._channel = channel
        self._icon_url = icon_url
        super().__init__()

    def post(
        self,
        chain: str,
        result: pd.DataFrame,
    ):
        result["msg"] = result.apply(lambda row: self.format_body(row), axis=1)

        msg_grouped = (
            result.sort_values(
                ["blknum", "block_timestamp", "txhash", "out_value"], ascending=False
            )
            .groupby(by=["track_id", "blknum", "block_timestamp", "txhash"])  # type: ignore
            .agg({"msg": list})
            .reset_index()  # type: ignore
        )
        for _, row in msg_grouped.iterrows():  # type: ignore
            payload = dict()
            if self._username:
                payload["username"] = self._username
            if self._icon_url:
                payload["icon_url"] = self._icon_url
            if self._icon_emoji:
                payload["icon_emoji"] = self._icon_emoji
            if self._channel:
                payload["channel"] = self._channel

            pretext = f"Chain: `{chain}` TrackID: `{row['track_id']}` Block: `{row['blknum']}` "
            pretext += f"Datetime: `{self.toDateTime(row['block_timestamp'])}` <{self.tx_url(chain, row['txhash'])}|Click here for more detail>"  # noqa: E501

            msg = "\n".join(row["msg"][:10])
            if len(row["msg"]) > 10:
                msg += f"\n and #{len(row['msg'])-10} more..."

            payload["attachments"] = [
                {
                    "color": "danger",
                    "fields": [
                        {"title": "Transfer outflow", "value": msg, "short": False}
                    ],
                    "pretext": pretext,
                    "fallback": pretext,
                }
            ]

            r = requests.post(self._url, json=payload)
            if r.status_code // 100 != 2:
                logging.warning("post slack failed: {r.status_code} {r.text}")

    def format_body(self, row):
        value, symbol = row["out_value"], row["token_name"]

        template = (
            ""
            + "txhash: `{txhash}`\n"
            + "from: `{fm}`\n"
            + "         --[{value} {symbol}]---HOP: {hop}--->\n"
            + "to: `{to}`\n"
            + "(STOP: `{stop}`)"
        )
        return template.format(
            txhash=row["txhash"],
            fm=row["from_address"],
            value=millify(value, precision=2),
            symbol=symbol,
            hop=row["hop"],
            to=row["address"],
            stop=row["label"] if row["stop"] else False,
        )

    def toDateTime(self, x: Union[datetime, int]) -> str:
        if isinstance(x, int):
            x = datetime.utcfromtimestamp(x)
        return x.strftime("%Y-%m-%d %H:%M:%S +00")

    def tx_url(self, chain: str, tx: str) -> str:
        return self._explorer.tx_url(chain, tx)
