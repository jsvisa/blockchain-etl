import requests
from typing import List, Dict, Optional

from . import BaseReceiver
from ..rule import Rule


class SlackReceiver(BaseReceiver):
    def __init__(
        self,
        url: str,
        username: str,
        icon_emoji: str = ":robot_face:",
        channel: Optional[str] = None,
        icon_url: Optional[str] = None,
    ):
        self._url = url
        self._username = username
        self._icon_emoji = icon_emoji
        self._channel = channel
        self._icon_url = icon_url
        super().__init__()

    def post(
        self,
        rule: Rule,
        result: List[Dict],
    ):
        payload = dict()
        if self._username:
            payload["username"] = self._username
        if self._icon_url:
            payload["icon_url"] = self._icon_url
        if self._icon_emoji:
            payload["icon_emoji"] = self._icon_emoji
        if self._channel:
            payload["channel"] = self._channel

        pretext = f"Chain: `{rule.chain}` RuleID: `{rule.id}`"

        message = [f"Rule description: `{rule.description}`"]
        for item in result:
            labels = [
                f"{k.title()}: `{v}`" for k, v in rule.labels.format(item).items()
            ]
            message.extend(labels)
            message.append(rule.output.format(item))
        msg = "\n".join(message)

        payload["attachments"] = [
            {
                "color": "info",
                "fields": [{"title": "Chain Alert", "value": msg, "short": False}],
                "pretext": pretext,
                "fallback": pretext,
            }
        ]

        requests.post(self._url, json=payload)
