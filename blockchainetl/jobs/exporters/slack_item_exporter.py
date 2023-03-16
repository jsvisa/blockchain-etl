import requests
from typing import List, Dict, Tuple, Optional
from .converters.composite_item_converter import CompositeItemConverter


class SlackItemExporter:
    def __init__(
        self,
        webhook: str,
        username: str,
        pretext: str,
        item_type_to_export_mapping: Dict[str, Tuple],
        converters=(),
        icon_emoji: str = ":robot_face:",
        channel: Optional[str] = None,
        icon_url: Optional[str] = None,
    ):
        self.converter = CompositeItemConverter(converters)

        self.webhook = webhook
        self.username = username
        self.pretext = pretext
        self.icon_emoji = icon_emoji
        self.channel = channel
        self.icon_url = icon_url
        self.item_type_to_export_mapping = item_type_to_export_mapping or dict()

    def open(self):
        pass

    def export_items(self, items: List[Dict]):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        if item.get("type") not in self.item_type_to_export_mapping:
            return

        item = self.converter.convert_item(item)
        exported_keys = self.item_type_to_export_mapping[item["type"]]
        payload = dict()
        if self.username:
            payload["username"] = self.username
        if self.icon_url:
            payload["icon_url"] = self.icon_url
        if self.icon_emoji:
            payload["icon_emoji"] = self.icon_emoji
        if self.channel:
            payload["channel"] = self.channel

        message = [f"{k.title()}: `{item.get(k)}`" for k in exported_keys if k in item]
        msg = "\n".join(message)

        payload["attachments"] = [
            {
                "color": "info",
                "fields": [{"title": "Chain Alert", "value": msg, "short": False}],
                "pretext": self.pretext,
                "fallback": self.pretext,
            }
        ]

        requests.post(self.webhook, json=payload)

    def close(self):
        pass
