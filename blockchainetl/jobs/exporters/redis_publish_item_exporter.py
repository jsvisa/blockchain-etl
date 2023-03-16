import redis
import json
import logging
import pypeln as pl
from time import time
from typing import Dict, Optional

from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


class RedisPublishItemExporter:
    def __init__(
        self,
        redis_url: str,
        item_type_to_channel_mapping: Dict[str, str],
        converters=(),
        max_workers=10,
        timely_export_interval: Optional[int] = None,
    ):
        self.red = redis.from_url(redis_url)
        self.item_type_to_channel_mapping = item_type_to_channel_mapping
        self.converter = CompositeItemConverter(converters)
        self.max_workers = max_workers
        self.timely_export_interval = timely_export_interval
        self.last_exported_time = None

    def open(self):
        pass

    def export_items(self, items):
        if len(items) == 0:
            return

        now = time()
        if (
            self.timely_export_interval is not None
            and self.last_exported_time is not None
            and now - self.last_exported_time < self.timely_export_interval
        ):
            logging.info("RedisPublishItemExporter export too frequency, skipped")
            return

        items_grouped_by_type = group_by_item_type(items)
        for item_type, channel in self.item_type_to_channel_mapping.items():
            item_group = items_grouped_by_type.get(item_type)

            if item_group is None:
                continue

            converted_items = self.convert_items(item_group)
            pl.thread.each(
                self.export_item,
                ((channel, item) for item in converted_items),
                workers=self.max_workers,
                run=True,
            )

        # what we promised is the receiver receive the alerts
        # at least once per last_exported_time
        self.last_exported_time = time()

    def export_item(self, channel_with_item):
        channel, item = channel_with_item
        self.red.publish(channel, json.dumps(item).encode())

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass
