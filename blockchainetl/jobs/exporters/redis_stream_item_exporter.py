import redis
from typing import Dict


from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


class RedisStreamItemExporter:
    def __init__(
        self,
        redis_url: str,
        item_type_to_stream_mapping: Dict[str, str],
        converters=(),
        max_workers=10,
    ):
        self.red = redis.from_url(redis_url)
        self.item_type_to_channel_mapping = item_type_to_stream_mapping
        self.converter = CompositeItemConverter(converters)
        self.max_workers = max_workers

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)
        for item_type, channel in self.item_type_to_channel_mapping.items():
            item_group = items_grouped_by_type.get(item_type)

            if item_group is None:
                continue

            for item in self.convert_items(item_group):
                self.export_item((channel, item))

    def export_item(self, stream_with_item):
        stream, item = stream_with_item
        self.red.xadd(stream, fields=item, id="*")

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass
