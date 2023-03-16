import simplejson as json

from kafka import KafkaProducer

from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


class KafkaItemExporter:
    def __init__(
        self,
        bootstrap_servers,
        item_type_to_topic_mapping,
        converters=(),
        ssl_mode=False,
    ):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SSL" if ssl_mode else "PLAINTEXT",
        )

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)
        for item_type, topic in self.item_type_to_topic_mapping.items():
            item_group = items_grouped_by_type.get(item_type)

            if item_group is None:
                continue

            converted_items = self.convert_items(item_group)
            for item in converted_items:
                data = json.dumps(item).encode("utf-8")
                self.producer.send(topic, data)

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass
