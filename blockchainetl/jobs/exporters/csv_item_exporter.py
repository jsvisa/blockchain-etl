import os
from typing import List, Dict, Set
from datetime import datetime

from blockchainetl.misc_utils import get_item_sink
from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


class CSVItemExporter:
    def __init__(
        self,
        output_dir: str,
        entity_types: Set[str],
        converters=(),
        notify_callback=None,
    ):
        self.output_dir = output_dir
        self.entity_types = entity_types
        self.converter = CompositeItemConverter(converters)
        self.notify_callback = notify_callback

    def open(self):
        pass

    def export_items(self, items: List[Dict]):
        if len(items) == 0:
            return

        items_grouped_by_type = group_by_item_type(items)
        for entity in self.entity_types:
            item_group = items_grouped_by_type.get(entity)
            if item_group is None or len(item_group) == 0:
                continue

            item0 = item_group[0]
            blknum = item0.get("number", item0.get("block_number"))
            ts = item0.get("timestamp", item0.get("block_timestamp"))

            base_dir = None
            if self.output_dir is not None:
                # use the time of the first row of this batch
                st_day = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                base_dir = os.path.join(self.output_dir, st_day)
                if not base_dir.startswith("s3://"):
                    os.makedirs(base_dir, exist_ok=True)
            output = os.path.join(base_dir, entity, f"{blknum}.csv")

            converted_items = self.convert_items(item_group)
            with get_item_sink(output, delimiter="^", quotechar="'") as sink:
                for item in converted_items:
                    sink(item)

            if self.notify_callback is not None:
                self.notify_callback(blknum, entity, output)

    def convert_items(self, items: List[Dict]):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass
