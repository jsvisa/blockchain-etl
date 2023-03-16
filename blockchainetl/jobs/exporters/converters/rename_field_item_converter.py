from typing import Optional, Dict


class RenameFieldItemConverter:
    def __init__(self, item_mapping: Optional[Dict[str, Dict[str, str]]] = None):
        self.item_mapping = item_mapping or dict()

    def convert_item(self, item):
        item_type = item["type"]
        if item_type not in self.item_mapping:
            return item

        item_converters = self.item_mapping[item_type]
        return {item_converters.get(key, key): value for key, value in item.items()}
