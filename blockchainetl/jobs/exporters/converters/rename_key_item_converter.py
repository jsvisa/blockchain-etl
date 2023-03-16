from .simple_item_converter import SimpleItemConverter


class RenameKeyItemConverter(SimpleItemConverter):
    def __init__(self, key_mapping=None):
        self.key_mapping = key_mapping or dict()

    def convert_key(self, key):
        return self.key_mapping.get(key, key)
