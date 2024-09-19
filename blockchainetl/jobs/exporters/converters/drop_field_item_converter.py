class DropFieldItemConverter:
    def __init__(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        self.drop_keys = keys

    def convert_item(self, item):
        for key in self.drop_keys:
            if key in item:
                del item[key]
        return item
