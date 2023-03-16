class DropFieldItemConverter:
    def __init__(self, keys):
        self.drop_keys = keys

    def convert_item(self, item):
        for key in self.drop_keys:
            if key in item:
                del item[key]
        return item
