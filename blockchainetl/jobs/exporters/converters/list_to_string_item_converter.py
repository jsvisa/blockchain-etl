from .simple_item_converter import SimpleItemConverter


class ListToStringItemConverter(SimpleItemConverter):
    def __init__(self, keys=None, join=False):
        self.keys = set(keys) if keys else None
        self.join = join

    def convert_field(self, key, value):
        if isinstance(value, list) and (self.keys is None or key in self.keys):
            if self.join is True:
                return ",".join(str(e) for e in value)
            return str(value)
        else:
            return value
