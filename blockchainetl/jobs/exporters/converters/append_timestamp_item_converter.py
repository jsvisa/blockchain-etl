from datetime import datetime, timezone


class AppendTimestampItemConverter:
    def __init__(self, timestamp_key="block_timestamp", st_key="_st"):
        self.timestamp_key = timestamp_key
        self.st_key = st_key

    def convert_item(self, item):
        if self.timestamp_key in item:
            item[self.st_key] = to_timestamp(item[self.timestamp_key])
        return item


def to_timestamp(st):
    if isinstance(st, int):
        return st
    else:
        return int(
            datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
