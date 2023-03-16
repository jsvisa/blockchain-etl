from datetime import datetime


class AppendDateItemConverter:
    def __init__(self, timestamp_key="block_timestamp", date_key="_st_day"):
        self.timestamp_key = timestamp_key
        self.date_key = date_key

    def convert_item(self, item):
        if self.timestamp_key in item:
            item[self.date_key] = to_date(item[self.timestamp_key])
        return item


def to_date(st):
    if isinstance(st, int):
        return datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")
    elif isinstance(st, str) and len(st) > 10:
        return st[:10]
    else:
        raise ValueError(f"st: {st} not a valid format of date/timestamp")
