from datetime import datetime
from typing import Union

from blockchainetl.service.label_service import LabelService


def label_of(url, schema, table="addr_labels"):
    if url is not None and schema is not None:
        ls = LabelService(url, table, schema)
    else:
        ls = None

    def _label_of(address):
        if ls is None:
            return ""
        return ls.label_of(address) or ""

    return _label_of


def tag_value_usd(value: int, *thresholds):
    if value is None:
        return ""

    level = 1
    for threshold in sorted(thresholds, reverse=True):
        if value > threshold:
            break
        else:
            level += 1
    if level > len(thresholds):
        return ""
    else:
        return f"Level {level} risk control alert"


def wei2eth(x: Union[int, str]) -> int:
    # FIXME: currently only hex string is supported
    if isinstance(x, str):
        if x == "0x":
            return 0
        x = int(x, 16)
    return x // 1e18


def toDateTime(x: Union[datetime, int]) -> str:
    if isinstance(x, int):
        x = datetime.utcfromtimestamp(x)
    return x.strftime("%Y-%m-%d %H:%M:%S +00")


def safe_int(x):
    if x is None:
        return None
    return int(x)


def safe_round(x, y=0):
    if x is None:
        return None
    return round(x, y)


ALL = {
    "int": int,
    "round": round,
    "safe_int": safe_int,
    "safe_round": safe_round,
    "wei2eth": wei2eth,
    "toDateTime": toDateTime,
    "toDatetime": toDateTime,
    "tag_value_usd": tag_value_usd,
}
