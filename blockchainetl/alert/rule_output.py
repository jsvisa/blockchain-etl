import json
from typing import Optional, Dict
from decimal import Decimal
from . import rule_udf as ru


# Decimal is returned by Bitcoin, Ethereum returns float
def EncodeDecimal(o):
    if isinstance(o, Decimal):
        return float(round(o, 8))
    raise TypeError(repr(o) + " is not JSON serializable")


# ref https://stackoverflow.com/a/1305682/2298986
# TODO: if attribute is not found, print useful messages
class dict2obj(object):
    def __init__(self, d: Dict):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [dict2obj(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, dict2obj(b) if isinstance(b, dict) else b)


class RuleOutput(object):
    def __init__(self, template: Optional[str] = None):
        self._template = template

    def __repr__(self):
        return self._template or "none"

    def format(self, s: Dict) -> str:
        if self._template is None:
            return json.dumps(s, default=EncodeDecimal)

        return self.fstr(**dict2obj(s).__dict__)

    def fstr(self, **kwargs):
        kwargs.update(ru.ALL)
        return eval(f"f'{self._template}'", kwargs)
