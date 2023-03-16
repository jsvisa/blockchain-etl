from typing import List, Dict
from ..rule import Rule


class BaseReceiver(object):
    def open(self):
        pass

    def post(self, rule: Rule, result: List[Dict]):
        rule, result = rule, result
        raise NotImplementedError

    def close(self):
        pass
