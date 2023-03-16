import json
from typing import List, Dict

from . import BaseReceiver
from ..rule import Rule


class PrintReceiver(BaseReceiver):
    def __init__(self, file=None):
        self.file = file

    def post(self, rule: Rule, result: List[Dict]):
        data = []
        for item in result:
            data.append(
                {
                    "item": item,
                    "output": rule.output.format(item),
                    "labels": rule.labels.format(item),
                }
            )
        data = {"rule": rule.to_dict(), "data": data}
        print(json.dumps(data, indent=2), file=self.file)
