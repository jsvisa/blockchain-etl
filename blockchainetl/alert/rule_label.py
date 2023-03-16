from typing import Dict
from .rule_output import RuleOutput


class RuleLabel(dict):
    def format(self, s: Dict) -> Dict:
        output = {}
        for key, val in self.items():
            output[key] = RuleOutput(val).format(s)
        return output
