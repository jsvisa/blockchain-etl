from typing import NamedTuple, Optional, Dict, List, Set
import rule_engine


from .rule_scope import RuleScope
from .rule_label import RuleLabel
from .rule_output import RuleOutput
from .rule_condition import RuleCondition


class Rule(NamedTuple):
    id: str
    chain: str
    where: str
    description: str
    scope: RuleScope = RuleScope.TX
    condition: Optional[RuleCondition] = None
    plugin: Optional[str] = None
    output: RuleOutput = RuleOutput()
    labels: RuleLabel = RuleLabel()
    rule: Optional[rule_engine.Rule] = None
    receivers: Set[str] = set()
    enabled: bool = True

    def filter(self, items: Dict[str, List]) -> List:
        assert self.rule is not None

        # if not enabled, or receiver is null, don't need to filter
        if self.enabled is False or len(self.receivers) == 0:
            return []

        todo = items.get(self.scope)
        if todo is None:
            return []
        return list(self.rule.filter(todo))

    @classmethod
    def load(cls, chain: str, provider: Optional[Dict], yaml_dict: Dict):
        context = rule_engine.Context(default_value=None)
        rule = rule_engine.Rule(yaml_dict["where"], context=context)
        output = RuleOutput(yaml_dict.get("output"))
        condition = None
        if "condition" in yaml_dict:
            if provider is None:
                raise ValueError("condition is given but no provider")
            condition = RuleCondition(condition, chain, provider.get("provider_uri"))

        labels = RuleLabel(yaml_dict.get("labels", {}))

        # TODO: check receivers is subset of global.receivers
        receivers = set(yaml_dict.get("receivers") or [])

        return cls(
            id=yaml_dict["id"],
            chain=chain,
            where=yaml_dict["where"],
            description=yaml_dict["description"],
            scope=RuleScope.from_str(yaml_dict.get("scope", RuleScope.TX)),
            condition=condition,
            plugin=yaml_dict.get("plugin"),
            output=output,
            labels=labels,
            receivers=receivers,
            rule=rule,
            enabled=yaml_dict.get("enabled", True),
        )

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "where": self.where,
            "scope": self.scope,
            "condition": str(self.condition or ""),
            "plugin": self.plugin,
            "output": str(self.output),
            "labels": str(self.labels),
            "receivers": str(self.receivers),
            "enabled": self.enabled,
        }
