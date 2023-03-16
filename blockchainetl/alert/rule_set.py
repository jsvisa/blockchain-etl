import os
import json
import yaml
from itertools import groupby
from typing import NamedTuple, List, Dict, Type, Union, Generator, Tuple, Optional
from concurrent.futures import as_completed, ThreadPoolExecutor, ProcessPoolExecutor
from importlib import import_module as module
from yamlinclude import YamlIncludeConstructor
from rule_engine.errors import EvaluationError
from jinja2 import Template

from .rule import Rule
from .receivers import BaseReceiver


def parse_jinja_rule(rule_file, rule_variable_dir):
    # 0. jinja render
    variables = {}
    for f in os.listdir(rule_variable_dir):
        file = os.path.join(rule_variable_dir, f)
        if not os.path.isfile(file) or not f.endswith(".var"):
            continue

        var = f.rstrip(".var")
        with open(file) as fw:
            variables[var] = json.load(fw)

    with open(rule_file) as fp:
        template = Template(fp.read())

    jinja_rendered = template.render(**variables)

    # 1. yaml include load
    YamlIncludeConstructor.add_to_loader_class(
        loader_class=yaml.FullLoader,
        base_dir=os.path.dirname(rule_file),
    )
    return yaml.load(jinja_rendered, Loader=yaml.FullLoader)


class RuleSet(NamedTuple):
    chain: str
    rules: Dict[str, Rule]

    @classmethod
    def load(cls, chain: str, provider: Optional[Dict], rulesets: List[Dict]):
        rules = {}
        for yaml_dict in rulesets:
            rule = Rule.load(chain, provider, yaml_dict)
            rules[rule.id] = rule
        return cls(chain, rules)

    def execute(
        self,
        executor: Union[Type[ProcessPoolExecutor], Type[ThreadPoolExecutor]],
        items: Dict[str, List],
        max_workers: int = 10,
    ) -> Generator[Tuple[str, List[Dict]], None, None]:
        with executor(max_workers=max_workers) as exec:
            futures = {
                exec.submit(rule.filter, items): rule.id for rule in self.rules.values()
            }

            for future in as_completed(futures):
                rule_id = futures[future]
                try:
                    matched = list(future.result())
                except EvaluationError as e:
                    yield (rule_id, ValueError(e))
                    continue

                yield (rule_id, matched)

    def execute_rule(
        self, rule_id: str, items: Dict[str, List]
    ) -> Tuple[str, List[Dict]]:
        return (rule_id, self[rule_id].filter(items))

    def get(self, rule_id: str) -> Optional[Rule]:
        return self.rules.get(rule_id)

    def __getitem__(self, rule_id: str) -> Rule:
        return self.rules[rule_id]

    def __contains__(self, rule_id: str) -> bool:
        return self.get(rule_id) is not None


class RuleSets(object):
    receivers: Dict[str, BaseReceiver] = dict()
    chain_rules: Dict[str, RuleSet] = dict()

    @classmethod
    def load(cls, rule_file: str, rule_variable_dir: str):
        yaml_data = parse_jinja_rule(rule_file, rule_variable_dir)
        receivers = yaml_data["receivers"]
        providers = yaml_data.get("providers", {})

        if receivers is None:
            receivers = {"print": {}}

        # dynamic load receiver with config
        # ref https://stackoverflow.com/a/19228066/2298986
        for name, args in receivers.items():
            typo = args["receiver"]
            init_args = args["init_args"]
            m = module(f"blockchainetl.alert.receivers.{typo}_receiver")
            c = getattr(m, f"{typo.title()}Receiver")
            cls.receivers[name] = c(**init_args)

        rulesets = yaml_data["rulesets"]
        for chain, items in groupby(rulesets, key=lambda d: d["chain"]):
            rulesets: List[Dict] = []
            for sets in items:
                rulesets.extend(sets["rules"])
            cls.chain_rules[chain] = RuleSet.load(chain, providers.get(chain), rulesets)

    @classmethod
    def open(cls):
        for _, receiver in cls.receivers.items():
            receiver.open()

    @classmethod
    def close(cls):
        for _, receiver in cls.receivers.items():
            receiver.close()

    def __getitem__(self, chain: str) -> RuleSet:
        return self.chain_rules[chain]

    def get(self, chain: str) -> List[RuleSet]:
        return self.__getitem__(chain)
