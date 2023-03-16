from typing import NamedTuple, Dict, List, Set

from .track_address import TrackAddress, normalize_address


class Track(NamedTuple):
    id: str
    chain: str
    label: str
    start_block: int
    description: str
    addresses: List[TrackAddress]
    receivers: Set[str] = set()
    enabled: bool = True

    @classmethod
    def load(cls, chain: str, yaml_dict: Dict):
        label = yaml_dict["label"]
        start_block = yaml_dict["start_block"]
        description = yaml_dict["description"]
        minmum_block = start_block

        addresses = []
        for item in yaml_dict["addresses"]:
            if "label" not in item:
                item["label"] = label
            if "start_block" not in item:
                item["start_block"] = start_block
            if "description" not in item:
                item["description"] = description
            item["address"] = normalize_address(chain, item["address"])
            addresses.append(TrackAddress(**item))

            minmum_block = min(minmum_block, item["start_block"])

        # TODO: check receivers is subset of global.receivers
        receivers = set(yaml_dict.get("receivers") or [])

        return cls(
            id=yaml_dict["id"],
            chain=chain,
            label=label,
            start_block=minmum_block,
            description=description,
            addresses=addresses,
            receivers=receivers,
            enabled=yaml_dict.get("enabled", True),
        )

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "description": self.description,
            "start_block": self.start_block,
            "label": self.label,
            "addresses": str(self.addresses),
            "enabled": self.enabled,
        }
