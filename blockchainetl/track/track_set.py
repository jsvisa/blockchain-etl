import os
import yaml
from itertools import groupby
from copy import copy
from typing import NamedTuple, List, Dict, Optional
from importlib import import_module as module
from yamlinclude import YamlIncludeConstructor


from blockchainetl.enumeration.chain import Chain
from ethereumetl.misc.constant import DEFAULT_TOKEN_ETH

from .track import Track
from .track_address import normalize_address
from .receivers import BaseReceiver
from .track_explorer import TrackExplorer


class TrackSet(NamedTuple):
    chain: str
    tracks: Dict[str, Track]

    @classmethod
    def load(cls, chain: str, tracksets: List[Dict]):
        tracks = {}
        for yaml_dict in tracksets:
            track = Track.load(chain, yaml_dict)
            tracks[track.id] = track
        return cls(chain, tracks)

    def get(self, track_id: str) -> Optional[Track]:
        return self.tracks.get(track_id)

    def start_block(self) -> int:
        return min(v.start_block for v in self.tracks.values())

    def __getitem__(self, track_id: str) -> Track:
        return self.tracks[track_id]

    def __contains__(self, track_id: str) -> bool:
        return self.get(track_id) is not None

    def dump(self) -> List[Dict]:
        tracks = []
        for track in self.tracks.values():
            for addr in track.addresses:
                base = {
                    "address": addr.address,
                    "track_id": track.id,
                    "start_block": addr.start_block,
                    "label": addr.label,
                    "description": addr.description,
                }
                if self.chain in Chain.ALL_ETHEREUM_FORKS:
                    base.update(
                        {
                            "token_address": DEFAULT_TOKEN_ETH,
                            "token_name": Chain.symbol(self.chain),
                        }
                    )

                # platform token is always being tracked
                # eg: gas fee
                tracks.append(base)
                for token_name, token_addr in (addr.tokens or dict()).items():
                    derive = copy(base)
                    derive.update(
                        {
                            "token_address": normalize_address(self.chain, token_addr),
                            "token_name": token_name,
                        }
                    )
                    tracks.append(derive)
        return tracks


class TrackSets(object):
    receivers: Dict[str, BaseReceiver] = dict()
    chain_tracks: Dict[str, TrackSet] = dict()

    @classmethod
    def load(cls, track_file):
        base_dir = os.path.dirname(track_file)
        YamlIncludeConstructor.add_to_loader_class(
            loader_class=yaml.FullLoader, base_dir=base_dir
        )
        with open(track_file) as fp:
            yaml_data = yaml.load(fp, Loader=yaml.FullLoader)
        receivers = yaml_data["receivers"]
        explorers = yaml_data.get("explorers", {})

        if receivers is None:
            receivers = {"print": {}}

        # dynamic load receiver with config
        # ref https://stackoverflow.com/a/19228066/2298986
        for name, args in receivers.items():
            typo = args["receiver"]
            init_args = args["init_args"]
            _m = args.get("module", f"blockchainetl.track.receivers.{typo}_receiver")
            m = module(_m)
            c = getattr(m, f"{typo.title()}Receiver")
            init_args.update({"explorer": TrackExplorer(explorers)})
            cls.receivers[name] = c(**init_args)

        tracksets = yaml_data.get("tracksets", [])
        for chain, items in groupby(tracksets, key=lambda d: d["chain"]):
            tracksets: List[Dict] = []
            for sets in items:
                tracksets.extend(sets["tracks"])
            cls.chain_tracks[chain] = TrackSet.load(chain, tracksets)

    @classmethod
    def open(cls):
        for _, receiver in cls.receivers.items():
            receiver.open()

    @classmethod
    def close(cls):
        for _, receiver in cls.receivers.items():
            receiver.close()

    def __getitem__(self, chain: str) -> TrackSet:
        return self.chain_tracks[chain]

    def get(self, chain: str) -> TrackSet:
        return self.__getitem__(chain)

    @classmethod
    def start_block(cls, chain: str) -> int:
        return cls.chain_tracks[chain].start_block()
