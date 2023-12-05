#!/usr/bin/env python3

import json
import requests
from typing import Tuple, List, Dict, Union
from collections import defaultdict


# ref: https://github.com/prysmaticlabs/go-bitfield/blob/master/bitlist.go
def bitlist_len(bitlist):
    if len(bitlist) == 0:
        return 0
    # The most significant bit is present in the last byte in the array.
    last = bitlist[-1]

    # Determine the position of the most significant bit.
    msb = last.bit_length()
    if msb == 0:
        return 0

    # The absolute position of the most significant bit will be the number of
    # bits in the preceding bytes plus the position of the most significant
    # bit. Subtract this value by 1 to determine the length of the bitlist.
    return (8 * (len(bitlist) - 1)) + msb - 1


def bitlist_bitat(bitlist: bytes, idx: int):
    if idx >= bitlist_len(bitlist):
        return False

    i = 1 << (idx % 8)
    return bitlist[idx // 8] & i == i


class BeaconClient:
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url.rstrip("/")
        self.session = requests.Session()

    def get_latest_epoch(self) -> Tuple[int, int]:
        url = f"{self.rpc_url}/eth/v1/beacon/headers/head"
        r = self.session.get(url)
        if r.status_code // 100 != 2:
            raise ValueError(f"GET latest epoch return not 2xx: {r.status_code}")
        rs = r.json()
        if not isinstance(rs["data"], dict):
            raise ValueError(f"response is invalid: {rs}")
        slot = int(rs["data"]["header"]["message"]["slot"])
        epoch = slot // 32
        return epoch, slot

    def get_validator_head_state(self, pubkey: str):
        url = f"{self.rpc_url}/eth/v1/beacon/states/head/validators"
        r = self.session.get(url, params=dict(id=pubkey))
        if r.status_code // 100 != 2:
            raise ValueError(f"GET states of {pubkey} return not 2xx: {r.status_code}")
        rs = r.json()
        if len(rs["data"]) != 1 or not isinstance(rs["data"][0], dict):
            raise ValueError(f"response for {pubkey} is invalid: {rs}")
        result = rs["data"][0]
        return result

    def get_slot(self, slot: Union[int, str]) -> Dict:
        url = f"{self.rpc_url}/eth/v2/beacon/blocks/{slot}"
        r = self.session.get(url)
        if r.status_code // 100 != 2:
            raise ValueError(f"GET slot {slot} return not 2xx: {r.status_code}")
        rs = r.json()
        if not isinstance(rs["data"], dict):
            raise ValueError(f"response is invalid: {rs}")
        return rs["data"]

    # eg: this is one attestation of slot: 488202
    # {
    #     "aggregation_bits": "0xffffffffefffff...",
    #     "data": {
    #         "slot": "488201",
    #         "index": "35",
    #         "beacon_block_root": "0x0a2edf7a9e802d60353867a28...",
    #         "source": {
    #             "epoch": "15255",
    #             "root": "0x602c5f2a379a7dd255d35212764c69fd289df4f9f79cda9b3f61a97ea144cd54",
    #         },
    #         "target": {
    #             "epoch": "15256",
    #             "root": "0xea853cb65f16366450fb80d1d3a7db28f776b80e908e8dc550e08515a91ccc64",
    #         },
    #     },
    #     "signature": "0xa399f07187470c75460dc8b61bb3c93d3e6d9ce...",
    # }
    def get_slot_attestations(self, slot: Union[int, str]) -> List[Dict]:
        body = self.get_slot(slot)
        if body["message"]["slot"] != str(slot):
            raise ValueError(
                f"response of slot notmatch want: {slot} have: {body['message']['slot']}"
            )
        return body["message"]["body"]["attestations"]

    # [
    #  {
    #   "pubkey": "0xabe20fedbbda5a1759569aa5439522ccce524c97176bf4a63ebb076180072ea677f31c5bf0...",
    #   "validator_index": "1510932",
    #   "committee_index": "49",
    #   "committee_length": "742",
    #   "committees_at_slot": "64",
    #   "validator_committee_index": "157",
    #   "slot": "488243",
    #  },
    # ]
    def get_validator_duties(self, epoch, validator_indices: List[Union[int, str]]):
        indices = [str(e) for e in validator_indices]
        url = f"{self.rpc_url}/eth/v1/validator/duties/attester/{epoch}"
        r = self.session.post(url, json.dumps(indices))
        if r.status_code // 100 != 2:
            raise ValueError(f"GET epoch {epoch} return not 2xx: {r.status_code}")
        rs = r.json()
        if not isinstance(rs["data"], list):
            raise ValueError(f"response is invalid: {rs}")
        return rs["data"]

    def check_attestation_included_in_epoch(
        self, epoch, validator_indices: List[Union[int, str]]
    ):
        duties = self.get_validator_duties(epoch, validator_indices)
        if len(duties) != len(validator_indices):
            raise ValueError(
                f"validator duties({duties}) not match validator indices({validator_indices})"
            )

        min_slot = int(min(e["slot"] for e in duties))
        max_slot = int(max(e["slot"] for e in duties))

        # the same slot+index maybe maybe more than one attestation in different slot
        attestations = defaultdict(list)
        for slot_id in range(min_slot + 1, max_slot + 1 + 32):
            for atte in self.get_slot_attestations(slot_id):
                bits = bytes.fromhex(atte["aggregation_bits"][2:])
                attestations[(atte["data"]["slot"], atte["data"]["index"])].append(bits)

        for duty in duties:
            included = False

            for bitlist in attestations[(duty["slot"], duty["committee_index"])]:
                if bitlist_bitat(bitlist, int(duty["validator_committee_index"])):
                    included = True
                    break

            duty["included"] = included

        return duties
