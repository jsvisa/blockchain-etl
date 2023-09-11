import math
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import groupby
from typing import Dict, List, Optional

import pypeln as pl

from blockchainetl.alert.receivers import BaseReceiver
from blockchainetl.alert.rule_set import RuleSet
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.service.label_service import LabelService
from blockchainetl.service.price_service import PriceService
from blockchainetl.service.token_service import TokenService
from ethereumetl.domain.token import EthToken


class AlertExporter:
    def __init__(
        self,
        chain: Chain,
        ruleset: RuleSet,
        receivers: Dict[str, BaseReceiver],
        max_workers: int = 5,
        worker_mode: str = "thread",
        rule_id: Optional[str] = None,
        token_service: Optional[TokenService] = None,
        price_service: Optional[PriceService] = None,
        label_service: Optional[LabelService] = None,
    ):
        self._chain = chain
        self._receivers = receivers
        self._ruleset = ruleset
        self._ruleid = rule_id
        self._max_workers = max_workers
        if worker_mode == "process":
            self._executor = ProcessPoolExecutor
        else:
            self._executor = ThreadPoolExecutor
        self._token_service = token_service
        self._price_service = price_service
        self._label_service = label_service

    def open(self):
        pass

    def remap_entity_type(self, entity_type: str) -> str:
        return {
            EntityType.TRANSACTION: "tx",
            EntityType.TOKEN_TRANSFER: "token_xfer",
        }.get(entity_type, entity_type)

    def restruct(self, items: List[Dict]) -> Dict[str, List[Dict]]:
        for item in items:
            item["type"] = self.remap_entity_type(item["type"])

        if self._chain in Chain.ALL_ETHEREUM_FORKS:
            self.enrich_items(items)

        grouped = {
            typo: list(sub) for typo, sub in groupby(items, key=lambda d: d["type"])
        }

        # run one block at a time
        assert len(grouped["block"]) == 1, "alert one block at a time"

        block = grouped["block"][0]
        txs = grouped.get("tx")
        if txs is None or len(txs) == 0:
            return {"block": [{"block": block}]}

        # convert plain list into dict-list
        txkey = (
            "txhash" if self._chain in Chain.ALL_BITCOIN_FORKS else "transaction_hash"
        )
        txhash_grouped = {}
        for key, values in grouped.items():
            if key == "block":
                grouped[key] = [{key: val} for val in values]
                continue
            if key == "tx":
                continue

            # log -> logs
            txhash_grouped[key + "s"] = {
                txhash: list(sub)
                for txhash, sub in groupby(values, key=lambda d: d[txkey])
            }

            # [{'type': 'log', ...}] -> [{'log': {'type': 'log', ...}}]
            grouped[key] = [{key: val} for val in values]

        # fill in other fields into tx, with key=txhash
        full_txs = []
        for tx in txs:
            full = {"block": block, "tx": tx}
            for key, values in txhash_grouped.items():
                full.update({key: values.get(tx["hash"])})
            full_txs.append(full)
        grouped["tx"] = full_txs
        return grouped

    def export_items(self, items: List[Dict]):
        if len(items) == 0:
            return

        full_items = self.restruct(items)

        if self._ruleid is not None:
            rule_results = [self._ruleset.execute_rule(self._ruleid, full_items)]
        else:
            rule_results = list(
                self._ruleset.execute(self._executor, full_items, self._max_workers)
            )

        logging.info(f"rule results: {rule_results}")

        for rule_id, result in rule_results:
            if isinstance(result, ValueError):
                raise Exception(f"failed to execute {rule_id} {result}")

            if len(result) == 0:
                continue

            # TODO: in async and safe mode
            rule = self._ruleset[rule_id]
            for rec, receiver in self._receivers.items():
                if rec in rule.receivers:
                    receiver.post(rule, result)

    def enrich_items(self, items: List[Dict]):
        pl.thread.each(self.enrich_item, items, workers=10, run=True)

    def enrich_item(self, item: Dict):
        ts = self._token_service
        ps = self._price_service
        lb = self._label_service

        if lb is not None and item["type"] in ("tx", "token_xfer", "trace"):
            self.enrich_address(lb, item)

        if item["type"] in ("tx", "trace"):
            self.enrich_ether(ps, item)
        elif ts is not None and item["type"] == "token_xfer":
            self.enrich_erc20(ts, ps, item)

    def enrich_address(self, lb: LabelService, item: Dict):
        item["from_label"] = lb.label_of(item["from_address"])
        item["to_label"] = lb.label_of(item["to_address"])

    def enrich_ether(self, ps: PriceService, item: Dict):
        # convert Decimals to float
        # else raise TypeError: unsupported operand type(s) for *: 'float' and 'decimal.Decimal'
        # The value field may not exist or is None,
        # eg: https://cronoscan.com/tx/0x0893bd0b33bcb00c69c5d8d9d59c0077efdfcb0eaa4a3d454607b910e6a6293d # noqa
        item["value_amount"] = float((item.get("value") or 0)) / 1e18

        if ps is not None:
            price = ps.get_price(self._chain, time=item.get("block_timestamp"))
        item["value_usd"] = item["value_amount"] * price if price else None

    def enrich_erc20(self, ts: TokenService, ps: PriceService, item: Dict):
        token_address = item["token_address"]

        # enrich only if item is rpc or from not enriched db
        if item.get("name") is None and item.get("decimals") is None:
            token: EthToken = ts.get_token(token_address)
            if item.get("name") is None:
                item["name"] = token.symbol or token.name

            item["decimals"] = token.decimals

        # set default attributes to None
        item["value_amount"] = None
        item["value_usd"] = None
        decimals = item.get("decimals")
        if decimals is not None:
            item["value_amount"] = float(item["value"]) / math.pow(10, decimals)

            if ps is not None:
                price = ps.get_price(
                    self._chain, token_address, time=item.get("block_timestamp")
                )
            item["value_usd"] = item["value_amount"] * price if price else None

    def close(self):
        pass
