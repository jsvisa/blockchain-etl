import pandas as pd
from typing import Optional
from blockchainetl.enumeration.chain import Chain
from blockchainetl.service.label_service import LabelService
from blockchainetl.service.profile_service import ProfileService

STOP_CATEGORIES = ("Exchange", "Service", "Dex")


class TrackOracle:
    def __init__(
        self,
        chain: str,
        labeler: LabelService,
        profiler: Optional[ProfileService] = None,
    ):
        self.chain = chain
        self.labeler = labeler
        self.profiler = profiler

    def shold_stop(self, row: pd.Series) -> bool:
        address = row["address"]

        # let's check label first
        categories = self.labeler.category_of(address)
        # check if address is Exchange or Service(Mixer)
        for category in STOP_CATEGORIES:
            if category in categories:
                return True

        # check is coinjoin or peeling chain
        if self.chain in Chain.ALL_BITCOIN_FORKS:
            return self.is_bitcoin_coinjoin(row)

        # check profile second
        if self.profiler is not None:
            profile = self.profiler.get_profile(address)
            vin_txs = sum(e["vin_txs"] for e in profile)
            out_txs = sum(e["out_txs"] for e in profile)
            # We think a hacker will not use an address more than 20
            return vin_txs > 20 or out_txs > 20

        return False

    # if address is stop of label, return the label
    # else return the stop reason
    def stop_of(self, address: str) -> str:
        labels = self.labeler.label_of(address)
        if labels is not None:
            return ";".join(labels)

        if self.profiler is not None:
            profile = self.profiler.get_profile(address)
            return "Profile,HighInOutTxs" + ";".join(
                f"{e['typo']}:vin-{e['vin_txs']},out-{e['out_txs']}" for e in profile
            )

        # TODO: found the stop reason
        return "Coinjoin"

    # TODO: migrate this algorithm from BlockSCI
    def is_bitcoin_coinjoin(self, row: pd.Series) -> bool:
        return row["n_tx_in_addr"] >= 5 and row["n_tx_out_addr"] >= 5
