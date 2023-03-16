import pandas as pd
from blockchainetl.enumeration.chain import Chain
from blockchainetl.service.label_service import LabelService

STOP_CATEGORIES = ("Exchange", "Service", "Dex")


class TrackOracle:
    def __init__(self, chain: Chain, db_url: str, db_table: str, db_schema: str):
        self._chain = chain
        self._labeler = LabelService(db_url, db_table, db_schema)

    def shold_stop(self, row: pd.Series) -> bool:
        address = row["address"]
        categories = self._labeler.category_of(address)
        # check if address is Exchange or Service(Mixer)
        for category in STOP_CATEGORIES:
            if category in categories:
                return True

        # check is coinjoin or peeling chain
        if self._chain in Chain.ALL_BITCOIN_FORKS:
            return self.is_bitcoin_coinjoin(row)

        return False

    # if address is stop of label, return the label
    # else return the stop reason
    def stop_of(self, address: str) -> str:
        labels = self._labeler.label_of(address)
        if labels is not None:
            return ";".join(labels)

        # TODO: found the stop reason
        return "Coinjoin"

    # TODO: migrate this algorithm from BlockSCI
    def is_bitcoin_coinjoin(self, row: pd.Series) -> bool:
        return row["n_tx_in_addr"] >= 5 and row["n_tx_out_addr"] >= 5
