import pandas as pd

from typing import Optional, List, Dict, Generator, Union
from blockchainetl.utils import hex_to_dec
from blockchainetl.enumeration.chain import Chain
from ethereumetl.domain.log import EthLog
from ethereumetl.domain.erc721_transfer import EthErc721Transfer
from ethereumetl.utils import to_normalized_address, split_to_words, word_to_address
from ethereumetl.misc.constant import ZERO_ADDR
from .token_transfer_extractor import TRANSFER_EVENT_TOPIC

CRYPTOPUNK_TOKEN_ADDRESS = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

# PunkTransfer(from, to, punkIndex)
PUNK_TRANSFER_EVENT_TOPIC = (
    "0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8"
)
# PunkBought(index punkIndex, value, index fromAddress, index toAddress)
PUNK_BOUGHT_EVENT_TOPIC = (
    "0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3"
)
# Assign(to, punkIndex)
ASSIGN_EVENT_TOPIC = (
    "0x8a0e37b73a0d9c82e205d4d1a3ff3d0b57ce5f4d7bccf6bac03336dc101cb7ba"
)

CRYPTOPUNK_TRANSFER_EVENT_TOPICS = (
    TRANSFER_EVENT_TOPIC,
    PUNK_TRANSFER_EVENT_TOPIC,
    PUNK_BOUGHT_EVENT_TOPIC,
    ASSIGN_EVENT_TOPIC,
)


class EthCryptoPunkTransferExtractor(object):
    def __init__(self, chain: Optional[str] = None):
        self.chain = chain or Chain.ETHEREUM

    def extract(self, log: EthLog) -> Optional[dict]:
        token_address = to_normalized_address(log.address)
        if self.chain != Chain.ETHEREUM or token_address != CRYPTOPUNK_TOKEN_ADDRESS:
            return None

        topics_0 = log.topics[0]
        if topics_0 == PUNK_TRANSFER_EVENT_TOPIC:
            # PunkTransfer(index from, index to, index punkIndex)
            from_index, to_index, id_index, required_length = 1, 2, 3, 4
        elif topics_0 == PUNK_BOUGHT_EVENT_TOPIC:
            # PunkBought(index punkIndex, value, index fromAddress, index toAddress)
            from_index, to_index, id_index, required_length = 2, 3, 1, 5

        elif topics_0 == TRANSFER_EVENT_TOPIC:
            # Transfer(index address from, index address to, uint256 value)
            from_index, to_index, id_index, required_length = 1, 2, None, 4
        elif topics_0 == ASSIGN_EVENT_TOPIC:
            # Assign(index to, punkIndex)
            from_index, to_index, id_index, required_length = None, 1, 2, 3
        else:
            return None

        topics_with_data = log.topics + split_to_words(log.data)
        if len(topics_with_data) != required_length:
            return None

        return {
            "topics_0": topics_0,
            "token_address": token_address,
            "from_address": (
                ZERO_ADDR
                if topics_0 == ASSIGN_EVENT_TOPIC
                else word_to_address(topics_with_data[from_index])
            ),
            "to_address": word_to_address(topics_with_data[to_index]),
            "token_id": (
                hex_to_dec(topics_with_data[id_index]) if id_index is not None else None
            ),
            "transaction_hash": log.transaction_hash,
            "transaction_index": log.transaction_index,
            "log_index": log.log_index,
            "block_number": log.block_number,
        }

    def merge(
        self, logs: List[Dict]
    ) -> Generator[Dict[str, Union[str, int]], None, None]:
        df = pd.DataFrame(logs)
        merged_keys = ["block_number", "transaction_hash", "transaction_index"]

        transfer_df = (
            df[df["topics_0"] == TRANSFER_EVENT_TOPIC]
            .copy()
            .rename(
                columns={
                    "topics_0": "s_topics_0",
                    "token_address": "s_token_address",
                    "token_id": "s_token_id",
                    "from_address": "s_from_address",
                    "to_address": "s_to_address",
                    "log_index": "s_log_index",
                }
            )
        )

        assign_df = (
            df[df["topics_0"] == ASSIGN_EVENT_TOPIC]
            .copy()
            .rename(columns={"to_address": "s_to_address", "log_index": "s_log_index"})
        )

        punk_transfer_df = (
            df[df["topics_0"] == PUNK_TRANSFER_EVENT_TOPIC]
            .merge(transfer_df, how="inner", on=merged_keys)
            .query("token_address == s_token_address")
            .query("log_index == s_log_index + 1")
            .copy()
        )

        bought_df = (
            df[
                (df["topics_0"] == PUNK_BOUGHT_EVENT_TOPIC)
                & (df["to_address"] != ZERO_ADDR)
            ]
            .merge(transfer_df, how="inner", on=merged_keys)
            .query("token_address == s_token_address")
            .query("log_index == s_log_index + 2")
            .copy()
        )

        bid_df = (
            df[(df["topics_0"] == PUNK_BOUGHT_EVENT_TOPIC)]
            .merge(transfer_df, how="inner", on=merged_keys)
            .query("token_address == s_token_address")
            .query("log_index == s_log_index + 1")
            .copy()
        )

        df = pd.concat(
            [assign_df, punk_transfer_df, bought_df, bid_df],
            ignore_index=True,
        )

        for _, row in df.iterrows():
            yield self._into_token_transfer(row)

    def _into_token_transfer(self, d: Dict) -> EthErc721Transfer:
        xfer = EthErc721Transfer()
        xfer.token_address = d["token_address"]
        xfer.from_address = d["from_address"]
        xfer.to_address = d["s_to_address"]
        xfer.id = int(d["token_id"])  # pandas may convert the int into float
        xfer.transaction_hash = d["transaction_hash"]
        xfer.transaction_index = d["transaction_index"]
        xfer.log_index = int(d["s_log_index"])
        xfer.block_number = int(d["block_number"])
        return xfer
