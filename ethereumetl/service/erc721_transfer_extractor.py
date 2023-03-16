from typing import Optional, Set
from blockchainetl.utils import hex_to_dec
from blockchainetl.enumeration.chain import Chain
from ethereumetl.domain.log import EthLog
from ethereumetl.domain.erc721_transfer import EthErc721Transfer
from ethereumetl.utils import to_normalized_address, split_to_words, word_to_address
from .token_transfer_extractor import TRANSFER_EVENT_TOPIC
from .cryptopunk_extractor import CRYPTOPUNK_TOKEN_ADDRESS


class EthErc721TransferExtractor(object):
    def __init__(self, erc20_tokens: Optional[Set] = None, chain: Optional[str] = None):
        self.erc20_tokens = erc20_tokens
        self.chain = chain or Chain.ETHEREUM

    def extract_transfer_from_log(self, log: EthLog) -> Optional[EthErc721Transfer]:
        topics = log.topics
        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return None

        token_address = to_normalized_address(log.address)

        # excluding the known ERC20 transfers,
        # all the others are assumed to be ERC721 transfers,
        # in which case there is redundant data, but no data loss
        if (
            not (
                self.chain == Chain.ETHEREUM
                and token_address == CRYPTOPUNK_TOKEN_ADDRESS
            )
            and topics[0] == TRANSFER_EVENT_TOPIC
            and (self.erc20_tokens is None or token_address not in self.erc20_tokens)
        ):
            # Transfer(index from, index to, _index id)
            return self._extract(log)

        return None

    def _extract(
        self,
        log: EthLog,
        required_length=4,
        from_index=1,
        to_index=2,
        id_index=3,
    ) -> Optional[EthErc721Transfer]:
        # TODO: Handle unindexed event fields
        topics_with_data = log.topics + split_to_words(log.data)
        # if the number of topics and fields in data part != 4, then it's a weird event
        if len(topics_with_data) != required_length:
            return None

        erc721_transfer = EthErc721Transfer()
        erc721_transfer.token_address = to_normalized_address(log.address)
        erc721_transfer.from_address = word_to_address(topics_with_data[from_index])
        erc721_transfer.to_address = word_to_address(topics_with_data[to_index])
        erc721_transfer.id = hex_to_dec(topics_with_data[id_index])
        erc721_transfer.transaction_hash = log.transaction_hash
        erc721_transfer.transaction_index = log.transaction_index
        erc721_transfer.log_index = log.log_index
        erc721_transfer.block_number = log.block_number
        return erc721_transfer
