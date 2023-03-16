import logging
from typing import Optional, List

from blockchainetl.utils import hex_to_dec
from ethereumetl.domain.log import EthLog
from ethereumetl.domain.erc1155_transfer import EthErc1155Transfer
from ethereumetl.utils import to_normalized_address, split_to_words, word_to_address

TRANSFER_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
TRANSFER_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

logger = logging.getLogger(__name__)


class EthErc1155TransferExtractor(object):
    def extract_transfer_from_log(
        self, log: EthLog
    ) -> Optional[List[EthErc1155Transfer]]:
        topics = log.topics
        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return None

        # event TransferSingle(
        #     address indexed operator,
        #     address indexed from,
        #     address indexed to,
        #     uint256 id,
        #     uint256 value
        # );
        # event TransferBatch(
        #     address indexed operator,
        #     address indexed from,
        #     address indexed to,
        #     uint256[] ids,
        #     uint256[] values
        # );
        topics_0 = topics[0]
        topics_with_data = topics + split_to_words(log.data)
        if topics_0 in (TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC):
            # Handle unindexed event fields
            # 1 dynamic array needs at least 2*32bytes, 2 is 4
            # eg:
            # In [216]: from eth_abi import encode_abi
            # In [216]: split_to_words('0x'+encode_abi(["uint256[]", "uint256[]"],  [[], []]).hex())
            # Out[216]:
            # ['0x0000000000000000000000000000000000000000000000000000000000000040',
            #  '0x0000000000000000000000000000000000000000000000000000000000000060',
            #  '0x0000000000000000000000000000000000000000000000000000000000000000',
            #  '0x0000000000000000000000000000000000000000000000000000000000000000']

            n_topics = len(topics_with_data)
            if (topics_0 == TRANSFER_SINGLE_TOPIC and n_topics != 6) or (
                topics_0 == TRANSFER_BATCH_TOPIC and n_topics < 8
            ):
                logger.warning(
                    "The number of topics and data parts "
                    "is not equal to 6 or greater then 8 in log {} of transaction {}".format(
                        log.log_index, log.transaction_hash
                    )
                )
                return None

            if topics_0 == TRANSFER_SINGLE_TOPIC:
                xfer = EthErc1155Transfer()
                xfer.token_address = to_normalized_address(log.address)
                xfer.operator = word_to_address(topics_with_data[1])
                xfer.from_address = word_to_address(topics_with_data[2])
                xfer.to_address = word_to_address(topics_with_data[3])
                xfer.id = hex_to_dec(topics_with_data[4])  # type: ignore
                xfer.value = hex_to_dec(topics_with_data[5])  # type: ignore
                xfer.id_pos = 0
                xfer.id_cnt = 1
                xfer.xfer_type = "TransferSingle"
                xfer.transaction_hash = log.transaction_hash
                xfer.transaction_index = log.transaction_index
                xfer.log_index = log.log_index
                xfer.block_number = log.block_number
                return [xfer]

            else:
                # dynamic array is head-tail encoding,
                # the first (N-array)*32bytes are header, stores each position of the array start location
                # the tail layout is array length + each element's value
                # ref https://medium.com/@hayeah/how-to-decipher-a-smart-contract-method-call-8ee980311603
                offset = 4 + 2
                id_cnt: int = hex_to_dec(topics_with_data[offset])  # type: ignore
                va_cnt: int = hex_to_dec(topics_with_data[offset + id_cnt + 1])  # type: ignore
                if id_cnt != va_cnt:
                    logging.warning(
                        "The number of ids({}) and values are not equal({})".format(
                            id_cnt, va_cnt
                        )
                    )
                    return None

                xfers: List[EthErc1155Transfer] = []
                for id_pos in range(0, id_cnt):
                    xfer = EthErc1155Transfer()
                    xfer.token_address = to_normalized_address(log.address)
                    xfer.operator = word_to_address(topics_with_data[1])
                    xfer.from_address = word_to_address(topics_with_data[2])
                    xfer.to_address = word_to_address(topics_with_data[3])

                    base = offset + 1 + id_pos
                    xfer.id = hex_to_dec(topics_with_data[base])  # type: ignore
                    xfer.value = hex_to_dec(topics_with_data[base + id_cnt + 1])  # type: ignore
                    xfer.id_pos = id_pos
                    xfer.id_cnt = id_cnt
                    xfer.xfer_type = "TransferBatch"
                    xfer.transaction_hash = log.transaction_hash
                    xfer.transaction_index = log.transaction_index
                    xfer.log_index = log.log_index
                    xfer.block_number = log.block_number
                    xfers.append(xfer)
                return xfers

        return None
