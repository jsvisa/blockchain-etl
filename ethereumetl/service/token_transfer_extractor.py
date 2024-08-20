# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import logging
from typing import Optional

from blockchainetl.utils import hex_to_dec
from blockchainetl.enumeration.chain import Chain
from ethereumetl.misc.constant import ZERO_ADDR
from ethereumetl.domain.log import EthLog
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.utils import to_normalized_address, split_to_words, word_to_address

# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
# Transfer(index from, index to, value)
TRANSFER_EVENT_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

# https://etherscan.io/token/0x9e9fbde7c7a83c43913bddc8779158f1368f0413
# ERC20Transfer(address indexed from, address indexed to, uint256 amount)
ERC20_TRANSFER_EVENT_TOPIC = (
    "0xe59fdd36d0d223c0c7d996db7ad796880f45e1936cb0bb7ac102e7082e031487"
)

# WETH: "https://etherscan.io/token/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
# the old contract missing Deposit/Withdrawal events
# WETH_OLD: = "https://etherscan.io/token/0x2956356cd2a2bf3202f771f50d3d14a367b48070"

# Deposit(address indexed dst, uint256 wad)
DEPOSIT_EVENT_TOPIC = (
    "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
)
# Withdrawal(address indexed src, uint256 wad)
WITHDRAWAL_EVENT_TOPIC = (
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
)

TRANSFER_EVENT_TOPICS = [
    TRANSFER_EVENT_TOPIC,
    ERC20_TRANSFER_EVENT_TOPIC,
    DEPOSIT_EVENT_TOPIC,
    WITHDRAWAL_EVENT_TOPIC,
]

logger = logging.getLogger(__name__)


class EthTokenTransferExtractor(object):
    def __init__(self, chain: None):
        self.chain = chain

    def extract_transfer_from_log(self, log: EthLog) -> Optional[EthTokenTransfer]:
        topics = log.topics
        n_topics = len(topics)
        if topics is None or n_topics < 1:
            # This is normal, topics can be empty for anonymous events
            return None

        topics_0 = topics[0]

        # FIXME: drop the Pandora ERC721 Transfer
        if (
            self.chain == Chain.ETHEREUM
            and log.address == "0x9e9fbde7c7a83c43913bddc8779158f1368f0413"
            and topics_0 == TRANSFER_EVENT_TOPIC
        ):
            return None

        # convert Deposit/Withdrawal events to Transfer
        if topics_0 in (DEPOSIT_EVENT_TOPIC, WITHDRAWAL_EVENT_TOPIC) and n_topics > 1:
            log.topics = [
                TRANSFER_EVENT_TOPIC,
                ZERO_ADDR if topics_0 == DEPOSIT_EVENT_TOPIC else topics[1],
                topics[1] if topics_0 == DEPOSIT_EVENT_TOPIC else ZERO_ADDR,
            ]
            topics_0 = TRANSFER_EVENT_TOPIC

        # event Transfer(address indexed from, address indexed to, uint256 value);
        if topics_0 in (TRANSFER_EVENT_TOPIC, ERC20_TRANSFER_EVENT_TOPIC):
            return self._extract(log)

        return None

    def _extract(self, log: EthLog) -> Optional[EthTokenTransfer]:
        # Handle unindexed event fields
        topics_with_data = log.topics + split_to_words(log.data)
        # if the number of topics and fields in data part != 4, then it's a weird event
        if len(topics_with_data) != 4:
            logger.warning(
                "The number of topics and data parts "
                "is not equal to 4 in log {} of transaction {}".format(
                    log.log_index, log.transaction_hash
                )
            )
            return None

        token_transfer = EthTokenTransfer()
        token_transfer.token_address = to_normalized_address(log.address)
        token_transfer.from_address = word_to_address(topics_with_data[1])
        token_transfer.to_address = word_to_address(topics_with_data[2])
        token_transfer.value = hex_to_dec(topics_with_data[3])
        token_transfer.transaction_hash = log.transaction_hash
        token_transfer.transaction_index = log.transaction_index
        token_transfer.log_index = log.log_index
        token_transfer.block_number = log.block_number
        return token_transfer
