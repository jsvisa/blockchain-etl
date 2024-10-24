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


from typing import Dict, Any, Union, List
from ethereumetl.domain.log import EthLog
from ethereumetl.utils import to_normalized_address
from blockchainetl.utils import hex_to_dec


class EthLogMapper(object):
    def json_dict_to_log(self, json_dict: Dict[str, Any]) -> EthLog:
        log = EthLog()

        log.log_index = hex_to_dec(json_dict.get("logIndex"))
        log.transaction_hash = json_dict.get("transactionHash")
        log.transaction_index = hex_to_dec(json_dict.get("transactionIndex"))
        log.block_hash = json_dict.get("blockHash")
        log.block_number = hex_to_dec(json_dict.get("blockNumber"))
        log.block_timestamp = hex_to_dec(json_dict.get("blockTimestamp"))
        log.address = to_normalized_address(json_dict.get("address"))
        log.data = json_dict.get("data")
        log.topics = json_dict.get("topics", [])

        return log

    def web3_dict_to_log(self, json_dict: Dict[str, Any]) -> EthLog:
        log = EthLog()

        log.log_index = json_dict.get("logIndex")

        transaction_hash = json_dict.get("transactionHash")
        if transaction_hash is not None:
            transaction_hash = transaction_hash.hex()
        log.transaction_hash = transaction_hash
        log.transaction_index = json_dict.get("transactionIndex")

        block_hash = json_dict.get("blockHash")
        if block_hash is not None:
            block_hash = block_hash.hex()
        log.block_hash = block_hash

        log.block_number = json_dict.get("blockNumber")
        log.block_timestamp = json_dict.get("blockTimestamp")
        log.address = to_normalized_address(json_dict.get("address"))
        log.data = json_dict.get("data")

        if "topics" in json_dict:
            log.topics = [topic.hex() for topic in json_dict["topics"]]

        return log

    def log_to_dict(self, log: EthLog) -> Dict[str, Union[str, int, None, List[str]]]:
        return {
            "type": "log",
            "log_index": log.log_index,
            "transaction_hash": log.transaction_hash,
            "transaction_index": log.transaction_index,
            "block_hash": log.block_hash,
            "block_number": log.block_number,
            "block_timestamp": log.block_timestamp,
            "address": log.address,
            "data": log.data,
            "topics": log.topics,
        }

    def dict_to_log(self, json_dict: Dict[str, Any]) -> EthLog:
        log = EthLog()

        log.log_index = json_dict.get("log_index")
        log.transaction_hash = json_dict.get("transaction_hash")
        log.transaction_index = json_dict.get("transaction_index")
        log.block_hash = json_dict.get("block_hash")
        log.block_number = json_dict.get("block_number")
        log.block_timestamp = json_dict.get("block_timestamp")
        log.address = to_normalized_address(json_dict.get("address"))
        log.data = json_dict.get("data")

        topics = json_dict.get("topics", [])
        if isinstance(topics, str):
            if len(topics.strip()) == 0:
                log.topics = []
            else:
                log.topics = topics.strip().split(",")
        else:
            log.topics = topics

        return log
