from typing import Dict, Any
from datetime import datetime
from blockchainetl.utils import hex_to_dec
from ethereumetl.domain.block import EthUncleBlock
from ethereumetl.utils import to_normalized_address


class EthUncleBlockMapper(object):
    def json_dict_to_block(self, json_dict: Dict[str, Any]) -> EthUncleBlock:
        block = EthUncleBlock()
        block.number = hex_to_dec(json_dict.get("number"))
        block.hash = json_dict.get("hash")
        block.parent_hash = json_dict.get("parentHash")
        block.mix_hash = json_dict.get("mixHash")
        block.nonce = json_dict.get("nonce")
        block.sha3_uncles = json_dict.get("sha3Uncles")
        block.logs_bloom = json_dict.get("logsBloom")
        block.transactions_root = json_dict.get("transactionsRoot")
        block.state_root = json_dict.get("stateRoot")
        block.receipts_root = json_dict.get("receiptsRoot")
        block.miner = to_normalized_address(json_dict.get("miner"))
        block.difficulty = hex_to_dec(json_dict.get("difficulty"))
        block.size = hex_to_dec(json_dict.get("size"))
        block.extra_data = json_dict.get("extraData")
        block.gas_limit = hex_to_dec(json_dict.get("gasLimit"))
        block.gas_used = hex_to_dec(json_dict.get("gasUsed"))
        block.timestamp = hex_to_dec(json_dict.get("timestamp"))
        block.base_fee_per_gas = hex_to_dec(json_dict.get("baseFeePerGas"))
        block.uncle_count = len(json_dict["uncles"] or [])

        return block

    def block_to_dict(self, block: EthUncleBlock) -> Dict[str, Any]:
        return {
            "type": "uncle",
            "_st": block.timestamp,
            "_st_day": datetime.utcfromtimestamp(block.timestamp).strftime("%Y-%m-%d"),
            "blknum": block.number,
            "blkhash": block.hash,
            "parent_hash": block.parent_hash,
            "mix_hash": block.mix_hash,
            "nonce": block.nonce,
            "sha3_uncles": block.sha3_uncles,
            "logs_bloom": block.logs_bloom,
            "txs_root": block.transactions_root,
            "state_root": block.state_root,
            "receipts_root": block.receipts_root,
            "miner": block.miner,
            "difficulty": block.difficulty,
            "blk_size": block.size,
            "extra_data": block.extra_data,
            "gas_limit": block.gas_limit,
            "gas_used": block.gas_used,
            "base_fee_per_gas": block.base_fee_per_gas,
            "uncle_count": block.uncle_count,
            "hermit_blknum": block.hermit_blknum,
            "hermit_uncle_pos": block.hermit_uncle_pos,
        }
