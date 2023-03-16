from typing import Dict, Union, List, Any, Generator

from ethereumetl.domain.txpool import EthTxpool
from blockchainetl.utils import hex_to_dec
from ethereumetl.utils import to_normalized_address
from blockchainetl.enumeration.entity_type import EntityType


class EthTxpoolMapper(object):
    def json_dict_to_txpools(self, json_dict: Dict[str, Any]) -> List[EthTxpool]:
        txpools = []
        for pool_type, contents in json_dict.items():
            results = list(self._iterate_address_content(contents, pool_type))
            txpools.extend(results)
        return txpools

    def _extract_tx(self, tx: Dict[str, Any], pool_type) -> EthTxpool:
        txpool = EthTxpool()
        txpool.hash = tx.get("hash")
        txpool.transaction_index = hex_to_dec(tx.get("transactionIndex"))
        txpool.nonce = hex_to_dec(tx.get("nonce"))
        txpool.from_address = to_normalized_address(tx.get("from"))
        txpool.to_address = to_normalized_address(tx.get("to"))
        txpool.value = hex_to_dec(tx.get("value"))
        txpool.gas = hex_to_dec(tx.get("gas"))
        txpool.gas_price = hex_to_dec(tx.get("gasPrice"))
        txpool.input = tx.get("input")
        txpool.max_fee_per_gas = hex_to_dec(tx.get("maxFeePerGas"))
        txpool.max_priority_fee_per_gas = hex_to_dec(tx.get("maxPriorityFeePerGas"))
        txpool.transaction_type = hex_to_dec(tx.get("type"))
        txpool.pool_type = pool_type
        return txpool

    def txpool_to_dict(self, txpool: EthTxpool) -> Dict[str, Union[int, str, None]]:
        return {
            "type": EntityType.TXPOOL,
            "txhash": txpool.hash,
            "nonce": txpool.nonce,
            "txpos": txpool.transaction_index,
            "from_address": txpool.from_address,
            "to_address": txpool.to_address,
            "value": txpool.value,
            "gas": txpool.gas,
            "gas_price": txpool.gas_price,
            "input": txpool.input,
            "max_fee_per_gas": txpool.max_fee_per_gas,
            "max_priority_fee_per_gas": txpool.max_priority_fee_per_gas,
            "tx_type": txpool.transaction_type,
            "pool_type": txpool.pool_type,
        }

    def _iterate_address_content(
        self, address_contents: Dict[str, Any], pool_type: str
    ) -> Generator[EthTxpool, None, None]:
        for address, content in address_contents.items():
            address = to_normalized_address(address)

            # FIXME: Check that the nonce is continuous
            for nonce, tx in content.items():
                from_addr = to_normalized_address(tx.get("from"))
                if from_addr != address:
                    raise ValueError(
                        f"item's from({from_addr}) <> address({address}), values {address_contents}"
                    )
                item_nonce = hex_to_dec(tx.get("nonce"))
                if item_nonce != int(nonce):
                    raise ValueError(
                        f"item's nonce({item_nonce}) <> nonce({nonce}) contents is {content}"
                    )
                yield self._extract_tx(tx, pool_type)
