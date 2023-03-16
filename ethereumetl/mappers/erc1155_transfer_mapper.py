from typing import Dict, Union

from ethereumetl.domain.erc1155_transfer import EthErc1155Transfer


class EthErc1155TransferMapper(object):
    def erc1155_transfer_to_dict(
        self, erc1155_transfer: EthErc1155Transfer
    ) -> Dict[str, Union[str, int, None]]:
        return {
            "type": "erc1155_transfer",
            "token_address": erc1155_transfer.token_address,
            "token_name": erc1155_transfer.token_name,
            "operator": erc1155_transfer.operator,
            "from_address": erc1155_transfer.from_address,
            "to_address": erc1155_transfer.to_address,
            "id": erc1155_transfer.id,
            "value": erc1155_transfer.value,
            "id_pos": erc1155_transfer.id_pos,
            "id_cnt": erc1155_transfer.id_cnt,
            "xfer_type": erc1155_transfer.xfer_type,
            "transaction_hash": erc1155_transfer.transaction_hash,
            "transaction_index": erc1155_transfer.transaction_index,
            "log_index": erc1155_transfer.log_index,
            "block_number": erc1155_transfer.block_number,
        }
