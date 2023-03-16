from typing import Dict, Union

from ethereumetl.domain.erc721_transfer import EthErc721Transfer


class EthErc721TransferMapper(object):
    def erc721_transfer_to_dict(
        self, erc721_transfer: EthErc721Transfer
    ) -> Dict[str, Union[str, int, None]]:
        return {
            "type": "erc721_transfer",
            "token_address": erc721_transfer.token_address,
            "token_name": erc721_transfer.token_name,
            "from_address": erc721_transfer.from_address,
            "to_address": erc721_transfer.to_address,
            "id": erc721_transfer.id,
            "transaction_hash": erc721_transfer.transaction_hash,
            "transaction_index": erc721_transfer.transaction_index,
            "log_index": erc721_transfer.log_index,
            "block_number": erc721_transfer.block_number,
        }
