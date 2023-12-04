import click
from typing import List


# hint: this class is not Enum
class EntityType:
    BLOCK = "block"
    UNCLE = "uncle"
    TRANSACTION = "transaction"
    TXPOOL = "txpool"
    RECEIPT = "receipt"
    LOG = "log"
    TOKEN_TRANSFER = "token_transfer"
    ERC721_TRANSFER = "erc721_transfer"
    ERC1155_TRANSFER = "erc1155_transfer"
    TRACE = "trace"
    CONTRACT = "contract"
    TOKEN = "token"
    TOKEN_BALANCE = "token_balance"
    ERC1155_BALANCE = "erc1155_balance"
    TOKEN_HOLDER = "token_holder"
    ERC1155_HOLDER = "erc1155_holder"
    ERC721_TOKENID = "erc721_tokenid"
    ERC1155_TOKENID = "erc1155_tokenid"
    NFT_ORDERBOOK = "nft_orderbook"
    HISTORY_BALANCE = "history_balance"
    LATEST_BALANCE = "latest_balance"
    TOKEN_HISTORY_BALANCE = "token_history_balance"
    TOKEN_LATEST_BALANCE = "token_latest_balance"
    ERC1155_HISTORY_BALANCE = "erc1155_history_balance"
    ERC1155_LATEST_BALANCE = "erc1155_latest_balance"

    ALL_FOR_STREAMING = [
        BLOCK,
        TRANSACTION,
        RECEIPT,
        LOG,
        TOKEN_TRANSFER,
        ERC721_TRANSFER,
        ERC1155_TRANSFER,
        TRACE,
        CONTRACT,
        TOKEN,
        TOKEN_BALANCE,
    ]

    ALL_FOR_ETL = [
        BLOCK,
        TRANSACTION,
        RECEIPT,
        LOG,
        TOKEN_TRANSFER,
        ERC721_TRANSFER,
        ERC1155_TRANSFER,
        TRACE,
    ]

    ALL_FOR_EASY_ETL = [
        BLOCK,
        TRANSACTION,
        LOG,
        TOKEN_TRANSFER,
        ERC1155_TRANSFER,
        TRACE,
    ]

    ALL_FOR_ALERT = [BLOCK, TRANSACTION, RECEIPT, LOG, TOKEN_TRANSFER]

    ALL_FOR_TRACK = [TRANSACTION, TOKEN_TRANSFER]


class EntityTable:
    def __init__(self):
        self._meta = {
            EntityType.BLOCK: "blocks",
            EntityType.TRANSACTION: "txs",
            EntityType.LOG: "logs",
            EntityType.TRACE: "traces",
            EntityType.CONTRACT: "contracts",
            EntityType.TOKEN: "tokens",
            EntityType.TOKEN_TRANSFER: "token_xfers",
            EntityType.ERC721_TRANSFER: "erc721_xfers",
            EntityType.ERC1155_TRANSFER: "erc1155_xfers",
            EntityType.TOKEN_BALANCE: "token_balances",
            EntityType.ERC1155_BALANCE: "erc1155_balances",
            EntityType.TOKEN_HOLDER: "addr_tokens",
            EntityType.ERC1155_HOLDER: "addr_erc1155_tokens",
            EntityType.ERC1155_TOKENID: "erc1155_token_ids",
            EntityType.ERC721_TOKENID: "erc721_token_ids",
        }

    def __getitem__(self, key: str) -> str:
        return self._meta[key]

    def get(self, key: str, default: str) -> str:
        return self._meta.get(key, default)


def chain_entity_table(
    chain: str,
    entity_type: str,
    dev_mode: bool = False,
    ignore_unknown: bool = False,
    external_mode: bool = False,
) -> str:
    if dev_mode:
        chain = chain + "_dev"
    if ignore_unknown is True:
        table = EntityTable().get(entity_type, entity_type + "s")
    else:
        table = EntityTable()[entity_type]

    if external_mode is True:
        table = "ext_" + table
    return chain + "." + table


def parse_entity_types(in_entity_types: str, ignore_unknown: bool = False) -> List[str]:
    entity_types = [c.strip() for c in in_entity_types.split(",") if c.strip() != ""]

    if ignore_unknown is True:
        return entity_types

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                "--entity-type",
                "{} is not an available entity type. "
                "Supply a comma separated list of types from {}".format(
                    entity_type, ",".join(EntityType.ALL_FOR_STREAMING)
                ),
            )

    return entity_types


ENTITY_TYPE_RENAMES = {
    EntityType.BLOCK: {
        "hash": "blkhash",
        "number": "blknum",
        "size": "blk_size",
        "transaction_count": "tx_count",
        "transactions_root": "txs_root",
        "timestamp": "_st",
    },
    EntityType.TRANSACTION: {
        "hash": "txhash",
        "transaction_index": "txpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.LOG: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.TOKEN_TRANSFER: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.ERC721_TRANSFER: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.ERC1155_TRANSFER: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.TRACE: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
    },
    EntityType.TOKEN: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
        "function_sighashes": "func_sighashes",
    },
    EntityType.CONTRACT: {
        "transaction_hash": "txhash",
        "transaction_index": "txpos",
        "log_index": "logpos",
        "block_number": "blknum",
        "block_timestamp": "_st",
        "function_sighashes": "func_sighashes",
    },
}
