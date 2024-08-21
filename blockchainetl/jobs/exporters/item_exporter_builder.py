from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
from blockchainetl.jobs.exporters.psycopg_item_exporter import PsycopgItemExporter
from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.converters import (
    IntToStringItemConverter,
    ListFieldItemConverter,
    AppendDateItemConverter,
    RenameKeyItemConverter,
    RenameFieldItemConverter,
    ListToStringItemConverter,
    ListCountItemConverter,
    UnixTimestampItemConverter,
    DropFieldItemConverter,
)
from ethereumetl.streaming import postgres_tables as pg
from ethereumetl.streaming import tsdb_tables as evm_ts
from bitcoinetl.streaming import tsdb_tables as btc_ts


def create_postgres_exporter(
    dbschema: str,
    connection_url: str,
    workers=2,
    pool_size=5,
    batch_size=100,
    print_sql=False,
):
    item_exporter_type = determine_item_exporter_type(connection_url)
    if item_exporter_type != ItemExporterType.POSTGRES:
        raise ValueError("not implemented")

    item_exporter = PostgresItemExporter(
        connection_url,
        dbschema,
        item_type_to_insert_stmt_mapping={
            EntityType.BLOCK: create_insert_statement_for_table(pg.BLOCKS, False),
            EntityType.TRANSACTION: create_insert_statement_for_table(
                pg.TRANSACTIONS, False
            ),
            EntityType.LOG: create_insert_statement_for_table(pg.LOGS, False),
            EntityType.TOKEN_TRANSFER: create_insert_statement_for_table(
                pg.TOKEN_TRANSFERS, False
            ),
            EntityType.ERC721_TRANSFER: create_insert_statement_for_table(
                pg.ERC721_TRANSFERS, False
            ),
            EntityType.ERC1155_TRANSFER: create_insert_statement_for_table(
                pg.ERC1155_TRANSFERS, False
            ),
            EntityType.TRACE: create_insert_statement_for_table(pg.TRACES, False),
            EntityType.TOKEN: create_insert_statement_for_table(pg.TOKENS, False),
            EntityType.CONTRACT: create_insert_statement_for_table(pg.CONTRACTS, False),
        },
        converters=[
            RenameFieldItemConverter(
                item_mapping={
                    EntityType.BLOCK: {
                        "hash": "blkhash",
                        "number": "blknum",
                        "size": "blk_size",
                        "transaction_count": "tx_count",
                        "transactions_root": "txs_root",
                    },
                    EntityType.TRANSACTION: {
                        "hash": "txhash",
                        "transaction_type": "tx_type",
                    },
                    EntityType.ERC721_TRANSFER: {
                        "id": "token_id",
                    },
                    EntityType.ERC1155_TRANSFER: {
                        "id": "token_id",
                    },
                }
            ),
            RenameKeyItemConverter(
                key_mapping={
                    "timestamp": "_st",
                    "block_timestamp": "_st",
                    "block_number": "blknum",
                    "transaction_hash": "txhash",
                    "transaction_index": "txpos",
                    "index": "txpos",  # bitcoin
                    "log_index": "logpos",
                }
            ),
            AppendDateItemConverter("_st", "_st_day"),
            IntToStringItemConverter(keys=["token_id", "value"]),
            ListCountItemConverter("topics", new_field_prefix="n_"),
            ListFieldItemConverter("topics", "topic", fill=4),
            ListToStringItemConverter(keys=["trace_address"]),
        ],
        print_sql=print_sql,
        workers=workers,
        pool_size=pool_size,
        batch_size=batch_size,
    )
    return item_exporter


def create_tsdb_exporter(
    chain: str,
    dbschema: str,
    connection_url: str,
    workers=2,
    pool_size=5,
    batch_size=100,
    print_sql=False,
):
    item_exporter_type = determine_item_exporter_type(connection_url)
    if item_exporter_type != ItemExporterType.POSTGRES:
        raise ValueError("not implemented")

    if chain in Chain.ALL_ETHEREUM_FORKS:
        item_type_to_insert_stmt_mapping = {
            EntityType.BLOCK: create_insert_statement_for_table(
                evm_ts.BLOCKS, False, schema=dbschema
            ),
            EntityType.TRANSACTION: create_insert_statement_for_table(
                evm_ts.TRANSACTIONS, False, schema=dbschema
            ),
            EntityType.LOG: create_insert_statement_for_table(
                evm_ts.LOGS, False, schema=dbschema
            ),
            EntityType.TOKEN_TRANSFER: create_insert_statement_for_table(
                evm_ts.TOKEN_TRANSFERS, False, schema=dbschema
            ),
            EntityType.ERC721_TRANSFER: create_insert_statement_for_table(
                evm_ts.ERC721_TRANSFERS, False, schema=dbschema
            ),
            EntityType.ERC1155_TRANSFER: create_insert_statement_for_table(
                evm_ts.ERC1155_TRANSFERS, False, schema=dbschema
            ),
            EntityType.TRACE: create_insert_statement_for_table(
                evm_ts.TRACES, False, schema=dbschema
            ),
            EntityType.TOKEN: create_insert_statement_for_table(
                evm_ts.TOKENS, False, schema=dbschema
            ),
            EntityType.CONTRACT: create_insert_statement_for_table(
                evm_ts.CONTRACTS, False, schema=dbschema
            ),
        }
        item_type_to_table_mapping = {
            EntityType.BLOCK: evm_ts.BLOCKS.fullname,
            EntityType.TRANSACTION: evm_ts.TRANSACTIONS.fullname,
            EntityType.LOG: evm_ts.LOGS.fullname,
            EntityType.TOKEN_TRANSFER: evm_ts.TOKEN_TRANSFERS.fullname,
            EntityType.ERC721_TRANSFER: evm_ts.ERC721_TRANSFERS.fullname,
            EntityType.ERC1155_TRANSFER: evm_ts.ERC1155_TRANSFERS.fullname,
            EntityType.TRACE: evm_ts.TRACES.fullname,
            EntityType.TOKEN: evm_ts.TOKENS.fullname,
            EntityType.CONTRACT: evm_ts.CONTRACTS.fullname,
        }

    else:
        item_type_to_insert_stmt_mapping = {
            EntityType.BLOCK: create_insert_statement_for_table(
                btc_ts.BLOCKS, False, schema=dbschema
            ),
            EntityType.TRANSACTION: create_insert_statement_for_table(
                btc_ts.TRANSACTIONS, False, schema=dbschema
            ),
            EntityType.TRACE: create_insert_statement_for_table(
                btc_ts.TRACES, False, schema=dbschema
            ),
        }
        item_type_to_table_mapping = {
            EntityType.BLOCK: btc_ts.BLOCKS.fullname,
            EntityType.TRANSACTION: btc_ts.TRANSACTIONS.fullname,
            EntityType.TRACE: btc_ts.TRACES.fullname,
        }

    if connection_url.startswith("postgresql+psycopg2://"):
        connection_url = connection_url.replace(
            "postgresql+psycopg2://", "postgresql://"
        )
        extra_converters = [
            DropFieldItemConverter(["type", "trace_id", "block_hash", "item_timestamp"])
        ]
        return PsycopgItemExporter(
            connection_url,
            dbschema,
            item_type_to_table_mapping=item_type_to_table_mapping,
            converters=evm_exporter_converters() + extra_converters,
            print_sql=print_sql,
        )

    return PostgresItemExporter(
        connection_url,
        dbschema,
        item_type_to_insert_stmt_mapping=item_type_to_insert_stmt_mapping,
        converters=evm_exporter_converters(),
        print_sql=print_sql,
        workers=workers,
        pool_size=pool_size,
        batch_size=batch_size,
    )


def evm_exporter_converters():
    return [
        UnixTimestampItemConverter(),
        RenameFieldItemConverter(
            item_mapping={
                EntityType.BLOCK: {
                    "hash": "blkhash",
                    "number": "blknum",
                    "timestamp": "block_timestamp",
                    "size": "blk_size",
                    "transaction_count": "tx_count",
                    "transactions_root": "txs_root",
                },
                EntityType.TRANSACTION: {
                    "hash": "txhash",
                    "transaction_type": "tx_type",
                },
                EntityType.CONTRACT: {
                    "function_sighashes": "func_sighashes",
                },
                EntityType.ERC721_TRANSFER: {
                    "id": "token_id",
                },
                EntityType.ERC1155_TRANSFER: {
                    "id": "token_id",
                },
            }
        ),
        RenameKeyItemConverter(
            key_mapping={
                "block_number": "blknum",
                "transaction_hash": "txhash",
                "transaction_index": "txpos",
                "index": "txpos",  # bitcoin
                "log_index": "logpos",
            }
        ),
        IntToStringItemConverter(keys=["token_id", "value"]),
        ListCountItemConverter("topics", new_field_prefix="n_"),
        ListToStringItemConverter(keys=["trace_address"], join=False),
        ListToStringItemConverter(keys=["func_sighashes"], join=False),
        ListToStringItemConverter(keys=["topics"], join=True),
    ]


def determine_item_exporter_type(output):
    if output is not None and output.startswith("postgresql"):
        return ItemExporterType.POSTGRES
    elif output is not None and output.startswith("mysql"):
        return ItemExporterType.MYSQL
    elif output is not None and output.startswith("/"):
        return ItemExporterType.FILE
    elif output is not None and output.startswith("s3://"):
        return ItemExporterType.S3
    elif output is None or output == "console":
        return ItemExporterType.CONSOLE
    else:
        return ItemExporterType.UNKNOWN


class ItemExporterType:
    POSTGRES = "postgres"
    MYSQL = "mysql"
    FILE = "file"
    S3 = "s3"
    CONSOLE = "console"
    UNKNOWN = "unknown"
