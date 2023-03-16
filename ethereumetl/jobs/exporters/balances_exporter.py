from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    "address",
    "balance",
    "nonce",
    "root",
    "code_hash",
    "key",
]


def balances_exporter(balance_output):
    return CompositeItemExporter(
        filename_mapping={"balance": balance_output},
        field_mapping={"balance": FIELDS_TO_EXPORT},
    )
