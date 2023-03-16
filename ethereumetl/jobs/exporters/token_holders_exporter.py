from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    "_st",
    "_st_day",
    "blknum",
    "address",
    "balance",
    "total_supply",
]


def erc20_token_holders_exporter(holders_output):
    return CompositeItemExporter(
        filename_mapping={"holder": holders_output},
        field_mapping={"holder": FIELDS_TO_EXPORT},
    )
