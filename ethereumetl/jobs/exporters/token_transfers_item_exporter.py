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


from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter

FIELDS_TO_EXPORT = [
    "token_address",
    "from_address",
    "to_address",
    "value",
    "transaction_hash",
    "log_index",
    "block_number",
]


def token_transfers_item_exporter(token_transfer_output, converters=()):
    return CompositeItemExporter(
        filename_mapping={"token_transfer": token_transfer_output},
        field_mapping={"token_transfer": FIELDS_TO_EXPORT},
        converters=converters,
    )


class TokenTransferPostresItemExporter:
    def __init__(self, pg_exporter: PostgresItemExporter, addon_erc721: bool = False):
        self.pg_exporter = pg_exporter
        self.addon_erc721 = addon_erc721

    def open(self):
        pass

    def export_items(self, items):
        rowcount = self.pg_exporter.export_items(items)

        if self.addon_erc721 is True:
            for item in items:
                item["id"] = item.pop("value")
                item["type"] = "erc721_transfer"
            rowcount += self.pg_exporter.export_items(items)
        return rowcount

    def export_item(self, item):
        self.pg_exporter.export_item(item)

        if self.addon_erc721 is True:
            item["id"] = item.pop("value")
            item["type"] = "erc721_transfer"
            self.pg_exporter.export_item(item)

    def close(self):
        pass
