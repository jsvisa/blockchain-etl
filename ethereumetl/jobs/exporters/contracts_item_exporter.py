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

FIELDS_TO_EXPORT = [
    "address",
    "bytecode",
    "function_sighashes",
    "is_erc20",
    "is_erc721",
    "block_number",
]

FULL_FIELDS_TO_EXPORT = [
    "_st",
    "_st_day",
    "address",
    "creater",
    "initcode",
    "bytecode",
    "function_sighashes",
    "is_erc20",
    "is_erc721",
    "block_number",
    "transaction_hash",
    "transaction_index",
    "trace_type",
    "trace_address",
]


def contracts_item_exporter(contracts_output, full_output=False):
    return CompositeItemExporter(
        filename_mapping={"contract": contracts_output},
        field_mapping={
            "contract": FULL_FIELDS_TO_EXPORT if full_output else FIELDS_TO_EXPORT
        },
    )
