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


import os
import json
import logging
import itertools
from typing import Union, Dict, Tuple, Any, List, Iterable
from collections import defaultdict
from blockchainetl import env


need_store_error = (
    env.STORE_ERROR_IF_ENRICH_NOT_MATCHED_PATH is not None
    and os.path.isdir(env.STORE_ERROR_IF_ENRICH_NOT_MATCHED_PATH)
)


logger = logging.getLogger(__name__)


def _handle_not_matched_error(
    left: Tuple[str, List],
    right: Tuple[str, List],
    result: Tuple[str, List],
    left_on: List[str],
    right_on: List[str],
):
    if need_store_error is True:
        d = env.STORE_ERROR_IF_ENRICH_NOT_MATCHED_PATH
        logger.warning(f"store mismatched {right[0]} into {d}")
        with open(os.path.join(d, left[0]) + ".json", "w") as fw:
            json.dump(left[1], fw, indent=2)
        with open(os.path.join(d, right[0]) + ".json", "w") as fw:
            json.dump(right[1], fw, indent=2)
        with open(os.path.join(d, result[0]) + ".json", "w") as fw:
            json.dump(result[1], fw, indent=2)

    if env.IGNORE_ENRICH_NOT_MATCHED_ERROR is False:
        left_items = set(tuple([e[x] for x in left_on]) for e in left[1])
        right_items = set(tuple([e[x] for x in right_on]) for e in right[1])
        left_only = left_items - right_items
        right_only = right_items - left_items
        raise ValueError(
            f"The number of {right[0]} is wrong "
            f"actual: {len(result[1])} <> expected: {len(right[1])} "
            f"left_on: {left_on} right_on: {right_on} "
            f"left_only: {left_only} right_only: {right_only} "
        )


def join(
    left: List[Dict[str, Any]],
    right: List[Dict[str, Any]],
    join_fields: Union[str, Tuple],
    left_fields: List[Union[str, Tuple]],
    right_fields: List[Union[str, Tuple]],
) -> Iterable[Dict[str, Any]]:
    left_join_field, right_join_field = join_fields

    def field_list_to_dict(field_list):
        result_dict = {}
        for field in field_list:
            if isinstance(field, tuple):
                result_dict[field[0]] = field[1]
            else:
                result_dict[field] = field
        return result_dict

    left_fields_as_dict = field_list_to_dict(left_fields)
    right_fields_as_dict = field_list_to_dict(right_fields)

    left_map = defaultdict(list)
    for item in left:
        key = (
            tuple(item[e] for e in left_join_field)
            if isinstance(left_join_field, tuple)
            else item[left_join_field]
        )
        left_map[key].append(item)

    right_map = defaultdict(list)
    for item in right:
        key = (
            tuple(item[e] for e in right_join_field)
            if isinstance(right_join_field, tuple)
            else item[right_join_field]
        )
        right_map[key].append(item)

    for key in left_map.keys():
        for left_item, right_item in itertools.product(left_map[key], right_map[key]):
            result_item = {}
            for src_field, dst_field in left_fields_as_dict.items():
                result_item[dst_field] = left_item.get(src_field)
            for src_field, dst_field in right_fields_as_dict.items():
                result_item[dst_field] = right_item.get(src_field)

            yield result_item


def enrich_transactions(transactions, receipts):
    result = list(
        join(
            transactions,
            receipts,
            (
                ("block_number", "hash"),
                ("block_number", "transaction_hash"),
            ),
            left_fields=[
                "type",
                "hash",
                "nonce",
                "transaction_index",
                "from_address",
                "to_address",
                "value",
                "gas",
                "gas_price",
                "input",
                "block_timestamp",
                "block_number",
                "block_hash",
                "max_fee_per_gas",
                "max_priority_fee_per_gas",
                "transaction_type",
            ],
            right_fields=[
                ("cumulative_gas_used", "receipt_cumulative_gas_used"),
                ("gas_used", "receipt_gas_used"),
                ("contract_address", "receipt_contract_address"),
                ("root", "receipt_root"),
                ("status", "receipt_status"),
                ("effective_gas_price", "receipt_effective_gas_price"),
                ("log_count", "receipt_log_count"),
            ],
        )
    )

    actual, expected = len(result), len(transactions)
    if actual != expected:
        _handle_not_matched_error(
            ("tx", transactions),
            ("receipt", receipts),
            ("result", result),
            left_on=["block_number", "hash"],
            right_on=["block_number", "transaction_hash"],
        )

    return result


def enrich_logs(blocks, logs):
    result = list(
        join(
            logs,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "log_index",
                "transaction_hash",
                "transaction_index",
                "address",
                "data",
                "topics",
                "block_number",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(logs)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("log", logs),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_token_transfers(blocks, token_transfers):
    result = list(
        join(
            token_transfers,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "token_address",
                "from_address",
                "to_address",
                "value",
                "transaction_hash",
                "transaction_index",
                "log_index",
                "block_number",
                "name",
                "symbol",
                "decimals",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(token_transfers)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("token_xfer", token_transfers),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_erc721_transfers(blocks, erc721_transfers):
    result = list(
        join(
            erc721_transfers,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "token_address",
                "token_name",
                "from_address",
                "to_address",
                "id",
                "transaction_hash",
                "transaction_index",
                "log_index",
                "block_number",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(erc721_transfers)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("erc721_xfer", erc721_transfers),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_erc1155_transfers(blocks, erc1155_transfers):
    result = list(
        join(
            erc1155_transfers,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "token_address",
                "token_name",
                "operator",
                "from_address",
                "to_address",
                "id",
                "value",
                "id_pos",
                "id_cnt",
                "xfer_type",
                "transaction_hash",
                "transaction_index",
                "log_index",
                "block_number",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(erc1155_transfers)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("erc1155_xfer", erc1155_transfers),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_traces(blocks, traces):
    result = list(
        join(
            traces,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "transaction_index",
                "from_address",
                "to_address",
                "value",
                "input",
                "output",
                "trace_type",
                "call_type",
                "reward_type",
                "gas",
                "gas_used",
                "subtraces",
                "trace_address",
                "error",
                "status",
                "transaction_hash",
                "block_number",
                "trace_id",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(traces)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("trace", traces),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_geth_traces(transactions, traces):
    result = list(
        join(
            traces,
            transactions,
            join_fields=(
                ("block_number", "transaction_index"),
                ("block_number", "transaction_index"),
            ),
            left_fields=[
                "type",
                "transaction_index",
                "from_address",
                "to_address",
                "value",
                "input",
                "output",
                "trace_type",
                "call_type",
                "reward_type",
                "gas",
                "gas_used",
                "subtraces",
                "trace_address",
                "error",
                "status",
                "transaction_hash",
                "block_number",
                "trace_id",
            ],
            right_fields=[
                "block_hash",
                "block_number",
                "block_timestamp",
                ("hash", "transaction_hash"),
            ],
        )
    )

    actual, expected = len(result), len(traces)
    if actual != expected:
        _handle_not_matched_error(
            ("txs", transactions),
            ("geth_trace", traces),
            ("result", result),
            left_on=["block_number", "transaction_index"],
            right_on=["block_number", "transaction_index"],
        )

    return result


def enrich_contracts(blocks, contracts):
    result = list(
        join(
            contracts,
            blocks,
            ("block_number", "number"),
            [
                "type",
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
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(contracts)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("contract", contracts),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result


def enrich_tokens(blocks, tokens):
    result = list(
        join(
            tokens,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "address",
                "symbol",
                "name",
                "decimals",
                "total_supply",
                "is_erc20",
                "is_erc721",
                "block_number",
                "transaction_hash",
                "transaction_index",
                "trace_address",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    actual, expected = len(result), len(tokens)
    if actual != expected:
        _handle_not_matched_error(
            ("block", blocks),
            ("token", tokens),
            ("result", result),
            left_on=["block_number"],
            right_on=["number"],
        )

    return result
