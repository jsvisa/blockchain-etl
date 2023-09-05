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

from typing import List, Dict, Union, Generator, Any, Optional, Tuple
from eth_utils.address import to_checksum_address

from blockchainetl import env


def generate_get_txpool_content_json_rpc() -> Dict[str, str]:
    return generate_json_rpc(
        method="txpool_content",
        params=[],
    )


def generate_get_block_by_number_json_rpc(
    block_numbers: List[int],
    include_transactions: bool,
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for idx, block_number in enumerate(block_numbers):
        yield generate_json_rpc(
            method="eth_getBlockByNumber",
            params=[hex(block_number), include_transactions],
            request_id=idx,
        )


def generate_get_uncle_by_block_hash_and_index_json_rpc(
    block_uncles: List[Tuple[int, str, int]]
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for hash_uncle in block_uncles:
        blknum, blk_hash, uncle_pos = hash_uncle
        yield generate_json_rpc(
            method="eth_getUncleByBlockHashAndIndex",
            params=[blk_hash, hex(uncle_pos)],
            request_id=f"{blknum}-{uncle_pos}",
        )


def generate_get_log_by_number_json_rpc(
    from_block: int,
    to_block: int,
    topics: Optional[List[str]] = None,
    address: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Union[str, int]]:
    # Ref: https://github.com/ethereum/go-ethereum/blob/v1.10.16/interfaces.go#L167-L174
    params = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
    if topics is not None and isinstance(topics, list) and len(topics) > 0:
        assert isinstance(
            topics[0], str
        ), f"topics must be a list of Event signature, not: {topics}"
        params["topics"] = [topics]
    if address is not None:
        if isinstance(address, list):
            address = [to_checksum_address(a) for a in address]
        elif isinstance(address, str):
            address = to_checksum_address(address)
        params["address"] = address
    return generate_json_rpc(
        method="eth_getLogs",
        params=[params],
        request_id=from_block,
    )


def generate_parity_trace_block_by_number_json_rpc(
    block_numbers: List[int],
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for block_number in block_numbers:
        yield generate_json_rpc(
            method="trace_block",
            params=[hex(block_number)],
            request_id=block_number,
        )


def generate_trace_block_by_number_json_rpc(
    block_numbers: List[int],
    tracer: str = env.GETH_TRACE_MODULE,
    tracer_config: Dict[str, bool] = None,
) -> Generator[Dict[str, Union[str, int]], None, None]:
    config = {"tracer": tracer, "timeout": env.GETH_DEBUG_API_TIMEOUT}
    if tracer_config is not None:
        config["tracerConfig"] = tracer_config

    for block_number in block_numbers:
        yield generate_json_rpc(
            method="debug_traceBlockByNumber",
            params=[
                hex(block_number),
                config,
            ],
            # save block_number in request ID, so later we can identify block number in response
            request_id=block_number,
        )


def generate_trace_transaction_json_rpc(
    txhashes: List[str],
    tracer: str = env.GETH_TRACE_MODULE,
    tracer_config: Dict[str, bool] = None,
) -> Generator[Dict[str, Union[str, int]], None, None]:
    config = {"tracer": tracer, "timeout": env.GETH_DEBUG_API_TIMEOUT}
    if tracer_config is not None:
        config["tracerConfig"] = tracer_config

    for txhash in txhashes:
        yield generate_json_rpc(
            method="debug_traceTransaction",
            params=[
                txhash,
                config,
            ],
            # save txhash in request ID, so later we can identify txhash in response
            request_id=txhash,
        )


def generate_get_receipt_json_rpc(
    transaction_hashes: List[str],
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for idx, transaction_hash in enumerate(transaction_hashes):
        yield generate_json_rpc(
            method="eth_getTransactionReceipt",
            params=[transaction_hash],
            request_id=idx,
        )


def generate_get_block_receipts_json_rpc(
    block_numbers: List[int],
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for block_number in block_numbers:
        yield generate_json_rpc(
            method="eth_getBlockReceipts",
            params=[hex(block_number)],
            request_id=block_number,
        )


def generate_get_code_json_rpc(
    contract_addresses: List[str],
    block: Union[int, str] = "latest",
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for idx, contract_address in enumerate(contract_addresses):
        yield generate_json_rpc(
            method="eth_getCode",
            params=[contract_address, hex(block) if isinstance(block, int) else block],
            request_id=idx,
        )


# ONLY used for Arbitrum
# Released in https://github.com/OffchainLabs/arbitrum/releases/tag/v1.3.0
def generate_arbtrace_block_by_number_json_rpc(
    block_numbers: List[int],
) -> Generator[Dict[str, Union[str, int]], None, None]:
    for block_number in block_numbers:
        yield generate_json_rpc(
            method="arbtrace_block",
            params=[hex(block_number)],
            # save block_number in request ID, so later we can identify block number in response
            request_id=block_number,
        )


def generate_json_rpc(
    method: str,
    params: Any,
    request_id: Union[int, str] = 1,
) -> Dict[str, Union[str, int]]:
    return {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": request_id,
    }
