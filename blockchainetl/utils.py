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


import time
import logging
import pandas as pd
from datetime import datetime
from typing import (
    Optional,
    Dict,
    List,
    Generator,
    Union,
    Iterable,
    TypeVar,
    Tuple,
    Callable,
)

from blockchainetl.misc.retriable_value_error import RetriableValueError

logger = logging.getLogger(__name__)


def hex_to_dec(
    hex_string: Optional[str], ignore_error=True
) -> Optional[Union[str, int]]:
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        msg = f"Not a hex string '{hex_string}'"
        if ignore_error is False:
            raise ValueError(msg)
        else:
            logging.warning(msg)
        return hex_string


def to_int_or_none(val: Optional[Union[int, str]]) -> Optional[int]:
    if isinstance(val, int):
        return val
    if val is None or val == "":
        return None
    try:
        return int(val)
    except ValueError:
        return None


def chunk_string(string, length):
    return (string[0 + i : length + i] for i in range(0, len(string), length))


def validate_range(range_start_incl, range_end_incl):
    if range_start_incl < 0 or range_end_incl < 0:
        raise ValueError("range_start and range_end must be greater or equal to 0")

    if range_end_incl < range_start_incl:
        raise ValueError("range_end must be greater or equal to range_start")


def rpc_response_batch_to_results(
    response: Union[Dict, List[Dict]], jsonrpc=2, ignore_error=False, with_id=False, requests: Optional[Union[List[Dict], Dict]]=None
) -> Generator[Union[Dict, Tuple[Dict, str]], None, None]:
    if isinstance(response, dict):
        response = [response]
    if isinstance(requests, dict):
        requests = [requests]


    for response_item in response:
        yield rpc_response_to_result(response_item, jsonrpc, ignore_error, with_id, requests)


def rpc_response_to_result(
    response: Dict,
    jsonrpc: int = 2,
    ignore_error: bool = False,
    with_id: bool = False,
    requests: Optional[List[Dict]]=None,
) -> Union[Dict, Tuple[Dict, str]]:
    # bitcoin's jsonrpc currently is 1.0, don't have the `result` field
    if jsonrpc == 1:
        result = response
    else:
        result = response.get("result")

    id = response.get('id')

    if result is None:
        error_message = "result is None in response {}".format(response)

        if id is not None and requests is not None:
            error_message += " for request {}".format([r for r in requests if r.get('id') == id])

        error = response.get("error")
        is_retriable = False
        if error is None:
            error_message = error_message + " Make sure the full node is synchronized."
            # When nodes are behind a load balancer,
            # it makes sense to retry the request in hopes it will go to other, synced node
            is_retriable = True
        elif error is not None and is_retriable_error(error.get("code")):
            is_retriable = True

        if ignore_error is True:
            logger.error(error_message)
        elif is_retriable:
            raise RetriableValueError(error_message)
        else:
            raise ValueError(error_message)

    if with_id is True:
        return result, response.get("id")

    return result


def is_retriable_error(error_code):
    if error_code is None:
        return False

    if not isinstance(error_code, int):
        return False

    # https://www.jsonrpc.org/specification#error_object
    if error_code == -32603 or (-32000 >= error_code >= -32099):
        return True

    return False


def split_to_batches(start_incl, end_incl, batch_size):
    """start_incl and end_incl are inclusive, the returned batch ranges are also inclusive"""
    for batch_start in range(start_incl, end_incl + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_incl)
        yield batch_start, batch_end


T = TypeVar("T")


def dynamic_batch_iterator(
    iterable: Iterable[T], batch_size_getter: Callable[[], int]
) -> Generator[List[T], None, None]:
    batch = []
    batch_size = batch_size_getter()
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
            batch_size = batch_size_getter()
    if len(batch) > 0:
        yield batch


def time_elapsed(start: float, now: Optional[float] = None, decimals: int = 2) -> float:
    if now is None:
        now = time.time()
    return round(now - start, decimals)


def as_st_day(st: int) -> str:
    return datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")


def chunkify(lst: List[int], n: int) -> List[List[int]]:
    if len(lst) == 0:
        return [[]]

    lst = sorted(lst)
    chunks = []
    chunk = [lst[0]]
    for idx in range(1, len(lst)):
        if lst[idx - 1] + 1 == lst[idx]:
            chunk.append(lst[idx])
        else:
            if len(chunk) > 0:
                chunks.append(chunk)
            chunk = [lst[idx]]

        if len(chunk) >= n:
            chunks.append(chunk)
            chunk = []
    if len(chunk) > 0:
        chunks.append(chunk)
    return chunks


def dates_of_timestamps(min_st, max_st) -> List[str]:
    def _to_date(st: int):
        return datetime.utcfromtimestamp(st).date()

    return list(
        e.strftime("%Y-%m-%d")
        for e in pd.date_range(
            _to_date(min_st),
            _to_date(max_st),
            freq="1D",
            inclusive="both",
        )
    )
