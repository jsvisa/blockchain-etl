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


from datetime import datetime, timezone, timedelta

from web3 import Web3

from ethereumetl.providers.auto import get_provider_from_uri
from blockchainetl.service.graph_operations import (
    GraphOperations,
    OutOfBoundsError,
    Point,
)
from blockchainetl.misc_utils import is_block_range, is_date_range, is_unix_time_range


class EthService(object):
    def __init__(self, web3):
        graph = BlockTimestampGraph(web3)
        self._graph_operations = GraphOperations(graph)

    def get_block_range_for_date(self, date):
        start_datetime = datetime.combine(
            date, datetime.min.time().replace(tzinfo=timezone.utc)
        )
        end_datetime = datetime.combine(
            date, datetime.max.time().replace(tzinfo=timezone.utc)
        )
        return self.get_block_range_for_timestamps(
            start_datetime.timestamp(), end_datetime.timestamp()
        )

    def get_block_range_for_timestamps(self, start_timestamp, end_timestamp):
        start_timestamp = int(start_timestamp)
        end_timestamp = int(end_timestamp)
        if start_timestamp > end_timestamp:
            raise ValueError(
                "start_timestamp must be greater or equal to end_timestamp"
            )

        try:
            start_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(
                start_timestamp
            )
        except OutOfBoundsError:
            start_block_bounds = (0, 0)

        try:
            end_block_bounds = self._graph_operations.get_bounds_for_y_coordinate(
                end_timestamp
            )
        except OutOfBoundsError as e:
            raise OutOfBoundsError(
                "The existing blocks do not completely cover the given time range"
            ) from e

        if (
            start_block_bounds == end_block_bounds
            and start_block_bounds[0] != start_block_bounds[1]
        ):
            raise ValueError("The given timestamp range does not cover any blocks")

        start_block = start_block_bounds[1]
        end_block = end_block_bounds[0]

        # The genesis block has timestamp 0 but we include it with the 1st block.
        if start_block == 1:
            start_block = 0

        return start_block, end_block


class BlockTimestampGraph(object):
    def __init__(self, web3: Web3):
        self._web3 = web3

    def get_first_point(self):
        # Ignore the genesis block as its timestamp is 0
        return block_to_point(self._web3.eth.get_block(1))

    def get_last_point(self):
        return block_to_point(self._web3.eth.get_block("latest"))

    def get_point(self, x):
        return block_to_point(self._web3.eth.get_block(x))

    def get_points(self, block_numbers):
        return [block_to_point(self._web3.eth.get_block(x)) for x in block_numbers]


def block_to_point(block):
    return Point(block.number, block.timestamp)


def get_partitions(start, end, partition_batch_size, provider_uri):
    """Yield partitions based on input data type."""
    if is_date_range(start, end) or is_unix_time_range(start, end):
        start_date, end_date = None, None
        if is_date_range(start, end):
            start_date = datetime.strptime(start, "%Y-%m-%d").date()
            end_date = datetime.strptime(end, "%Y-%m-%d").date()

        elif is_unix_time_range(start, end):
            if len(start) == 10 and len(end) == 10:
                start_date = datetime.utcfromtimestamp(int(start)).date()
                end_date = datetime.utcfromtimestamp(int(end)).date()

            elif len(start) == 13 and len(end) == 13:
                start_date = datetime.utcfromtimestamp(int(start) / 1e3).date()
                end_date = datetime.utcfromtimestamp(int(end) / 1e3).date()

        if start_date is None or end_date is None:
            raise ValueError("invalid start-end")

        day = timedelta(days=1)

        provider = get_provider_from_uri(provider_uri)
        web3 = Web3(provider)
        eth_service = EthService(web3)

        while start_date <= end_date:
            batch_start_block, batch_end_block = eth_service.get_block_range_for_date(
                start_date
            )
            partition_dir = "/date={start_date!s}/".format(start_date=start_date)
            yield batch_start_block, batch_end_block, partition_dir
            start_date += day

    elif is_block_range(start, end):
        start_block = int(start)
        end_block = int(end)

        for batch_start_block in range(
            start_block, end_block + 1, partition_batch_size
        ):
            batch_end_block = batch_start_block + partition_batch_size - 1
            if batch_end_block > end_block:
                batch_end_block = end_block

            padded_batch_start_block = str(batch_start_block).zfill(8)
            padded_batch_end_block = str(batch_end_block).zfill(8)
            partition_dir = "/start_block={padded_batch_start_block}/end_block={padded_batch_end_block}".format(
                padded_batch_start_block=padded_batch_start_block,
                padded_batch_end_block=padded_batch_end_block,
            )
            yield batch_start_block, batch_end_block, partition_dir

    else:
        raise ValueError(
            "start and end must be either block numbers or ISO dates or Unix times"
        )
