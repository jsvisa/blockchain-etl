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


import logging
import os
import time
from datetime import datetime

from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from blockchainetl.file_utils import smart_open
from blockchainetl import env
from blockchainetl.streaming.streamer_jsonl_skiper import StreamerJsonlSkiper


class Streamer:
    def __init__(
        self,
        blockchain_streamer_adapter=StreamerAdapterStub(),
        last_synced_block_file="last_synced_block.txt",
        lag=0,
        start_block=None,
        end_block=None,
        period_seconds=10,
        block_batch_size=10,
        retry_errors=True,
        pid_file=None,
    ):
        last_synced_block_dir = os.path.dirname(last_synced_block_file)
        if last_synced_block_dir != "":
            os.makedirs(last_synced_block_dir, exist_ok=True)

        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file

        # write the start_block-1 into syncfile if not exists.
        if not os.path.isfile(self.last_synced_block_file):
            # -1 stands for the last block
            if start_block == -1:
                current = blockchain_streamer_adapter.get_current_block_number()
                if isinstance(current, tuple):
                    start_block = current[0] - lag
                elif isinstance(current, int):
                    start_block = current - lag
                if start_block < 0:
                    start_block = 0
            write_last_synced_block(self.last_synced_block_file, (start_block or 0) - 1)

        self.last_synced_block = read_last_synced_block(self.last_synced_block_file)

        self.skiper = lambda _, __: None
        if env.SKIP_STREAM_IF_FAILED is True:
            self.skiper = StreamerJsonlSkiper(env.SKIP_STREAM_SAVE_PATH)
        self.block_range = (None, None)

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info("Creating pid file {}".format(self.pid_file))
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            self._do_stream()
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info("Deleting pid file {}".format(self.pid_file))
                delete_file(self.pid_file)

    def _do_stream(self):
        while self.end_block is None or self.last_synced_block < self.end_block:
            synced_blocks = 0

            try:
                synced_blocks = self._sync_cycle()
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception("An exception occurred while syncing block data.")
                if env.SKIP_STREAM_IF_FAILED is True:
                    logging.info(f"Skip and save {self.block_range}")
                    self.skiper(*self.block_range)
                    self._write_last_synced_block(self.block_range[1])
                    continue

                if not self.retry_errors:
                    # write a log, then exit
                    logging.fatal(e)

            if synced_blocks <= 0:
                logging.info(
                    "Nothing to sync. Sleeping for {} seconds...".format(
                        self.period_seconds
                    )
                )
                time.sleep(self.period_seconds)

    def _sync_cycle(self):
        current = self.blockchain_streamer_adapter.get_current_block_number()
        if isinstance(current, tuple):
            current_block = current[0]
            current_timestamp = current[1]
        else:
            current_block = current
            current_timestamp = None
        last_synced = self.last_synced_block
        target_block = self._calculate_target_block(current_block, last_synced)
        blocks_to_sync = max(target_block - last_synced, 0)

        if current_timestamp is not None:
            current_time = datetime.utcfromtimestamp(current_timestamp)
        else:
            current_time = None

        logging.info(
            f"Current block {current_block}({current_time}), target block {target_block}, "
            f"last synced block {last_synced}, blocks to sync #{blocks_to_sync}, "
            f"lag #{current_block-last_synced}"
        )

        self.block_range = (last_synced + 1, target_block)
        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(*self.block_range)
            self._write_last_synced_block(target_block)

        return blocks_to_sync

    def _calculate_target_block(self, current_block: int, last_synced: int) -> int:
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced + self.block_batch_size)
        if self.end_block is not None:
            target_block = min(target_block, self.end_block)
        return target_block

    def _write_last_synced_block(self, target_block):
        if target_block is None:
            return
        logging.debug("Writing last synced block {}".format(target_block))
        write_last_synced_block(self.last_synced_block_file, target_block)
        self.last_synced_block = target_block


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + "\n")


def init_last_synced_block_file(start_block, last_synced_block_file):
    # if the last synced file is exists, check the previous block number
    if os.path.isfile(last_synced_block_file):
        last_synced_block = read_last_synced_block(last_synced_block_file)
        if last_synced_block < start_block:
            raise ValueError(
                "last synced block in file {} is {}, which is smaller than start block {}. "
                "Either remove the {} file or change --start-block option.".format(
                    last_synced_block_file,
                    last_synced_block,
                    start_block,
                    last_synced_block_file,
                )
            )
    write_last_synced_block(last_synced_block_file, start_block)


def read_last_synced_block(file: str) -> int:
    with smart_open(file, "r") as fr:
        return int(str(fr.read()))


def write_to_file(file: str, content: str) -> None:
    with smart_open(file, "w") as fw:
        fw.write(content)
