import redis
import logging
from redis import DataError
from time import time, sleep
from typing import Callable, Dict, Optional, Any
from enum import Enum
from threading import Thread
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


class RedisConsumerGroupWorkerMode(Enum):
    Thread = "thread"
    Process = "process"
    ThreadPool = "thread-pool"
    ProcessPool = "process-pool"

    @staticmethod
    def from_str(s: str = "thread"):
        s = s.lower()
        if s == "thread":
            return RedisConsumerGroupWorkerMode.Thread
        elif s == "process":
            return RedisConsumerGroupWorkerMode.Process
        if s == "thread-pool":
            return RedisConsumerGroupWorkerMode.ThreadPool
        elif s == "process":
            return RedisConsumerGroupWorkerMode.ProcessPool
        else:
            raise ValueError("Invalid mode")


class RedisConsumerGroup(object):
    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        consumer_group: str,
        consumer_prefix: Optional[str] = None,
        workers: int = 1,
        worker_mode: str = "thread",
        period_seconds: int = 5,
    ):
        self._red = redis.from_url(redis_url)
        self._stream_name = stream_name
        self._consumer_group = consumer_group
        self._consumer_prefix = consumer_prefix or ""
        self._workers = workers
        self._worker_mode = RedisConsumerGroupWorkerMode.from_str(worker_mode)
        self._period_seconds = period_seconds

    def _mp(self):
        if self._worker_mode == RedisConsumerGroupWorkerMode.Process:
            return Process
        elif self._worker_mode == RedisConsumerGroupWorkerMode.Thread:
            return Thread
        else:
            raise ValueError

    def _mp_pool(self):
        if self._worker_mode == RedisConsumerGroupWorkerMode.ThreadPool:
            return ThreadPoolExecutor
        elif self._worker_mode == RedisConsumerGroupWorkerMode.ProcessPool:
            return ProcessPoolExecutor
        else:
            raise ValueError

    def consume(
        self,
        handler: Callable[[float, Dict], None],
        initer: Optional[Callable[..., Any]] = None,
        deiniter: Optional[Callable[..., Any]] = None,
        count: int = 1,
        block: int = 10,
        is_leader: bool = False,
    ):
        stream = self._stream_name
        cgroup = self._consumer_group

        logging.info(f"Consume stream={stream} group={cgroup}")

        # if the stream is not created yet, XINFO will failed
        # catch this exception and auto create it.
        try:
            stats = self._red.xinfo_groups(stream)
            logging.info(f"Consumer group stats for stream={stream} are: {stats}")
            if self._consumer_group not in [e["name"].decode() for e in stats]:
                self._red.xgroup_create(stream, cgroup, id="0", mkstream=True)
        except redis.ResponseError:
            logging.info(
                f"Consumer group for stream={stream} is not ready, auto created"
            )

        if self._workers == 1:
            # running in current process/thread
            self.consumer_consume(
                self.consumer_name(0),
                handler,
                initer=initer,
                deiniter=deiniter,
                autoclaim=is_leader,
                leader=is_leader,
                count=count,
                block=block,
            )

        mp = self._mp()
        threads = [
            mp(
                target=self.consumer_consume,
                args=(self.consumer_name(idx), handler),
                kwargs=dict(
                    initer=initer,
                    deiniter=deiniter,
                    autoclaim=False,
                    leader=idx == 0,
                    count=count,
                    block=block,
                ),
            )
            for idx in range(0, self._workers)
        ]
        threads.append(
            mp(
                target=self.consumer_consume,
                args=(self.consumer_name(-1), handler),
                kwargs=dict(
                    initer=initer,
                    deiniter=deiniter,
                    autoclaim=True,
                    leader=False,
                    count=count,
                    block=block,
                ),
            )
        )

        for p in threads:
            p.start()

        for p in threads:
            if p.is_alive():
                p.join()

        logging.info("finish all tasks")

    def consumer_name(self, idx: int) -> str:
        return f"{self._consumer_group}:{self._consumer_prefix}-{idx}"

    def consumer_consume(
        self,
        consumer: str,
        handler: Callable[[Any, float, Dict], None],
        initer: Optional[Callable] = None,
        deiniter: Optional[Callable] = None,
        autoclaim: bool = False,
        leader: bool = False,
        count: int = 1,
        block: int = 10,
    ):
        stream = self._stream_name
        cgroup = self._consumer_group

        logging.info(
            f"start consume with stream={stream} group={cgroup} consumer={consumer} "
            f"autoclaim={autoclaim}"
        )

        inited = initer() if initer else None

        def handle(ackid, keyvals):
            logging.info(f"consumer:{consumer} => ({ackid}, {keyvals})")
            handler(inited, time(), keyvals)
            self._red.xack(stream, cgroup, ackid)

        try:
            self._loop_consume(consumer, handle, autoclaim, leader, count, block)
        finally:
            if deiniter is not None:
                deiniter(inited)  # type: ignore

    def _loop_consume(
        self,
        consumer: str,
        handle: Callable[[str, Dict], None],
        autoclaim: bool = False,
        leader: bool = False,
        count: int = 1,
        block: int = 10,
    ):
        if autoclaim is True:
            self._claim_consume(consumer, handle)
        else:
            self._normal_consume(consumer, handle, leader, count, block)

    def _claim_consume(self, consumer: str, handle: Callable[[str, Dict], None]):
        # handle the pending items first
        start_id = 0
        claim_count = 10
        while True:
            reply = self.xautoclaim(
                self._stream_name,
                self._consumer_group,
                consumer,
                min_idle_time=600_000,  # 10mins
                start_id=start_id,
                count=claim_count,
            )
            if len(reply) == 0:  # the empty messages
                sleep(10)
                continue

            if isinstance(reply[0], bytes):  # the standard way
                start_id = reply[0]
                messages = reply[1]
            else:
                last_id = reply[-1][0].decode().split("-")
                assert (
                    len(last_id) == 2
                ), f"last message's id is malformed: {reply[-1][0]}"
                start_id = last_id[0] + "-" + str(int(last_id[1]) + 1)
                messages = reply

            # The redis protocol reply as below:
            # [
            #     b"0-0", # next start id
            #     [
            #         (b'1633482125411-0', {b'10575710': b'/jfs/etl/2020-08-01/transaction/10575710.csv'}), # noqa
            #         (b'1633482125911-0', {b'10575711': b'/jfs/etl/2020-08-01/transaction/10575711.csv'}), # noqa
            #     ],
            # ]
            # but in our case, return as below:
            #  [
            #      (b'1633482125411-0', {b'10575710': b'/jfs/etl/2020-08-01/transaction/10575710.csv'}), # noqa
            #      (b'1633482125911-0', {b'10575711': b'/jfs/etl/2020-08-01/transaction/10575711.csv'}), # noqa
            #  ]

            for message in messages:
                ackid, kvs = message
                if isinstance(kvs, list):
                    kvs = {kvs[e]: kvs[e + 1] for e in range(0, len(kvs), 2)}
                handle(ackid, kvs)

            # no more messages
            if len(messages) < claim_count:
                logging.info("no more to claim, sleep 60s, reset start_id=0")
                sleep(60)
                start_id = 0

    def _normal_consume(
        self,
        consumer: str,
        handle: Callable[[str, Dict], None],
        leader: bool = False,
        count: int = 1,
        block: int = 10,
    ):
        stream = self._stream_name
        cgroup = self._consumer_group
        while True:
            reply = self._red.xreadgroup(
                cgroup,
                consumer,
                {stream: ">"},  # > stands for all new messages
                count=count,
                block=block,
            )
            if len(reply) == 0:
                if leader is True:
                    logging.info(
                        f"no more messages to read for stream {stream} and group {cgroup}"
                    )
                sleep(self._period_seconds)
                continue

            assert len(reply) == 1
            # record looks like:
            # [
            #     [
            #         b"stream",
            #         [
            #             (b"1631105362465-0", {b"a": b"2"}),
            #             (b"1631105358779-0", {b"a": b"1"}),
            #         ],
            #     ]
            # ]
            for message in reply[0][1]:
                handle(*message)

    # backport from https://github.com/andymccurdy/redis-py/blob/e9837c1d6360d27fac0d8fed6384fd9b2b568b5c/redis/commands.py#L1791-L1829 # noqa
    def xautoclaim(
        self,
        name,
        groupname,
        consumername,
        min_idle_time,
        start_id=0,
        count=None,
        justid=False,
    ):
        """
        Transfers ownership of pending stream entries that match the specified
        criteria. Conceptually, equivalent to calling XPENDING and then XCLAIM,
        but provides a more straightforward way to deal with message delivery
        failures via SCAN-like semantics.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds.
        start_id: filter messages with equal or greater ID.
        count: optional integer, upper limit of the number of entries that the
        command attempts to claim. Set to 100 by default.
        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message
        """
        try:
            if int(min_idle_time) < 0:
                raise DataError(
                    "XAUTOCLAIM min_idle_time must be a non" "negative integer"
                )
        except TypeError:
            pass

        kwargs = {}
        pieces = [name, groupname, consumername, min_idle_time, start_id]

        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([b"COUNT", count])
        except TypeError:
            pass
        if justid:
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True

        return self._red.execute_command("XAUTOCLAIM", *pieces, **kwargs)
