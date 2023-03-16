import logging
from typing import Union

import redis

from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType


RED_NOTIFY_BTC_NEWLY_BLOCK_SCRIPT = r"""
local sync_queue    = KEYS[1]
local bala_zset     = KEYS[2]

local blknum        = tonumber(ARGV[1])
local blk_file      = tostring(ARGV[2])
local into_gp       = tonumber(ARGV[3])
local into_ba       = tonumber(ARGV[4])

if into_gp == 1 then
    redis.pcall('LPUSH', sync_queue, blk_file)
end

-- sorted by blknum
if into_ba == 1 then
    redis.pcall('ZADD', bala_zset, blknum, blk_file)
end
"""

RED_UNIQUE_STREAM_SCRIPT = r"""
local stream    = KEYS[1]
local chekey    = KEYS[2]

local key       = tostring(ARGV[1])
local val       = tostring(ARGV[2])

local exists = redis.pcall('GET', chekey)
if tonumber(exists) ~= 1 then
    redis.pcall('XADD', stream, '*', key, val)
    redis.pcall('SETEX', chekey, 600, 1)
    return 1
end
return 0
"""


def fmt_redis_key_name(
    namespace: Union[str, Chain], prefix: str, entity_type: Union[str, EntityType]
) -> str:
    return f"{namespace}:{prefix}{entity_type}"


class RedisStreamService:
    def __init__(self, redis_url, entity_types):
        self._red = redis.from_url(redis_url)
        self._sha = self._red.script_load(RED_UNIQUE_STREAM_SCRIPT)
        self._entity_types = entity_types

    def create_notify(
        self,
        namespace: Union[str, Chain],
        stream_prefix: str,
        result_prefix: str,
        rewrite_entity_type=None,
    ):
        def notify(key: int, entity_type: EntityType, entity_file: str) -> bool:
            if entity_type not in self._entity_types:
                raise ValueError(
                    f"entity_type({entity_type}) not in supported types:({self._entity_types})"
                )

            if rewrite_entity_type is not None:
                entity_type = rewrite_entity_type(entity_type)

            stream = fmt_redis_key_name(namespace, stream_prefix, entity_type)
            result = fmt_redis_key_name(namespace, result_prefix, entity_type)
            added = self._red.evalsha(
                self._sha,
                2,
                stream,
                f"{result}:{key}",
                key,
                entity_file,
            )
            logging.info(
                f"redis notify {key, entity_file} to {stream}/{result} -> {added}"
            )
            return bool(added)

        return notify
