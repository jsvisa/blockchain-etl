# The MIT License (MIT)
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
import logging
import decimal
import json
import hashlib
from copy import copy
from typing import Optional, Dict, Any
import diskcache as dc

from bitcoinetl.rpc.request import make_jsonrpc_request


logger = logging.getLogger("bitcoin_rpc")


class BitcoinRpc:
    def __init__(self, provider_uri, timeout=60, cache_path=None):
        self.provider_uri = provider_uri
        self.timeout = timeout
        self.cache = None
        if cache_path is not None:
            os.makedirs(cache_path, exist_ok=True)
            self.cache = dc.Cache(cache_path)

    def batch(self, commands, skip_cache=False):
        rpc_calls = []
        cache_keys = []
        result = []
        for id, command in enumerate(commands):
            m = command.pop(0)
            call = {"jsonrpc": "2.0", "method": m, "params": command, "id": id}
            cache_or_none = None
            if skip_cache is True or self.cache is None:
                rpc_calls.append(call)
            else:
                cache_key = self._cache_key(call)
                cached_val = self._get_cache(cache_key)
                if cached_val is None:
                    cache_keys.append(cache_key)
                    rpc_calls.append(call)
                else:
                    cache_or_none = cached_val

            result.append(cache_or_none)

        raw_response = make_jsonrpc_request(
            self.provider_uri,
            rpc_calls,
            timeout=self.timeout,
        )

        response = self._decode_rpc_response(raw_response)

        for id, r in enumerate(result):
            if r is not None:
                continue
            resp_item = response.pop(0)
            resp_result = resp_item.get("result")
            if resp_result is None:
                raise ValueError(
                    '"result" is None in the JSON RPC response {}. Request: {}',
                    resp_item.get("error"),
                    rpc_calls,
                )
            result[id] = resp_result
            if skip_cache is False and self.cache is not None:
                cache_key = cache_keys.pop(0)
                self._set_cache(cache_key, resp_result)
        return result

    def getblockhash(self, param) -> Optional[Dict]:
        response = self.batch([["getblockhash", param]])
        return response[0] if len(response) > 0 else None

    def getblock(self, param) -> Optional[Dict]:
        response = self.batch([["getblock", param]])
        return response[0] if len(response) > 0 else None

    def getblockcount(self) -> Optional[int]:
        response = self.batch([["getblockcount"]], skip_cache=True)
        return response[0] if len(response) > 0 else None

    def _decode_rpc_response(self, response):
        response_text = response.decode("utf-8")
        return json.loads(response_text, parse_float=decimal.Decimal)

    def _cache_key(self, req: Dict):
        req1 = copy(req)
        # don't cache with id
        del req1["id"]
        data = json.dumps(req1, skipkeys=True)
        return hashlib.md5(data.encode()).hexdigest()

    def _get_cache(self, key: str):
        if self.cache is None:
            return None
        return self.cache.get(key)

    def _set_cache(self, key: str, val: Any):
        if self.cache is None:
            return
        logger.debug(f"cache {key}")
        return self.cache.set(key, val)

    def _print_cache_stat(self):
        assert self.cache is not None
        hits, miss = self.cache.hits, self.cache.misses
        if hits + miss > 0:
            hit_rate = hits * 100 / (hits + miss)
        else:
            hit_rate = 0
        logger.info(f"cache stat: #H={hits} #M={miss} %hit={round(hit_rate, 2)}%")
