# The MIT License (MIT)
#
# Copyright (c) 2016 Piper Merriam
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

import sys
import requests
import logging
from typing import Any, Dict, List


# This Polygon block's trace raised exception:
#   RecursionError: blockmaximum recursion depth exceeded
#   while decoding a JSON array from a unicode string
# {
#     "jsonrpc": "2.0",
#     "method": "debug_traceBlockByNumber",
#     "params": ["0x1e3a99a", {"tracer": "callTracer", "timeout": "60s"}],
#     "id": 31697306,
# }
sys.setrecursionlimit(3000)


class BatchHTTPProvider:
    def __init__(self, endpoint_uri: str, request_kwargs: Dict[str, Any] = None):
        self.endpoint_uri = endpoint_uri
        self.session = requests.Session()
        self.request_kwargs = request_kwargs or {}
        self.logger = logging.getLogger(__name__)

    def make_batch_request(self, batch_requests: List[Dict]):
        self.logger.debug(
            "Making request HTTP. URI: %s, Request: %s",
            self.endpoint_uri,
            batch_requests,
        )
        raw_response = self.session.post(
            self.endpoint_uri, json=batch_requests, **self.request_kwargs
        )
        response = raw_response.json()
        self.logger.debug(
            "Getting response HTTP. URI: %s, " "Request: %s, Response: %s",
            self.endpoint_uri,
            batch_requests,
            response,
        )
        return response
