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
from web3 import HTTPProvider
from web3._utils.request import make_post_request

# This Polygon block's trace raised exception:
#   RecursionError: blockmaximum recursion depth exceeded while decoding a JSON array from a unicode string
# {
#     "jsonrpc": "2.0",
#     "method": "debug_traceBlockByNumber",
#     "params": ["0x1e3a99a", {"tracer": "callTracer", "timeout": "60s"}],
#     "id": 31697306,
# }
sys.setrecursionlimit(3000)


# Mostly copied from web3.py/providers/rpc.py. Supports batch requests.
# Will be removed once batch feature is added to web3.py
# https://github.com/ethereum/web3.py/issues/832
class BatchHTTPProvider(HTTPProvider):
    def make_batch_request(self, text):
        self.logger.debug(
            "Making request HTTP. URI: %s, Request: %s", self.endpoint_uri, text
        )
        request_data = text.encode("utf-8")
        raw_response = make_post_request(
            self.endpoint_uri, request_data, **self.get_request_kwargs()
        )
        response = self.decode_rpc_response(raw_response)
        self.logger.debug(
            "Getting response HTTP. URI: %s, " "Request: %s, Response: %s",
            self.endpoint_uri,
            text,
            response,
        )
        return response
